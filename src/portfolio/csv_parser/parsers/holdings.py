"""保有銘柄CSV解析モジュール"""

import csv
import io
from typing import Any

from src.utils.logging_config import get_logger

from ..base import BaseCSVParser
from ..utils import normalize_code, parse_number

logger = get_logger("portfolio.csv_parser.parsers.holdings")


class HoldingsParser(BaseCSVParser):
    """保有銘柄CSVパーサー"""

    @classmethod
    def _parse_content(cls, csv_content: str) -> list[dict[str, Any]]:
        """
        CSVコンテンツを解析

        複数のSBI証券CSVフォーマットに対応:
        1. 標準形式: 銘柄コード,銘柄名,市場,保有数量,取得単価,現在値,評価損益,評価損益率(%),...
        2. 保有証券_現物形式: 複雑なヘッダー構造、位置ベースの列
        3. SaveFile形式: セクション分割された形式
        """
        # フォーマットを判定
        lines = csv_content.strip().split("\n")
        if not lines:
            return []

        # 保有証券_現物形式の判定（「銘柄」が複数回出現、またはBOMがある場合）
        # またはカラム数が非常に多い場合（20以上）
        first_line_cols = len(lines[0].split(",")) if lines else 0
        if lines and (
            lines[0].count("銘柄") >= 2
            or lines[0].startswith("﻿銘柄")
            or first_line_cols > 20
        ):
            logger.debug(
                f"詳細形式と判定: 銘柄数={lines[0].count('銘柄')}, カラム数={first_line_cols}"
            )
            return cls._parse_holdings_detailed_format(csv_content)
        # SaveFile形式の判定（「保有証券一覧」などのヘッダーがある）
        elif "保有証券一覧" in csv_content or "評価額合計" in csv_content:
            return cls._parse_holdings_savefile_format(csv_content)
        else:
            # 標準形式として処理を試みる
            return cls._parse_holdings_standard_format(csv_content)

    @classmethod
    def _parse_holdings_standard_format(cls, csv_content: str) -> list[dict[str, Any]]:
        """標準形式の保有銘柄CSVを解析"""
        holdings: list[dict[str, Any]] = []

        try:
            # CSVを読み込み
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            rows = list(csv_reader)

            if not rows:
                return holdings

            for row in rows:
                # カラム名のバリエーションに対応
                code = row.get("銘柄コード") or row.get("コード") or ""
                code = normalize_code(code)

                if not code:
                    continue

                # 口座タイプの判定（NISA/特定/つみたてNISA等）
                account_type = "特定"  # デフォルト
                account_type_col = (
                    row.get("口座区分") or row.get("預り") or row.get("預り区分") or ""
                )
                if "NISA" in account_type_col:
                    if "つみたて" in account_type_col:
                        account_type = "つみたてNISA"
                    elif "旧NISA" in account_type_col:
                        account_type = "旧NISA"
                    else:
                        account_type = "NISA"
                elif "特定" in account_type_col:
                    account_type = "特定"
                elif "一般" in account_type_col:
                    account_type = "一般"

                holding = {
                    "code": code,
                    "name": row.get("銘柄名", "").strip(),
                    "account_type": account_type,
                    "quantity": parse_number(
                        row.get("保有数量") or row.get("保有株数") or row.get("数量")
                    ),
                    "average_price": parse_number(
                        row.get("取得単価") or row.get("平均取得単価")
                    ),
                    "current_price": parse_number(row.get("現在値") or row.get("株価")),
                    "market_value": parse_number(
                        row.get("評価額") or row.get("時価評価額")
                    ),
                    "profit_loss": parse_number(row.get("評価損益") or row.get("損益")),
                    "profit_loss_ratio": parse_number(
                        row.get("評価損益率(%)") or row.get("損益率(%)")
                    ),
                }

                # 必須フィールドのチェック
                if holding["code"] and holding["quantity"] is not None:
                    holdings.append(holding)
                    logger.debug(
                        f"保有銘柄解析: {holding['code']} - {holding['quantity']}株 ({account_type})"
                    )

            logger.info(f"保有銘柄CSV解析完了（標準形式）: {len(holdings)}銘柄")
            return holdings

        except Exception as e:
            logger.error(f"保有銘柄CSV解析エラー（標準形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

    @classmethod
    def _parse_holdings_savefile_format(cls, csv_content: str) -> list[dict[str, Any]]:
        """SaveFile形式の保有銘柄CSVを解析"""
        holdings: list[dict[str, Any]] = []

        try:
            lines = csv_content.strip().split("\n")

            # 現在のセクション（口座タイプ）を追跡
            current_account_type = "特定"  # デフォルト
            current_section_name = ""  # 現在のセクション名

            # セクション見出しと口座タイプのマッピング
            section_mapping = {
                "株式（特定預り）": "特定",
                "株式（NISA預り（成長投資枠））": "NISA",
                "株式（旧NISA預り）": "旧NISA",
                # 投資信託セクション
                "投資信託（金額/NISA預り（つみたて投資枠））": "つみたてNISA",
                "投資信託（金額/特定預り）": "特定",
                "投資信託（金額/NISA預り（成長投資枠））": "NISA",
                "投資信託（金額/旧NISA預り）": "旧NISA",
                "投資信託（口数/特定預り）": "特定",
                "投資信託（口数/NISA預り（成長投資枠））": "NISA",
                "投資信託（口数/NISA預り（つみたて投資枠））": "つみたてNISA",
                "投資信託（口数/旧NISA預り）": "旧NISA",
            }

            # 各行を処理
            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # セクション見出しをチェック
                if line in section_mapping:
                    current_account_type = section_mapping[line]
                    current_section_name = line  # セクション名を記憶
                    logger.debug(
                        f"セクション検出: {line} -> 口座タイプ: {current_account_type}"
                    )
                    i += 1
                    continue

                # データ行の判定（"で始まる行）
                if line.startswith('"'):
                    # CSVとして解析
                    reader = csv.reader(io.StringIO(line))
                    row = next(reader, None)
                    if row and len(row) >= 8:
                        # 投資信託の場合は9列必要
                        if "投資信託" in current_section_name and len(row) < 9:
                            logger.debug(f"投資信託データの列数が不足: {len(row)}列")
                            continue
                        # 投資信託セクションかどうか判定
                        is_fund = "投資信託" in current_section_name

                        if is_fund:
                            # 投資信託の場合
                            fund_name = row[0].strip() if row[0] else ""
                            if fund_name:
                                # デバッグ用にrow内容を出力
                                logger.debug(f"投資信託行データ: {row}")
                                logger.debug(f"行データ長: {len(row)}")

                                # 口数を取得（列1 - 「口」を除去）
                                quantity = None
                                if len(row) > 1:
                                    # 「口」を除去してから数値解析
                                    quantity_str = row[1].replace("口", "").strip()
                                    quantity = parse_number(quantity_str)
                                    logger.debug(
                                        f"口数解析: row[1]='{row[1]}' -> '{quantity_str}' -> {quantity}"
                                    )

                                # quantityがNoneまたは0の場合はスキップ
                                if quantity is None or quantity == 0:
                                    logger.warning(
                                        f"投資信託の口数が無効です: {fund_name} "
                                        f"(口数: {quantity}, 口座: {current_account_type})"
                                    )
                                    continue

                                # 投資信託データを保持（ファンド名で識別）
                                holding = {
                                    "fund_name": fund_name,
                                    "account_type": current_account_type,
                                    "quantity": quantity,  # 口数
                                    "average_price": (
                                        parse_number(row[3]) if len(row) > 3 else 0
                                    ),  # 取得単価
                                    "current_price": (
                                        parse_number(row[4]) if len(row) > 4 else None
                                    ),  # 基準価額
                                    "market_value": (
                                        parse_number(row[6]) if len(row) > 6 else None
                                    ),  # 評価額
                                    "profit_loss": (
                                        parse_number(row[7]) if len(row) > 7 else None
                                    ),  # 評価損益
                                    "profit_loss_ratio": None,
                                    "is_fund": True,  # 投資信託フラグ
                                    "code": None,  # 投資信託にはコードがない
                                }
                                holdings.append(holding)
                                logger.info(
                                    f"投資信託データを取得: {fund_name} "
                                    f"口数: {quantity}, 平均取得価額: {holding['average_price']} "
                                    f"(口座: {current_account_type})"
                                )
                        else:
                            # 株式の場合（既存の処理）
                            code = normalize_code(row[0])
                            if code:
                                holding = {
                                    "code": code,
                                    "name": row[1].strip() if len(row) > 1 else "",
                                    "account_type": current_account_type,  # 現在のセクションの口座タイプを設定
                                    "quantity": (
                                        parse_number(row[2]) if len(row) > 2 else None
                                    ),
                                    "average_price": (
                                        parse_number(row[4]) if len(row) > 4 else None
                                    ),
                                    "current_price": (
                                        parse_number(row[5]) if len(row) > 5 else None
                                    ),
                                    "market_value": (
                                        parse_number(row[7]) if len(row) > 7 else None
                                    ),
                                    "profit_loss": (
                                        parse_number(row[8]) if len(row) > 8 else None
                                    ),
                                    "profit_loss_ratio": None,  # SaveFile形式には評価損益率がない
                                    "is_fund": False,  # 株式フラグ
                                }

                                # 必須フィールドのチェック
                                if holding["code"] and holding["quantity"] is not None:
                                    holdings.append(holding)
                                    logger.debug(
                                        f"保有銘柄解析（SaveFile形式）: {holding['code']} - "
                                        f"{holding['quantity']}株 ({current_account_type})"
                                    )

                i += 1

            logger.info(f"保有銘柄CSV解析完了（SaveFile形式）: {len(holdings)}銘柄")
            return holdings

        except Exception as e:
            logger.error(f"保有銘柄CSV解析エラー（SaveFile形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

    @classmethod
    def _parse_holdings_detailed_format(cls, csv_content: str) -> list[dict[str, Any]]:
        """保有証券_現物形式のCSVを解析"""
        holdings: list[dict[str, Any]] = []

        try:
            lines = csv_content.strip().split("\n")
            if len(lines) < 2:
                return holdings

            # ヘッダー行を解析してカラム位置を特定
            header_line = lines[0]
            reader = csv.reader(io.StringIO(header_line))
            headers = next(reader, [])

            # カラムインデックスのマッピングを作成
            col_map = {}

            # 特殊なパターン: 銘柄が連続して出現する場合
            # 例: 銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数...
            if len(headers) > 10 and headers[0] == "銘柄" and headers[1] == "銘柄":
                # 固定位置でマッピング
                # 5列目（インデックス4）が銘柄コード
                col_map["code"] = 4
                # 6列目（インデックス5）が銘柄名
                col_map["name"] = 5
                # 残りのカラムを通常通り検索
                for i in range(7, len(headers)):
                    header = headers[i]
                    if "保有株数" in header:
                        col_map["quantity"] = i
                    elif "取得単価" in header:
                        col_map["average_price"] = i
                    elif "現在値" in header and "current_price" not in col_map:
                        col_map["current_price"] = i
                    elif (
                        "評価損益" in header
                        and "率" not in header
                        and "profit_loss" not in col_map
                    ):
                        col_map["profit_loss"] = i
                    elif "評価損益(%)" in header:
                        col_map["profit_loss_ratio"] = i
                    elif "評価額" in header and "market_value" not in col_map:
                        col_map["market_value"] = i
                    elif "預り区分" in header or "預り" in header:
                        col_map["account_type"] = i
                    elif "予想PER" in header:
                        col_map["expected_per"] = i
                    elif "実績PBR" in header:
                        col_map["actual_pbr"] = i
                    elif "配当利回り" in header:
                        col_map["dividend_yield"] = i
                    elif "予想1株配当" in header:
                        col_map["expected_dividend"] = i
                    elif "予想EPS" in header:
                        col_map["expected_eps"] = i
                    elif "実績BPS" in header:
                        col_map["actual_bps"] = i
                    elif "貸借区分" in header:
                        col_map["lending_type"] = i
            else:
                # 通常のパターン
                for i, header in enumerate(headers):
                    # 銘柄コードを探す（「銘柄(コード)」「銘柄コード」など）
                    if ("銘柄" in header and "コード" in header) or header == "コード":
                        col_map["code"] = i
                    # 銘柄名を探す（「銘柄(名称)」「銘柄名」など）
                    elif ("銘柄" in header and ("名" in header or "称" in header)) or (
                        header == "銘柄" and "name" not in col_map
                    ):
                        col_map["name"] = i
                    elif "株数" in header or "保有数量" in header or "数量" in header:
                        col_map["quantity"] = i
                    elif "取得" in header and "単価" in header:
                        col_map["average_price"] = i
                    elif "現在値" in header:
                        col_map["current_price"] = i
                    elif "評価額" in header:
                        col_map["market_value"] = i
                    elif "評価損益" in header and "率" not in header:
                        col_map["profit_loss"] = i
                    elif "評価損益率" in header:
                        col_map["profit_loss_ratio"] = i
                    elif "預り" in header or "口座" in header:
                        col_map["account_type"] = i
                    elif "予想PER" in header:
                        col_map["expected_per"] = i
                    elif "実績PBR" in header:
                        col_map["actual_pbr"] = i
                    elif "配当利回り" in header:
                        col_map["dividend_yield"] = i
                    elif "予想1株配当" in header:
                        col_map["expected_dividend"] = i
                    elif "予想EPS" in header:
                        col_map["expected_eps"] = i
                    elif "実績BPS" in header:
                        col_map["actual_bps"] = i
                    elif "貸借区分" in header:
                        col_map["lending_type"] = i

            logger.debug(f"カラムマッピング: {col_map}")
            logger.debug(f"ヘッダー数: {len(headers)}")
            if len(headers) > 0:
                logger.debug(f"最初の10ヘッダー: {headers[:10]}")

            # データ行をパース（ヘッダー行をスキップ）
            for line in lines[1:]:
                if not line.strip():
                    continue

                # CSVとして解析
                reader = csv.reader(io.StringIO(line))
                row = next(reader, None)
                if not row:
                    continue

                # カラムマッピングに基づいてデータを取得
                code = ""
                if "code" in col_map and col_map["code"] < len(row):
                    code = normalize_code(row[col_map["code"]])
                elif len(row) > 4:  # フォールバック
                    code = normalize_code(row[4])

                if not code:
                    continue

                # 口座タイプの判定
                account_type = "特定"  # デフォルト
                if "account_type" in col_map and col_map["account_type"] < len(row):
                    account_type_val = row[col_map["account_type"]].strip()
                    if "NISA" in account_type_val:
                        if "つみたて" in account_type_val:
                            account_type = "つみたてNISA"
                        elif "旧NISA" in account_type_val:
                            account_type = "旧NISA"
                        else:
                            account_type = "NISA"
                    elif "特定" in account_type_val:
                        account_type = "特定"
                    elif "一般" in account_type_val:
                        account_type = "一般"

                holding = {
                    "code": code,
                    "name": (
                        row[col_map["name"]].strip()
                        if "name" in col_map and col_map["name"] < len(row)
                        else ""
                    ),
                    "account_type": account_type,
                    "quantity": (
                        parse_number(row[col_map["quantity"]])
                        if "quantity" in col_map and col_map["quantity"] < len(row)
                        else None
                    ),
                    "average_price": (
                        parse_number(row[col_map["average_price"]])
                        if "average_price" in col_map
                        and col_map["average_price"] < len(row)
                        else None
                    ),
                    "current_price": (
                        parse_number(row[col_map["current_price"]])
                        if "current_price" in col_map
                        and col_map["current_price"] < len(row)
                        else None
                    ),
                    "market_value": (
                        parse_number(row[col_map["market_value"]])
                        if "market_value" in col_map
                        and col_map["market_value"] < len(row)
                        else None
                    ),
                    "profit_loss": (
                        parse_number(row[col_map["profit_loss"]])
                        if "profit_loss" in col_map
                        and col_map["profit_loss"] < len(row)
                        else None
                    ),
                    "profit_loss_ratio": (
                        parse_number(row[col_map["profit_loss_ratio"]])
                        if "profit_loss_ratio" in col_map
                        and col_map["profit_loss_ratio"] < len(row)
                        else None
                    ),
                }

                # 株価指標データ（オプション）
                if "expected_per" in col_map and col_map["expected_per"] < len(row):
                    holding["expected_per"] = parse_number(row[col_map["expected_per"]])
                if "actual_pbr" in col_map and col_map["actual_pbr"] < len(row):
                    holding["actual_pbr"] = parse_number(row[col_map["actual_pbr"]])
                if "dividend_yield" in col_map and col_map["dividend_yield"] < len(row):
                    holding["dividend_yield"] = parse_number(
                        row[col_map["dividend_yield"]]
                    )
                if "expected_dividend" in col_map and col_map[
                    "expected_dividend"
                ] < len(row):
                    holding["expected_dividend"] = parse_number(
                        row[col_map["expected_dividend"]]
                    )
                if "expected_eps" in col_map and col_map["expected_eps"] < len(row):
                    holding["expected_eps"] = parse_number(row[col_map["expected_eps"]])
                if "actual_bps" in col_map and col_map["actual_bps"] < len(row):
                    holding["actual_bps"] = parse_number(row[col_map["actual_bps"]])
                if "lending_type" in col_map and col_map["lending_type"] < len(row):
                    holding["lending_type"] = row[col_map["lending_type"]].strip()

                # 必須フィールドのチェック
                if holding["code"] and holding["quantity"] is not None:
                    holdings.append(holding)
                    logger.debug(
                        f"保有銘柄解析（詳細形式）: {holding['code']} - {holding['quantity']}株 ({account_type})"
                    )
                else:
                    logger.debug(
                        f"保有銘柄スキップ: code={holding.get('code')}, quantity={holding.get('quantity')}"
                    )

            logger.info(f"保有銘柄CSV解析完了（詳細形式）: {len(holdings)}銘柄")
            return holdings

        except Exception as e:
            logger.error(f"保有銘柄CSV解析エラー（詳細形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

"""SBI証券のCSVファイル解析モジュール（再構築版）"""

import csv
import io
import re
from datetime import datetime
from typing import Any

import chardet

from src.utils.logging_config import get_logger

logger = get_logger("portfolio.csv_parser")


class SBICSVParser:
    """SBI証券のCSVファイルパーサー（再構築版）"""

    @staticmethod
    def detect_encoding(content: bytes) -> str:
        """バイト列のエンコーディングを検出"""
        # BOMをチェック
        if content.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"

        # chardetで検出
        result = chardet.detect(content)
        encoding = result["encoding"]

        # 日本語のエンコーディングを優先
        if encoding and encoding.lower() in [
            "shift_jis",
            "cp932",
            "euc-jp",
            "iso-2022-jp",
        ]:
            return str(encoding)
        elif encoding and "utf" in encoding.lower():
            return str(encoding)
        else:
            # デフォルトでShift-JISとUTF-8を試す
            return "shift_jis"

    @staticmethod
    def parse_transactions_csv(csv_content: str | bytes) -> list[dict[str, Any]]:
        """
        取引履歴CSVを解析

        Args:
            csv_content: CSVファイルの内容（文字列またはバイト列）

        Returns:
            取引履歴情報のリスト
        """
        # バイト列の場合はエンコーディングを検出してデコード
        if isinstance(csv_content, bytes):
            encoding = SBICSVParser.detect_encoding(csv_content)
            logger.info(f"検出されたエンコーディング: {encoding}")
            try:
                csv_content = csv_content.decode(encoding)  # type: ignore[union-attr]
            except UnicodeDecodeError:
                # フォールバック
                for enc in ["utf-8-sig", "shift_jis", "cp932", "utf-8"]:
                    try:
                        csv_content = csv_content.decode(enc)  # type: ignore[union-attr]
                        logger.info(
                            f"フォールバックエンコーディング {enc} でデコード成功"
                        )
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError("CSVファイルのエンコーディングを判定できません")

        # BOMを除去
        if csv_content.startswith("\ufeff"):
            csv_content = csv_content[1:]

        # 形式を判定
        if (
            "銘柄（コード）" in csv_content
            and "取引区分" in csv_content
            and "約定代金" in csv_content
        ):
            # 注文一覧形式（新フォーマット）
            return SBICSVParser._parse_order_list_format(csv_content)
        elif "受渡金額・決済損益" in csv_content and "取引区分" in csv_content:
            # 注文一覧形式（旧フォーマット）
            return SBICSVParser._parse_order_list_format_old(csv_content)
        elif "約定履歴照会" in csv_content or (
            "約定日" in csv_content and "受渡金額/決済損益" in csv_content
        ):
            # SaveFile形式
            return SBICSVParser._parse_savefile_format(csv_content)
        elif (
            "約定日" in csv_content
            and "銘柄コード" in csv_content
            and "売買区分" in csv_content
        ):
            # 標準形式
            return SBICSVParser._parse_standard_format(csv_content)
        else:
            # その他の形式
            logger.warning("未対忌のCSV形式です")
            return []

    @staticmethod
    def _parse_order_list_format(csv_content: str) -> list[dict[str, Any]]:
        """注文一覧形式のCSVを解析"""
        transactions: list[dict[str, Any]] = []

        lines = csv_content.strip().split("\n")
        if len(lines) < 2:
            return transactions

        # ヘッダー行をスキップして、データ行を処理
        for i in range(1, len(lines)):
            line = lines[i].strip()
            if not line:
                continue

            reader = csv.reader(io.StringIO(line))
            row = next(reader, None)
            if not row or len(row) < 13:
                continue

            # 列インデックス（0ベース）
            # [0] 銘柄コード, [1] 銘柄名, [2] 市場, [3] 取引区分,
            # [6] 約定日, [8] 株数, [9] 平均約定単価,
            # [10] 手数料, [11] 税額, [12] 受渡金額・決済損益

            code = SBICSVParser._normalize_code(row[0])
            if not code:
                continue

            trade_type = row[3].strip()
            transaction_type, detailed_type = SBICSVParser._parse_trade_type(trade_type)

            # 決済損益の解析（注文一覧形式では約定代金と入金額が別列）
            realized_profit = None
            if len(row) > 13:
                # 売却時は入金額が存在
                if row[13] and row[13] != "" and row[13] != "--":
                    if detailed_type in ["決済売り", "決済買い"]:
                        # 入金額から決済損益を計算する方法もあるが、
                        # この形式では正確な決済損益は取得できない
                        realized_profit = None

            transaction = {
                "code": code,
                "name": row[1].strip(),
                "transaction_date": SBICSVParser._parse_date(row[6]),
                "transaction_type": transaction_type,
                "detailed_type": detailed_type,
                "quantity": SBICSVParser._parse_number(row[8]),
                "price": SBICSVParser._parse_number(row[9]),
                "commission": SBICSVParser._parse_number(row[10], default=0),
                "tax": SBICSVParser._parse_number(row[11], default=0),
                "total_amount": None,  # この形式では計算が必要
                "realized_profit": realized_profit,
                "remarks": "",
            }

            # 受渡金額を計算
            quantity = transaction["quantity"]
            price = transaction["price"]
            commission = transaction["commission"] or 0
            tax = transaction["tax"] or 0

            if quantity is not None and price is not None:
                base_amount = float(quantity) * float(price)
                if transaction_type == "buy":
                    transaction["total_amount"] = (
                        base_amount + float(commission) + float(tax)
                    )
                else:
                    transaction["total_amount"] = (
                        base_amount - float(commission) - float(tax)
                    )

            transactions.append(transaction)
            logger.debug(
                f"取引解析: {transaction['transaction_date']} {transaction['code']} "
                f"{transaction['detailed_type']} {transaction['quantity']}株"
            )

        logger.info(f"注文一覧形式CSV解析完了: {len(transactions)}件")
        return transactions

    @staticmethod
    def _parse_savefile_format(csv_content: str) -> list[dict[str, Any]]:
        """SaveFile形式のCSVを解析"""
        transactions: list[dict[str, Any]] = []

        lines = csv_content.split("\n")

        # ヘッダー行を探す
        header_index = -1
        for i, line in enumerate(lines):
            if "約定日" in line and "銘柄コード" in line:
                header_index = i
                break

        if header_index == -1:
            logger.error("SaveFile形式のヘッダーが見つかりません")
            return transactions

        # データ行を処理
        for i in range(header_index + 1, len(lines)):
            line = lines[i].strip()
            if not line or line.startswith("(注)"):
                continue

            reader = csv.reader(io.StringIO(line))
            row = next(reader, None)
            if not row or len(row) < 10:
                continue

            # 列インデックス（0ベース）
            # [0] 約定日, [1] 銘柄, [2] 銘柄コード, [3] 市場, [4] 取引,
            # [5] 期限, [6] 預り, [7] 課税, [8] 約定数量, [9] 約定単価,
            # [10] 手数料, [11] 税額, [12] 受渡日, [13] 受渡金額/決済損益

            code = SBICSVParser._normalize_code(row[2])
            if not code:
                continue

            trade_type = row[4].strip()
            transaction_type, detailed_type = SBICSVParser._parse_trade_type(trade_type)

            # 決済損益の解析
            realized_profit = None
            if len(row) > 13 and row[13] and row[13] != "--":
                # 信用新規買/売の場合は決済損益なし
                if detailed_type not in ["新規買い", "新規売り"]:
                    realized_profit = SBICSVParser._parse_number(row[13])

            transaction = {
                "code": code,
                "name": row[1].strip(),
                "transaction_date": SBICSVParser._parse_date(row[0]),
                "transaction_type": transaction_type,
                "detailed_type": detailed_type,
                "quantity": SBICSVParser._parse_number(row[8]),
                "price": SBICSVParser._parse_number(row[9]),
                "commission": SBICSVParser._parse_number(row[10], default=0),
                "tax": SBICSVParser._parse_number(row[11], default=0),
                "total_amount": None,  # この形式では計算が必要
                "realized_profit": realized_profit,
                "remarks": "",
            }

            # 受渡金額を計算
            quantity = transaction["quantity"]
            price = transaction["price"]
            commission = transaction["commission"] or 0
            tax = transaction["tax"] or 0

            if quantity is not None and price is not None:
                base_amount = float(quantity) * float(price)
                if transaction_type == "buy":
                    transaction["total_amount"] = (
                        base_amount + float(commission) + float(tax)
                    )
                else:
                    transaction["total_amount"] = (
                        base_amount - float(commission) - float(tax)
                    )

            transactions.append(transaction)
            logger.debug(
                f"取引解析: {transaction['transaction_date']} {transaction['code']} "
                f"{transaction['detailed_type']} {transaction['quantity']}株"
            )

        logger.info(f"SaveFile形式CSV解析完了: {len(transactions)}件")
        return transactions

    @staticmethod
    def _parse_standard_format(csv_content: str) -> list[dict[str, Any]]:
        """標準形式のCSVを解析"""
        transactions: list[dict[str, Any]] = []

        try:
            # CSVを読み込み
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            rows = list(csv_reader)

            if not rows:
                return transactions

            for row in rows:
                # カラム名のバリエーションに対応
                code = row.get("銘柄コード") or row.get("コード") or ""
                code = SBICSVParser._normalize_code(code)

                if not code:
                    continue

                # 売買区分の判定
                trade_type = row.get("売買区分", "").strip()
                if "買" in trade_type:
                    transaction_type = "buy"
                    detailed_type = "新規買い"
                elif "売" in trade_type:
                    transaction_type = "sell"
                    detailed_type = "決済売り"
                else:
                    transaction_type = "buy"
                    detailed_type = "新規買い"

                transaction = {
                    "code": code,
                    "name": row.get("銘柄名", "").strip(),
                    "transaction_date": SBICSVParser._parse_date(row.get("約定日")),
                    "transaction_type": transaction_type,
                    "detailed_type": detailed_type,
                    "quantity": SBICSVParser._parse_number(
                        row.get("数量") or row.get("株数")
                    ),
                    "price": SBICSVParser._parse_number(
                        row.get("約定単価") or row.get("単価")
                    ),
                    "commission": SBICSVParser._parse_number(
                        row.get("手数料"), default=0
                    ),
                    "tax": SBICSVParser._parse_number(
                        row.get("税金") or row.get("税額"), default=0
                    ),
                    "total_amount": SBICSVParser._parse_number(row.get("受渡金額")),
                    "realized_profit": None,
                    "remarks": row.get("備考", ""),
                }

                # 必須フィールドのチェック
                if transaction["code"] and transaction["quantity"] is not None:
                    transactions.append(transaction)
                    logger.debug(
                        f"取引解析: {transaction['transaction_date']} {transaction['code']} "
                        f"{transaction['detailed_type']} {transaction['quantity']}株"
                    )

            # 日付順にソート
            transactions.sort(key=lambda x: x["transaction_date"] or "")

            logger.info(f"標準形式CSV解析完了: {len(transactions)}件")
            return transactions

        except Exception as e:
            logger.error(f"取引履歴CSV解析エラー（標準形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

    @staticmethod
    def _parse_order_list_format_old(csv_content: str) -> list[dict[str, Any]]:
        """注文一覧形式（旧フォーマット）のCSVを解析"""
        transactions: list[dict[str, Any]] = []

        lines = csv_content.strip().split("\n")
        if len(lines) < 2:
            return transactions

        # ヘッダー行をスキップして、データ行を処理
        for i in range(1, len(lines)):
            line = lines[i].strip()
            if not line:
                continue

            reader = csv.reader(io.StringIO(line))
            row = next(reader, None)
            if not row or len(row) < 10:
                continue

            # 列インデックス（0ベース）
            # [0] 銘柄コード, [1] 銘柄名, [2] 市場, [3] 取引区分,
            # [6] 約定日, [8] 株数, [9] 平均約定単価,
            # [10] 手数料・諸経費等, [11] 課税額・譲渡益税,
            # [12] 受渡金額・決済損益

            code = SBICSVParser._normalize_code(row[0])
            if not code:
                continue

            trade_type = row[3].strip()
            transaction_type, detailed_type = SBICSVParser._parse_trade_type(trade_type)

            # 決済損益の解析
            realized_profit = None
            if len(row) > 12 and row[12] and row[12] != "--":
                if detailed_type in ["決済売り", "決済買い"]:
                    realized_profit = SBICSVParser._parse_number(row[12])

            transaction = {
                "code": code,
                "name": row[1].strip(),
                "transaction_date": SBICSVParser._parse_date(row[6]),
                "transaction_type": transaction_type,
                "detailed_type": detailed_type,
                "quantity": SBICSVParser._parse_number(row[8]),
                "price": SBICSVParser._parse_number(row[9]),
                "commission": SBICSVParser._parse_number(row[10], default=0),
                "tax": SBICSVParser._parse_number(row[11], default=0),
                "total_amount": None,  # この形式では計算が必要
                "realized_profit": realized_profit,
                "remarks": "信用" if "信用" in trade_type else "",
            }

            # 受渡金額を計算
            quantity = transaction["quantity"]
            price = transaction["price"]
            commission = transaction["commission"] or 0
            tax = transaction["tax"] or 0

            if quantity is not None and price is not None:
                base_amount = float(quantity) * float(price)
                if transaction_type == "buy":
                    transaction["total_amount"] = (
                        base_amount + float(commission) + float(tax)
                    )
                else:
                    transaction["total_amount"] = (
                        base_amount - float(commission) - float(tax)
                    )

            transactions.append(transaction)
            logger.debug(
                f"取引解析（旧形式）: {transaction['transaction_date']} {transaction['code']} "
                f"{transaction['detailed_type']} {transaction['quantity']}株"
            )

        logger.info(f"注文一覧形式（旧）CSV解析完了: {len(transactions)}件")
        return transactions

    @staticmethod
    def _parse_trade_type(trade_type: str) -> tuple[str, str]:
        """取引区分を解析してbuy/sellと詳細タイプを返す"""
        if not trade_type:
            return "buy", "新規買い"

        # 信用取引の判定
        if "信用新規買" in trade_type:
            return "buy", "新規買い"
        elif "信用新規売" in trade_type:
            return "sell", "新規売り"
        elif "信用返済買" in trade_type:
            return "buy", "決済買い"
        elif "信用返済売" in trade_type:
            return "sell", "決済売り"
        # 現物取引の判定
        elif "現物買" in trade_type:
            return "buy", "新規買い"
        elif "現物売" in trade_type:
            return "sell", "決済売り"
        # その他
        elif "買" in trade_type:
            return "buy", "新規買い"
        elif "売" in trade_type:
            return "sell", "決済売り"
        else:
            return "buy", "新規買い"

    @staticmethod
    def _normalize_code(code: str) -> str:
        """銘柄コードを4桁に正規化"""
        if not code:
            return ""

        # 372Aのような英字付きコードに対応
        code = str(code).strip()
        # 4桁の数字部分を抽出（英字は保持）
        if len(code) == 4:
            return code
        elif len(code) < 4 and code.isdigit():
            return code.zfill(4)
        else:
            # 数字のみを抽出
            digits = re.findall(r"\d+", code)
            if digits:
                return str(digits[0]).zfill(4)
        return code

    @staticmethod
    def _parse_number(value: Any, default: float | None = None) -> float | None:
        """数値を解析（カンマ、マイナス記号対応）"""
        if value is None or value == "" or value == "--":
            return default

        try:
            value_str = str(value).strip()

            # 矢印記号の場合はデフォルト値を返す
            if value_str in ["↑", "↓", "→", "←"]:
                return default

            # カンマを除去
            value_str = value_str.replace(",", "")

            # △や▲をマイナス記号に変換
            if value_str.startswith("△") or value_str.startswith("▲"):
                value_str = "-" + value_str[1:]

            # 括弧付きマイナス値の処理（例: "(1,000)")
            if value_str.startswith("(") and value_str.endswith(")"):
                value_str = "-" + value_str[1:-1]

            # マイナス記号の処理（例: "-1,000")
            if value_str.startswith('"') and value_str.endswith('"'):
                value_str = value_str[1:-1]

            # %記号を除去
            if value_str.endswith("%"):
                value_str = value_str[:-1]

            # 範囲表記（例: "194 ~ 200"）の場合は平均値を返す
            if "~" in value_str:
                parts = value_str.split("~")
                if len(parts) == 2:
                    try:
                        min_val = float(parts[0].strip())
                        max_val = float(parts[1].strip())
                        return (min_val + max_val) / 2
                    except ValueError:
                        pass

            return float(value_str)
        except (ValueError, AttributeError):
            # 矢印記号はエラーログを出さない
            if str(value).strip() not in ["↑", "↓", "→", "←"]:
                logger.warning(f"数値解析エラー: {value}")
            return default

    @staticmethod
    def _parse_date(date_str: Any) -> str | None:
        """日付文字列を解析してYYYY-MM-DD形式に変換"""
        if not date_str:
            return None

        date_str = str(date_str).strip()

        # 一般的な日付フォーマットを試行
        date_formats = [
            "%Y/%m/%d %H:%M:%S",  # 時刻付き
            "%Y/%m/%d",
            "%Y-%m-%d",
            "%Y年%m月%d日",
            "%y/%m/%d",  # 2桁年
        ]

        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue

        logger.warning(f"日付解析エラー: {date_str}")
        return None

    @staticmethod
    def parse_holdings_csv(csv_content: str | bytes) -> list[dict[str, Any]]:
        """
        保有銘柄CSVを解析

        複数のSBI証券CSVフォーマットに対応:
        1. 標準形式: 銘柄コード,銘柄名,市場,保有数量,取得単価,現在値,評価損益,評価損益率(%),...
        2. 保有証券_現物形式: 複雑なヘッダー構造、位置ベースの列
        3. SaveFile形式: セクション分割された形式

        Args:
            csv_content: CSVファイルの内容（文字列またはバイト列）

        Returns:
            保有銘柄情報のリスト
        """
        # バイト列の場合はエンコーディングを検出してデコード
        if isinstance(csv_content, bytes):
            encoding = SBICSVParser.detect_encoding(csv_content)
            logger.info(f"検出されたエンコーディング: {encoding}")
            try:
                csv_content = csv_content.decode(encoding)  # type: ignore[union-attr]
            except UnicodeDecodeError:
                # フォールバック
                for enc in ["utf-8-sig", "shift_jis", "cp932", "utf-8"]:
                    try:
                        csv_content = csv_content.decode(enc)  # type: ignore[union-attr]
                        logger.info(
                            f"フォールバックエンコーディング {enc} でデコード成功"
                        )
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError("CSVファイルのエンコーディングを判定できません")

        # BOMを除去
        if csv_content.startswith("\ufeff"):
            csv_content = csv_content[1:]

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
            return SBICSVParser._parse_holdings_detailed_format(csv_content)
        # SaveFile形式の判定（「保有証券一覧」などのヘッダーがある）
        elif "保有証券一覧" in csv_content or "評価額合計" in csv_content:
            return SBICSVParser._parse_holdings_savefile_format(csv_content)
        else:
            # 標準形式として処理を試みる
            return SBICSVParser._parse_holdings_standard_format(csv_content)

    @staticmethod
    def _parse_holdings_standard_format(csv_content: str) -> list[dict[str, Any]]:
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
                code = SBICSVParser._normalize_code(code)

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
                    "quantity": SBICSVParser._parse_number(
                        row.get("保有数量") or row.get("保有株数") or row.get("数量")
                    ),
                    "average_price": SBICSVParser._parse_number(
                        row.get("取得単価") or row.get("平均取得単価")
                    ),
                    "current_price": SBICSVParser._parse_number(
                        row.get("現在値") or row.get("株価")
                    ),
                    "market_value": SBICSVParser._parse_number(
                        row.get("評価額") or row.get("時価評価額")
                    ),
                    "profit_loss": SBICSVParser._parse_number(
                        row.get("評価損益") or row.get("損益")
                    ),
                    "profit_loss_ratio": SBICSVParser._parse_number(
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

    @staticmethod
    def _parse_holdings_savefile_format(csv_content: str) -> list[dict[str, Any]]:
        """SaveFile形式の保有銘柄CSVを解析"""
        holdings: list[dict[str, Any]] = []

        try:
            lines = csv_content.strip().split("\n")

            # 現在のセクション（口座タイプ）を追跡
            current_account_type = "特定"  # デフォルト

            # セクション見出しと口座タイプのマッピング
            section_mapping = {
                "株式（特定預り）": "特定",
                "株式（NISA預り（成長投資枠））": "NISA",
                "株式（旧NISA預り）": "旧NISA",
                # TODO: 投資信託セクションは現在スキップ対象
                "投資信託（金額/NISA預り（つみたて投資枠））": "つみたてNISA",
            }

            # 各行を処理
            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # セクション見出しをチェック
                if line in section_mapping:
                    current_account_type = section_mapping[line]
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
                        # 投資信託セクションかどうか判定
                        is_fund = (
                            "つみたてNISA" in current_account_type and len(row) >= 9
                        )

                        if is_fund:
                            # 投資信託の場合
                            fund_name = row[0].strip() if row[0] else ""
                            if fund_name:
                                # TODO: 投資信託の取り込み機能を実装する
                                # 現在の課題:
                                # 1. 投資信託には標準的な4桁銘柄コードが存在しない
                                # 2. ファンド名のみでの識別となるため、名称変更時の追跡が困難
                                # 3. 複数の販売会社で同一ファンドが異なるコードで管理される
                                #
                                # 実装案:
                                # - ISINコードやファンドコードなど一意識別子の利用を検討
                                # - ファンド名のマスターテーブルを作成し、名称変更に対応
                                # - 投資信託専用のテーブル（fund_holdings）を作成
                                # - 銘柄コードの代わりにファンドIDを使用
                                # - ファンド名とハッシュ値のマッピングテーブルを管理
                                #
                                # 一時的に投資信託のインポートをスキップ
                                logger.warning(
                                    f"投資信託のインポートはスキップされました: {fund_name} "
                                    f"(口座: {current_account_type})"
                                )
                        else:
                            # 株式の場合（既存の処理）
                            code = SBICSVParser._normalize_code(row[0])
                            if code:
                                holding = {
                                    "code": code,
                                    "name": row[1].strip() if len(row) > 1 else "",
                                    "account_type": current_account_type,  # 現在のセクションの口座タイプを設定
                                    "quantity": (
                                        SBICSVParser._parse_number(row[2])
                                        if len(row) > 2
                                        else None
                                    ),
                                    "average_price": (
                                        SBICSVParser._parse_number(row[4])
                                        if len(row) > 4
                                        else None
                                    ),
                                    "current_price": (
                                        SBICSVParser._parse_number(row[5])
                                        if len(row) > 5
                                        else None
                                    ),
                                    "market_value": (
                                        SBICSVParser._parse_number(row[7])
                                        if len(row) > 7
                                        else None
                                    ),
                                    "profit_loss": (
                                        SBICSVParser._parse_number(row[8])
                                        if len(row) > 8
                                        else None
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

    @staticmethod
    def _parse_holdings_detailed_format(csv_content: str) -> list[dict[str, Any]]:
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
                    code = SBICSVParser._normalize_code(row[col_map["code"]])
                elif len(row) > 4:  # フォールバック
                    code = SBICSVParser._normalize_code(row[4])

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
                        SBICSVParser._parse_number(row[col_map["quantity"]])
                        if "quantity" in col_map and col_map["quantity"] < len(row)
                        else None
                    ),
                    "average_price": (
                        SBICSVParser._parse_number(row[col_map["average_price"]])
                        if "average_price" in col_map
                        and col_map["average_price"] < len(row)
                        else None
                    ),
                    "current_price": (
                        SBICSVParser._parse_number(row[col_map["current_price"]])
                        if "current_price" in col_map
                        and col_map["current_price"] < len(row)
                        else None
                    ),
                    "market_value": (
                        SBICSVParser._parse_number(row[col_map["market_value"]])
                        if "market_value" in col_map
                        and col_map["market_value"] < len(row)
                        else None
                    ),
                    "profit_loss": (
                        SBICSVParser._parse_number(row[col_map["profit_loss"]])
                        if "profit_loss" in col_map
                        and col_map["profit_loss"] < len(row)
                        else None
                    ),
                    "profit_loss_ratio": (
                        SBICSVParser._parse_number(row[col_map["profit_loss_ratio"]])
                        if "profit_loss_ratio" in col_map
                        and col_map["profit_loss_ratio"] < len(row)
                        else None
                    ),
                }

                # 株価指標データ（オプション）
                if "expected_per" in col_map and col_map["expected_per"] < len(row):
                    holding["expected_per"] = SBICSVParser._parse_number(
                        row[col_map["expected_per"]]
                    )
                if "actual_pbr" in col_map and col_map["actual_pbr"] < len(row):
                    holding["actual_pbr"] = SBICSVParser._parse_number(
                        row[col_map["actual_pbr"]]
                    )
                if "dividend_yield" in col_map and col_map["dividend_yield"] < len(row):
                    holding["dividend_yield"] = SBICSVParser._parse_number(
                        row[col_map["dividend_yield"]]
                    )
                if "expected_dividend" in col_map and col_map[
                    "expected_dividend"
                ] < len(row):
                    holding["expected_dividend"] = SBICSVParser._parse_number(
                        row[col_map["expected_dividend"]]
                    )
                if "expected_eps" in col_map and col_map["expected_eps"] < len(row):
                    holding["expected_eps"] = SBICSVParser._parse_number(
                        row[col_map["expected_eps"]]
                    )
                if "actual_bps" in col_map and col_map["actual_bps"] < len(row):
                    holding["actual_bps"] = SBICSVParser._parse_number(
                        row[col_map["actual_bps"]]
                    )
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

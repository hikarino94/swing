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
                    transaction["total_amount"] = base_amount + float(commission) + float(tax)
                else:
                    transaction["total_amount"] = base_amount - float(commission) - float(tax)

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
                    transaction["total_amount"] = base_amount + float(commission) + float(tax)
                else:
                    transaction["total_amount"] = base_amount - float(commission) - float(tax)

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
                    transaction["total_amount"] = base_amount + float(commission) + float(tax)
                else:
                    transaction["total_amount"] = base_amount - float(commission) - float(tax)

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
        if lines and (lines[0].count("銘柄") >= 4 or lines[0].startswith("﻿銘柄")):
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

                holding = {
                    "code": code,
                    "name": row.get("銘柄名", "").strip(),
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
                        f"保有銘柄解析: {holding['code']} - {holding['quantity']}株"
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

            # ヘッダー行を探す（文字化けしている可能性があるので、列数で判定）
            header_index = -1
            for i, line in enumerate(lines):
                # CSVの列数をチェック
                reader = csv.reader(io.StringIO(line))
                row = next(reader, None)
                if row and len(row) >= 9:  # 保有銘柄データは9列以上
                    # 最初のデータ行を見つける（"で始まる行）
                    if line.strip().startswith('"'):
                        header_index = i - 1
                        break

            if header_index == -1:
                logger.error("保有銘柄のデータ行が見つかりません")
                return holdings

            # データ行を処理
            for i in range(header_index + 1, len(lines)):
                line = lines[i].strip()
                if not line or not line.startswith('"'):
                    continue

                # CSVとして解析
                reader = csv.reader(io.StringIO(line))
                row = next(reader, None)
                if not row or len(row) < 8:
                    continue

                code = SBICSVParser._normalize_code(row[0])
                if not code:
                    continue

                holding = {
                    "code": code,
                    "name": row[1].strip() if len(row) > 1 else "",
                    "quantity": (
                        SBICSVParser._parse_number(row[2]) if len(row) > 2 else None
                    ),
                    "average_price": (
                        SBICSVParser._parse_number(row[4]) if len(row) > 4 else None
                    ),
                    "current_price": (
                        SBICSVParser._parse_number(row[5]) if len(row) > 5 else None
                    ),
                    "market_value": (
                        SBICSVParser._parse_number(row[7]) if len(row) > 7 else None
                    ),
                    "profit_loss": (
                        SBICSVParser._parse_number(row[8]) if len(row) > 8 else None
                    ),
                    "profit_loss_ratio": None,  # SaveFile形式には評価損益率がない
                }

                # 必須フィールドのチェック
                if holding["code"] and holding["quantity"] is not None:
                    holdings.append(holding)
                    logger.debug(
                        f"保有銘柄解析（SaveFile形式）: {holding['code']} - {holding['quantity']}株"
                    )

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

            # データ行をパース（ヘッダー行をスキップ）
            for line in lines[1:]:
                if not line.strip():
                    continue

                # CSVとして解析
                reader = csv.reader(io.StringIO(line))
                row = next(reader, None)
                if not row or len(row) < 20:
                    continue

                # 4列目が銘柄コード
                code = SBICSVParser._normalize_code(row[4]) if len(row) > 4 else ""
                if not code:
                    continue

                holding = {
                    "code": code,
                    "name": row[5].strip() if len(row) > 5 else "",  # 5列目が銘柄名
                    "quantity": (
                        SBICSVParser._parse_number(row[8]) if len(row) > 8 else None
                    ),  # 8列目が保有株数
                    "average_price": (
                        SBICSVParser._parse_number(row[10]) if len(row) > 10 else None
                    ),  # 10列目が取得単価
                    "current_price": (
                        SBICSVParser._parse_number(row[11]) if len(row) > 11 else None
                    ),  # 11列目が現在値
                    "market_value": (
                        SBICSVParser._parse_number(row[16]) if len(row) > 16 else None
                    ),  # 16列目が評価額
                    "profit_loss": (
                        SBICSVParser._parse_number(row[13]) if len(row) > 13 else None
                    ),  # 13列目が評価損益
                    "profit_loss_ratio": (
                        SBICSVParser._parse_number(row[14]) if len(row) > 14 else None
                    ),  # 14列目が評価損益率
                    # 株価指標データ
                    "expected_per": (
                        SBICSVParser._parse_number(row[27]) if len(row) > 27 else None
                    ),  # 27列目が予想PER
                    "actual_pbr": (
                        SBICSVParser._parse_number(row[28]) if len(row) > 28 else None
                    ),  # 28列目が実績PBR
                    "dividend_yield": (
                        SBICSVParser._parse_number(row[29]) if len(row) > 29 else None
                    ),  # 29列目が予想配当利回り
                    "expected_eps": (
                        SBICSVParser._parse_number(row[30]) if len(row) > 30 else None
                    ),  # 30列目が予想EPS
                    "actual_bps": (
                        SBICSVParser._parse_number(row[31]) if len(row) > 31 else None
                    ),  # 31列目が実績BPS
                    "expected_dividend": (
                        SBICSVParser._parse_number(row[32]) if len(row) > 32 else None
                    ),  # 32列目が予想1株配当
                    "lending_type": (
                        row[33].strip() if len(row) > 33 else ""
                    ),  # 33列目が貸借区分
                }

                # 必須フィールドのチェック
                if holding["code"] and holding["quantity"] is not None:
                    holdings.append(holding)
                    logger.debug(
                        f"保有銘柄解析（詳細形式）: {holding['code']} - {holding['quantity']}株"
                    )

            logger.info(f"保有銘柄CSV解析完了（詳細形式）: {len(holdings)}銘柄")
            return holdings

        except Exception as e:
            logger.error(f"保有銘柄CSV解析エラー（詳細形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

"""取引履歴CSV解析モジュール"""

import csv
import io
from typing import Any

from src.utils.logging_config import get_logger

from ..base import BaseCSVParser
from ..utils import normalize_code, parse_date, parse_number

logger = get_logger("portfolio.csv_parser.parsers.transactions")


def parse_trade_type(trade_type: str) -> tuple[str, str]:
    """取引区分を解析してbuy/sellと詳細タイプを返す"""
    if not trade_type:
        return "buy", "新規買い"

    # 現引き取引はスキップ（特別な値を返す）
    if "現引" in trade_type:
        return "skip", "現引き"

    # 投資信託取引はスキップ
    if "投信" in trade_type or "投資信託" in trade_type:
        return "skip", "投資信託"

    # 信用取引の判定
    if "信用新規買" in trade_type:
        return "buy", "新規買い"
    elif "信用新規売" in trade_type:
        return "sell", "新規売り"
    elif "信用返済買" in trade_type or "信用決済買" in trade_type:
        return "buy", "決済買い"
    elif "信用返済売" in trade_type or "信用決済売" in trade_type:
        return "sell", "決済売り"
    # 現物取引の判定（「株式現物買」「株式現物売」にも対応）
    elif "現物買" in trade_type or "株式現物買" in trade_type:
        return "buy", "新規買い"
    elif "現物売" in trade_type or "株式現物売" in trade_type:
        return "sell", "決済売り"
    # その他
    elif "買" in trade_type:
        return "buy", "新規買い"
    elif "売" in trade_type:
        return "sell", "決済売り"
    else:
        return "buy", "新規買い"


class TransactionsParser(BaseCSVParser):
    """取引履歴CSVパーサー"""

    @classmethod
    def _parse_content(cls, csv_content: str) -> list[dict[str, Any]]:
        """CSVコンテンツを解析"""
        # 形式を判定
        if (
            "銘柄（コード）" in csv_content
            and "取引区分" in csv_content
            and "約定代金" in csv_content
        ):
            # 注文一覧形式（新フォーマット）
            return cls._parse_order_list_format(csv_content)
        elif "受渡金額・決済損益" in csv_content and "取引区分" in csv_content:
            # 注文一覧形式（旧フォーマット）
            return cls._parse_order_list_format_old(csv_content)
        elif "約定履歴照会" in csv_content or (
            "約定日" in csv_content and "受渡金額/決済損益" in csv_content
        ):
            # SaveFile形式
            return cls._parse_savefile_format(csv_content)
        elif (
            "約定日" in csv_content
            and "銘柄コード" in csv_content
            and "売買区分" in csv_content
        ):
            # 標準形式
            return cls._parse_standard_format(csv_content)
        else:
            # その他の形式
            logger.warning("未対忌のCSV形式です")
            return []

    @classmethod
    def _parse_order_list_format(cls, csv_content: str) -> list[dict[str, Any]]:
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

            code = normalize_code(row[0])
            if not code:
                # 銘柄コードが空の場合（投資信託など）はスキップ
                logger.debug(f"銘柄コードが空のためスキップ: {row[1]}")
                continue

            trade_type = row[3].strip()
            transaction_type, detailed_type = parse_trade_type(trade_type)

            # 現引きや投資信託取引はスキップ
            if transaction_type == "skip":
                logger.info(f"{detailed_type}取引をスキップ: {row[1]} ({row[0]})")
                continue

            # 決済損益の解析（注文一覧形式では約定代金と入金額が別列）
            realized_profit = None
            if len(row) > 13:
                # 売却時は入金額が存在
                if row[13] and row[13] != "" and row[13] != "--":
                    if detailed_type in ["決済売り", "決済買い"]:
                        # 入金額から決済損益を計算する方法もあるが、
                        # この形式では正確な決済損益は取得できない
                        realized_profit = None

            # 信用取引かどうかを判定してremarksに設定
            remarks = ""
            if "信用" in trade_type:
                remarks = "信用"
            # 新規売りは信用取引（空売り）
            elif detailed_type == "新規売り":
                remarks = "信用"
            # 決済買いも信用取引（空売りの決済）
            elif detailed_type == "決済買い":
                remarks = "信用"

            transaction = {
                "code": code,
                "name": row[1].strip(),
                "transaction_date": parse_date(row[6]),
                "transaction_type": transaction_type,
                "detailed_type": detailed_type,
                "quantity": parse_number(row[8]),
                "price": parse_number(row[9]),
                "commission": parse_number(row[10], default=0),
                "tax": parse_number(row[11], default=0),
                "total_amount": None,  # この形式では計算が必要
                "realized_profit": realized_profit,
                "remarks": remarks,
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

    @classmethod
    def _parse_savefile_format(cls, csv_content: str) -> list[dict[str, Any]]:
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

            code = normalize_code(row[2])
            if not code:
                # 銘柄コードが空の場合（投資信託など）はスキップ
                logger.debug(f"銘柄コードが空のためスキップ: {row[1]}")
                continue

            trade_type = row[4].strip()
            transaction_type, detailed_type = parse_trade_type(trade_type)

            # 現引きや投資信託取引はスキップ
            if transaction_type == "skip":
                logger.info(f"{detailed_type}取引をスキップ: {row[1]} ({row[2]})")
                continue

            # 決済損益の解析
            realized_profit = None
            if len(row) > 13 and row[13] and row[13] != "--":
                # 信用新規買/売の場合は決済損益なし
                if detailed_type not in ["新規買い", "新規売り"]:
                    realized_profit = parse_number(row[13])

            # 信用取引かどうかを判定してremarksに設定
            remarks = ""
            if "信用" in trade_type:
                remarks = "信用"
            # 新規売りは信用取引（空売り）
            elif detailed_type == "新規売り":
                remarks = "信用"
            # 決済買いも信用取引（空売りの決済）
            elif detailed_type == "決済買い":
                remarks = "信用"

            transaction = {
                "code": code,
                "name": row[1].strip(),
                "transaction_date": parse_date(row[0]),
                "transaction_type": transaction_type,
                "detailed_type": detailed_type,
                "quantity": parse_number(row[8]),
                "price": parse_number(row[9]),
                "commission": parse_number(row[10], default=0),
                "tax": parse_number(row[11], default=0),
                "total_amount": None,  # この形式では計算が必要
                "realized_profit": realized_profit,
                "remarks": remarks,
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

    @classmethod
    def _parse_standard_format(cls, csv_content: str) -> list[dict[str, Any]]:
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
                code = normalize_code(code)

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

                # 信用取引かどうかを判定
                remarks = row.get("備考", "")
                # 売買区分から信用取引を判定（標準形式では備考欄に情報がない場合があるため）
                if not remarks and trade_type:
                    # 新規売りは信用取引（空売り）
                    if detailed_type == "新規売り":
                        remarks = "信用"
                    # 決済買いも信用取引（空売りの決済）
                    elif detailed_type == "決済買い":
                        remarks = "信用"

                transaction = {
                    "code": code,
                    "name": row.get("銘柄名", "").strip(),
                    "transaction_date": parse_date(row.get("約定日")),
                    "transaction_type": transaction_type,
                    "detailed_type": detailed_type,
                    "quantity": parse_number(row.get("数量") or row.get("株数")),
                    "price": parse_number(row.get("約定単価") or row.get("単価")),
                    "commission": parse_number(row.get("手数料"), default=0),
                    "tax": parse_number(row.get("税金") or row.get("税額"), default=0),
                    "total_amount": parse_number(row.get("受渡金額")),
                    "realized_profit": None,
                    "remarks": remarks,
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

    @classmethod
    def _parse_order_list_format_old(cls, csv_content: str) -> list[dict[str, Any]]:
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

            code = normalize_code(row[0])
            if not code:
                # 銘柄コードが空の場合（投資信託など）はスキップ
                logger.debug(f"銘柄コードが空のためスキップ: {row[1]}")
                continue

            trade_type = row[3].strip()
            transaction_type, detailed_type = parse_trade_type(trade_type)

            # 現引きや投資信託取引はスキップ
            if transaction_type == "skip":
                logger.info(f"{detailed_type}取引をスキップ: {row[1]} ({row[0]})")
                continue

            # 決済損益の解析
            realized_profit = None
            if len(row) > 12 and row[12] and row[12] != "--":
                if detailed_type in ["決済売り", "決済買い"]:
                    realized_profit = parse_number(row[12])

            # 信用取引かどうかを判定してremarksに設定
            remarks = "信用" if "信用" in trade_type else ""

            transaction = {
                "code": code,
                "name": row[1].strip(),
                "transaction_date": parse_date(row[6]),
                "transaction_type": transaction_type,
                "detailed_type": detailed_type,
                "quantity": parse_number(row[8]),
                "price": parse_number(row[9]),
                "commission": parse_number(row[10], default=0),
                "tax": parse_number(row[11], default=0),
                "total_amount": None,  # この形式では計算が必要
                "realized_profit": realized_profit,
                "remarks": remarks,
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

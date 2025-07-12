"""取引履歴CSVパーサー"""

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.logging_config import get_logger

from .base import BaseCSVParser

logger = get_logger("portfolio.parsers.transactions")


class TransactionsCSVParser(BaseCSVParser):
    """取引履歴CSVのパーサー"""

    def detect_format(self, df: pd.DataFrame) -> str | None:
        """CSVフォーマットを検出"""
        if df.empty:
            return None

        columns = set(df.columns)

        # 取引履歴の一般的なカラム
        transaction_indicators = {
            "約定日",
            "受渡日",
            "銘柄コード",
            "銘柄名",
            "売買区分",
            "数量",
            "約定単価",
            "約定金額",
            "手数料",
            "税金",
        }

        # 詳細タイプのカラム
        has_detailed_type = "取引区分" in columns or "売買詳細" in columns

        if len(columns & transaction_indicators) >= 6:
            return "detailed" if has_detailed_type else "standard"
        else:
            return None

    def parse(self, file_path: Path) -> list[dict[str, Any]]:
        """CSVファイルを解析してデータを返す"""
        df = self.read_csv(file_path)
        format_type = self.detect_format(df)

        if not format_type:
            raise ValueError("未知の取引履歴CSVフォーマットです")

        logger.info(f"検出されたフォーマット: {format_type}")

        return self._parse_transactions(
            df, has_detailed_type=(format_type == "detailed")
        )

    def _parse_transactions(
        self, df: pd.DataFrame, has_detailed_type: bool
    ) -> list[dict[str, Any]]:
        """取引履歴を解析"""
        results = []

        # カラム名のマッピング
        column_mapping = {
            "約定日": "transaction_date",
            "取引日": "transaction_date",
            "受渡日": "settlement_date",
            "銘柄コード": "code",
            "証券コード": "code",
            "銘柄名": "name",
            "銘柄": "name",
            "売買区分": "transaction_type",
            "売買": "transaction_type",
            "取引区分": "transaction_type",
            "数量": "quantity",
            "株数": "quantity",
            "約定単価": "price",
            "単価": "price",
            "約定金額": "total_amount",
            "金額": "total_amount",
            "手数料": "commission",
            "手数料/税金": "commission",
            "税金": "tax",
            "消費税": "tax",
            "受渡金額": "settlement_amount",
            "損益": "realized_profit",
            "実現損益": "realized_profit",
        }

        if has_detailed_type:
            column_mapping.update(
                {
                    "取引区分": "detailed_type",
                    "売買詳細": "detailed_type",
                    "取引種別": "detailed_type",
                }
            )

        # カラム名を正規化
        df_normalized = df.rename(columns=column_mapping)

        for _, row in df_normalized.iterrows():
            code = self.format_code(row.get("code"))
            if not code:
                continue

            # 取引日の解析
            transaction_date = self._parse_date(row.get("transaction_date"))
            if not transaction_date:
                continue

            # 売買区分の正規化
            transaction_type = self._normalize_transaction_type(
                str(row.get("transaction_type", ""))
            )
            if not transaction_type:
                continue

            quantity = self.clean_quantity(row.get("quantity", 0))
            if quantity == 0:
                continue

            # 手数料と税金の処理
            commission = self.clean_numeric(row.get("commission", 0))
            tax = self.clean_numeric(row.get("tax", 0))

            # 手数料/税金が合算されている場合
            if commission > 0 and tax == 0 and "手数料/税金" in df_normalized.columns:
                # 簡易的に消費税率10%で分離
                total_fee = commission
                commission = total_fee / 1.1
                tax = total_fee - commission

            data = {
                "code": code,
                "name": str(row.get("name", "")),
                "transaction_date": transaction_date,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "price": self.clean_numeric(row.get("price", 0)),
                "total_amount": self.clean_numeric(row.get("total_amount", 0)),
                "commission": commission,
                "tax": tax,
            }

            # 詳細タイプ
            if has_detailed_type and "detailed_type" in df_normalized.columns:
                detailed_type = str(row.get("detailed_type", "")).strip()
                data["detailed_type"] = self._normalize_detailed_type(detailed_type)

            # 実現損益
            if "realized_profit" in df_normalized.columns:
                realized_profit = row.get("realized_profit")
                if pd.notna(realized_profit) and str(realized_profit).strip() not in [
                    "",
                    "-",
                    "－",
                ]:
                    data["realized_profit"] = self.clean_numeric(realized_profit)

            # 備考（銘柄名を使用）
            data["remarks"] = data["name"]

            results.append(data)

        return results

    def _parse_date(self, date_value: Any) -> str | None:
        """日付を解析してYYYY-MM-DD形式に変換"""
        if pd.isna(date_value):
            return None

        date_str = str(date_value).strip()

        # 一般的な日付フォーマットを試す
        date_formats = [
            "%Y/%m/%d",
            "%Y-%m-%d",
            "%Y年%m月%d日",
            "%Y.%m.%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
        ]

        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # 8桁の数字（YYYYMMDD）の場合
        if len(date_str) == 8 and date_str.isdigit():
            try:
                dt = datetime.strptime(date_str, "%Y%m%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        logger.warning(f"日付の解析に失敗: {date_str}")
        return None

    def _normalize_transaction_type(self, transaction_type: str) -> str | None:
        """売買区分を正規化"""
        transaction_type = transaction_type.strip().lower()

        # 買い
        if any(
            keyword in transaction_type for keyword in ["買", "buy", "購入", "取得"]
        ):
            return "buy"
        # 売り
        elif any(
            keyword in transaction_type for keyword in ["売", "sell", "売却", "処分"]
        ):
            return "sell"
        else:
            return None

    def _normalize_detailed_type(self, detailed_type: str) -> str:
        """詳細取引タイプを正規化"""
        detailed_type = detailed_type.strip()

        # 一般的な取引タイプ
        if "現物" in detailed_type:
            return "現物"
        elif "信用" in detailed_type:
            if "新規" in detailed_type:
                return "信用新規"
            elif "返済" in detailed_type:
                return "信用返済"
            else:
                return "信用"
        else:
            return detailed_type or "現物"

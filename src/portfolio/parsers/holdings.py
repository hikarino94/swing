"""保有銘柄CSVパーサー"""

from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.logging_config import get_logger

from .base import BaseCSVParser

logger = get_logger("portfolio.parsers.holdings")


class HoldingsCSVParser(BaseCSVParser):
    """保有銘柄CSVのパーサー"""

    def detect_format(self, df: pd.DataFrame) -> str | None:
        """CSVフォーマットを検出"""
        if df.empty:
            return None

        columns = set(df.columns)

        # 標準形式の判定
        standard_indicators = {
            "証券コード",
            "銘柄名",
            "保有数量",
            "平均取得単価",
            "現在値",
            "評価額",
            "評価損益",
            "評価損益率",
            "予想PER",
            "実績PBR",
            "予想配当利回り",
        }

        # SaveFile形式の判定
        savefile_indicators = {
            "コード",
            "銘柄",
            "数量",
            "取得単価",
            "取得価額",
            "前日比",
        }

        # 口座タイプの検出
        has_account_type = "預り区分" in columns or "口座" in columns

        if len(columns & standard_indicators) >= 8:
            return "standard" if has_account_type else "standard_no_account"
        elif len(columns & savefile_indicators) >= 4:
            return "savefile" if has_account_type else "savefile_no_account"
        else:
            return None

    def parse(self, file_path: Path) -> list[dict[str, Any]]:
        """CSVファイルを解析してデータを返す"""
        df = self.read_csv(file_path)
        format_type = self.detect_format(df)

        if not format_type:
            raise ValueError("未知のCSVフォーマットです")

        logger.info(f"検出されたフォーマット: {format_type}")

        if format_type.startswith("standard"):
            return self._parse_standard_format(
                df, has_account_type="no_account" not in format_type
            )
        else:
            return self._parse_savefile_format(
                df, has_account_type="no_account" not in format_type
            )

    def _parse_standard_format(
        self, df: pd.DataFrame, has_account_type: bool
    ) -> list[dict[str, Any]]:
        """標準形式のCSVを解析"""
        results = []

        # カラム名のマッピング
        column_mapping = {
            "証券コード": "code",
            "銘柄コード": "code",
            "銘柄名": "name",
            "保有数量": "quantity",
            "数量": "quantity",
            "平均取得単価": "average_price",
            "平均取得価額": "average_price",
            "現在値": "current_price",
            "評価額": "market_value",
            "評価損益": "profit_loss",
            "評価損益率": "profit_loss_ratio",
            "評価損益率(%)": "profit_loss_ratio",
            "予想PER": "expected_per",
            "実績PBR": "actual_pbr",
            "予想配当利回り": "dividend_yield",
            "予想配当利回り(%)": "dividend_yield",
            "予想EPS": "expected_eps",
            "実績BPS": "actual_bps",
            "予想配当": "expected_dividend",
            "貸借区分": "lending_type",
        }

        # 口座タイプのカラム
        if has_account_type:
            column_mapping.update(
                {
                    "預り区分": "account_type",
                    "口座": "account_type",
                    "口座区分": "account_type",
                }
            )

        # カラム名を正規化
        df_normalized = df.rename(columns=column_mapping)

        for _, row in df_normalized.iterrows():
            code = self.format_code(row.get("code"))
            if not code:
                continue

            data = {
                "code": code,
                "name": str(row.get("name", "")),
                "quantity": self.clean_quantity(row.get("quantity", 0)),
                "average_price": self.clean_numeric(row.get("average_price", 0)),
                "current_price": self.clean_numeric(row.get("current_price", 0)),
                "market_value": self.clean_numeric(row.get("market_value", 0)),
                "profit_loss": self.clean_numeric(row.get("profit_loss", 0)),
                "profit_loss_ratio": self.clean_numeric(
                    row.get("profit_loss_ratio", 0)
                ),
            }

            # 株価指標（オプション）
            for key in [
                "expected_per",
                "actual_pbr",
                "dividend_yield",
                "expected_eps",
                "actual_bps",
                "expected_dividend",
            ]:
                if key in df_normalized.columns:
                    value = row.get(key)
                    if pd.notna(value) and str(value).strip() not in [
                        "",
                        "-",
                        "－",
                        "N/A",
                    ]:
                        data[key] = self.clean_numeric(value)

            # 貸借区分
            if "lending_type" in df_normalized.columns:
                data["lending_type"] = str(row.get("lending_type", "")).strip()

            # 口座タイプ
            if has_account_type and "account_type" in df_normalized.columns:
                account_type = str(row.get("account_type", "特定")).strip()
                data["account_type"] = self._normalize_account_type(account_type)
            else:
                data["account_type"] = "特定"

            results.append(data)

        return results

    def _parse_savefile_format(
        self, df: pd.DataFrame, has_account_type: bool
    ) -> list[dict[str, Any]]:
        """SaveFile形式のCSVを解析"""
        results = []

        # カラム名のマッピング
        column_mapping = {
            "コード": "code",
            "銘柄コード": "code",
            "銘柄": "name",
            "銘柄名": "name",
            "数量": "quantity",
            "保有数": "quantity",
            "取得単価": "average_cost",
            "平均取得単価": "average_cost",
            "取得価額": "total_cost",
        }

        if has_account_type:
            column_mapping.update(
                {
                    "預り区分": "account_type",
                    "口座": "account_type",
                }
            )

        # カラム名を正規化
        df_normalized = df.rename(columns=column_mapping)

        for _, row in df_normalized.iterrows():
            code = self.format_code(row.get("code"))
            if not code:
                continue

            quantity = self.clean_quantity(row.get("quantity", 0))
            if quantity == 0:
                continue

            # 平均取得単価を計算
            if "average_cost" in df_normalized.columns:
                average_cost = self.clean_numeric(row.get("average_cost", 0))
            elif "total_cost" in df_normalized.columns:
                total_cost = self.clean_numeric(row.get("total_cost", 0))
                average_cost = total_cost / quantity if quantity > 0 else 0
            else:
                average_cost = 0

            data = {
                "code": code,
                "name": str(row.get("name", "")),
                "quantity": quantity,
                "average_cost": average_cost,
            }

            # 口座タイプ
            if has_account_type and "account_type" in df_normalized.columns:
                account_type = str(row.get("account_type", "特定")).strip()
                data["account_type"] = self._normalize_account_type(account_type)
            else:
                data["account_type"] = "特定"

            results.append(data)

        return results

    def _normalize_account_type(self, account_type: str) -> str:
        """口座タイプを正規化"""
        account_type = account_type.strip()

        # 一般的な表記の正規化
        if account_type in ["特定", "特定口座"]:
            return "特定"
        elif account_type in ["一般", "一般口座"]:
            return "一般"
        elif account_type in ["NISA", "nisa", "ニーサ"]:
            return "NISA"
        else:
            # デフォルトは特定口座
            return "特定"

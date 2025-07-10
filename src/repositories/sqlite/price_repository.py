"""
株価データのSQLiteリポジトリ実装
"""

from datetime import date, datetime
from typing import Any

import pandas as pd

from ..interfaces import PriceRepository
from .base import SqliteBaseRepository


class SqlitePriceRepository(SqliteBaseRepository, PriceRepository):
    """株価データのSQLiteリポジトリ実装"""

    def find_by_code_and_date_range(
        self, code: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """指定銘柄・期間の株価データを取得"""
        query = """
            SELECT * FROM prices
            WHERE Code = ? AND Date >= ? AND Date <= ?
            ORDER BY Date
        """
        cursor = self.execute(
            query, (code, start_date.isoformat(), end_date.isoformat())
        )

        # 結果をDataFrameに変換
        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(data, columns=columns)
        df["Date"] = pd.to_datetime(df["Date"])
        return df

    def find_latest_by_code(self, code: str) -> dict[str, Any] | None:
        """指定銘柄の最新株価を取得"""
        query = """
            SELECT * FROM prices
            WHERE Code = ?
            ORDER BY Date DESC
            LIMIT 1
        """
        cursor = self.execute(query, (code,))
        row = cursor.fetchone()

        if row:
            return dict(row)
        return None

    def find_all_by_date(self, target_date: date) -> pd.DataFrame:
        """指定日の全銘柄の株価データを取得"""
        query = """
            SELECT * FROM prices
            WHERE Date = ?
            ORDER BY Code
        """
        cursor = self.execute(query, (target_date.isoformat(),))

        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(data, columns=columns)
        df["Date"] = pd.to_datetime(df["Date"])
        return df

    def save_batch(self, data: pd.DataFrame) -> int:
        """株価データを一括保存"""
        if data.empty:
            return 0

        # DataFrameをレコードのリストに変換
        records = []
        for _, row in data.iterrows():
            # Dateをstring形式に変換
            date_value = row.get("Date")
            if pd.isna(date_value):
                date_str = None
            elif hasattr(date_value, "strftime"):
                date_str = date_value.strftime("%Y-%m-%d")
            else:
                date_str = str(date_value)

            record = (
                date_str,
                row.get("Code"),
                row.get("Open"),
                row.get("High"),
                row.get("Low"),
                row.get("Close"),
                row.get("Volume"),
                row.get("TurnoverValue"),
                row.get("AdjustmentFactor"),
                row.get("AdjustmentOpen"),
                row.get("AdjustmentHigh"),
                row.get("AdjustmentLow"),
                row.get("AdjustmentClose"),
                row.get("AdjustmentVolume"),
            )
            records.append(record)

        query = """
            INSERT OR REPLACE INTO prices (
                Date, Code, Open, High, Low, Close, Volume, TurnoverValue,
                AdjustmentFactor, AdjustmentOpen, AdjustmentHigh,
                AdjustmentLow, AdjustmentClose, AdjustmentVolume
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor = self.executemany(query, records)
        return cursor.rowcount or 0

    def delete_by_date_range(self, start_date: date, end_date: date) -> int:
        """指定期間のデータを削除"""
        query = """
            DELETE FROM prices
            WHERE Date >= ? AND Date <= ?
        """
        cursor = self.execute(query, (start_date.isoformat(), end_date.isoformat()))
        return cursor.rowcount or 0

    def get_latest_date(self) -> date | None:
        """最新のデータ日付を取得"""
        query = """
            SELECT MAX(Date) as latest_date FROM prices
        """
        cursor = self.execute(query)
        row = cursor.fetchone()

        if row and row["latest_date"]:
            return datetime.fromisoformat(row["latest_date"]).date()
        return None

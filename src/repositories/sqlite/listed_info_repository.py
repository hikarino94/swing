"""
上場企業情報のSQLiteリポジトリ実装
"""

from typing import Any

import pandas as pd

from ..interfaces import ListedInfoRepository
from .base import SqliteBaseRepository


class SqliteListedInfoRepository(SqliteBaseRepository, ListedInfoRepository):
    """上場企業情報のSQLiteリポジトリ実装"""

    def find_by_code(self, code: str) -> dict[str, Any] | None:
        """指定銘柄の企業情報を取得"""
        query = """
            SELECT * FROM listed_info
            WHERE Code = ?
        """
        cursor = self.execute(query, (code,))
        row = cursor.fetchone()

        if row:
            return dict(row)
        return None

    def find_all_active(self) -> pd.DataFrame:
        """削除フラグが立っていない全企業情報を取得"""
        query = """
            SELECT * FROM listed_info
            WHERE delete_flag = 0 OR delete_flag IS NULL
            ORDER BY Code
        """
        cursor = self.execute(query)

        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        return pd.DataFrame(data, columns=columns)

    def find_by_sector(self, sector_code: str) -> pd.DataFrame:
        """指定セクターの企業情報を取得"""
        query = """
            SELECT * FROM listed_info
            WHERE (Sector17Code = ? OR Sector33Code = ?)
            AND (delete_flag = 0 OR delete_flag IS NULL)
            ORDER BY Code
        """
        cursor = self.execute(query, (sector_code, sector_code))

        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        return pd.DataFrame(data, columns=columns)

    def save_batch(self, data: pd.DataFrame) -> int:
        """企業情報を一括保存"""
        if data.empty:
            return 0

        # DataFrameをレコードのリストに変換
        records = []
        for _, row in data.iterrows():
            record = (
                row.get("Date"),
                row.get("Code"),
                row.get("CompanyName"),
                row.get("CompanyNameEnglish"),
                row.get("Sector17Code"),
                row.get("Sector17CodeName"),
                row.get("Sector33Code"),
                row.get("Sector33CodeName"),
                row.get("ScaleCategory"),
                row.get("MarketCode"),
                row.get("MarketCodeName"),
                row.get("MarginCode"),
                row.get("MarginCodeName"),
                int(row.get("delete_flag", 0)),
            )
            records.append(record)

        query = """
            INSERT OR REPLACE INTO listed_info (
                Date, Code, CompanyName, CompanyNameEnglish,
                Sector17Code, Sector17CodeName, Sector33Code, Sector33CodeName,
                ScaleCategory, MarketCode, MarketCodeName,
                MarginCode, MarginCodeName, delete_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor = self.executemany(query, records)
        return cursor.rowcount or 0

    def mark_as_deleted(self, codes: list[str]) -> int:
        """指定銘柄に削除フラグを設定"""
        if not codes:
            return 0

        placeholders = ",".join(["?" for _ in codes])
        query = f"""
            UPDATE listed_info
            SET delete_flag = 1
            WHERE Code IN ({placeholders})
        """

        cursor = self.execute(query, tuple(codes))
        return cursor.rowcount or 0

    def update_delete_flags(self, active_codes: list[str]) -> int:
        """アクティブな銘柄以外に削除フラグを設定"""
        if not active_codes:
            # アクティブな銘柄がない場合は全て削除フラグを立てる
            query = "UPDATE listed_info SET delete_flag = 1"
            cursor = self.execute(query)
            return cursor.rowcount or 0

        # まず全ての削除フラグをリセット
        self.execute("UPDATE listed_info SET delete_flag = 0")

        # アクティブでない銘柄に削除フラグを設定
        placeholders = ",".join(["?" for _ in active_codes])
        query = f"""
            UPDATE listed_info
            SET delete_flag = 1
            WHERE Code NOT IN ({placeholders})
        """

        cursor = self.execute(query, tuple(active_codes))
        return cursor.rowcount or 0

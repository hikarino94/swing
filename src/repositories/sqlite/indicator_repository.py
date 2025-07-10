"""
テクニカル指標データのSQLiteリポジトリ実装
"""

from datetime import date
from typing import Any

import pandas as pd

from ..interfaces import IndicatorRepository
from .base import SqliteBaseRepository


class SqliteIndicatorRepository(SqliteBaseRepository, IndicatorRepository):
    """テクニカル指標データのSQLiteリポジトリ実装"""

    def find_by_code_and_date_range(
        self,
        code: str,
        start_date: date,
        end_date: date,
        indicators: list[str] | None = None,
    ) -> pd.DataFrame:
        """指定銘柄・期間のテクニカル指標を取得"""
        # 基本的なカラム
        base_columns = ["date", "code"]

        # 選択するカラムの決定
        if indicators:
            # 指定された指標に関連するカラムのみ選択
            select_columns = base_columns.copy()
            for indicator in indicators:
                if indicator == "sma":
                    select_columns.extend(["sma_5", "sma_20", "sma_60"])
                elif indicator == "ema":
                    select_columns.extend(["ema_5", "ema_20", "ema_60"])
                elif indicator == "macd":
                    select_columns.extend(["macd", "macd_signal", "macd_hist"])
                elif indicator == "rsi":
                    select_columns.append("rsi")
                elif indicator == "bb":
                    select_columns.extend(
                        ["bb_upper", "bb_middle", "bb_lower", "bb_width", "bb_percent"]
                    )
                elif indicator == "volume":
                    select_columns.extend(["volume_ratio", "volume_sma_20"])
                elif indicator == "crosses":
                    select_columns.extend(
                        [
                            "golden_cross",
                            "dead_cross",
                            "macd_golden_cross",
                            "macd_dead_cross",
                        ]
                    )
                elif indicator == "bb_signals":
                    select_columns.extend(["bb_squeeze", "bb_expansion"])

            # 重複を除去
            select_columns = list(dict.fromkeys(select_columns))
            columns_str = ", ".join(select_columns)
        else:
            columns_str = "*"

        query = f"""
            SELECT {columns_str} FROM technical_indicators
            WHERE code = ? AND date >= ? AND date <= ?
            ORDER BY date
        """

        cursor = self.execute(
            query, (code, start_date.isoformat(), end_date.isoformat())
        )

        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(data, columns=columns)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def save_batch(self, data: pd.DataFrame) -> int:
        """テクニカル指標データを一括保存"""
        if data.empty:
            return 0

        # テーブルの全カラムを取得
        cursor = self.execute("PRAGMA table_info(technical_indicators)")
        table_columns = [row[1] for row in cursor.fetchall()]

        # DataFrameに存在するカラムのみを使用
        save_columns = [col for col in table_columns if col in data.columns]

        # レコードの作成
        records = []
        for _, row in data.iterrows():
            record: list[Any] = []
            for col in save_columns:
                value = row.get(col)
                # NaNやNoneを適切に処理
                if pd.isna(value):
                    record.append(None)
                else:
                    record.append(value)
            records.append(tuple(record))

        placeholders = ",".join(["?" for _ in save_columns])
        query = f"""
            INSERT OR REPLACE INTO technical_indicators ({','.join(save_columns)})
            VALUES ({placeholders})
        """

        cursor = self.executemany(query, records)
        return cursor.rowcount or 0

    def get_available_indicators(self) -> list[str]:
        """利用可能な指標名のリストを取得"""
        # テクニカル指標テーブルのカラムから指標を推定
        cursor = self.execute("PRAGMA table_info(technical_indicators)")
        columns = [row[1] for row in cursor.fetchall()]

        # 基本的なカラムを除外
        exclude_columns = {"date", "code"}
        indicator_columns = [col for col in columns if col not in exclude_columns]

        # カラム名から指標グループを推定
        indicators = set()
        for col in indicator_columns:
            if col.startswith("sma_"):
                indicators.add("sma")
            elif col.startswith("ema_"):
                indicators.add("ema")
            elif col.startswith("macd"):
                indicators.add("macd")
            elif col == "rsi":
                indicators.add("rsi")
            elif col.startswith("bb_"):
                if col in ["bb_squeeze", "bb_expansion"]:
                    indicators.add("bb_signals")
                else:
                    indicators.add("bb")
            elif col.startswith("volume_"):
                indicators.add("volume")
            elif col in ["golden_cross", "dead_cross"]:
                indicators.add("crosses")

        return sorted(indicators)

    def delete_by_date_range(self, start_date: date, end_date: date) -> int:
        """指定期間のデータを削除"""
        query = """
            DELETE FROM technical_indicators
            WHERE date >= ? AND date <= ?
        """
        cursor = self.execute(query, (start_date.isoformat(), end_date.isoformat()))
        return cursor.rowcount or 0

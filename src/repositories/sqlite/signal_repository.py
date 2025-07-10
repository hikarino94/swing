"""
シグナルデータのSQLiteリポジトリ実装
"""

from datetime import date
from typing import Any

import pandas as pd

from ..interfaces import SignalRepository
from .base import SqliteBaseRepository


class SqliteSignalRepository(SqliteBaseRepository, SignalRepository):
    """シグナルデータのSQLiteリポジトリ実装"""

    def find_fundamental_signals(
        self, start_date: date, end_date: date, signal_types: list[str] | None = None
    ) -> pd.DataFrame:
        """ファンダメンタルシグナルを取得"""
        base_query = """
            SELECT * FROM fundamental_signals
            WHERE signal_date >= ? AND signal_date <= ?
        """
        params = [start_date.isoformat(), end_date.isoformat()]

        if signal_types:
            placeholders = ",".join(["?" for _ in signal_types])
            base_query += f" AND signal_type IN ({placeholders})"
            params.extend(signal_types)

        base_query += " ORDER BY signal_date DESC, code"

        cursor = self.execute(base_query, tuple(params))

        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(data, columns=columns)
        df["signal_date"] = pd.to_datetime(df["signal_date"])
        return df

    def find_technical_signals(
        self, start_date: date, end_date: date, indicators: list[str] | None = None
    ) -> pd.DataFrame:
        """テクニカルシグナルを取得"""
        base_query = """
            SELECT * FROM technical_indicators
            WHERE date >= ? AND date <= ?
        """
        params = [start_date.isoformat(), end_date.isoformat()]

        # 指標でフィルタリング
        if indicators:
            conditions = []
            for indicator in indicators:
                # 各指標のフラグカラムをチェック
                if indicator == "golden_cross":
                    conditions.append("golden_cross = 1")
                elif indicator == "dead_cross":
                    conditions.append("dead_cross = 1")
                elif indicator == "macd_golden_cross":
                    conditions.append("macd_golden_cross = 1")
                elif indicator == "macd_dead_cross":
                    conditions.append("macd_dead_cross = 1")
                elif indicator == "rsi_oversold":
                    conditions.append("rsi < 30")
                elif indicator == "rsi_overbought":
                    conditions.append("rsi > 70")
                elif indicator == "bb_squeeze":
                    conditions.append("bb_squeeze = 1")
                elif indicator == "bb_expansion":
                    conditions.append("bb_expansion = 1")

            if conditions:
                base_query += f" AND ({' OR '.join(conditions)})"

        base_query += " ORDER BY date DESC, code"

        cursor = self.execute(base_query, tuple(params))

        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(data, columns=columns)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def save_fundamental_signals(self, signals: pd.DataFrame) -> int:
        """ファンダメンタルシグナルを保存"""
        if signals.empty:
            return 0

        # 必要なカラムのみを選択
        required_columns = [
            "signal_date",
            "code",
            "company_name",
            "signal_type",
            "reason",
            "details",
        ]

        # DataFrameから必要なカラムのみを抽出
        save_columns = [col for col in required_columns if col in signals.columns]

        # レコードの作成
        records = []
        for _, row in signals.iterrows():
            record = tuple(row.get(col) for col in save_columns)
            records.append(record)

        placeholders = ",".join(["?" for _ in save_columns])
        query = f"""
            INSERT OR REPLACE INTO fundamental_signals ({','.join(save_columns)})
            VALUES ({placeholders})
        """

        cursor = self.executemany(query, records)
        return cursor.rowcount or 0

    def save_technical_signals(self, signals: pd.DataFrame) -> int:
        """テクニカルシグナルを保存"""
        if signals.empty:
            return 0

        # technical_indicatorsテーブルの全カラムを取得
        cursor = self.execute("PRAGMA table_info(technical_indicators)")
        table_columns = [row[1] for row in cursor.fetchall()]

        # DataFrameに存在するカラムのみを使用
        save_columns = [col for col in table_columns if col in signals.columns]

        # レコードの作成
        records = []
        for _, row in signals.iterrows():
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

    def delete_old_signals(self, cutoff_date: date, signal_type: str) -> int:
        """古いシグナルを削除"""
        if signal_type == "fundamental":
            query = """
                DELETE FROM fundamental_signals
                WHERE signal_date < ?
            """
        elif signal_type == "technical":
            query = """
                DELETE FROM technical_indicators
                WHERE date < ?
            """
        else:
            raise ValueError(f"Unknown signal type: {signal_type}")

        cursor = self.execute(query, (cutoff_date.isoformat(),))
        return cursor.rowcount or 0

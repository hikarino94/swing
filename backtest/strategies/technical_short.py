"""テクニカル指標ショート戦略"""

import sqlite3

import pandas as pd

from screening.thresholds import SHORT_SIGNAL_COUNT_MIN

from .base_strategy import BaseStrategy


class TechnicalShortStrategy(BaseStrategy):
    """テクニカル指標に基づくショート戦略"""

    def get_entry_signals(self, conn: sqlite3.Connection, as_of: str) -> pd.DataFrame:
        """ショートエントリーシグナルを取得"""
        return pd.read_sql(
            "SELECT code FROM technical_indicators "
            "WHERE signal_date=? "
            "AND signals_short_count>=? "
            "AND signals_short_first=1 "
            "AND signals_oversold=0",
            conn,
            params=(as_of, SHORT_SIGNAL_COUNT_MIN),
        )

    def calculate_profit(
        self, entry_price: float, exit_price: float
    ) -> tuple[float, float]:
        """ショートポジションの利益計算"""
        profit_pct = (entry_price - exit_price) / entry_price
        profit_jpy = profit_pct * self.capital
        return profit_pct, profit_jpy

    def get_stop_price(self, entry_price: float) -> float:
        """ショートポジションの損切り価格（上方向）"""
        return entry_price * (1 + self.stop_loss_pct)

    def get_side_label(self) -> str:
        """戦略タイプのラベル"""
        return "short"

"""戦略基底クラス"""

import logging
import sqlite3
from abc import ABC, abstractmethod

import pandas as pd

logger = logging.getLogger("backtest.strategies.base")


class BaseStrategy(ABC):
    """バックテスト戦略の基底クラス"""

    def __init__(
        self,
        capital: int = 1_000_000,
        hold_days: int = 60,
        stop_loss_pct: float = 0.05,
        min_price: float = 300,
    ):
        self.capital = capital
        self.hold_days = hold_days
        self.stop_loss_pct = stop_loss_pct
        self.min_price = min_price

    @abstractmethod
    def get_entry_signals(self, conn: sqlite3.Connection, as_of: str) -> pd.DataFrame:
        """エントリーシグナルを取得（サブクラスで実装）"""
        pass

    @abstractmethod
    def calculate_profit(
        self, entry_price: float, exit_price: float
    ) -> tuple[float, float]:
        """利益計算（サブクラスで実装）

        Returns:
            (profit_pct, profit_jpy)
        """
        pass

    @abstractmethod
    def get_stop_price(self, entry_price: float) -> float:
        """損切り価格を計算（サブクラスで実装）"""
        pass

    @abstractmethod
    def get_side_label(self) -> str:
        """戦略タイプのラベルを取得（サブクラスで実装）"""
        pass

    def calculate_position_size(self, price: float) -> int:
        """ポジションサイズを計算"""
        return int(self.capital / price)

    def is_stopped_out(self, price: float, stop_price: float, side: str) -> bool:
        """損切り判定"""
        if side == "long":
            return price <= stop_price
        else:  # short
            return price >= stop_price

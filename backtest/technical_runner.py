"""テクニカル戦略バックテスト実行エンジン"""

import datetime as dt
import logging
import sqlite3

import pandas as pd

from .strategies.base_strategy import BaseStrategy

logger = logging.getLogger("backtest.technical_runner")


class TechnicalBacktestRunner:
    """テクニカル戦略のバックテスト実行クラス"""

    def __init__(self, conn: sqlite3.Connection, strategy: BaseStrategy):
        self.conn = conn
        self.strategy = strategy

    def run_single_day(self, as_of: str) -> pd.DataFrame:
        """特定日のバックテストを実行"""
        # エントリーシグナルを取得
        sig_df = self.strategy.get_entry_signals(self.conn, as_of)

        logger.info(
            f"{as_of}: Found {len(sig_df)} {self.strategy.get_side_label()} entry signals."
        )

        if sig_df.empty:
            return pd.DataFrame()

        trades = []
        for _, row in sig_df.iterrows():
            trade = self._execute_trade(row["code"], as_of)
            if trade:
                trades.append(trade)

        return pd.DataFrame(trades)

    def _execute_trade(self, code: str, entry_date: str) -> dict | None:
        """個別銘柄の取引を実行"""
        # エントリー価格を取得
        next_data = pd.read_sql(
            "SELECT date, adj_close FROM prices "
            "WHERE code=? AND date>? "
            "ORDER BY date LIMIT 1",
            self.conn,
            params=(code, entry_date),
        )

        if next_data.empty:
            logger.debug(f"  {code}: No price after {entry_date}")
            return None

        next_date = next_data.iloc[0]["date"]
        entry_price = next_data.iloc[0]["adj_close"]

        # 最低価格チェック
        if entry_price < self.strategy.min_price:
            logger.debug(
                f"  {code}: Entry price {entry_price} < {self.strategy.min_price}"
            )
            return None

        # ポジションサイズ計算
        num_shares = self.strategy.calculate_position_size(entry_price)
        if num_shares < 1:
            logger.debug(f"  {code}: Cannot afford at {entry_price}")
            return None

        # 損切り価格
        stop_price = self.strategy.get_stop_price(entry_price)

        # エグジット日とエグジット価格を決定
        exit_info = self._find_exit(code, next_date, stop_price)

        if not exit_info:
            return None

        exit_date = exit_info["date"]
        exit_price = exit_info["price"]
        exit_reason = exit_info["reason"]

        # 利益計算
        profit_pct, profit_jpy = self.strategy.calculate_profit(entry_price, exit_price)

        return {
            "code": code,
            "side": self.strategy.get_side_label(),
            "signal_date": entry_date,
            "entry_date": next_date,
            "exit_date": exit_date,
            "hold_days": (
                dt.datetime.strptime(exit_date, "%Y-%m-%d")
                - dt.datetime.strptime(next_date, "%Y-%m-%d")
            ).days,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "profit_pct": profit_pct,
            "profit_jpy": profit_jpy,
            "num_shares": num_shares,
            "stop_price": stop_price,
            "exit_reason": exit_reason,
        }

    def _find_exit(self, code: str, entry_date: str, stop_price: float) -> dict | None:
        """エグジット条件を探す"""
        # 保有期間中の価格データを取得
        exit_date = (
            dt.datetime.strptime(entry_date, "%Y-%m-%d")
            + dt.timedelta(days=self.strategy.hold_days)
        ).strftime("%Y-%m-%d")

        price_data = pd.read_sql(
            "SELECT date, adj_close FROM prices "
            "WHERE code=? AND date>? AND date<=? "
            "ORDER BY date",
            self.conn,
            params=(code, entry_date, exit_date),
        )

        if price_data.empty:
            return None

        # 損切り判定
        for _, day in price_data.iterrows():
            if self.strategy.is_stopped_out(
                day["adj_close"], stop_price, self.strategy.get_side_label()
            ):
                return {
                    "date": day["date"],
                    "price": day["adj_close"],
                    "reason": "stop_loss",
                }

        # 保有期限到達
        last_row = price_data.iloc[-1]
        return {
            "date": last_row["date"],
            "price": last_row["adj_close"],
            "reason": "hold_period",
        }

    def run_period(self, start_date: str, end_date: str) -> pd.DataFrame:
        """期間指定でバックテストを実行"""
        # 期間内の営業日を取得
        trading_days = pd.read_sql(
            "SELECT DISTINCT signal_date FROM technical_indicators "
            "WHERE signal_date>=? AND signal_date<=? "
            "ORDER BY signal_date",
            self.conn,
            params=(start_date, end_date),
        )

        if trading_days.empty:
            logger.warning(f"No trading days found between {start_date} and {end_date}")
            return pd.DataFrame()

        all_trades = []
        for _, row in trading_days.iterrows():
            as_of = row["signal_date"]
            day_trades = self.run_single_day(as_of)
            if not day_trades.empty:
                all_trades.append(day_trades)

        if all_trades:
            return pd.concat(all_trades, ignore_index=True)
        else:
            return pd.DataFrame()

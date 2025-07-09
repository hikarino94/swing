"""バックテストの基底クラス"""

import json
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd

from src.utils.data_utils import DataProcessor
from src.utils.db_utils import get_db_connection
from src.utils.file_utils import get_timestamped_output_path
from src.utils.logging_config import get_logger

logger = get_logger("backtest.base")


class BacktestBase(ABC):
    """バックテストの基底クラス"""

    def __init__(
        self,
        capital: int = 1_000_000,
        hold_days: int = 60,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        """
        Args:
            capital: 初期資金
            hold_days: 保有日数
            start_date: バックテスト開始日
            end_date: バックテスト終了日
        """
        self.capital = capital
        self.hold_days = hold_days
        self.start_date = start_date
        self.end_date = end_date
        self.data_processor = DataProcessor()

    @abstractmethod
    def get_signals(self, conn: sqlite3.Connection) -> pd.DataFrame:
        """シグナルを取得（サブクラスで実装）"""
        pass

    @abstractmethod
    def get_backtest_name(self) -> str:
        """バックテスト名を取得（サブクラスで実装）"""
        pass

    def run(self, show_results: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
        """バックテストを実行

        Args:
            show_results: 結果を表示するかどうか

        Returns:
            (trades_df, summary_df) のタプル
        """
        logger.info(f"{self.get_backtest_name()} バックテスト開始")

        with get_db_connection() as conn:
            # シグナルを取得
            signals_df = self.get_signals(conn)

            if signals_df.empty:
                logger.warning("シグナルが見つかりません")
                return pd.DataFrame(), pd.DataFrame()

            logger.info(f"シグナル数: {len(signals_df)}")

            # 価格データを取得
            prices_df = self._get_price_data(conn, signals_df)

            # 取引を計算
            trades_df = self.calculate_trades(signals_df, prices_df)

            # サマリーを作成
            summary_df = self.create_summary(trades_df)

            # 結果を保存
            self.save_results(trades_df, summary_df)

            if show_results:
                self.display_results(trades_df, summary_df)

            return trades_df, summary_df

    def calculate_trades(
        self, signals_df: pd.DataFrame, prices_df: pd.DataFrame
    ) -> pd.DataFrame:
        """取引を計算"""
        trades = []

        for _, signal in signals_df.iterrows():
            trade = self._calculate_single_trade(signal, prices_df)
            if trade:
                trades.append(trade)

        if not trades:
            return pd.DataFrame()

        trades_df = pd.DataFrame(trades)

        # 集計情報を追加
        trades_df = self._add_aggregate_info(trades_df)

        return trades_df

    def create_summary(self, trades_df: pd.DataFrame) -> pd.DataFrame:
        """サマリーを作成"""
        if trades_df.empty:
            return pd.DataFrame(
                {
                    "metric": [
                        "trades",
                        "win_rate",
                        "total_profit",
                        "avg_return",
                        "sharpe_ratio",
                    ],
                    "value": [0, 0, 0, 0, 0],
                }
            )

        # パフォーマンス指標を計算
        metrics = self.data_processor.calculate_performance_metrics(
            trades_df["ret_pct"]
        )

        summary_data = {
            "metric": [
                "trades",
                "win_rate",
                "total_profit",
                "avg_return",
                "sharpe_ratio",
                "max_drawdown",
            ],
            "value": [
                len(trades_df),
                metrics["win_rate"] * 100,
                trades_df["profit_jpy"].sum(),
                metrics["mean_return"] * 100,
                metrics["sharpe_ratio"],
                metrics["max_drawdown"] * 100,
            ],
        }

        return pd.DataFrame(summary_data)

    def save_results(self, trades_df: pd.DataFrame, summary_df: pd.DataFrame) -> None:
        """結果を保存"""
        # タイムスタンプ付きファイルパス
        base_name = self.get_backtest_name().replace(" ", "_")

        # JSON形式で保存
        json_path = get_timestamped_output_path("backtest", base_name, "json")
        result_data = {
            "metadata": {
                "name": self.get_backtest_name(),
                "capital": self.capital,
                "hold_days": self.hold_days,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "timestamp": datetime.now().isoformat(),
            },
            "summary": summary_df.to_dict("records"),
            "trades": trades_df.to_dict("records"),
        }

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result_data, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"結果を保存: {json_path}")

        # Excel形式で保存
        xlsx_path = get_timestamped_output_path("backtest", base_name, "xlsx")
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            summary_df.to_excel(writer, sheet_name="サマリー", index=False)
            if not trades_df.empty:
                trades_df.to_excel(writer, sheet_name="取引履歴", index=False)

        logger.info(f"Excel保存: {xlsx_path}")

    def display_results(
        self, trades_df: pd.DataFrame, summary_df: pd.DataFrame
    ) -> None:
        """結果を表示"""
        print("\n=== バックテスト結果 ===")
        print(f"バックテスト名: {self.get_backtest_name()}")
        print(f"期間: {self.start_date} ～ {self.end_date}")
        print(f"初期資金: {self.capital:,}円")
        print(f"保有日数: {self.hold_days}日")

        print("\n--- サマリー ---")
        for _, row in summary_df.iterrows():
            metric = row["metric"]
            value = row["value"]

            if metric in ["win_rate", "avg_return", "sharpe_ratio", "max_drawdown"]:
                print(f"{metric}: {value:.2f}")
            elif metric == "total_profit":
                print(f"{metric}: {value:,.0f}円")
            else:
                print(f"{metric}: {value}")

    def _get_price_data(
        self, conn: sqlite3.Connection, signals_df: pd.DataFrame
    ) -> pd.DataFrame:
        """価格データを取得"""
        # 必要な銘柄コードを取得
        codes = signals_df["code"].unique().tolist()

        # 日付範囲を決定
        min_date = signals_df["signal_date"].min()
        max_date = pd.to_datetime(signals_df["signal_date"].max()) + pd.Timedelta(
            days=self.hold_days + 10
        )

        # 価格データを取得
        query = """
            SELECT code, date, adj_close as close
            FROM prices
            WHERE code IN ({})
            AND date >= ?
            AND date <= ?
            ORDER BY code, date
        """.format(
            ",".join("?" * len(codes))
        )

        params = codes + [min_date, max_date.strftime("%Y-%m-%d")]

        return pd.read_sql_query(query, conn, params=params)

    def _calculate_single_trade(
        self, signal: pd.Series, prices_df: pd.DataFrame
    ) -> dict[str, Any] | None:
        """個別の取引を計算"""
        code = signal["code"]
        signal_date = pd.to_datetime(signal["signal_date"])

        # 該当銘柄の価格データ
        code_prices = prices_df[prices_df["code"] == code].copy()
        code_prices["date"] = pd.to_datetime(code_prices["date"])
        code_prices = code_prices.set_index("date").sort_index()

        # エントリー価格を取得
        entry_prices = code_prices[code_prices.index > signal_date]
        if entry_prices.empty:
            return None

        entry_date = entry_prices.index[0]
        entry_price = entry_prices.iloc[0]["close"]

        # エグジット日を計算
        exit_date = entry_date + pd.Timedelta(days=self.hold_days)

        # エグジット価格を取得
        exit_prices = code_prices[code_prices.index >= exit_date]
        if exit_prices.empty:
            # 最後の価格を使用
            exit_date = code_prices.index[-1]
            exit_price = code_prices.iloc[-1]["close"]
        else:
            exit_date = exit_prices.index[0]
            exit_price = exit_prices.iloc[0]["close"]

        # リターンを計算
        ret_pct = (exit_price - entry_price) / entry_price

        # 取引結果
        return {
            "code": code,
            "signal_date": signal_date.strftime("%Y-%m-%d"),
            "entry_date": entry_date.strftime("%Y-%m-%d"),
            "exit_date": exit_date.strftime("%Y-%m-%d"),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "ret_pct": ret_pct,
            "profit_jpy": ret_pct * self.capital,
            "hold_days": (exit_date - entry_date).days,
        }

    def _add_aggregate_info(self, trades_df: pd.DataFrame) -> pd.DataFrame:
        """集計情報を追加"""
        if trades_df.empty:
            return trades_df

        # 累積リターンを計算
        trades_df["cum_ret"] = (1 + trades_df["ret_pct"]).cumprod() - 1

        # 累積利益を計算
        trades_df["cum_profit"] = trades_df["profit_jpy"].cumsum()

        return trades_df

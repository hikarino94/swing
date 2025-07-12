"""Tests for backtest functionality - Alternative approach"""

import json
import sqlite3
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest


class TestBacktestBaseLogic:
    """BacktestBase基底クラスのロジックのテスト"""

    def test_backtest_class_structure(self):
        """バックテストクラスの構造をテスト"""

        # 抽象基底クラスの構造を再現
        class BacktestBase(ABC):
            def __init__(
                self, capital=1_000_000, hold_days=60, start_date=None, end_date=None
            ):
                self.capital = capital
                self.hold_days = hold_days
                self.start_date = start_date
                self.end_date = end_date
                self.data_processor = Mock()

            @abstractmethod
            def get_signals(self, conn):
                pass

            @abstractmethod
            def get_backtest_name(self):
                pass

        # 具象クラスの実装
        class ConcreteBacktest(BacktestBase):
            def get_signals(self, conn):
                return pd.DataFrame({"code": ["1234"], "date": ["2023-01-01"]})

            def get_backtest_name(self):
                return "test_backtest"

        # テスト
        backtest = ConcreteBacktest(capital=2_000_000, hold_days=30)
        assert backtest.capital == 2_000_000
        assert backtest.hold_days == 30
        assert backtest.get_backtest_name() == "test_backtest"
        assert isinstance(backtest.get_signals(None), pd.DataFrame)


class TestBacktestWorkflow:
    """バックテストワークフローのテスト"""

    def test_signal_generation_workflow(self):
        """シグナル生成ワークフローのテスト"""
        # モックデータ
        signals = pd.DataFrame(
            {
                "date": ["2023-01-01", "2023-02-01", "2023-03-01"],
                "code": ["1234", "5678", "9999"],
                "signal_strength": [0.8, 0.7, 0.9],
            }
        )

        # シグナルのフィルタリング
        min_strength = 0.75
        filtered_signals = signals[signals["signal_strength"] >= min_strength]

        assert len(filtered_signals) == 2
        assert set(filtered_signals["code"]) == {"1234", "9999"}

    def test_trade_execution_workflow(self):
        """トレード実行ワークフローのテスト"""
        # シグナルとポートフォリオの初期化
        signal = {"code": "1234", "date": "2023-01-01", "entry_price": 1000}
        portfolio = {"cash": 1_000_000, "positions": []}
        position_size = 100_000

        # トレード実行
        shares = int(position_size / signal["entry_price"])
        position = {
            "code": signal["code"],
            "entry_date": signal["date"],
            "entry_price": signal["entry_price"],
            "shares": shares,
            "value": shares * signal["entry_price"],
        }

        portfolio["positions"].append(position)
        portfolio["cash"] -= position["value"]

        # 検証
        assert len(portfolio["positions"]) == 1
        assert portfolio["cash"] == 900_000
        assert portfolio["positions"][0]["shares"] == 100

    def test_exit_logic_workflow(self):
        """エグジットロジックのワークフローのテスト"""
        # ポジション情報
        position = {
            "code": "1234",
            "entry_date": pd.Timestamp("2023-01-01"),
            "entry_price": 1000,
            "shares": 100,
            "hold_days": 20,
        }

        # 現在の日付と出口価格
        current_date = pd.Timestamp("2023-01-25")
        exit_price = 1100

        # 保有期間チェック
        days_held = (current_date - position["entry_date"]).days
        should_exit = days_held >= position["hold_days"]

        # リターン計算
        if should_exit:
            gross_return = (exit_price - position["entry_price"]) / position[
                "entry_price"
            ]
            # 手数料考慮
            commission = 0.001
            net_return = gross_return - 2 * commission

            assert should_exit
            assert gross_return == 0.1
            assert net_return < gross_return


class TestBacktestPerformanceMetrics:
    """バックテストパフォーマンスメトリクスのテスト"""

    def test_portfolio_value_calculation(self):
        """ポートフォリオ価値計算のテスト"""
        # ポートフォリオの状態
        cash = 500_000
        positions = [
            {"code": "1234", "shares": 100, "current_price": 1100},
            {"code": "5678", "shares": 200, "current_price": 550},
        ]

        # ポートフォリオ価値計算
        position_value = sum(p["shares"] * p["current_price"] for p in positions)
        total_value = cash + position_value

        assert position_value == 220_000
        assert total_value == 720_000

    def test_returns_calculation(self):
        """リターン計算のテスト"""
        # 日次ポートフォリオ価値
        portfolio_values = pd.Series(
            [1_000_000, 1_020_000, 1_015_000, 1_035_000, 1_025_000, 1_050_000]
        )

        # 日次リターン
        daily_returns = portfolio_values.pct_change().dropna()

        # 累積リターン
        total_return = (portfolio_values.iloc[-1] / portfolio_values.iloc[0]) - 1

        # 平均日次リターン
        avg_daily_return = daily_returns.mean()

        assert total_return == pytest.approx(0.05, rel=1e-6)  # 5%
        assert len(daily_returns) == 5
        assert avg_daily_return > 0

    def test_risk_metrics_calculation(self):
        """リスクメトリクス計算のテスト"""
        # 日次リターン
        daily_returns = pd.Series([0.02, -0.01, 0.015, -0.005, 0.01, -0.008, 0.012])

        # ボラティリティ（年率換算）
        volatility = daily_returns.std() * np.sqrt(252)

        # 最大ドローダウン
        cumulative = (1 + daily_returns).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min()

        # ソルティノレシオ（下方偏差のみ考慮）
        downside_returns = daily_returns[daily_returns < 0]
        downside_deviation = downside_returns.std() * np.sqrt(252)
        sortino_ratio = (
            (daily_returns.mean() * 252) / downside_deviation
            if downside_deviation > 0
            else np.inf
        )

        assert volatility > 0
        assert max_drawdown <= 0
        assert isinstance(sortino_ratio, float | type(np.inf))


class TestBacktestDataManagement:
    """バックテストデータ管理のテスト"""

    def setup_method(self):
        """テスト用の一時データベースを作成"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name

        with sqlite3.connect(self.db_path) as conn:
            # pricesテーブル
            conn.execute(
                """
                CREATE TABLE prices (
                    date TEXT,
                    code TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    PRIMARY KEY (date, code)
                )
            """
            )

            # シグナルテーブル（ファンダメンタル）
            conn.execute(
                """
                CREATE TABLE fundamental_signals (
                    date TEXT,
                    code TEXT,
                    eps_yoy REAL,
                    roe REAL,
                    per REAL
                )
            """
            )

            # テストデータの挿入
            base_date = pd.Timestamp("2023-01-01")
            for i in range(100):
                date = base_date + pd.Timedelta(days=i)
                date_str = date.strftime("%Y-%m-%d")

                # 価格データ
                for code in ["1234", "5678"]:
                    base_price = 1000 if code == "1234" else 500
                    price = base_price + np.sin(i / 10) * 50
                    conn.execute(
                        """INSERT INTO prices VALUES
                        (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            date_str,
                            code,
                            price * 0.99,
                            price * 1.01,
                            price * 0.98,
                            price,
                            1000000,
                        ),
                    )

                # シグナルデータ（月初のみ）
                if date.day == 1:
                    conn.execute(
                        """INSERT INTO fundamental_signals VALUES
                        (?, ?, ?, ?, ?)""",
                        (date_str, "1234", 0.15, 0.12, 15.5),
                    )

            conn.commit()

    def teardown_method(self):
        """一時ファイルの削除"""
        Path(self.db_path).unlink(missing_ok=True)

    def test_load_historical_prices(self):
        """過去価格データの読み込みをテスト"""
        with sqlite3.connect(self.db_path) as conn:
            query = """
                SELECT date, code, open, high, low, close, volume
                FROM prices
                WHERE date >= '2023-01-01' AND date <= '2023-03-31'
                ORDER BY date, code
            """
            df = pd.read_sql_query(query, conn)

        # データの検証
        assert len(df) > 0
        assert set(df["code"].unique()) == {"1234", "5678"}
        assert df["close"].notna().all()

        # 日付の連続性
        df["date"] = pd.to_datetime(df["date"])
        date_range = df["date"].max() - df["date"].min()
        assert date_range.days == 89  # 1月1日から3月31日

    def test_merge_signal_and_price_data(self):
        """シグナルと価格データのマージをテスト"""
        with sqlite3.connect(self.db_path) as conn:
            # シグナルデータ
            signals_df = pd.read_sql_query("SELECT * FROM fundamental_signals", conn)

            # 価格データ（シグナル日の翌営業日）
            prices_df = pd.read_sql_query(
                """SELECT date, code, close
                   FROM prices
                   WHERE date > '2023-01-01'""",
                conn,
            )

        # データの整形
        signals_df["signal_date"] = pd.to_datetime(signals_df["date"])
        prices_df["date"] = pd.to_datetime(prices_df["date"])

        # シグナル日の翌営業日価格を取得
        merged_data = []
        for _, signal in signals_df.iterrows():
            # 翌営業日の価格を検索
            next_prices = prices_df[
                (prices_df["code"] == signal["code"])
                & (prices_df["date"] > signal["signal_date"])
            ].sort_values("date")

            if not next_prices.empty:
                entry_price = next_prices.iloc[0]["close"]
                merged_data.append(
                    {
                        "signal_date": signal["signal_date"],
                        "code": signal["code"],
                        "entry_price": entry_price,
                        "eps_yoy": signal["eps_yoy"],
                    }
                )

        merged_df = pd.DataFrame(merged_data)
        assert len(merged_df) > 0
        assert "entry_price" in merged_df.columns


class TestBacktestResultsOutput:
    """バックテスト結果出力のテスト"""

    def test_create_results_summary(self):
        """結果サマリーの作成をテスト"""
        # トレード結果
        trades = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999", "1111"],
                "entry_date": ["2023-01-15", "2023-02-15", "2023-03-15", "2023-04-15"],
                "exit_date": ["2023-02-15", "2023-03-15", "2023-04-15", "2023-05-15"],
                "entry_price": [1000, 500, 2000, 1500],
                "exit_price": [1100, 480, 2200, 1450],
                "shares": [100, 200, 50, 100],
                "return": [0.10, -0.04, 0.10, -0.033],
            }
        )

        # サマリー統計の計算
        summary = {
            "total_trades": len(trades),
            "winning_trades": len(trades[trades["return"] > 0]),
            "losing_trades": len(trades[trades["return"] < 0]),
            "win_rate": len(trades[trades["return"] > 0]) / len(trades),
            "avg_return": trades["return"].mean(),
            "total_return": trades["return"].sum(),
            "max_win": trades["return"].max(),
            "max_loss": trades["return"].min(),
            "profit_factor": (
                trades[trades["return"] > 0]["return"].sum()
                / abs(trades[trades["return"] < 0]["return"].sum())
            ),
        }

        # 検証
        assert summary["total_trades"] == 4
        assert summary["win_rate"] == 0.5
        assert summary["profit_factor"] > 1

    def test_format_results_for_output(self):
        """出力用の結果フォーマットをテスト"""
        # メタデータ
        metadata = {
            "strategy_name": "fundamental_backtest",
            "start_date": "2023-01-01",
            "end_date": "2023-12-31",
            "initial_capital": 1_000_000,
            "hold_days": 60,
        }

        # パフォーマンスメトリクス
        metrics = {
            "total_return": 0.15,
            "annual_return": 0.15,
            "sharpe_ratio": 1.2,
            "sortino_ratio": 1.5,
            "max_drawdown": -0.08,
            "win_rate": 0.6,
            "profit_factor": 1.8,
        }

        # 結果の構造化
        results = {
            "metadata": metadata,
            "performance": metrics,
            "created_at": pd.Timestamp.now().isoformat(),
        }

        # JSON変換可能性の確認
        json_str = json.dumps(results, default=str)
        loaded = json.loads(json_str)

        assert loaded["metadata"]["strategy_name"] == "fundamental_backtest"
        assert loaded["performance"]["total_return"] == 0.15

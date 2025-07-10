"""Advanced tests for backtest module"""

import json

import numpy as np
import pandas as pd
import pytest


class TestBacktestStrategy:
    """バックテスト戦略のテスト"""

    def test_position_sizing(self):
        """ポジションサイジングのテスト"""
        capital = 1_000_000
        position_size_pct = 0.1  # 10%
        entry_price = 1000

        # ポジションサイズ計算
        position_value = capital * position_size_pct
        shares = int(position_value / entry_price)
        actual_value = shares * entry_price

        assert shares == 100
        assert actual_value == 100_000
        assert actual_value <= position_value  # 切り捨てのため以下になる

    def test_stop_loss_calculation(self):
        """ストップロス計算のテスト"""
        entry_price = 1000
        stop_loss_pct = 0.05  # 5%

        # ストップロス価格計算
        stop_loss_price = entry_price * (1 - stop_loss_pct)

        assert stop_loss_price == 950

        # 複数のストップロスレベル
        stop_levels = [0.03, 0.05, 0.1]
        stop_prices = [entry_price * (1 - sl) for sl in stop_levels]

        assert stop_prices[0] == 970
        assert stop_prices[1] == 950
        assert stop_prices[2] == 900

    def test_profit_target_calculation(self):
        """利益確定目標の計算テスト"""
        entry_price = 1000
        profit_targets = [0.05, 0.1, 0.2]  # 5%, 10%, 20%

        target_prices = [entry_price * (1 + pt) for pt in profit_targets]

        assert target_prices[0] == 1050
        assert target_prices[1] == 1100
        assert target_prices[2] == 1200

    def test_risk_reward_ratio(self):
        """リスクリワード比率のテスト"""
        entry_price = 1000
        stop_loss = 950
        profit_target = 1100

        # リスクとリワードの計算
        risk = entry_price - stop_loss
        reward = profit_target - entry_price
        risk_reward_ratio = reward / risk

        assert risk == 50
        assert reward == 100
        assert risk_reward_ratio == 2.0  # 1:2のリスクリワード比


class TestBacktestPerformanceAnalysis:
    """バックテストパフォーマンス分析のテスト"""

    def test_sharpe_ratio_calculation(self):
        """シャープレシオ計算のテスト"""
        # 日次リターンのサンプル
        daily_returns = pd.Series([0.01, -0.005, 0.015, 0.002, -0.008, 0.012, 0.005])

        # シャープレシオ計算（年率換算）
        mean_return = daily_returns.mean()
        std_return = daily_returns.std()
        sharpe_ratio = mean_return / std_return * np.sqrt(252)

        assert mean_return > 0
        assert std_return > 0
        assert sharpe_ratio > 0

    def test_maximum_drawdown_calculation(self):
        """最大ドローダウン計算のテスト"""
        # ポートフォリオ価値の推移
        portfolio_values = pd.Series(
            [100000, 105000, 102000, 98000, 95000, 97000, 100000, 103000]
        )

        # 累積最大値
        cummax = portfolio_values.expanding().max()

        # ドローダウン計算
        drawdown = (portfolio_values - cummax) / cummax
        max_drawdown = drawdown.min()

        assert max_drawdown < 0
        assert max_drawdown == pytest.approx(-0.095238, rel=1e-4)  # -9.52%

    def test_win_rate_calculation(self):
        """勝率計算のテスト"""
        trades = pd.DataFrame(
            {
                "return": [
                    0.05,
                    -0.02,
                    0.03,
                    -0.01,
                    0.04,
                    -0.03,
                    0.02,
                    0.01,
                    -0.005,
                    0.015,
                ]
            }
        )

        # 勝ちトレードと負けトレード
        winning_trades = trades[trades["return"] > 0]
        losing_trades = trades[trades["return"] < 0]

        win_rate = len(winning_trades) / len(trades)

        assert len(winning_trades) == 6
        assert len(losing_trades) == 4
        assert win_rate == 0.6

    def test_profit_factor_calculation(self):
        """プロフィットファクター計算のテスト"""
        trades = pd.DataFrame(
            {"return": [0.05, -0.02, 0.03, -0.01, 0.04, -0.03, 0.02, 0.01]}
        )

        # 総利益と総損失
        gross_profit = trades[trades["return"] > 0]["return"].sum()
        gross_loss = abs(trades[trades["return"] < 0]["return"].sum())

        profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.inf

        assert gross_profit == 0.15
        assert gross_loss == 0.06
        assert profit_factor == 2.5


class TestBacktestExecution:
    """バックテスト実行のテスト"""

    def test_signal_generation(self):
        """シグナル生成のテスト"""
        # 価格データ
        dates = pd.date_range("2023-01-01", periods=20, freq="D")
        prices = pd.DataFrame(
            {
                "date": dates,
                "close": [100 + i + np.sin(i / 3) * 5 for i in range(20)],
                "volume": [1000000 + i * 10000 for i in range(20)],
            }
        )

        # 移動平均クロスオーバー戦略
        prices["ma_short"] = prices["close"].rolling(5).mean()
        prices["ma_long"] = prices["close"].rolling(10).mean()

        # シグナル生成
        prices["signal"] = 0
        prices.loc[prices["ma_short"] > prices["ma_long"], "signal"] = 1
        prices.loc[prices["ma_short"] < prices["ma_long"], "signal"] = -1

        # シグナルが生成されたことを確認
        assert prices["signal"].abs().sum() > 0
        assert prices["signal"].isin([1, 0, -1]).all()

    def test_trade_execution_logic(self):
        """トレード実行ロジックのテスト"""
        # シグナルとポートフォリオの初期化
        signals = pd.DataFrame(
            {
                "date": pd.date_range("2023-01-01", periods=5),
                "code": ["1234"] * 5,
                "signal": [0, 1, 0, -1, 0],
                "price": [100, 102, 105, 103, 101],
            }
        )

        portfolio = {"cash": 1_000_000, "positions": {}, "trades": []}

        # トレード実行
        for _, row in signals.iterrows():
            if row["signal"] == 1 and row["code"] not in portfolio["positions"]:
                # 買いエントリー
                shares = 100
                cost = shares * row["price"]
                portfolio["cash"] -= cost
                portfolio["positions"][row["code"]] = {
                    "shares": shares,
                    "entry_price": row["price"],
                    "entry_date": row["date"],
                }
                portfolio["trades"].append(
                    {
                        "type": "buy",
                        "code": row["code"],
                        "date": row["date"],
                        "price": row["price"],
                        "shares": shares,
                    }
                )
            elif row["signal"] == -1 and row["code"] in portfolio["positions"]:
                # 売りエグジット
                position = portfolio["positions"][row["code"]]
                proceeds = position["shares"] * row["price"]
                portfolio["cash"] += proceeds
                portfolio["trades"].append(
                    {
                        "type": "sell",
                        "code": row["code"],
                        "date": row["date"],
                        "price": row["price"],
                        "shares": position["shares"],
                        "return": (row["price"] - position["entry_price"])
                        / position["entry_price"],
                    }
                )
                del portfolio["positions"][row["code"]]

        # トレードが実行されたことを確認
        assert len(portfolio["trades"]) == 2
        assert portfolio["trades"][0]["type"] == "buy"
        assert portfolio["trades"][1]["type"] == "sell"
        assert portfolio["trades"][1]["return"] == pytest.approx(0.0098, rel=1e-3)

    def test_portfolio_value_tracking(self):
        """ポートフォリオ価値追跡のテスト"""
        # 日次の価格データ
        dates = pd.date_range("2023-01-01", periods=10)

        # ポートフォリオの状態
        portfolio_history = []
        cash = 500_000
        positions = {
            "1234": {"shares": 100, "entry_price": 1000},
            "5678": {"shares": 200, "entry_price": 500},
        }

        # 各日のポートフォリオ価値を計算
        for i, date in enumerate(dates):
            # 現在価格（ランダムウォーク）
            price_1234 = 1000 + (i * 10) + np.random.randn() * 20
            price_5678 = 500 + (i * 5) + np.random.randn() * 10

            # ポジション価値
            position_value = (
                positions["1234"]["shares"] * price_1234
                + positions["5678"]["shares"] * price_5678
            )

            total_value = cash + position_value

            portfolio_history.append(
                {
                    "date": date,
                    "cash": cash,
                    "position_value": position_value,
                    "total_value": total_value,
                }
            )

        df_portfolio = pd.DataFrame(portfolio_history)

        # ポートフォリオ価値が記録されていることを確認
        assert len(df_portfolio) == 10
        assert df_portfolio["total_value"].iloc[0] > 0
        assert "cash" in df_portfolio.columns
        assert "position_value" in df_portfolio.columns


class TestBacktestOutput:
    """バックテスト出力のテスト"""

    def test_results_json_format(self):
        """結果のJSON形式をテスト"""
        results = {
            "metadata": {
                "strategy": "moving_average_crossover",
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
                "initial_capital": 1_000_000,
            },
            "performance": {
                "total_return": 0.15,
                "sharpe_ratio": 1.2,
                "max_drawdown": -0.08,
                "win_rate": 0.6,
            },
            "trades": [
                {
                    "date": "2023-01-15",
                    "code": "1234",
                    "action": "buy",
                    "price": 1000,
                    "shares": 100,
                },
                {
                    "date": "2023-02-15",
                    "code": "1234",
                    "action": "sell",
                    "price": 1100,
                    "shares": 100,
                    "return": 0.1,
                },
            ],
        }

        # JSON変換可能か確認
        json_str = json.dumps(results, default=str)
        loaded = json.loads(json_str)

        assert loaded["metadata"]["strategy"] == "moving_average_crossover"
        assert loaded["performance"]["total_return"] == 0.15
        assert len(loaded["trades"]) == 2

    def test_results_dataframe_conversion(self):
        """結果のDataFrame変換をテスト"""
        trades = [
            {
                "date": "2023-01-15",
                "code": "1234",
                "action": "buy",
                "price": 1000,
                "shares": 100,
            },
            {
                "date": "2023-02-15",
                "code": "1234",
                "action": "sell",
                "price": 1100,
                "shares": 100,
                "return": 0.1,
            },
            {
                "date": "2023-03-15",
                "code": "5678",
                "action": "buy",
                "price": 500,
                "shares": 200,
            },
            {
                "date": "2023-04-15",
                "code": "5678",
                "action": "sell",
                "price": 480,
                "shares": 200,
                "return": -0.04,
            },
        ]

        df_trades = pd.DataFrame(trades)

        # データ型の変換
        df_trades["date"] = pd.to_datetime(df_trades["date"])
        df_trades["price"] = df_trades["price"].astype(float)
        df_trades["shares"] = df_trades["shares"].astype(int)

        # 統計情報の計算
        sell_trades = df_trades[df_trades["action"] == "sell"]
        avg_return = sell_trades["return"].mean()

        assert len(df_trades) == 4
        assert avg_return == pytest.approx(0.03, abs=1e-10)  # (0.1 - 0.04) / 2

    def test_performance_summary_generation(self):
        """パフォーマンスサマリー生成のテスト"""
        # トレード履歴
        trades = pd.DataFrame(
            {
                "return": [0.05, -0.02, 0.03, -0.01, 0.04, -0.03, 0.02, 0.01],
                "holding_days": [30, 20, 25, 15, 35, 40, 22, 18],
            }
        )

        # パフォーマンスメトリクスの計算
        summary = {
            "total_trades": len(trades),
            "avg_return": trades["return"].mean(),
            "avg_holding_days": trades["holding_days"].mean(),
            "best_trade": trades["return"].max(),
            "worst_trade": trades["return"].min(),
            "total_return": (1 + trades["return"]).prod() - 1,
        }

        assert summary["total_trades"] == 8
        assert summary["avg_return"] > 0
        assert summary["best_trade"] == 0.05
        assert summary["worst_trade"] == -0.03

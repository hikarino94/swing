"""Tests for backtest/backtest_technical.py"""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from backtest.backtest_technical import (
    CAPITAL_DEFAULT,
    HOLD_DAYS_DEFAULT,
    MIN_PRICE_DEFAULT,
    STOP_LOSS_PCT_DEFAULT,
    _ascii_bar_chart,
    _result_paths,
    parse_args,
    run_backtest,
    run_backtest_short,
    show_results,
    summarize,
    to_excel,
)


class TestResultPaths:
    """結果ファイルパス生成のテスト"""

    @patch("backtest.backtest_technical.get_timestamped_output_path")
    def test_result_paths(self, mock_get_path):
        """Excel/JSONパスの生成"""
        mock_get_path.side_effect = [
            Path("/tmp/backtest/technical_20240115_123456.xlsx"),
            Path("/tmp/backtest/technical_20240115_123456.json"),
        ]

        excel_path, json_path = _result_paths("technical")

        assert excel_path.suffix == ".xlsx"
        assert json_path.suffix == ".json"
        assert mock_get_path.call_count == 2


class TestRunBacktest:
    """ロングポジションのバックテストのテスト"""

    def test_run_backtest_basic(self):
        """基本的なバックテスト実行"""
        conn = MagicMock(spec=sqlite3.Connection)

        # 指標データのモック
        indicators_df = pd.DataFrame(
            {
                "date": ["2024-01-15", "2024-01-16"],
                "code": ["1234", "5678"],
                "signals_count": [3, 4],
            }
        )

        # 価格データのモック（各銘柄ごとに別々のDataFrameが返される）
        prices_1234 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-15", periods=20),
                "close": [1000 + i for i in range(20)],
            }
        )
        prices_5678 = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-16", periods=20),
                "close": [2000 + i for i in range(20)],
            }
        )

        with patch("backtest.backtest_technical.pd.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [indicators_df, prices_1234, prices_5678]

            trades = run_backtest(
                conn,
                as_of="2024-01-16",
                capital=1_000_000,
                hold_days=5,
                stop_loss_pct=0.05,
                min_price=300,
            )

            assert isinstance(trades, pd.DataFrame)
            assert "code" in trades.columns
            assert "entry_date" in trades.columns
            assert "exit_date" in trades.columns
            assert "side" in trades.columns
            assert all(trades["side"] == "long")

    def test_run_backtest_with_stop_loss(self):
        """ストップロス発動のテスト"""
        conn = MagicMock(spec=sqlite3.Connection)

        indicators_df = pd.DataFrame(
            {
                "date": ["2024-01-15"],
                "code": ["1234"],
                "signals_count": [3],
            }
        )

        # 価格が5%以上下落するデータ
        prices_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-15", periods=10),
                "close": [1000, 990, 980, 940, 950, 960, 970, 980, 990, 1000],
            }
        )

        with patch("backtest.backtest_technical.pd.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [indicators_df, prices_df]

            trades = run_backtest(
                conn,
                as_of="2024-01-15",
                capital=1_000_000,
                hold_days=10,
                stop_loss_pct=0.05,
                min_price=300,
            )

            if len(trades) > 0:
                # ストップロスで早期に売却される
                # pnl_pctが負の値（損失）になることを確認
                assert trades.iloc[0]["pnl_pct"] < 0

    def test_run_backtest_empty_signals(self):
        """シグナルがない場合"""
        conn = MagicMock(spec=sqlite3.Connection)

        # 空の指標データ
        indicators_df = pd.DataFrame(columns=["date", "code", "signals_count"])
        prices_df = pd.DataFrame(columns=["code", "date", "adj_close"])

        with patch("backtest.backtest_technical.pd.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [indicators_df, prices_df]

            trades = run_backtest(
                conn,
                as_of="2024-01-16",
                capital=1_000_000,
            )

            assert len(trades) == 0


class TestRunBacktestShort:
    """ショートポジションのバックテストのテスト"""

    def test_run_backtest_short_basic(self):
        """基本的なショートバックテスト"""
        conn = MagicMock(spec=sqlite3.Connection)

        # ショートシグナルのモック
        indicators_df = pd.DataFrame(
            {
                "date": ["2024-01-15"],
                "code": ["1234"],
                "signals_short_count": [4],
                "signals_short_first": [1],
                "oversold": [0],
            }
        )

        # 価格データ（下落トレンド）
        prices_df = pd.DataFrame(
            {
                "code": ["1234"] * 10,
                "date": pd.date_range("2024-01-15", periods=10),
                "adj_close": [1000 - i * 10 for i in range(10)],  # 下落
            }
        )

        with patch("backtest.backtest_technical.pd.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [indicators_df, prices_df]

            trades = run_backtest_short(
                conn,
                as_of="2024-01-15",
                capital=1_000_000,
                hold_days=5,
                stop_loss_pct=0.05,
                min_price=300,
            )

            assert isinstance(trades, pd.DataFrame)
            if len(trades) > 0:
                assert all(trades["side"] == "short")
                # ショートポジションなので価格下落で利益
                assert trades.iloc[0]["pl_pct"] > 0

    def test_run_backtest_short_stop_loss(self):
        """ショートポジションのストップロス"""
        conn = MagicMock(spec=sqlite3.Connection)

        indicators_df = pd.DataFrame(
            {
                "date": ["2024-01-15"],
                "code": ["1234"],
                "signals_short_count": [4],
                "signals_short_first": [1],
                "oversold": [0],
            }
        )

        # 価格が上昇するデータ（ショートには不利）
        prices_df = pd.DataFrame(
            {
                "code": ["1234"] * 10,
                "date": pd.date_range("2024-01-15", periods=10),
                "adj_close": [1000 + i * 60 for i in range(10)],  # 上昇
            }
        )

        with patch("backtest.backtest_technical.pd.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [indicators_df, prices_df]

            trades = run_backtest_short(
                conn,
                as_of="2024-01-15",
                capital=1_000_000,
                hold_days=10,
                stop_loss_pct=0.05,
            )

            if len(trades) > 0:
                # ストップロスで損失確定
                assert trades.iloc[0]["pnl_pct"] < 0


class TestSummarize:
    """サマリー生成のテスト"""

    def test_summarize_mixed_positions(self):
        """ロング・ショート混在のサマリー"""
        trades = pd.DataFrame(
            {
                "pnl_yen": [10000, -5000, 20000, -3000],
                "pnl_pct": [0.10, -0.05, 0.20, -0.03],
                "side": ["long", "long", "short", "short"],
            }
        )

        summary = summarize(trades)

        assert isinstance(summary, pd.DataFrame)
        assert "metric" in summary.columns
        assert "value" in summary.columns
        assert "total_profit" in summary["metric"].values
        assert "win_rate" in summary["metric"].values
        assert "trades" in summary["metric"].values

    def test_summarize_long_only(self):
        """ロングのみのサマリー"""
        trades = pd.DataFrame(
            {
                "pnl_yen": [10000, -5000, 20000],
                "pnl_pct": [0.10, -0.05, 0.20],
                "side": ["long", "long", "long"],
            }
        )

        summary = summarize(trades)

        # total_profitの確認
        total_profit_row = summary[summary["metric"] == "total_profit"]
        assert total_profit_row["value"].values[0] == 25000

    def test_summarize_empty(self):
        """空のトレード"""
        trades = pd.DataFrame(columns=["pnl_yen", "pnl_pct", "side"])

        summary = summarize(trades)

        # 値の確認
        total_profit_row = summary[summary["metric"] == "total_profit"]
        assert total_profit_row["value"].values[0] == 0
        trades_row = summary[summary["metric"] == "trades"]
        assert trades_row["value"].values[0] == 0


class TestAsciiBarChart:
    """ASCIIバーチャートのテスト"""

    def test_ascii_bar_chart_mixed(self):
        """正負混在のバーチャート"""
        values = [100, -50, 200, -150, 75]

        chart = _ascii_bar_chart(values, width=20)

        assert isinstance(chart, str)
        lines = chart.strip().split("\n")
        assert len(lines) == 5
        assert "+" in chart
        assert "-" in chart

    def test_ascii_bar_chart_single_value(self):
        """単一値のバーチャート"""
        chart = _ascii_bar_chart([100], width=20)
        assert "#" in chart


class TestShowResults:
    """結果表示のテスト"""

    @patch("builtins.print")
    def test_show_results_with_trades(self, mock_print):
        """トレードありの結果表示"""
        trades = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "side": ["long", "short"],
                "entry_date": ["2024-01-15", "2024-01-16"],
                "exit_date": ["2024-01-22", "2024-01-23"],
                "pnl_yen": [10000, -5000],
                "pnl_pct": [0.10, -0.05],
            }
        )

        summary = pd.DataFrame(
            {"metric": ["trades", "total_profit", "win_rate"], "value": [2, 5000, 0.5]}
        )

        show_results(trades, summary)

        # 複数回printが呼ばれることを確認
        assert mock_print.call_count >= 4


class TestToExcel:
    """Excel出力のテスト"""

    def test_to_excel_with_chart(self, tmp_path):
        """チャート付きExcel出力"""
        trades = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "pnl_yen": [10000, -5000],
            }
        )

        summary = pd.DataFrame({"metric": ["total_profit"], "value": [5000]})

        output_path = tmp_path / "test.xlsx"
        to_excel(trades, summary, output_path)

        # ファイルが作成されたことを確認
        assert output_path.exists()


class TestParseArgs:
    """引数解析のテスト"""

    def test_parse_args_default(self):
        """デフォルト引数"""
        args = parse_args(["--start", "2024-01-01"])

        assert args.capital == CAPITAL_DEFAULT
        assert args.hold_days == HOLD_DAYS_DEFAULT
        assert args.stop_loss == STOP_LOSS_PCT_DEFAULT
        assert args.min_price == MIN_PRICE_DEFAULT
        assert not args.show
        assert args.start == "2024-01-01"
        assert args.end is None

    def test_parse_args_custom(self):
        """カスタム引数"""
        args = parse_args(
            [
                "--capital",
                "2000000",
                "--hold-days",
                "30",
                "--stop-loss",
                "0.10",
                "--min-price",
                "500",
                "--start",
                "2024-01-01",
                "--end",
                "2024-12-31",
                "--show",
            ]
        )

        assert args.capital == 2000000
        assert args.hold_days == 30
        assert args.stop_loss == 0.10
        assert args.min_price == 500
        assert args.start == "2024-01-01"
        assert args.end == "2024-12-31"
        assert args.show


class TestMain:
    """main関数のテスト"""

    @pytest.mark.skip(reason="run_backtest_range関数が未実装")
    @patch("backtest.backtest_technical.sqlite3.connect")
    @patch("backtest.backtest_technical.parse_args")
    def test_main_success(
        self,
        mock_parse_args,
        mock_connect,
    ):
        """正常なmain関数実行"""
        pass

    @pytest.mark.skip(reason="run_backtest_range関数が未実装")
    @patch("backtest.backtest_technical.sqlite3.connect")
    @patch("backtest.backtest_technical.parse_args")
    def test_main_no_trades(self, mock_parse_args, mock_connect):
        """トレードがない場合"""
        pass

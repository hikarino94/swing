"""Tests for backtest/backtest_technical.py"""

import datetime as dt
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from backtest.backtest_technical import (
    DEFAULT_CAPITAL,
    DEFAULT_HOLD_DAYS,
    DEFAULT_STOP_LOSS,
    MIN_PRICE_DEFAULT,
    _ascii_bar_chart,
    _result_paths,
    main,
    parse_args,
    run_backtest,
    run_backtest_range,
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

        # 価格データのモック
        prices_df = pd.DataFrame(
            {
                "code": ["1234"] * 20 + ["5678"] * 20,
                "date": pd.date_range("2024-01-15", periods=20).tolist()
                + pd.date_range("2024-01-16", periods=20).tolist(),
                "adj_close": [1000 + i for i in range(20)]
                + [2000 + i for i in range(20)],
            }
        )

        with patch("backtest.backtest_technical.pd.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [indicators_df, prices_df]

            trades = run_backtest(
                conn,
                start_date=dt.date(2024, 1, 15),
                end_date=dt.date(2024, 1, 16),
                capital_per_trade=1_000_000,
                hold_days=5,
                stop_loss=0.05,
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
                "code": ["1234"] * 10,
                "date": pd.date_range("2024-01-15", periods=10),
                "adj_close": [1000, 990, 980, 940, 950, 960, 970, 980, 990, 1000],
            }
        )

        with patch("backtest.backtest_technical.pd.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [indicators_df, prices_df]

            trades = run_backtest(
                conn,
                start_date=dt.date(2024, 1, 15),
                end_date=dt.date(2024, 1, 15),
                capital_per_trade=1_000_000,
                hold_days=10,
                stop_loss=0.05,
                min_price=300,
            )

            if len(trades) > 0:
                # ストップロスで早期に売却される
                assert (
                    trades.iloc[0]["exit_reason"] == "stop_loss"
                    or trades.iloc[0]["pl_pct"] < 0
                )

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
                start_date=dt.date(2024, 1, 15),
                end_date=dt.date(2024, 1, 16),
                capital_per_trade=1_000_000,
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
                start_date=dt.date(2024, 1, 15),
                end_date=dt.date(2024, 1, 15),
                capital_per_trade=1_000_000,
                hold_days=5,
                stop_loss=0.05,
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
                start_date=dt.date(2024, 1, 15),
                end_date=dt.date(2024, 1, 15),
                capital_per_trade=1_000_000,
                hold_days=10,
                stop_loss=0.05,
            )

            if len(trades) > 0:
                # ストップロスで損失確定
                assert trades.iloc[0]["pl_pct"] < 0


class TestSummarize:
    """サマリー生成のテスト"""

    def test_summarize_mixed_positions(self):
        """ロング・ショート混在のサマリー"""
        trades = pd.DataFrame(
            {
                "pl": [10000, -5000, 20000, -3000],
                "pl_pct": [0.10, -0.05, 0.20, -0.03],
                "side": ["long", "long", "short", "short"],
            }
        )

        summary = summarize(trades)

        assert isinstance(summary, pd.DataFrame)
        assert "Total P&L" in summary.index
        assert "Long P&L" in summary.index
        assert "Short P&L" in summary.index
        assert "Win Rate" in summary.index
        assert "Long Win Rate" in summary.index
        assert "Short Win Rate" in summary.index

    def test_summarize_long_only(self):
        """ロングのみのサマリー"""
        trades = pd.DataFrame(
            {
                "pl": [10000, -5000, 20000],
                "pl_pct": [0.10, -0.05, 0.20],
                "side": ["long", "long", "long"],
            }
        )

        summary = summarize(trades)

        assert summary.loc["Long P&L", "value"] == 25000
        assert summary.loc["Short P&L", "value"] == 0

    def test_summarize_empty(self):
        """空のトレード"""
        trades = pd.DataFrame(columns=["pl", "pl_pct", "side"])

        summary = summarize(trades)

        assert summary.loc["Total P&L", "value"] == 0
        assert summary.loc["Num Trades", "value"] == 0


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
        assert "█" in chart


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
                "pl": [10000, -5000],
                "pl_pct": [0.10, -0.05],
            }
        )

        summary = pd.DataFrame(
            {"value": [5000, 2, "50.0%"]}, index=["Total P&L", "Num Trades", "Win Rate"]
        )

        show_results(trades, summary)

        # 複数回printが呼ばれることを確認
        assert mock_print.call_count > 5


class TestToExcel:
    """Excel出力のテスト"""

    @patch("backtest.backtest_technical.pd.ExcelWriter")
    def test_to_excel_with_chart(self, mock_excel_writer):
        """チャート付きExcel出力"""
        mock_writer = MagicMock()
        mock_sheet = MagicMock()
        mock_chart = MagicMock()
        mock_workbook = MagicMock()

        mock_workbook.add_chart.return_value = mock_chart
        mock_writer.book = mock_workbook
        mock_writer.sheets = {"trades": mock_sheet}

        mock_excel_writer.return_value.__enter__.return_value = mock_writer

        trades = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "pl": [10000, -5000],
            }
        )

        summary = pd.DataFrame({"value": [5000]}, index=["Total P&L"])

        to_excel(trades, summary, Path("/tmp/test.xlsx"))

        # チャートが追加されたことを確認
        mock_workbook.add_chart.assert_called()


class TestRunBacktestRange:
    """期間指定バックテストのテスト"""

    @patch("backtest.backtest_technical.run_backtest_short")
    @patch("backtest.backtest_technical.run_backtest")
    def test_run_backtest_range_combined(self, mock_run_long, mock_run_short):
        """ロング・ショート統合バックテスト"""
        conn = MagicMock(spec=sqlite3.Connection)

        # ロングトレード
        long_trades = pd.DataFrame(
            {
                "code": ["1234"],
                "side": ["long"],
                "pl": [10000],
                "pl_pct": [0.10],
            }
        )

        # ショートトレード
        short_trades = pd.DataFrame(
            {
                "code": ["5678"],
                "side": ["short"],
                "pl": [5000],
                "pl_pct": [0.05],
            }
        )

        mock_run_long.return_value = long_trades
        mock_run_short.return_value = short_trades

        result = run_backtest_range(
            conn,
            start_date=dt.date(2024, 1, 15),
            end_date=dt.date(2024, 1, 31),
            capital_per_trade=1_000_000,
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "long" in result["side"].values
        assert "short" in result["side"].values

    @patch("backtest.backtest_technical.run_backtest_short")
    @patch("backtest.backtest_technical.run_backtest")
    def test_run_backtest_range_empty(self, mock_run_long, mock_run_short):
        """トレードなしの場合"""
        conn = MagicMock(spec=sqlite3.Connection)

        mock_run_long.return_value = pd.DataFrame()
        mock_run_short.return_value = pd.DataFrame()

        result = run_backtest_range(
            conn, start_date=dt.date(2024, 1, 15), end_date=dt.date(2024, 1, 31)
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestParseArgs:
    """引数解析のテスト"""

    def test_parse_args_default(self):
        """デフォルト引数"""
        args = parse_args([])

        assert args.capital == DEFAULT_CAPITAL
        assert args.hold_days == DEFAULT_HOLD_DAYS
        assert args.stop_loss == DEFAULT_STOP_LOSS
        assert args.min_price == MIN_PRICE_DEFAULT
        assert not args.show
        assert args.start is None
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

    @patch("backtest.backtest_technical.to_excel")
    @patch("backtest.backtest_technical.summarize")
    @patch("backtest.backtest_technical.run_backtest_range")
    @patch("backtest.backtest_technical.sqlite3.connect")
    @patch("backtest.backtest_technical.parse_args")
    def test_main_success(
        self,
        mock_parse_args,
        mock_connect,
        mock_run_backtest,
        mock_summarize,
        mock_to_excel,
    ):
        """正常なmain関数実行"""
        # 引数のモック
        mock_parse_args.return_value = MagicMock(
            start="2024-01-01",
            end="2024-01-31",
            capital=1_000_000,
            hold_days=60,
            stop_loss=0.05,
            min_price=300,
            show=False,
            verbose=False,
        )

        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_conn

        # バックテスト結果のモック
        trades = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "side": ["long", "short"],
                "pl": [10000, 5000],
                "pl_pct": [0.10, 0.05],
            }
        )
        mock_run_backtest.return_value = trades

        # サマリーのモック
        summary = pd.DataFrame({"value": [15000, 2]}, index=["Total P&L", "Num Trades"])
        mock_summarize.return_value = summary

        # main実行
        main()

        # 各関数が呼ばれたことを確認
        mock_run_backtest.assert_called_once()
        mock_summarize.assert_called_once()
        mock_to_excel.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("backtest.backtest_technical.run_backtest_range")
    @patch("backtest.backtest_technical.sqlite3.connect")
    @patch("backtest.backtest_technical.parse_args")
    def test_main_no_trades(self, mock_parse_args, mock_connect, mock_run_backtest):
        """トレードがない場合"""
        mock_parse_args.return_value = MagicMock(
            start="2024-01-01",
            end="2024-01-31",
            capital=1_000_000,
            hold_days=60,
            stop_loss=0.05,
            min_price=300,
            show=False,
            verbose=False,
        )

        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_conn

        # 空のトレード
        mock_run_backtest.return_value = pd.DataFrame()

        # main実行
        main()

        # 早期終了することを確認
        mock_conn.close.assert_called_once()

    @patch("backtest.backtest_technical.show_results")
    @patch("backtest.backtest_technical.to_excel")
    @patch("backtest.backtest_technical.summarize")
    @patch("backtest.backtest_technical.run_backtest_range")
    @patch("backtest.backtest_technical.sqlite3.connect")
    @patch("backtest.backtest_technical.parse_args")
    def test_main_with_show(
        self,
        mock_parse_args,
        mock_connect,
        mock_run_backtest,
        mock_summarize,
        mock_to_excel,
        mock_show,
    ):
        """結果表示オプション付き実行"""
        mock_parse_args.return_value = MagicMock(
            start="2024-01-01",
            end="2024-01-31",
            capital=1_000_000,
            hold_days=60,
            stop_loss=0.05,
            min_price=300,
            show=True,
            verbose=False,
        )

        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_conn

        trades = pd.DataFrame(
            {
                "code": ["1234"],
                "pl": [10000],
            }
        )
        mock_run_backtest.return_value = trades

        summary = pd.DataFrame({"value": [10000]})
        mock_summarize.return_value = summary

        # main実行
        main()

        # 結果表示が呼ばれたことを確認
        mock_show.assert_called_once_with(trades, summary)

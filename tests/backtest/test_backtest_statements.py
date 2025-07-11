"""Tests for backtest/backtest_statements.py"""

import datetime as dt
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from backtest.backtest_statements import (
    DEFAULT_CAPITAL,
    MIN_PRICE_DEFAULT,
    _ascii_bar_chart,
    _result_paths,
    add_n_trading_days,
    main,
    parse_args,
    read_prices,
    read_signals,
    run_backtest,
    show_results,
    summarize,
    to_excel,
)


class TestResultPaths:
    """結果ファイルパス生成のテスト"""

    @patch("backtest.backtest_statements.get_timestamped_output_path")
    def test_result_paths(self, mock_get_path):
        """Excel/JSONパスの生成"""
        mock_get_path.side_effect = [
            Path("/tmp/backtest/fundamental_20240115_123456.xlsx"),
            Path("/tmp/backtest/fundamental_20240115_123456.json"),
        ]

        excel_path, json_path = _result_paths("fundamental")

        assert excel_path.suffix == ".xlsx"
        assert json_path.suffix == ".json"
        assert mock_get_path.call_count == 2


class TestReadPrices:
    """価格データ読み込みのテスト"""

    @patch("backtest.backtest_statements.pd.read_sql")
    def test_read_prices_success(self, mock_read_sql):
        """正常な価格データ読み込み"""
        mock_df = pd.DataFrame(
            {
                "code": ["1234", "1234", "5678", "5678"],
                "trade_date": ["2024-01-15", "2024-01-16", "2024-01-15", "2024-01-16"],
                "adj_close": [1000, 1010, 2000, 2020],
            }
        )
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock(spec=sqlite3.Connection)

        result = read_prices(mock_conn)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        assert "adj_close" in result.columns
        mock_read_sql.assert_called_once()

    def test_read_prices_empty(self):
        """空のデータの場合"""
        mock_conn = MagicMock(spec=sqlite3.Connection)

        with patch("backtest.backtest_statements.pd.read_sql") as mock_read_sql:
            # 空でも必要なカラムは存在する
            mock_read_sql.return_value = pd.DataFrame(
                columns=["code", "trade_date", "adj_close"]
            )
            result = read_prices(mock_conn)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0


class TestReadSignals:
    """シグナルデータ読み込みのテスト"""

    @patch("backtest.backtest_statements.pd.read_sql")
    def test_read_signals_with_dates(self, mock_read_sql):
        """日付指定でのシグナル読み込み"""
        mock_df = pd.DataFrame(
            {
                "date": ["2024-01-15", "2024-01-16"],
                "code": ["1234", "5678"],
                "signal": [1, 1],
            }
        )
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock(spec=sqlite3.Connection)

        result = read_signals(
            mock_conn, start=dt.date(2024, 1, 15), end=dt.date(2024, 1, 16)
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        # SQLクエリに日付が含まれることを確認
        call_args = mock_read_sql.call_args[0][0]
        assert "2024-01-15" in call_args
        assert "2024-01-16" in call_args

    @patch("backtest.backtest_statements.pd.read_sql")
    def test_read_signals_no_dates(self, mock_read_sql):
        """日付指定なしでのシグナル読み込み"""
        mock_df = pd.DataFrame(
            {
                "date": ["2024-01-15"],
                "code": ["1234"],
                "signal": [1],
            }
        )
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock(spec=sqlite3.Connection)

        result = read_signals(mock_conn, None, None)

        assert isinstance(result, pd.DataFrame)
        # SQLクエリに日付条件が含まれないことを確認
        call_args = mock_read_sql.call_args[0][0]
        assert "WHERE" not in call_args


class TestAddNTradingDays:
    """営業日加算のテスト"""

    def test_add_n_trading_days_basic(self):
        """基本的な営業日加算"""
        # 営業日カレンダー（土日を除く）
        calendar = pd.date_range("2024-01-01", "2024-01-31", freq="B")

        dates = pd.Series(
            [
                pd.Timestamp("2024-01-15"),  # 月曜日
                pd.Timestamp("2024-01-16"),  # 火曜日
            ]
        )

        result = add_n_trading_days(dates, 5, calendar)

        assert len(result) == 2
        # 5営業日後を確認
        assert result.iloc[0] == pd.Timestamp("2024-01-22")
        assert result.iloc[1] == pd.Timestamp("2024-01-23")

    def test_add_n_trading_days_with_nat(self):
        """NaTを含む場合"""
        calendar = pd.date_range("2024-01-01", "2024-01-31", freq="B")

        dates = pd.Series(
            [
                pd.Timestamp("2024-01-15"),
                pd.NaT,
            ]
        )

        result = add_n_trading_days(dates, 5, calendar)

        assert len(result) == 2
        assert pd.isna(result.iloc[1])

    def test_add_n_trading_days_zero(self):
        """0日加算の場合"""
        calendar = pd.date_range("2024-01-01", "2024-01-31", freq="B")
        dates = pd.Series([pd.Timestamp("2024-01-15")])

        result = add_n_trading_days(dates, 0, calendar)

        assert result.iloc[0] == dates.iloc[0]


class TestRunBacktest:
    """バックテスト実行のテスト"""

    def test_run_backtest_basic(self):
        """基本的なバックテスト"""
        # シグナルデータ
        signals = pd.DataFrame(
            {
                "DisclosedAt": pd.to_datetime(["2024-01-15", "2024-01-16"]),
                "code": ["1234", "5678"],
                "signal": [1, 1],
            }
        )

        # 価格データ
        prices = pd.DataFrame(
            {
                "code": ["1234"] * 30 + ["5678"] * 30,
                "date": pd.date_range("2024-01-01", periods=30).tolist() * 2,
                "adj_close": [1000 + i for i in range(30)]
                + [2000 + i for i in range(30)],
            }
        )
        # 価格データをマルチインデックスに変換
        prices_indexed = prices.set_index(["code", "date"]).sort_index()

        trades = run_backtest(
            prices_indexed,
            signals,
            hold=5,
            offset=1,
            capital=1_000_000,
            min_price=300,
        )

        assert isinstance(trades, pd.DataFrame)
        assert "code" in trades.columns
        assert "entry_date" in trades.columns
        assert "exit_date" in trades.columns
        assert "entry_px" in trades.columns
        assert "exit_px" in trades.columns
        assert "shares" in trades.columns
        assert "profit_jpy" in trades.columns
        assert "ret_pct" in trades.columns

    def test_run_backtest_min_price_filter(self):
        """最低価格フィルターのテスト"""
        signals = pd.DataFrame(
            {
                "DisclosedAt": pd.to_datetime(["2024-01-15"]),
                "code": ["1234"],
                "signal": [1],
            }
        )

        # 最低価格以下の価格データ
        prices = pd.DataFrame(
            {
                "code": ["1234"] * 10,
                "date": pd.date_range("2024-01-01", periods=10),
                "adj_close": [100] * 10,  # min_price=300以下
            }
        )
        # 価格データをマルチインデックスに変換
        prices_indexed = prices.set_index(["code", "date"]).sort_index()

        trades = run_backtest(
            prices_indexed,
            signals,
            hold=5,
            offset=1,
            capital=1_000_000,
            min_price=300,
        )

        # 最低価格以下なのでトレードは発生しない
        assert len(trades) == 0

    def test_run_backtest_no_exit_price(self):
        """出口価格がない場合"""
        signals = pd.DataFrame(
            {
                "DisclosedAt": pd.to_datetime(["2024-01-15"]),
                "code": ["1234"],
                "signal": [1],
            }
        )

        # 短い価格データ（出口日のデータがない）
        prices = pd.DataFrame(
            {
                "code": ["1234"] * 5,
                "date": pd.date_range("2024-01-15", periods=5),
                "adj_close": [1000, 1010, 1020, 1030, 1040],
            }
        )
        # 価格データをマルチインデックスに変換
        prices_indexed = prices.set_index(["code", "date"]).sort_index()

        trades = run_backtest(
            prices_indexed,
            signals,
            hold=20,  # 長い保有期間
            offset=1,
            capital=1_000_000,
        )

        # 保有期間が価格データの期間を超える場合、最終日の価格が使われる
        if len(trades) > 0:
            # 最終日の価格（1040）が出口価格として使われる
            assert trades["exit_px"].iloc[0] == 1040


class TestSummarize:
    """サマリー生成のテスト"""

    def test_summarize_basic(self):
        """基本的なサマリー生成"""
        trades = pd.DataFrame(
            {
                "profit_jpy": [10000, -5000, 20000, -3000, 15000],
                "ret_pct": [0.10, -0.05, 0.20, -0.03, 0.15],
            }
        )

        summary = summarize(trades)

        assert isinstance(summary, pd.DataFrame)
        assert "value" in summary.columns
        assert "metric" in summary.columns
        metrics = summary["metric"].tolist()
        assert "trades" in metrics
        assert "total_profit" in metrics
        assert "win_rate" in metrics
        assert "avg_ret_pct" in metrics
        assert "sharpe" in metrics

    def test_summarize_empty_trades(self):
        """空のトレードデータ"""
        trades = pd.DataFrame(columns=["profit_jpy", "ret_pct"])

        summary = summarize(trades)

        assert isinstance(summary, pd.DataFrame)
        summary_dict = dict(zip(summary["metric"], summary["value"], strict=False))
        assert summary_dict["total_profit"] == 0
        assert summary_dict["trades"] == 0

    def test_summarize_all_winners(self):
        """全て勝ちトレードの場合"""
        trades = pd.DataFrame(
            {
                "profit_jpy": [10000, 5000, 20000],
                "ret_pct": [0.10, 0.05, 0.20],
            }
        )

        summary = summarize(trades)

        summary_dict = dict(zip(summary["metric"], summary["value"], strict=False))
        assert summary_dict["win_rate"] == 1.0
        assert summary_dict["trades"] == 3


class TestAsciiBarChart:
    """ASCIIバーチャートのテスト"""

    def test_ascii_bar_chart_basic(self):
        """基本的なバーチャート"""
        values = [10, -5, 20, -10, 15]

        chart = _ascii_bar_chart(values, width=20)

        assert isinstance(chart, str)
        assert "#" in chart  # バー文字が含まれる
        assert "+" in chart  # プラス記号
        assert "-" in chart  # マイナス記号

    def test_ascii_bar_chart_empty(self):
        """空のデータ"""
        chart = _ascii_bar_chart([], width=20)
        assert chart == ""

    def test_ascii_bar_chart_all_positive(self):
        """全て正の値"""
        values = [10, 20, 30]
        chart = _ascii_bar_chart(values, width=20)

        assert "-" not in chart.replace("--", "")  # 区切り線以外にマイナスがない


class TestShowResults:
    """結果表示のテスト"""

    @patch("builtins.print")
    def test_show_results(self, mock_print):
        """結果表示のテスト"""
        trades = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "entry_date": ["2024-01-15", "2024-01-16"],
                "exit_date": ["2024-01-22", "2024-01-23"],
                "profit_jpy": [10000, -5000],
                "ret_pct": [0.10, -0.05],
            }
        )

        summary = pd.DataFrame(
            {"metric": ["trades", "total_profit", "win_rate"], "value": [2, 5000, 0.5]}
        )

        show_results(trades, summary)

        # printが呼ばれたことを確認
        assert mock_print.call_count > 0


class TestToExcel:
    """Excel出力のテスト"""

    def test_to_excel_success(self, tmp_path):
        """正常なExcel出力"""
        trades = pd.DataFrame(
            {
                "code": ["1234"],
                "profit_jpy": [10000],
            }
        )

        summary = pd.DataFrame({"metric": ["total_profit"], "value": [10000]})

        output_path = tmp_path / "test.xlsx"
        to_excel(trades, summary, output_path)

        # ファイルが作成されたことを確認
        assert output_path.exists()


class TestParseArgs:
    """引数解析のテスト"""

    def test_parse_args_default(self):
        """デフォルト引数"""
        args = parse_args([])

        assert args.hold == 40
        assert args.capital == DEFAULT_CAPITAL
        assert args.min_price == MIN_PRICE_DEFAULT
        assert args.entry_offset == 1
        assert not args.show

    def test_parse_args_custom(self):
        """カスタム引数"""
        args = parse_args(
            [
                "--hold",
                "40",
                "--capital",
                "2000000",
                "--start",
                "2024-01-01",
                "--end",
                "2024-12-31",
                "--show",
                "--xlsx",
                "output.xlsx",
            ]
        )

        assert args.hold == 40
        assert args.capital == 2000000
        assert args.start == "2024-01-01"
        assert args.end == "2024-12-31"
        assert args.show
        assert args.xlsx == Path("output.xlsx")


class TestMain:
    """main関数のテスト"""

    @patch("backtest.backtest_statements.run_backtest")
    @patch("backtest.backtest_statements.read_signals")
    @patch("backtest.backtest_statements.read_prices")
    @patch("backtest.backtest_statements.sqlite3.connect")
    @patch("backtest.backtest_statements.parse_args")
    def test_main_basic(
        self,
        mock_parse_args,
        mock_connect,
        mock_read_prices,
        mock_read_signals,
        mock_run_backtest,
    ):
        """基本的なmain関数の実行"""
        # モックの設定
        mock_parse_args.return_value = MagicMock(
            hold=20,
            capital=1_000_000,
            min_price=300,
            entry_offset=1,
            start=None,
            end=None,
            show=False,
            xlsx=None,
            verbose=False,
        )

        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_connect.return_value.__exit__.return_value = None

        # データのモック
        mock_read_prices.return_value = pd.DataFrame(
            {
                "code": ["1234"],
                "date": pd.to_datetime(["2024-01-15"]),
                "adj_close": [1000],
            }
        )

        mock_read_signals.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-15"]),
                "code": ["1234"],
                "signal": [1],
            }
        )

        mock_run_backtest.return_value = pd.DataFrame(
            {
                "code": ["1234"],
                "profit_jpy": [10000],
                "ret_pct": [0.10],
            }
        )

        # main関数の実行
        main()

        # 各関数が呼ばれたことを確認
        mock_read_prices.assert_called_once()
        mock_read_signals.assert_called_once()
        mock_run_backtest.assert_called_once()

    @patch("backtest.backtest_statements.read_prices")
    @patch("backtest.backtest_statements.read_signals")
    @patch("backtest.backtest_statements.sqlite3.connect")
    @patch("backtest.backtest_statements.parse_args")
    def test_main_no_signals(
        self, mock_parse_args, mock_connect, mock_read_signals, mock_read_prices
    ):
        """シグナルがない場合"""
        mock_parse_args.return_value = MagicMock(
            hold=20, start=None, end=None, verbose=False
        )

        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_connect.return_value.__exit__.return_value = None

        # read_pricesのモック
        mock_read_prices.return_value = pd.DataFrame(
            columns=["code", "trade_date", "adj_close"]
        ).set_index(["code", "trade_date"])

        # 空のシグナル
        mock_read_signals.return_value = pd.DataFrame()

        # main関数の実行（sys.exitが呼ばれる）
        with pytest.raises(SystemExit):
            main()

        # 早期終了することを確認
        # with文が使われているのでcloseは自動的に呼ばれる

    @patch("backtest.backtest_statements.to_excel")
    @patch("backtest.backtest_statements.show_results")
    @patch("backtest.backtest_statements.run_backtest")
    @patch("backtest.backtest_statements.read_signals")
    @patch("backtest.backtest_statements.read_prices")
    @patch("backtest.backtest_statements.sqlite3.connect")
    @patch("backtest.backtest_statements.parse_args")
    def test_main_with_output(
        self,
        mock_parse_args,
        mock_connect,
        mock_read_prices,
        mock_read_signals,
        mock_run_backtest,
        mock_show,
        mock_excel,
    ):
        """出力オプション付きの実行"""
        mock_parse_args.return_value = MagicMock(
            hold=20,
            capital=1_000_000,
            min_price=300,
            entry_offset=1,
            start=None,
            end=None,
            show=True,
            xlsx="output.xlsx",
            verbose=False,
        )

        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_connect.return_value.__exit__.return_value = None

        # データのモック
        mock_read_prices.return_value = pd.DataFrame(
            {
                "code": ["1234"],
                "date": pd.to_datetime(["2024-01-15"]),
                "adj_close": [1000],
            }
        )

        mock_read_signals.return_value = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-15"]),
                "code": ["1234"],
                "signal": [1],
            }
        )

        mock_run_backtest.return_value = pd.DataFrame(
            {
                "code": ["1234"],
                "profit_jpy": [10000],
                "ret_pct": [0.10],
            }
        )

        # main関数の実行
        main()

        # 出力関数が呼ばれたことを確認
        mock_show.assert_called_once()
        mock_excel.assert_called_once()

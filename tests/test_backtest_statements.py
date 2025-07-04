#!/usr/bin/env python
"""
ファンダメンタルバックテストモジュール (backtest/backtest_statements.py) のテスト

テスト対象:
- シグナル日付からエントリー日計算
- 売買ロジック（資金管理、株数計算）
- バックテスト結果の計算（リターン、勝率等）
- Excel/JSON出力機能
- CLI引数処理
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from backtest import backtest_statements


@pytest.fixture
def backtest_db():
    """バックテスト用のテストデータベース"""
    import os

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)

    # prices テーブル作成
    conn.execute(
        """
        CREATE TABLE prices (
            code TEXT,
            date TEXT,
            adj_close REAL,
            PRIMARY KEY (code, date)
        )
    """
    )

    # fundamental_signals テーブル作成
    conn.execute(
        """
        CREATE TABLE fundamental_signals (
            code TEXT,
            DisclosedAt TEXT,
            turnaround INTEGER,
            cf_quality INTEGER,
            treasury_delta_ok INTEGER,
            eta_delta_ok INTEGER,
            PRIMARY KEY (code, DisclosedAt)
        )
    """
    )

    # listed_info テーブル作成
    conn.execute(
        """
        CREATE TABLE listed_info (
            code TEXT PRIMARY KEY,
            market_code TEXT,
            comp_name TEXT
        )
    """
    )

    conn.commit()
    conn.close()

    yield db_path

    os.unlink(db_path)


class TestResultPaths:
    """ファイルパス生成のテスト"""

    def test_result_paths_format(self):
        """タイムスタンプ付きファイルパス生成のテスト"""
        with mock.patch(
            "backtest.backtest_statements.get_timestamped_output_path"
        ) as mock_get_path:
            mock_get_path.side_effect = [
                Path("data/output/backtest/test_prefix_20240601_120000.xlsx"),
                Path("data/output/backtest/test_prefix_20240601_120000.json"),
            ]

            xlsx_path, json_path = backtest_statements._result_paths("test_prefix")

            assert xlsx_path == Path(
                "data/output/backtest/test_prefix_20240601_120000.xlsx"
            )
            assert json_path == Path(
                "data/output/backtest/test_prefix_20240601_120000.json"
            )

            # Verify the calls
            assert mock_get_path.call_count == 2
            mock_get_path.assert_any_call("backtest", "test_prefix", ".xlsx")
            mock_get_path.assert_any_call("backtest", "test_prefix", ".json")


class TestTradeOperations:
    """売買操作関連のテスト"""

    def test_add_n_trading_days(self, backtest_db):
        """営業日加算のテスト"""
        # 取引日カレンダーの作成
        dates = pd.bdate_range(start="2024-01-01", end="2024-02-01")
        calendar = pd.DatetimeIndex(dates)

        # テストデータ
        s = pd.Series(
            [
                pd.Timestamp("2024-01-10"),
                pd.Timestamp("2024-01-15"),
                pd.Timestamp("2024-01-25"),
            ]
        )

        # 1営業日後のテスト
        result = backtest_statements.add_n_trading_days(s, 1, calendar)
        assert result[0] == pd.Timestamp("2024-01-11")
        assert result[1] == pd.Timestamp("2024-01-16")
        assert result[2] == pd.Timestamp("2024-01-26")

        # 5営業日後のテスト
        result = backtest_statements.add_n_trading_days(s, 5, calendar)
        assert result[0] == pd.Timestamp("2024-01-17")
        assert result[1] == pd.Timestamp("2024-01-22")
        assert result[2] == pd.Timestamp("2024-02-01")

        # 範囲外の場合（最終日に丸められる）
        s_end = pd.Series([pd.Timestamp("2024-01-31")])
        result = backtest_statements.add_n_trading_days(s_end, 10, calendar)
        assert result[0] == pd.Timestamp("2024-02-01")  # 最終日

    def test_run_backtest_basic(self, backtest_db):
        """基本的なバックテストのテスト"""
        conn = sqlite3.connect(backtest_db)

        # 価格データ（LocalCodeとtrade_dateでインデックス）
        dates = pd.bdate_range(start="2024-01-01", end="2024-03-01")
        for date in dates:
            conn.execute(
                "INSERT INTO prices VALUES ('1234', ?, ?)",
                (date.strftime("%Y-%m-%d"), 1000 + (date - dates[0]).days * 10),
            )
        conn.commit()

        # 価格データの読み込み
        prices = backtest_statements.read_prices(conn)

        # シグナルデータ
        signals = pd.DataFrame(
            [
                {"code": "1234", "DisclosedAt": pd.Timestamp("2024-01-10")},
                {"code": "1234", "DisclosedAt": pd.Timestamp("2024-02-01")},
            ]
        )

        # バックテスト実行
        trades = backtest_statements.run_backtest(
            prices, signals, hold=20, offset=1, capital=1000000, min_price=100
        )

        assert len(trades) == 2

        # 最初のトレードの確認
        trade1 = trades.iloc[0]
        assert trade1["code"] == "1234"
        assert trade1["shares"] > 0
        assert trade1["ret_pct"] > 0  # 上昇トレンドなので利益

        conn.close()

    def test_run_backtest_min_price_filter(self, backtest_db):
        """最低価格フィルターのテスト"""
        conn = sqlite3.connect(backtest_db)

        # 価格データ（最低価格以下）
        dates = pd.bdate_range(start="2024-01-01", end="2024-02-01")
        for date in dates:
            conn.execute(
                "INSERT INTO prices VALUES ('1234', ?, 100)",  # 最低価格以下
                (date.strftime("%Y-%m-%d"),),
            )
        conn.commit()

        prices = backtest_statements.read_prices(conn)
        signals = pd.DataFrame(
            [{"code": "1234", "DisclosedAt": pd.Timestamp("2024-01-10")}]
        )

        # 最低価格300円でバックテスト
        trades = backtest_statements.run_backtest(
            prices, signals, hold=20, offset=1, capital=1000000, min_price=300
        )

        # 最低価格以下なので取引なし
        assert len(trades) == 0

        conn.close()


class TestBacktestMetrics:
    """バックテスト指標計算のテスト"""

    def test_summarize_profitable(self):
        """利益が出る取引のサマリーテスト"""
        trades_df = pd.DataFrame(
            [
                {"profit_jpy": 100000, "ret_pct": 0.10},
                {"profit_jpy": 50000, "ret_pct": 0.05},
                {"profit_jpy": -20000, "ret_pct": -0.02},
                {"profit_jpy": 80000, "ret_pct": 0.08},
            ]
        )

        summary = backtest_statements.summarize(trades_df)

        # サマリーの構造確認
        assert "metric" in summary.columns
        assert "value" in summary.columns

        # 指標の確認
        metrics_dict = dict(zip(summary["metric"], summary["value"], strict=False))
        assert metrics_dict["trades"] == 4
        assert metrics_dict["total_profit"] == 210000
        assert metrics_dict["win_rate"] == 0.75
        assert metrics_dict["avg_ret_pct"] == pytest.approx(0.0525, rel=1e-4)
        assert "sharpe" in metrics_dict

    def test_summarize_all_losses(self):
        """全て損失の場合のサマリーテスト"""
        trades_df = pd.DataFrame(
            [
                {"profit_jpy": -50000, "ret_pct": -0.05},
                {"profit_jpy": -100000, "ret_pct": -0.10},
                {"profit_jpy": -30000, "ret_pct": -0.03},
            ]
        )

        summary = backtest_statements.summarize(trades_df)
        metrics_dict = dict(zip(summary["metric"], summary["value"], strict=False))

        assert metrics_dict["win_rate"] == 0.0
        assert metrics_dict["total_profit"] == -180000
        assert metrics_dict["avg_ret_pct"] < 0

    def test_summarize_empty(self):
        """取引がない場合のサマリーテスト"""
        trades_df = pd.DataFrame(columns=["profit_jpy", "ret_pct"])

        summary = backtest_statements.summarize(trades_df)
        metrics_dict = dict(zip(summary["metric"], summary["value"], strict=False))

        assert metrics_dict["trades"] == 0
        assert metrics_dict["total_profit"] == 0.0


class TestOutputFormats:
    """出力フォーマットのテスト"""

    def test_ascii_bar_chart(self):
        """ASCIIバーチャート生成のテスト"""
        values = [100000, -50000, 200000, -30000, 150000]

        chart = backtest_statements._ascii_bar_chart(values)

        lines = chart.split("\n")
        assert len(lines) == 5
        assert "1" in lines[0]
        assert "+100000" in lines[0]
        assert "-50000" in lines[1]

    def test_ascii_bar_chart_empty(self):
        """空のリストでのASCIIバーチャートテスト"""
        chart = backtest_statements._ascii_bar_chart([])
        assert chart == ""

    @mock.patch("builtins.print")
    def test_show_results(self, mock_print):
        """結果表示機能のテスト"""
        trades = pd.DataFrame([{"profit_jpy": 100000}, {"profit_jpy": -50000}])
        summary = pd.DataFrame(
            {"metric": ["trades", "total_profit"], "value": [2, 50000]}
        )

        backtest_statements.show_results(trades, summary)

        # print が呼ばれることを確認
        assert mock_print.call_count >= 2
        calls = [str(call) for call in mock_print.call_args_list]
        assert any("=== Summary ===" in call for call in calls)
        assert any("=== Profit per Trade ===" in call for call in calls)

    @mock.patch("builtins.print")
    def test_show_results_empty_trades(self, mock_print):
        """空の取引データでの結果表示テスト"""
        trades = pd.DataFrame()
        summary = pd.DataFrame({"metric": ["trades"], "value": [0]})

        backtest_statements.show_results(trades, summary)

        calls = [str(call) for call in mock_print.call_args_list]
        assert any("=== Summary ===" in call for call in calls)
        # 空の取引データの場合はチャートは表示されない
        assert not any("=== Profit per Trade ===" in call for call in calls)

    def test_to_excel(self, tmp_path):
        """Excel出力機能のテスト"""
        trades = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedAt": "2024-01-10",
                    "entry_date": "2024-01-11",
                    "exit_date": "2024-02-11",
                    "entry_px": 1000,
                    "exit_px": 1100,
                    "shares": 1000,
                    "profit_jpy": 100000,
                    "ret_pct": 0.10,
                }
            ]
        )

        summary = pd.DataFrame(
            {
                "metric": ["trades", "total_profit", "win_rate"],
                "value": [1, 100000, 1.0],
            }
        )

        xlsx_path = tmp_path / "test_output.xlsx"

        # Excel出力実行
        backtest_statements.to_excel(trades, summary, str(xlsx_path))

        # ファイルが作成されることを確認
        assert xlsx_path.exists()

        # ファイルサイズが0より大きいことを確認
        assert xlsx_path.stat().st_size > 0

    def test_parse_args_function(self):
        """parse_args関数のテスト"""
        # 実際のparse_args関数をテスト
        args = backtest_statements.parse_args(
            ["--db", "/tmp/test.db", "--hold", "30", "--capital", "2000000", "--show"]
        )

        assert args.db == "/tmp/test.db"
        assert args.hold == 30
        assert args.capital == 2000000
        assert args.show is True


class TestCLI:
    """コマンドライン引数のテスト"""

    def test_parse_args_basic(self):
        """基本的な引数パースのテスト"""
        parser = argparse.ArgumentParser()
        parser.add_argument("--db", default="stock.db")
        parser.add_argument("--capital", type=int, default=1000000)
        parser.add_argument("--hold", type=int, default=40)
        parser.add_argument("--entry-offset", type=int, default=0)
        parser.add_argument("--start")
        parser.add_argument("--end")
        parser.add_argument("--xlsx")
        parser.add_argument("--json")
        parser.add_argument("--show", action="store_true")
        parser.add_argument("--min-price", type=float, default=300)
        parser.add_argument("-v", "--verbose", action="store_true")

        args = parser.parse_args(
            [
                "--db",
                "/tmp/test.db",
                "--capital",
                "2000000",
                "--hold",
                "30",
                "--entry-offset",
                "1",
                "--start",
                "2024-01-01",
                "--end",
                "2024-12-31",
                "--xlsx",
                "output.xlsx",
                "--json",
                "output.json",
                "--show",
                "--min-price",
                "500",
                "-v",
            ]
        )

        assert args.db == "/tmp/test.db"
        assert args.capital == 2000000
        assert args.hold == 30
        assert args.entry_offset == 1
        assert args.start == "2024-01-01"
        assert args.end == "2024-12-31"
        assert args.xlsx == "output.xlsx"
        assert args.json == "output.json"
        assert args.show is True
        assert args.min_price == 500
        assert args.verbose is True


class TestMainFunction:
    """メイン関数のテスト"""

    @mock.patch("backtest.backtest_statements.sqlite3.connect")
    @mock.patch("backtest.backtest_statements.read_prices")
    @mock.patch("backtest.backtest_statements.read_signals")
    @mock.patch("backtest.backtest_statements.run_backtest")
    @mock.patch("backtest.backtest_statements.summarize")
    @mock.patch("backtest.backtest_statements.to_excel")
    @mock.patch("backtest.backtest_statements.show_results")
    @mock.patch("builtins.print")
    def test_main_function_with_show_option(
        self,
        mock_print,
        mock_show_results,
        mock_to_excel,
        mock_summarize,
        mock_run_backtest,
        mock_read_signals,
        mock_read_prices,
        mock_connect,
    ):
        """--showオプション付きのmain関数テスト"""
        # モックデータ設定
        mock_prices = pd.DataFrame(
            {"adj_close": [1000, 1100]},
            index=pd.MultiIndex.from_tuples(
                [
                    ("1234", pd.Timestamp("2024-01-01")),
                    ("1234", pd.Timestamp("2024-01-02")),
                ],
                names=["Code", "trade_date"],
            ),
        )
        mock_signals = pd.DataFrame(
            [{"code": "1234", "DisclosedAt": pd.Timestamp("2024-01-10")}]
        )
        mock_trades = pd.DataFrame(
            [{"code": "1234", "profit_jpy": 100000, "ret_pct": 0.10}]
        )
        mock_summary = pd.DataFrame(
            [
                {"metric": "trades", "value": 1},
                {"metric": "total_profit", "value": 100000},
            ]
        )

        mock_read_prices.return_value = mock_prices
        mock_read_signals.return_value = mock_signals
        mock_run_backtest.return_value = mock_trades
        mock_summarize.return_value = mock_summary

        # メイン関数をモック引数で実行
        import sys

        original_argv = sys.argv
        try:
            sys.argv = ["backtest_statements.py", "--show", "--db", "test.db"]
            backtest_statements.main()
        finally:
            sys.argv = original_argv

        # 結果表示関数が呼ばれることを確認
        mock_show_results.assert_called_once_with(mock_trades, mock_summary)

    @mock.patch("backtest.backtest_statements.sqlite3.connect")
    @mock.patch("backtest.backtest_statements.read_prices")
    @mock.patch("backtest.backtest_statements.read_signals")
    @mock.patch("backtest.backtest_statements.logger")
    def test_main_function_no_signals(
        self, mock_logger, mock_read_signals, mock_read_prices, mock_connect
    ):
        """シグナルがない場合のmain関数テスト"""
        # 空のシグナルデータ
        mock_read_signals.return_value = pd.DataFrame()
        mock_read_prices.return_value = pd.DataFrame()

        import sys

        original_argv = sys.argv
        try:
            sys.argv = [
                "backtest_statements.py",
                "--db",
                "test.db",
                "--start",
                "2024-01-01",
            ]
            with pytest.raises(SystemExit):
                backtest_statements.main()
        finally:
            sys.argv = original_argv

        # 警告ログが出力されることを確認
        mock_logger.warning.assert_called_with("No signals to back‑test.")

    @mock.patch("sys.argv", ["backtest_statements.py", "--help"])
    def test_main_function_help(self):
        """ヘルプオプションのテスト"""
        with pytest.raises(SystemExit):
            backtest_statements.main()


class TestIntegration:
    """統合テスト"""

    def create_test_data(self, conn):
        """テストデータの作成"""
        # 価格データ（3ヶ月分）
        dates = pd.bdate_range(start="2024-01-01", end="2024-03-31")

        for code in ["1234", "5678"]:
            for i, date in enumerate(dates):
                # 銘柄ごとに異なる価格動向
                if code == "1234":
                    price = 1000 + i * 10  # 上昇トレンド
                else:
                    price = 2000 - i * 5  # 下降トレンド

                conn.execute(
                    "INSERT INTO prices VALUES (?, ?, ?)",
                    (code, date.strftime("%Y-%m-%d"), price),
                )

        # シグナルデータ（LocalCodeとDisclosedAtカラム）
        signals = [
            ("1234", "2024-01-15 10:00:00"),
            ("1234", "2024-02-15 10:00:00"),
            ("5678", "2024-01-20 10:00:00"),
        ]
        for code, disclosed_date in signals:
            conn.execute(
                "INSERT INTO fundamental_signals (code, DisclosedAt, turnaround, cf_quality, treasury_delta_ok, eta_delta_ok) VALUES (?, ?, 1, 1, 1, 1)",
                (code, disclosed_date),
            )

        conn.commit()

    def test_full_workflow(self, backtest_db):
        """完全なワークフローのテスト"""
        conn = sqlite3.connect(backtest_db)

        # テストデータ作成
        self.create_test_data(conn)

        # 1. 価格データ読み込み
        prices = backtest_statements.read_prices(conn)
        assert not prices.empty
        assert prices.index.names == ["code", "trade_date"]

        # 2. シグナル読み込み
        signals = backtest_statements.read_signals(conn, "2024-01-01", "2024-03-31")
        assert not signals.empty
        assert "code" in signals.columns
        assert "DisclosedAt" in signals.columns

        # 3. バックテスト実行
        trades = backtest_statements.run_backtest(
            prices, signals, hold=20, offset=1, capital=1000000, min_price=300
        )

        assert len(trades) > 0
        assert "code" in trades.columns
        assert "ret_pct" in trades.columns
        assert "profit_jpy" in trades.columns

        # 4. サマリー作成
        summary = backtest_statements.summarize(trades)
        assert not summary.empty

        conn.close()

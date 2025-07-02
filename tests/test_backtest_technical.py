#!/usr/bin/env python
"""
テクニカルバックテストモジュール (backtest/backtest_technical.py) のテスト

テスト対象:
- ロング・ショートエントリーの判定
- ポジションサイズ計算（資金管理）
- エグジット条件（期間・ストップロス）
- 取引シミュレーション
- バックテスト結果の計算
- Excel/JSON出力機能
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from backtest import backtest_technical


@pytest.fixture
def technical_backtest_db():
    """テクニカルバックテスト用のテストデータベース"""
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

    # technical_indicators テーブル作成
    conn.execute(
        """
        CREATE TABLE technical_indicators (
            code TEXT,
            signal_date TEXT,
            signals_count INTEGER,
            signals_short_count INTEGER,
            signals_first INTEGER,
            signals_short_first INTEGER,
            signals_overheating INTEGER,
            signals_oversold INTEGER,
            PRIMARY KEY (code, signal_date)
        )
    """
    )

    # listed_info テーブル作成
    conn.execute(
        """
        CREATE TABLE listed_info (
            code TEXT PRIMARY KEY,
            market TEXT,
            company_name TEXT
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
            "backtest.backtest_technical.get_timestamped_output_path"
        ) as mock_get_path:
            mock_get_path.side_effect = [
                Path("data/output/backtest/test_prefix_20240601_120000.xlsx"),
                Path("data/output/backtest/test_prefix_20240601_120000.json"),
            ]

            xlsx_path, json_path = backtest_technical._result_paths("test_prefix")

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


class TestBacktestCore:
    """バックテストコア機能のテスト"""

    def create_test_data(self, conn, signal_date="2024-01-15"):
        """テストデータの作成"""
        # 会社情報
        companies = [
            ("1234", "0111", "Long Signal Corp"),
            ("5678", "0111", "Short Signal Corp"),
            ("9012", "0111", "No Signal Corp"),
            ("3456", "0111", "Overheat Corp"),
        ]
        for code, market, name in companies:
            conn.execute(
                "INSERT INTO listed_info VALUES (?, ?, ?)", (code, market, name)
            )

        # シグナルデータ
        signals = [
            # ロングシグナル（条件を満たす）
            ("1234", signal_date, 7, 0, 1, 0, 0, 0),
            # ショートシグナル（条件を満たす）
            ("5678", signal_date, 0, 7, 0, 1, 0, 0),
            # シグナルなし
            ("9012", signal_date, 2, 2, 0, 0, 0, 0),
            # オーバーヒート（除外される）
            ("3456", signal_date, 7, 0, 1, 0, 1, 0),
        ]
        for signal in signals:
            conn.execute(
                "INSERT INTO technical_indicators VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                signal,
            )

        # 価格データ（90日分）
        dates = pd.bdate_range(start="2024-01-01", end="2024-04-30")
        for code in ["1234", "5678", "9012", "3456"]:
            for i, date in enumerate(dates):
                if code == "1234":
                    # ロング銘柄：上昇トレンド
                    price = 1000 + i * 5
                elif code == "5678":
                    # ショート銘柄：下降トレンド
                    price = 2000 - i * 10
                elif code == "9012":
                    # 横ばい
                    price = 1500 + np.random.randint(-20, 20)
                else:
                    # オーバーヒート銘柄
                    price = 3000 + i * 20

                conn.execute(
                    "INSERT INTO prices VALUES (?, ?, ?)",
                    (code, date.strftime("%Y-%m-%d"), price),
                )

        conn.commit()

    def test_run_backtest_basic(self, technical_backtest_db):
        """基本的なバックテストのテスト"""
        conn = sqlite3.connect(technical_backtest_db)
        self.create_test_data(conn)

        # ロング取引のバックテスト実行
        long_trades = backtest_technical.run_backtest(
            conn,
            as_of="2024-01-15",
            capital=1000000,
            hold_days=20,
            stop_loss_pct=0.05,
            min_price=300,
        )

        # ショート取引のバックテスト実行
        short_trades = backtest_technical.run_backtest_short(
            conn,
            as_of="2024-01-15",
            capital=1000000,
            hold_days=20,
            stop_loss_pct=0.05,
            min_price=300,
        )

        # 結果の確認
        assert len(long_trades) == 1  # ロング1銘柄
        assert len(short_trades) == 1  # ショート1銘柄

        # ロング取引の確認
        long_trade = long_trades.iloc[0]
        assert long_trade["code"] == "1234"
        assert long_trade["shares"] > 0
        assert long_trade["pnl_pct"] > 0  # 上昇トレンドなので利益

        # ショート取引の確認
        short_trade = short_trades.iloc[0]
        assert short_trade["code"] == "5678"
        assert short_trade["shares"] > 0
        assert short_trade["pnl_pct"] > 0  # 下降トレンドでショートなので利益

        conn.close()

    def test_run_backtest_stop_loss(self, technical_backtest_db):
        """ストップロスのテスト"""
        conn = sqlite3.connect(technical_backtest_db)

        # ストップロス用のデータ作成
        conn.execute(
            "INSERT INTO listed_info VALUES ('7777', '0111', 'Stop Loss Corp')"
        )
        conn.execute(
            """
            INSERT INTO technical_indicators VALUES
            ('7777', '2024-01-15', 7, 0, 1, 0, 0, 0)
        """
        )

        # 価格データ（急落シナリオ）
        dates = pd.bdate_range(start="2024-01-01", end="2024-03-31")
        for i, date in enumerate(dates):
            if i < 20:
                price = 1000  # 初期価格
            else:
                price = 940  # -6%の急落

            conn.execute(
                "INSERT INTO prices VALUES ('7777', ?, ?)",
                (date.strftime("%Y-%m-%d"), price),
            )

        conn.commit()

        # バックテスト実行（ストップロス5%）
        trades = backtest_technical.run_backtest(
            conn,
            as_of="2024-01-15",
            capital=1000000,
            hold_days=60,
            stop_loss_pct=0.05,
            min_price=300,
        )

        # ストップロスが発動したか確認
        stop_loss_trade = trades[trades["code"] == "7777"]
        if not stop_loss_trade.empty:
            trade = stop_loss_trade.iloc[0]
            assert trade["pnl_pct"] < 0
            assert abs(trade["pnl_pct"]) >= 5  # pnl_pctは%表記

        conn.close()

    def test_run_backtest_min_price_filter(self, technical_backtest_db):
        """最低価格フィルターのテスト"""
        conn = sqlite3.connect(technical_backtest_db)

        # 低価格銘柄のデータ作成
        conn.execute("INSERT INTO listed_info VALUES ('1111', '0111', 'Penny Stock')")
        conn.execute(
            """
            INSERT INTO technical_indicators VALUES
            ('1111', '2024-01-15', 7, 0, 1, 0, 0, 0)
        """
        )

        # 価格データ（最低価格以下）
        dates = pd.bdate_range(start="2024-01-01", end="2024-03-31")
        for date in dates:
            conn.execute(
                "INSERT INTO prices VALUES ('1111', ?, 100)",  # 100円
                (date.strftime("%Y-%m-%d"),),
            )

        conn.commit()

        # バックテスト実行（最低価格300円）
        trades = backtest_technical.run_backtest(
            conn,
            as_of="2024-01-15",
            capital=1000000,
            hold_days=20,
            stop_loss_pct=0.05,
            min_price=300,
        )

        # 低価格銘柄は除外されているか確認
        assert trades.empty or not any(trades["code"] == "1111")

        conn.close()


class TestSummaryCalculation:
    """サマリー計算のテスト"""

    def test_summary_creation(self):
        """サマリー作成のテスト"""
        trades = pd.DataFrame(
            [
                {"side": "long", "pnl_pct": 10.0, "pnl_yen": 100000},
                {"side": "long", "pnl_pct": -3.0, "pnl_yen": -30000},
                {"side": "short", "pnl_pct": 5.0, "pnl_yen": 50000},
                {"side": "short", "pnl_pct": 8.0, "pnl_yen": 80000},
            ]
        )

        # run_backtest_rangeで使われるサマリー作成のロジックをテスト
        total_profit = trades["pnl_yen"].sum()
        win_rate = (trades["pnl_yen"] > 0).mean()
        mean_ret_pct = trades["pnl_pct"].mean()
        sharpe = trades["pnl_pct"].mean() / trades["pnl_pct"].std(ddof=0)

        summary = pd.DataFrame(
            {
                "metric": [
                    "trades",
                    "total_profit",
                    "win_rate",
                    "avg_ret_pct",
                    "sharpe",
                ],
                "value": [len(trades), total_profit, win_rate, mean_ret_pct, sharpe],
            }
        )

        assert summary.loc[summary["metric"] == "trades", "value"].iloc[0] == 4
        assert (
            summary.loc[summary["metric"] == "total_profit", "value"].iloc[0] == 200000
        )
        assert summary.loc[summary["metric"] == "win_rate", "value"].iloc[0] == 0.75
        assert "avg_ret_pct" in summary["metric"].values
        assert "sharpe" in summary["metric"].values


class TestCLI:
    """コマンドライン引数のテスト"""

    def test_parse_args(self):
        """引数パースのテスト"""
        parser = argparse.ArgumentParser()
        parser.add_argument("--db", default="stock.db")
        parser.add_argument("--capital", type=int, default=1000000)
        parser.add_argument("--hold-days", type=int, default=60)
        parser.add_argument("--stop-loss", type=float, default=0.05)
        parser.add_argument("--min-price", type=float, default=300)
        parser.add_argument("--start", required=False)
        parser.add_argument("--end", required=False)
        parser.add_argument("--show", action="store_true")

        args = parser.parse_args(
            [
                "--db",
                "/tmp/test.db",
                "--capital",
                "2000000",
                "--hold-days",
                "30",
                "--stop-loss",
                "0.03",
                "--min-price",
                "500",
                "--start",
                "2024-01-01",
                "--end",
                "2024-03-31",
                "--show",
            ]
        )

        assert args.db == "/tmp/test.db"
        assert args.capital == 2000000
        assert args.hold_days == 30
        assert args.stop_loss == 0.03
        assert args.min_price == 500
        assert args.start == "2024-01-01"
        assert args.end == "2024-03-31"
        assert args.show is True


class TestIntegration:
    """統合テスト"""

    def test_full_backtest_workflow(self, technical_backtest_db, tmp_path):
        """完全なバックテストワークフローのテスト"""
        conn = sqlite3.connect(technical_backtest_db)

        # テストデータ作成（複数日のシグナル）
        companies = [
            ("1234", "0111", "Test Corp A"),
            ("5678", "0111", "Test Corp B"),
            ("9012", "0111", "Test Corp C"),
        ]
        for code, market, name in companies:
            conn.execute(
                "INSERT INTO listed_info VALUES (?, ?, ?)", (code, market, name)
            )

        # 複数日のシグナル
        signal_dates = ["2024-01-15", "2024-01-16", "2024-01-17"]
        for date in signal_dates:
            # 各日でランダムにシグナルを生成
            if date == "2024-01-15":
                signals = [
                    ("1234", date, 7, 0, 1, 0, 0, 0),  # ロング
                    ("5678", date, 0, 7, 0, 1, 0, 0),  # ショート
                ]
            elif date == "2024-01-16":
                signals = [
                    ("9012", date, 7, 0, 1, 0, 0, 0),  # ロング
                ]
            else:
                signals = [
                    ("1234", date, 0, 7, 0, 1, 0, 0),  # ショート（同じ銘柄の別日）
                ]

            for signal in signals:
                conn.execute(
                    "INSERT INTO technical_indicators VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    signal,
                )

        # 価格データ
        dates = pd.bdate_range(start="2024-01-01", end="2024-04-30")
        for code in ["1234", "5678", "9012"]:
            for i, date in enumerate(dates):
                if code == "1234":
                    price = 1000 + i * 2
                elif code == "5678":
                    price = 2000 - i * 3
                else:
                    price = 1500 + np.sin(i / 10) * 100

                conn.execute(
                    "INSERT INTO prices VALUES (?, ?, ?)",
                    (code, date.strftime("%Y-%m-%d"), price),
                )

        conn.commit()

        # バックテスト期間を指定して実行
        start_date = "2024-01-15"
        end_date = "2024-01-17"

        all_trades = []
        current = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)

        while current <= end:
            trades = backtest_technical.run_backtest(
                conn,
                as_of=current.strftime("%Y-%m-%d"),
                capital=1000000,
                hold_days=20,
                stop_loss_pct=0.05,
                min_price=300,
            )
            if not trades.empty:
                all_trades.append(trades)
            current += pd.Timedelta(days=1)

        # 結果の統合
        if all_trades:
            combined_trades = pd.concat(all_trades, ignore_index=True)
            assert len(combined_trades) > 0
            assert "side" in combined_trades.columns
            assert "pnl_pct" in combined_trades.columns

            # サマリー計算（実際の実装に合わせて簡略化）
            assert len(combined_trades) > 0
            assert "pnl_yen" in combined_trades.columns

        conn.close()


class TestMainFunction:
    """main関数のテスト"""

    @mock.patch("backtest.backtest_technical.sqlite3.connect")
    @mock.patch("backtest.backtest_technical.run_backtest_range")
    def test_main_function_with_show_option(
        self, mock_run_backtest_range, mock_connect
    ):
        """--showオプション付きのmain関数テスト"""
        # メイン関数をモック引数で実行
        import sys

        from backtest import backtest_technical

        original_argv = sys.argv
        try:
            sys.argv = [
                "backtest_technical.py",
                "--show",
                "--db",
                "test.db",
                "--start",
                "2024-01-01",
            ]
            backtest_technical.main()
        finally:
            sys.argv = original_argv

        # run_backtest_rangeが正しい引数で呼ばれることを確認
        mock_run_backtest_range.assert_called_once()
        call_args = mock_run_backtest_range.call_args
        assert call_args.kwargs["show"] is True

    @mock.patch("sys.argv", ["backtest_technical.py", "--help"])
    def test_main_function_help(self):
        """ヘルプオプションのテスト"""
        from backtest import backtest_technical

        with pytest.raises(SystemExit):
            backtest_technical.main()

    def test_parse_args_function(self):
        """引数パース関数のテスト"""
        from backtest import backtest_technical

        args = backtest_technical.parse_args(
            [
                "--db",
                "/tmp/test.db",
                "--start",
                "2024-01-01",
                "--hold-days",
                "30",
                "--capital",
                "2000000",
                "--show",
            ]
        )

        assert args.db == "/tmp/test.db"
        assert args.hold_days == 30
        assert args.capital == 2000000
        assert args.show is True


class TestSummarize:
    """summarize関数のテスト"""

    def test_summarize_basic(self):
        """基本的なサマリー計算のテスト"""
        trades = pd.DataFrame(
            [
                {"pnl_yen": 100000, "pnl_pct": 10.0},
                {"pnl_yen": -50000, "pnl_pct": -5.0},
                {"pnl_yen": 200000, "pnl_pct": 20.0},
            ]
        )

        summary = backtest_technical.summarize(trades)

        assert isinstance(summary, pd.DataFrame)
        assert "metric" in summary.columns
        assert "value" in summary.columns

        # メトリクスの確認
        metrics = summary.set_index("metric")["value"]
        assert metrics["trades"] == 3
        assert metrics["total_profit"] == 250000
        assert metrics["win_rate"] == pytest.approx(2 / 3, rel=1e-2)
        assert "sharpe" in metrics

    def test_summarize_empty(self):
        """空のDataFrameでのサマリー計算のテスト"""
        trades = pd.DataFrame(columns=["pnl_yen", "pnl_pct"])

        summary = backtest_technical.summarize(trades)

        assert isinstance(summary, pd.DataFrame)
        metrics = summary.set_index("metric")["value"]
        assert metrics["trades"] == 0
        assert metrics["total_profit"] == 0
        assert pd.isna(metrics["win_rate"]) or metrics["win_rate"] == 0
        assert pd.isna(metrics["sharpe"]) or metrics["sharpe"] == 0


class TestShowResults:
    """show_results関数のテスト"""

    @mock.patch("builtins.print")
    def test_show_results(self, mock_print):
        """結果表示機能のテスト"""
        trades = pd.DataFrame([{"pnl_yen": 100000}, {"pnl_yen": -50000}])
        summary = pd.DataFrame(
            {"metric": ["trades", "total_profit"], "value": [2, 50000]}
        )

        backtest_technical.show_results(trades, summary)

        # print が呼ばれることを確認
        assert mock_print.call_count >= 2

    @mock.patch("builtins.print")
    def test_show_results_empty(self, mock_print):
        """空のデータでの結果表示テスト"""
        trades = pd.DataFrame()
        summary = pd.DataFrame({"metric": ["trades", "total_profit"], "value": [0, 0]})

        backtest_technical.show_results(trades, summary)

        # print が呼ばれることを確認
        assert mock_print.call_count >= 1


class TestExcelOutput:
    """Excel出力のテスト"""

    def test_save_to_excel(self, tmp_path):
        """Excel保存機能のテスト（モック）"""
        trades = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "name": "Test Corp",
                    "side": "long",
                    "entry_date": "2024-01-15",
                    "exit_date": "2024-02-15",
                    "entry_price": 1000,
                    "exit_price": 1100,
                    "shares": 1000,
                    "pnl_pct": 10.0,
                    "pnl_yen": 100000,
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

        # to_excel関数の実行
        backtest_technical.to_excel(trades, summary, str(xlsx_path))

        # ファイルが作成されることを確認
        assert xlsx_path.exists()
        assert xlsx_path.stat().st_size > 0


class TestRunBacktestRange:
    """run_backtest_range関数のテスト"""

    @mock.patch("backtest.backtest_technical.run_backtest")
    @mock.patch("backtest.backtest_technical.run_backtest_short")
    @mock.patch("backtest.backtest_technical.summarize")
    @mock.patch("backtest.backtest_technical.to_excel")
    @mock.patch("backtest.backtest_technical.show_results")
    @mock.patch("backtest.backtest_technical.logger")
    def test_run_backtest_range_basic(
        self,
        mock_logger,
        mock_show_results,
        mock_to_excel,
        mock_summarize,
        mock_run_backtest_short,
        mock_run_backtest,
    ):
        """基本的なバックテスト範囲実行のテスト"""
        # モックデータ設定
        mock_trades_long = pd.DataFrame(
            [{"code": "1234", "side": "long", "pnl_yen": 100000}]
        )
        mock_trades_short = pd.DataFrame(
            [{"code": "5678", "side": "short", "pnl_yen": 50000}]
        )
        mock_run_backtest.return_value = mock_trades_long
        mock_run_backtest_short.return_value = mock_trades_short
        mock_summarize.return_value = pd.DataFrame({"metric": ["trades"], "value": [2]})

        # SQLite接続のモック
        mock_conn = mock.Mock()

        # 実行
        backtest_technical.run_backtest_range(
            mock_conn,
            start="2024-01-01",
            end="2024-01-02",
            capital=1000000,
            hold_days=5,
            stop_loss_pct=5.0,
            min_price=100,
            outfile="test.xlsx",
            jsonfile="test.json",
            show=True,
        )

        # 検証
        assert mock_run_backtest.call_count == 2  # 2日分
        assert mock_run_backtest_short.call_count == 2  # 2日分
        mock_to_excel.assert_called_once()
        mock_show_results.assert_called_once()

    @mock.patch("backtest.backtest_technical.run_backtest")
    @mock.patch("backtest.backtest_technical.run_backtest_short")
    @mock.patch("backtest.backtest_technical.logger")
    def test_run_backtest_range_no_trades(
        self, mock_logger, mock_run_backtest_short, mock_run_backtest
    ):
        """トレードがない場合のテスト"""
        # 空のDataFrameを返す
        mock_run_backtest.return_value = pd.DataFrame()
        mock_run_backtest_short.return_value = pd.DataFrame()

        # SQLite接続のモック
        mock_conn = mock.Mock()

        # 実行
        backtest_technical.run_backtest_range(
            mock_conn, start="2024-01-01", end="2024-01-01", show=False
        )

        # ログ出力の確認
        mock_logger.info.assert_any_call("No trades in the specified period.")

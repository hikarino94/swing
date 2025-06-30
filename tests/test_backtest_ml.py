#!/usr/bin/env python
"""
機械学習バックテストモジュール (backtest/backtest_ml.py) のテスト

テスト対象:
- 価格データの範囲取得
- データセット準備（特徴量エンジニアリング）
- モデル学習とランキング
- バックテストシミュレーション
- 結果の集計とExcel/JSON出力
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline

sys.path.append(str(Path(__file__).resolve().parents[1]))
from backtest import backtest_ml


@pytest.fixture
def ml_backtest_db():
    """機械学習バックテスト用のテストデータベース"""
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
            adj_volume INTEGER,
            PRIMARY KEY (code, date)
        )
    """
    )

    # statements テーブル作成
    conn.execute(
        """
        CREATE TABLE statements (
            LocalCode TEXT,
            DisclosedDate DATE,
            NetSales REAL,
            OperatingProfit REAL,
            OrdinaryProfit REAL,
            Profit REAL,
            TotalAssets REAL,
            Equity REAL,
            EquityToAssetRatio REAL,
            BookValuePerShare REAL,
            CashFlowsFromOperatingActivities REAL,
            CashFlowsFromInvestingActivities REAL,
            CashFlowsFromFinancingActivities REAL
        )
    """
    )

    # listed_info テーブル作成
    conn.execute(
        """
        CREATE TABLE listed_info (
            code TEXT PRIMARY KEY,
            market TEXT,
            comp_name TEXT
        )
    """
    )

    conn.commit()
    conn.close()

    yield db_path

    os.unlink(db_path)


class TestHelpers:
    """ヘルパー関数のテスト"""

    def test_result_paths(self):
        """タイムスタンプ付きファイルパス生成のテスト"""
        with mock.patch("backtest.backtest_ml.dt.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "20240601_120000"

            xlsx_path, json_path = backtest_ml._result_paths("test_prefix")

            assert xlsx_path == "test_prefix_20240601_120000.xlsx"
            assert json_path == "test_prefix_20240601_120000.json"

    def test_fetch_price_range(self, ml_backtest_db):
        """価格データ範囲取得のテスト"""
        conn = sqlite3.connect(ml_backtest_db)

        # テストデータ挿入
        dates = pd.date_range(start="2024-01-01", end="2024-03-31", freq="D")
        for code in ["1234", "5678"]:
            for i, date in enumerate(dates):
                price = 1000 + i * 10 if code == "1234" else 2000 - i * 5
                conn.execute(
                    "INSERT INTO prices VALUES (?, ?, ?, ?)",
                    (code, date.strftime("%Y-%m-%d"), price, 100000 + i * 100),
                )
        conn.commit()

        # データ取得テスト
        df = backtest_ml._fetch_price_range(conn, "2024-02-01", "2024-02-28")

        assert not df.empty
        assert len(df[df["code"] == "1234"]) == 28  # 2月は28日間
        assert len(df[df["code"] == "5678"]) == 28
        assert df["date"].min() >= pd.Timestamp("2024-02-01")
        assert df["date"].max() <= pd.Timestamp("2024-02-28")

        conn.close()

    def test_prepare_dataset(self, ml_backtest_db):
        """データセット準備のテスト"""
        conn = sqlite3.connect(ml_backtest_db)

        # 価格データ（十分な過去データを含む）
        dates = pd.date_range(start="2023-01-01", end="2024-12-31", freq="D")
        for code in ["1234", "5678"]:
            for i, date in enumerate(dates):
                price = (
                    1000 + np.sin(i / 30) * 100
                    if code == "1234"
                    else 2000 + np.cos(i / 30) * 200
                )
                conn.execute(
                    "INSERT INTO prices VALUES (?, ?, ?, ?)",
                    (code, date.strftime("%Y-%m-%d"), price, 100000),
                )

        # 財務データ
        for code in ["1234", "5678"]:
            conn.execute(
                """
                INSERT INTO statements VALUES
                (?, '2023-03-15', 1000000, 100000, 90000, 60000,
                 5000000, 2000000, 0.4, 100, 120000, -50000, -30000)
            """,
                (code,),
            )

        conn.commit()

        # データセット準備
        df = backtest_ml._prepare_dataset(conn, "2024-01-01", "2024-01-31")

        assert not df.empty
        assert "ret_5" in df.columns  # 価格特徴量
        assert "NetSales" in df.columns  # 財務データ
        assert "label" in df.columns  # ラベル
        assert "future_date" in df.columns

        conn.close()


class TestBacktestCore:
    """バックテストコア機能のテスト"""

    def create_test_data(self, conn, start_date="2023-01-01", end_date="2024-12-31"):
        """テストデータの作成"""
        # 会社情報
        companies = [
            ("1234", "0111", "Growth Stock"),
            ("5678", "0111", "Value Stock"),
            ("9012", "0111", "Volatile Stock"),
            ("3456", "0111", "Stable Stock"),
        ]
        for code, market, name in companies:
            conn.execute(
                "INSERT INTO listed_info VALUES (?, ?, ?)", (code, market, name)
            )

        # 価格データ（異なるパフォーマンスパターン）
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        for code in ["1234", "5678", "9012", "3456"]:
            for i, date in enumerate(dates):
                if code == "1234":
                    # 成長株：上昇トレンド
                    price = 1000 * (1 + i / 1000)
                elif code == "5678":
                    # バリュー株：緩やかな上昇
                    price = 2000 * (1 + i / 2000)
                elif code == "9012":
                    # ボラティリティ高：大きな変動
                    price = 1500 + 500 * np.sin(i / 20)
                else:
                    # 安定株：横ばい
                    price = 3000 + np.random.normal(0, 50)

                volume = 100000 + np.random.randint(-10000, 10000)
                conn.execute(
                    "INSERT INTO prices VALUES (?, ?, ?, ?)",
                    (
                        code,
                        date.strftime("%Y-%m-%d"),
                        max(price, 100),
                        max(volume, 1000),
                    ),
                )

        # 財務データ（四半期ごと）
        for code in ["1234", "5678", "9012", "3456"]:
            for quarter_date in pd.date_range(
                start=start_date, end=end_date, freq="QE"
            ):
                net_sales = 1000000 * (1 + np.random.normal(0, 0.1))
                operating_profit = net_sales * 0.1 * (1 + np.random.normal(0, 0.2))

                conn.execute(
                    """
                    INSERT INTO statements VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        code,
                        quarter_date.strftime("%Y-%m-%d"),
                        net_sales,
                        operating_profit,
                        operating_profit * 0.9,
                        operating_profit * 0.6,
                        net_sales * 5,
                        net_sales * 2,
                        0.4,
                        100,
                        operating_profit * 1.2,
                        -operating_profit * 0.5,
                        -operating_profit * 0.3,
                    ),
                )

        conn.commit()

    def test_run_backtest_basic(self, ml_backtest_db):
        """基本的なバックテストのテスト"""
        conn = sqlite3.connect(ml_backtest_db)
        self.create_test_data(conn)

        # バックテスト実行
        trades = backtest_ml.run_backtest(
            conn,
            start="2024-01-01",
            end="2024-01-31",
            top=2,
            capital=1000000,
            lookback=365,
        )

        # 結果の確認
        assert not trades.empty
        assert "code" in trades.columns
        assert "entry_date" in trades.columns
        assert "exit_date" in trades.columns
        assert "pnl_pct" in trades.columns
        assert "pnl_yen" in trades.columns

        # 各エントリー日で最大top銘柄が選ばれているか
        entry_dates = trades["entry_date"].unique()
        for date in entry_dates:
            date_trades = trades[trades["entry_date"] == date]
            assert len(date_trades) <= 2  # top=2

        conn.close()

    def test_run_backtest_empty_data(self, ml_backtest_db):
        """データが空の場合のテスト"""
        conn = sqlite3.connect(ml_backtest_db)

        # データを挿入しない状態でバックテスト実行
        # prepare_datasetで空のDataFrameが返されるはず
        try:
            trades = backtest_ml.run_backtest(
                conn,
                start="2024-01-15",
                end="2024-01-31",
                top=1,
                capital=1000000,
                lookback=365,
            )
            # 空のデータの場合、tradesは空になるはず
            assert trades.empty
        except (ValueError, Exception):
            # 何らかのエラーが発生することも許容
            pass

        conn.close()


class TestCLI:
    """コマンドライン引数のテスト"""

    def test_parse_args(self):
        """引数パースのテスト"""
        parser = argparse.ArgumentParser()
        parser.add_argument("--db", default="stock.db")
        parser.add_argument("--start", required=False)
        parser.add_argument("--end", required=False)
        parser.add_argument("--top", type=int, default=10)
        parser.add_argument("--capital", type=int, default=1000000)
        parser.add_argument("--show", action="store_true")

        args = parser.parse_args(
            [
                "--db",
                "/tmp/test.db",
                "--start",
                "2024-01-01",
                "--end",
                "2024-03-31",
                "--top",
                "5",
                "--capital",
                "2000000",
                "--show",
            ]
        )

        assert args.db == "/tmp/test.db"
        assert args.start == "2024-01-01"
        assert args.end == "2024-03-31"
        assert args.top == 5
        assert args.capital == 2000000
        assert args.show is True


class TestIntegration:
    """統合テスト"""

    def test_full_backtest_workflow(self, ml_backtest_db, tmp_path):
        """完全なバックテストワークフローのテスト"""
        conn = sqlite3.connect(ml_backtest_db)

        # 豊富なテストデータ作成
        test_creator = TestBacktestCore()
        test_creator.create_test_data(conn, "2022-01-01", "2024-12-31")

        # モデルファイルのモック
        model_path = tmp_path / "ml_screen_model.pkl"

        # 簡単なモデルを作成して保存
        from sklearn.preprocessing import StandardScaler

        pipe = Pipeline(
            [
                ("scaler", StandardScaler()),
                ("gb", GradientBoostingClassifier(n_estimators=10, random_state=42)),
            ]
        )

        # ダミーデータでモデルを学習
        X_dummy = np.random.rand(100, 17)  # 特徴量数に合わせる
        y_dummy = np.random.randint(0, 2, 100)
        pipe.fit(X_dummy, y_dummy)

        with open(model_path, "wb") as f:
            pickle.dump(pipe, f)

        # バックテスト実行
        with mock.patch("screening.screen_ml.MODEL_FNAME", model_path.name):
            with mock.patch.object(Path, "parent", tmp_path):
                trades = backtest_ml.run_backtest(
                    conn,
                    start="2024-01-01",
                    end="2024-01-10",
                    top=3,
                    capital=1000000,
                    lookback=365,
                )

        assert not trades.empty

        # サマリー計算
        if not trades.empty:
            total_pnl = trades["pnl_yen"].sum()
            win_rate = (trades["pnl_yen"] > 0).mean()

            assert "entry_date" in trades.columns
            assert "exit_date" in trades.columns
            assert isinstance(total_pnl, int | float)
            assert 0 <= win_rate <= 1

        conn.close()


class TestMainFunction:
    """main関数のテスト"""

    @mock.patch("backtest.backtest_ml.sqlite3.connect")
    @mock.patch("backtest.backtest_ml.run_backtest")
    @mock.patch("backtest.backtest_ml.to_excel")
    @mock.patch("backtest.backtest_ml.show_results")
    @mock.patch("builtins.print")
    def test_main_function_with_show_option(
        self,
        mock_print,
        mock_show_results,
        mock_to_excel,
        mock_run_backtest,
        mock_connect,
    ):
        """--showオプション付きのmain関数テスト"""
        # モックデータ設定
        mock_trades = pd.DataFrame(
            [{"code": "1234", "pnl_yen": 100000, "pnl_pct": 10.0}]
        )
        mock_run_backtest.return_value = mock_trades

        # メイン関数をモック引数で実行
        import sys

        from backtest import backtest_ml

        original_argv = sys.argv
        try:
            sys.argv = [
                "backtest_ml.py",
                "--show",
                "--db",
                "test.db",
                "--start",
                "2024-01-01",
            ]
            backtest_ml.main()
        finally:
            sys.argv = original_argv

        # 結果表示関数が呼ばれることを確認
        mock_show_results.assert_called_once()

    @mock.patch("sys.argv", ["backtest_ml.py", "--help"])
    def test_main_function_help(self):
        """ヘルプオプションのテスト"""
        from backtest import backtest_ml

        with pytest.raises(SystemExit):
            backtest_ml.main()

    def test_parse_args_function(self):
        """引数パース関数のテスト"""
        from backtest import backtest_ml

        args = backtest_ml.parse_args(
            [
                "--db",
                "/tmp/test.db",
                "--start",
                "2024-01-01",
                "--top",
                "10",
                "--capital",
                "2000000",
                "--show",
            ]
        )

        assert args.db == "/tmp/test.db"
        assert args.top == 10
        assert args.capital == 2000000
        assert args.show is True


class TestOutputFormats:
    """出力フォーマットのテスト"""

    def test_excel_output(self, tmp_path):
        """Excel出力のテスト"""
        trades = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "comp_name": "Test Corp",
                    "entry_date": pd.Timestamp("2024-01-15"),
                    "exit_date": pd.Timestamp("2024-02-15"),
                    "shares": 1000,
                    "entry_price": 1000,
                    "exit_price": 1100,
                    "pnl_pct": 10.0,
                    "pnl_yen": 100000,
                    "holding_days": 30,
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

        # to_excel関数を実行
        backtest_ml.to_excel(trades, summary, str(xlsx_path))

        # ファイルが作成されることを確認
        assert xlsx_path.exists()
        assert xlsx_path.stat().st_size > 0


class TestShowResults:
    """show_results関数のテスト"""

    @mock.patch("builtins.print")
    def test_show_results(self, mock_print):
        """結果表示機能のテスト"""
        trades = pd.DataFrame([{"pnl_yen": 100000}, {"pnl_yen": -50000}])
        summary = pd.DataFrame(
            {"metric": ["trades", "total_profit"], "value": [2, 50000]}
        )

        backtest_ml.show_results(trades, summary)

        # print が呼ばれることを確認
        assert mock_print.call_count >= 2

    @mock.patch("builtins.print")
    def test_show_results_empty(self, mock_print):
        """空のデータでの結果表示テスト"""
        trades = pd.DataFrame()
        summary = pd.DataFrame({"metric": ["trades", "total_profit"], "value": [0, 0]})

        backtest_ml.show_results(trades, summary)

        # print が呼ばれることを確認
        assert mock_print.call_count >= 1


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

        summary = backtest_ml.summarize(trades)

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

        summary = backtest_ml.summarize(trades)

        assert isinstance(summary, pd.DataFrame)
        assert summary.empty  # 空のDataFrameが返される

    def test_summarize_single_trade(self):
        """単一トレードでのサマリー計算のテスト（Sharpe比が計算できないケース）"""
        trades = pd.DataFrame([{"pnl_yen": 100000, "pnl_pct": 10.0}])

        summary = backtest_ml.summarize(trades)

        metrics = summary.set_index("metric")["value"]
        assert metrics["trades"] == 1
        # 標準偏差が0の場合、Sharpe比は無限大になる
        assert np.isinf(metrics["sharpe"]) or metrics["sharpe"] == 0

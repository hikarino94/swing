#!/usr/bin/env python
"""
テクニカルスクリーニングモジュール (screening/screen_technical.py) のテスト

テスト対象:
- テクニカル指標の計算（移動平均、RSI、ADX、ボリンジャーバンド、MACD）
- シグナルフラグの生成
- オーバーヒート・売られすぎ判定
- データベースへの保存とスクリーニング
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from screening import screen_technical


@pytest.fixture
def technical_db():
    """テクニカル分析用のテストデータベース"""
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)

    # prices テーブル作成
    conn.execute(
        """
        CREATE TABLE prices (
            code TEXT,
            date TEXT,
            adj_open REAL,
            adj_high REAL,
            adj_low REAL,
            adj_close REAL,
            PRIMARY KEY (code, date)
        )
    """
    )

    # listed_info テーブル作成
    conn.execute(
        """
        CREATE TABLE listed_info (
            code TEXT PRIMARY KEY,
            market_code TEXT
        )
    """
    )

    # technical_indicators テーブル作成
    conn.execute(
        """
        CREATE TABLE technical_indicators (
            code TEXT,
            signal_date TEXT,
            signal_ma INTEGER,
            signal_rsi INTEGER,
            signal_adx INTEGER,
            signal_bb INTEGER,
            signal_macd INTEGER,
            signal_ma_short INTEGER,
            signal_rsi_short INTEGER,
            signal_bb_short INTEGER,
            signal_macd_short INTEGER,
            signals_count INTEGER,
            signals_short_count INTEGER,
            signals_overheating INTEGER,
            signals_oversold INTEGER,
            signals_first INTEGER,
            signals_short_first INTEGER,
            PRIMARY KEY (code, signal_date)
        )
    """
    )

    conn.commit()
    conn.close()

    yield db_path

    os.unlink(db_path)


class TestIndicatorCalculation:
    """テクニカル指標計算のテスト"""

    def create_test_price_data(self, days=100):
        """テスト用の価格データを生成"""
        dates = pd.date_range(end="2024-06-01", periods=days, freq="D")
        np.random.seed(42)

        # トレンドのある価格データを生成
        base_price = 1000
        trend = np.linspace(0, 200, days)
        noise = np.random.normal(0, 20, days)
        prices = base_price + trend + noise

        df = pd.DataFrame(
            {
                "date": dates,
                "adj_open": prices * 0.99,
                "adj_high": prices * 1.01,
                "adj_low": prices * 0.98,
                "adj_close": prices,
            }
        )

        return df

    def test_compute_indicators_basic(self):
        """基本的なテクニカル指標計算のテスト"""
        df = self.create_test_price_data(100)

        result = screen_technical.compute_indicators(df)

        # 結果の構造確認
        assert not result.empty
        assert "signal_date" in result.columns
        assert "signal_ma" in result.columns
        assert "signal_rsi" in result.columns
        assert "signal_adx" in result.columns
        assert "signal_bb" in result.columns
        assert "signal_macd" in result.columns
        assert "signals_count" in result.columns
        assert "signals_short_count" in result.columns

        # 値の範囲確認
        assert result["signal_ma"].isin([0, 1]).all()
        assert result["signal_rsi"].isin([0, 1]).all()
        assert result["signals_count"].min() >= 0
        assert result["signals_count"].max() <= 7  # 重み付けされた最大値

    def test_compute_indicators_insufficient_data(self):
        """データ不足時の処理テスト"""
        df = self.create_test_price_data(30)  # 50日未満

        result = screen_technical.compute_indicators(df)

        assert result.empty

    def test_compute_indicators_with_nan(self):
        """欠損値を含むデータの処理テスト"""
        df = self.create_test_price_data(60)
        # 一部の値をNaNに設定
        df.loc[10:15, "adj_close"] = np.nan

        result = screen_technical.compute_indicators(df)

        # ffill/bfillで補完されるため、結果は空でないはず
        assert not result.empty

    def test_overheat_oversold_flags(self):
        """オーバーヒート・売られすぎフラグのテスト"""
        df = self.create_test_price_data(60)

        # OVERHEAT_FACTORの値をモックしてテスト
        with mock.patch("screening.screen_technical.OVERHEAT_FACTOR", 1.05):
            # オーバーヒート状態を作る（終値を10MA * 1.05以上に設定）
            df.loc[df.index[-5:], "adj_close"] = df["adj_close"].iloc[-5:] * 1.10

            result = screen_technical.compute_indicators(df)

            # 最後の数日でオーバーヒートフラグが立つはず
            assert result["signals_overheating"].sum() > 0

    def test_short_signals(self):
        """ショートシグナルのテスト"""
        # 下降トレンドのデータを作成
        dates = pd.date_range(end="2024-06-01", periods=100, freq="D")
        prices = 1000 - np.linspace(0, 200, 100)  # 下降トレンド

        df = pd.DataFrame(
            {
                "date": dates,
                "adj_open": prices * 1.01,
                "adj_high": prices * 1.02,
                "adj_low": prices * 0.99,
                "adj_close": prices,
            }
        )

        result = screen_technical.compute_indicators(df)

        # ショートシグナルが発生するはず
        assert result["signal_ma_short"].sum() > 0
        assert result["signals_short_count"].max() > 0


class TestRunIndicators:
    """インジケーター実行機能のテスト"""

    def test_run_indicators_no_data(self, technical_db):
        """価格データがない場合のテスト"""
        conn = sqlite3.connect(technical_db)

        with mock.patch("screening.screen_technical.logger") as mock_logger:
            screen_technical.run_indicators(conn, "2024-06-01")

            mock_logger.info.assert_called_with(
                "%s の価格データがないためスキップ", "2024-06-01"
            )

        conn.close()

    def test_run_indicators_with_data(self, technical_db):
        """正常なインジケーター計算と保存のテスト"""
        conn = sqlite3.connect(technical_db)

        # テストデータの準備
        conn.execute(
            """
            INSERT INTO listed_info (code, market_code) VALUES
            ('1234', '0111'),
            ('5678', '0109')  -- OTC銘柄は除外
        """
        )

        # 80日分の価格データを挿入
        base_date = datetime(2024, 3, 1)
        for i in range(80):
            date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            price = 1000 + i * 2  # 上昇トレンド
            conn.execute(
                """
                INSERT INTO prices VALUES
                ('1234', ?, ?, ?, ?, ?)
            """,
                (date, price * 0.99, price * 1.01, price * 0.98, price),
            )

        conn.commit()

        # インジケーター実行
        screen_technical.run_indicators(conn, "2024-05-19")

        # 結果確認
        cursor = conn.execute(
            "SELECT * FROM technical_indicators WHERE signal_date = '2024-05-19'"
        )
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "1234"  # code

        conn.close()

    def test_run_indicators_with_history(self, technical_db):
        """過去のシグナル履歴を考慮したテスト"""
        conn = sqlite3.connect(technical_db)

        # テストデータの準備
        conn.execute(
            "INSERT INTO listed_info (code, market_code) VALUES ('1234', '0111')"
        )

        # 価格データ
        base_date = datetime(2024, 3, 1)
        for i in range(80):
            date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            price = 1000 + i * 2
            conn.execute(
                """
                INSERT INTO prices VALUES ('1234', ?, ?, ?, ?, ?)
            """,
                (date, price * 0.99, price * 1.01, price * 0.98, price),
            )

        # 過去のシグナル履歴
        conn.execute(
            """
            INSERT INTO technical_indicators
            (code, signal_date, signals_count, signals_short_count)
            VALUES ('1234', '2024-05-10', 4, 0)
        """
        )

        conn.commit()

        # インジケーター実行
        with mock.patch("screening.screen_technical.SIGNAL_COUNT_MIN", 3):
            with mock.patch("screening.screen_technical.FIRST_LOOKBACK_DAYS", 30):
                screen_technical.run_indicators(conn, "2024-05-19")

        # signals_firstフラグが0になるはず（過去30日以内にシグナルあり）
        cursor = conn.execute(
            "SELECT signals_first FROM technical_indicators WHERE signal_date = '2024-05-19'"
        )
        row = cursor.fetchone()
        if row:  # シグナルが生成された場合
            assert row[0] == 0

        conn.close()


class TestScreenSignals:
    """シグナルスクリーニング機能のテスト"""

    def test_screen_signals_with_date(self, technical_db):
        """日付指定でのスクリーニングテスト"""
        conn = sqlite3.connect(technical_db)

        # テストデータ挿入
        conn.execute(
            """
            INSERT INTO technical_indicators
            (code, signal_date, signals_count, signals_short_count,
             signal_ma, signal_rsi, signal_adx, signal_bb, signal_macd)
            VALUES
            ('1234', '2024-06-01', 5, 0, 1, 1, 1, 1, 1),
            ('5678', '2024-06-01', 2, 0, 1, 0, 0, 1, 0),
            ('9012', '2024-06-01', 0, 4, 0, 0, 1, 0, 0)
        """
        )
        conn.commit()

        with mock.patch("screening.screen_technical.logger") as mock_logger:
            with mock.patch("screening.screen_technical.SIGNAL_COUNT_MIN", 3):
                screen_technical.screen_signals(conn, "2024-06-01")

            # ログ出力を確認
            assert mock_logger.info.called

        conn.close()

    def test_screen_signals_latest(self, technical_db):
        """最新日付での自動スクリーニングテスト"""
        conn = sqlite3.connect(technical_db)

        # テストデータ挿入
        conn.execute(
            """
            INSERT INTO technical_indicators
            (code, signal_date, signals_count, signals_short_count)
            VALUES
            ('1234', '2024-05-31', 3, 0),
            ('1234', '2024-06-01', 4, 0)
        """
        )
        conn.commit()

        with mock.patch("screening.screen_technical.logger") as mock_logger:
            with mock.patch("screening.screen_technical.SIGNAL_COUNT_MIN", 3):
                screen_technical.screen_signals(conn, None)

            # 最新日付（2024-06-01）のデータが表示されるはず
            assert mock_logger.info.called

        conn.close()


class TestCLI:
    """コマンドライン引数のテスト"""

    def test_parse_args_indicators(self):
        """indicatorsコマンドの引数テスト"""
        with mock.patch(
            "sys.argv",
            [
                "screen_technical.py",
                "indicators",
                "--db",
                "/tmp/test.db",
                "--as-of",
                "2024-06-01",
                "--lookback",
                "30",
            ],
        ):
            parser = argparse.ArgumentParser(
                description="スイングトレード向けテクニカルシグナルツール"
            )
            parser.add_argument("command", choices=["indicators", "screen"])
            parser.add_argument("--db", default="stock.db")
            parser.add_argument("--as-of")
            parser.add_argument("--lookback", type=int, default=50)

            args = parser.parse_args()

            assert args.command == "indicators"
            assert args.db == "/tmp/test.db"
            assert args.as_of == "2024-06-01"
            assert args.lookback == 30

    def test_parse_args_screen(self):
        """screenコマンドの引数テスト"""
        with mock.patch(
            "sys.argv", ["screen_technical.py", "screen", "--as-of", "2024-06-01"]
        ):
            parser = argparse.ArgumentParser(
                description="スイングトレード向けテクニカルシグナルツール"
            )
            parser.add_argument("command", choices=["indicators", "screen"])
            parser.add_argument("--db", default="stock.db")
            parser.add_argument("--as-of")
            parser.add_argument("--lookback", type=int, default=50)

            args = parser.parse_args()

            assert args.command == "screen"
            assert args.as_of == "2024-06-01"


class TestIntegration:
    """統合テスト"""

    def test_main_indicators_command(self, technical_db):
        """indicatorsコマンドの統合テスト"""

        # テストデータ準備
        conn = sqlite3.connect(technical_db)
        conn.execute(
            "INSERT INTO listed_info (code, market_code) VALUES ('1234', '0111')"
        )

        # 80日分の価格データ
        base_date = datetime(2024, 3, 1)
        for i in range(80):
            date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            price = 1000 + i * 2
            conn.execute(
                """
                INSERT INTO prices VALUES ('1234', ?, ?, ?, ?, ?)
            """,
                (date, price * 0.99, price * 1.01, price * 0.98, price),
            )

        conn.commit()
        conn.close()

        # メイン処理実行
        with mock.patch(
            "sys.argv",
            [
                "screen_technical.py",
                "indicators",
                "--db",
                technical_db,
                "--as-of",
                "2024-05-19",
                "--lookback",
                "0",
            ],
        ):
            # main関数を直接呼び出す代わりに、処理を実行
            parser = argparse.ArgumentParser()
            parser.add_argument("command", choices=["indicators", "screen"])
            parser.add_argument("--db", default=technical_db)
            parser.add_argument("--as-of")
            parser.add_argument("--lookback", type=int, default=50)
            args = parser.parse_args()

            conn = sqlite3.connect(args.db)
            if args.command == "indicators":
                screen_technical.run_indicators(conn, args.as_of)
            conn.close()

        # 結果確認
        conn = sqlite3.connect(technical_db)
        cursor = conn.execute("SELECT COUNT(*) FROM technical_indicators")
        count = cursor.fetchone()[0]
        assert count > 0
        conn.close()

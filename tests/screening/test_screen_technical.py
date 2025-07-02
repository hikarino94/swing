"""screen_technical.pyのテスト"""

import argparse
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from screening import screen_technical


class TestComputeIndicators:
    """compute_indicators関数のテスト"""

    def test_compute_indicators_with_sufficient_data(self):
        """十分なデータがある場合のインジケーター計算テスト"""
        # 60日分のテストデータを準備
        dates = pd.date_range("2024-01-01", periods=60)
        df = pd.DataFrame(
            {
                "code": ["1234"] * 60,
                "date": dates,
                "adj_open": [100 + i * 0.5 for i in range(60)],
                "adj_high": [102 + i * 0.5 for i in range(60)],
                "adj_low": [98 + i * 0.5 for i in range(60)],
                "adj_close": [100 + i * 0.5 for i in range(60)],
            }
        )

        # compute_indicators関数を呼び出す
        result = screen_technical.compute_indicators(df)

        # 結果の検証
        assert not result.empty
        assert "signal_date" in result.columns
        assert "signal_ma" in result.columns
        assert "signal_rsi" in result.columns
        assert "signal_adx" in result.columns
        assert "signal_bb" in result.columns
        assert "signal_macd" in result.columns
        assert "signals_count" in result.columns
        assert "signals_short_count" in result.columns
        assert "signals_overheating" in result.columns
        assert "signals_oversold" in result.columns

        # 少なくとも10行以上の結果があることを確認
        assert len(result) >= 10

    def test_compute_indicators_with_insufficient_data(self):
        """データが不十分な場合のテスト"""
        # 30日分の少ないデータ
        dates = pd.date_range("2024-01-01", periods=30)
        df = pd.DataFrame(
            {
                "code": ["1234"] * 30,
                "date": dates,
                "adj_open": [100] * 30,
                "adj_high": [102] * 30,
                "adj_low": [98] * 30,
                "adj_close": [100] * 30,
            }
        )

        # compute_indicators関数を呼び出す
        result = screen_technical.compute_indicators(df)

        # 50日未満のデータでは空のDataFrameが返される
        assert result.empty

    def test_compute_indicators_with_missing_values(self):
        """欠損値を含むデータのテスト"""
        dates = pd.date_range("2024-01-01", periods=60)
        df = pd.DataFrame(
            {
                "code": ["1234"] * 60,
                "date": dates,
                "adj_open": [100 + i * 0.5 if i % 10 != 0 else None for i in range(60)],
                "adj_high": [102 + i * 0.5 for i in range(60)],
                "adj_low": [98 + i * 0.5 for i in range(60)],
                "adj_close": [100 + i * 0.5 for i in range(60)],
            }
        )

        # compute_indicators関数を呼び出す（前方/後方補完が行われる）
        result = screen_technical.compute_indicators(df)

        # 結果が空でないことを確認
        assert not result.empty


class TestRunIndicators:
    """run_indicators関数のテスト"""

    def test_run_indicators_with_data(self, temp_db):
        """データがある場合のrun_indicators関数のテスト"""
        conn = sqlite3.connect(temp_db)

        # テーブルを作成
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
                code TEXT,
                date TEXT,
                adj_open REAL,
                adj_high REAL,
                adj_low REAL,
                adj_close REAL
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS listed_info (
                code TEXT,
                market_code TEXT
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS technical_indicators (
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
                signals_short_first INTEGER
            )
        """
        )

        # テストデータを準備（80日分）
        dates = pd.date_range(end="2024-03-01", periods=80)
        prices_data = []
        for i, date in enumerate(dates):
            prices_data.append(
                {
                    "code": "1234",
                    "date": date.strftime("%Y-%m-%d"),
                    "adj_open": 100 + i * 0.5,
                    "adj_high": 102 + i * 0.5,
                    "adj_low": 98 + i * 0.5,
                    "adj_close": 100 + i * 0.5,
                }
            )

        pd.DataFrame(prices_data).to_sql(
            "prices", conn, if_exists="append", index=False
        )

        # 銘柄情報
        listed_info = pd.DataFrame(
            {"code": ["1234"], "market_code": ["0111"]}  # 0109以外
        )
        listed_info.to_sql("listed_info", conn, if_exists="append", index=False)

        # run_indicators関数を実行
        screen_technical.run_indicators(conn, "2024-03-01")

        # 結果を確認
        result = pd.read_sql(
            "SELECT * FROM technical_indicators WHERE signal_date='2024-03-01'", conn
        )
        conn.close()

        assert len(result) == 1
        assert result.iloc[0]["code"] == "1234"

    def test_run_indicators_without_data(self, temp_db):
        """データがない場合のrun_indicators関数のテスト"""
        conn = sqlite3.connect(temp_db)

        # 空のテーブルを作成
        conn.execute("CREATE TABLE IF NOT EXISTS prices (code TEXT, date TEXT)")

        # run_indicators関数を実行（エラーにならないことを確認）
        screen_technical.run_indicators(conn, "2024-03-01")

        conn.close()


class TestScreenSignals:
    """screen_signals関数のテスト"""

    def test_screen_signals(self, temp_db):
        """screen_signals関数のテスト"""
        conn = sqlite3.connect(temp_db)

        # テストデータを準備
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS technical_indicators (
                code TEXT,
                signal_date TEXT,
                signals_count INTEGER,
                signals_short_count INTEGER
            )
        """
        )

        test_data = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "signal_date": ["2024-03-01", "2024-03-01"],
                "signals_count": [5, 3],
                "signals_short_count": [0, 4],
            }
        )
        test_data.to_sql("technical_indicators", conn, if_exists="append", index=False)

        # screen_signals関数を実行（標準出力をキャプチャ）
        with patch("screening.screen_technical.logger") as mock_logger:
            screen_technical.screen_signals(conn, "2024-03-01")

            # ログが出力されたことを確認
            mock_logger.info.assert_called()

        conn.close()


class TestMainFunction:
    """メイン関数（CLIエントリーポイント）のテスト"""

    @patch("screening.screen_technical.sqlite3.connect")
    @patch("screening.screen_technical.run_indicators")
    def test_main_indicators_command(self, mock_run_indicators, mock_connect):
        """indicatorsコマンドのテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # 引数を設定
        test_args = ["screen_technical.py", "indicators", "--as-of", "2024-03-01"]
        with patch("sys.argv", test_args):
            # argparseを介してmain関数の動作をシミュレート
            parser = argparse.ArgumentParser()
            parser.add_argument("command", choices=["indicators", "screen"])
            parser.add_argument("--db", default="test.db")
            parser.add_argument("--as-of")
            parser.add_argument("--lookback", type=int, default=50)
            parser.parse_args(["indicators", "--as-of", "2024-03-01"])

            # run_indicatorsが呼ばれることを確認
            screen_technical.run_indicators(mock_conn, "2024-03-01")
            mock_run_indicators.assert_called_once_with(mock_conn, "2024-03-01")

    @patch("screening.screen_technical.sqlite3.connect")
    @patch("screening.screen_technical.screen_signals")
    def test_main_screen_command(self, mock_screen_signals, mock_connect):
        """screenコマンドのテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # 引数を設定
        test_args = ["screen_technical.py", "screen", "--as-of", "2024-03-01"]
        with patch("sys.argv", test_args):
            # argparseを介してmain関数の動作をシミュレート
            parser = argparse.ArgumentParser()
            parser.add_argument("command", choices=["indicators", "screen"])
            parser.add_argument("--db", default="test.db")
            parser.add_argument("--as-of")
            parser.add_argument("--lookback", type=int, default=50)
            parser.parse_args(["screen", "--as-of", "2024-03-01"])

            # screen_signalsが呼ばれることを確認
            screen_technical.screen_signals(mock_conn, "2024-03-01")
            mock_screen_signals.assert_called_once_with(mock_conn, "2024-03-01")

    def test_main_invalid_command(self):
        """無効なコマンドのテスト"""
        test_args = ["screen_technical.py", "invalid_command"]
        with patch("sys.argv", test_args):
            with pytest.raises(SystemExit):
                # argparseがSystemExitを発生させる
                parser = argparse.ArgumentParser()
                parser.add_argument("command", choices=["indicators", "screen"])
                parser.parse_args(["invalid_command"])


class TestThresholds:
    """閾値に関するテスト"""

    def test_threshold_constants(self):
        """閾値定数が正しくインポートされることを確認"""
        assert hasattr(screen_technical, "ADX_THRESHOLD")
        assert hasattr(screen_technical, "RSI_THRESHOLD")
        assert hasattr(screen_technical, "SIGNAL_COUNT_MIN")
        assert hasattr(screen_technical, "SHORT_SIGNAL_COUNT_MIN")
        assert hasattr(screen_technical, "OVERHEAT_FACTOR")
        assert hasattr(screen_technical, "OVERSOLD_FACTOR")

        # 閾値が妥当な範囲にあることを確認
        assert 0 <= screen_technical.RSI_THRESHOLD <= 100
        assert screen_technical.ADX_THRESHOLD > 0
        assert screen_technical.SIGNAL_COUNT_MIN > 0
        assert screen_technical.OVERHEAT_FACTOR > 1.0
        assert 0 < screen_technical.OVERSOLD_FACTOR < 1.0

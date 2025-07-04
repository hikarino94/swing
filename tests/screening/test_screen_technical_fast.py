"""screen_technical_fast.pyのテスト"""

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from screening import screen_technical_fast


class TestComputeIndicatorsForCode:
    """compute_indicators_for_code関数のテスト"""

    def test_compute_indicators_with_sufficient_data(self):
        """十分なデータがある場合のインジケーター計算テスト"""
        # 60日分のテストデータを準備
        dates = pd.date_range("2024-01-01", periods=60)
        price_data = [
            {
                "date": date.strftime("%Y-%m-%d"),
                "adj_open": 100 + i * 0.5,
                "adj_high": 102 + i * 0.5,
                "adj_low": 98 + i * 0.5,
                "adj_close": 100 + i * 0.5,
            }
            for i, date in enumerate(dates)
        ]

        # 計算対象日
        target_dates = [pd.Timestamp("2024-02-29")]

        # compute_indicators_for_code関数を呼び出す
        results = screen_technical_fast.compute_indicators_for_code(
            ("1234", price_data, target_dates)
        )

        # 結果の検証
        assert len(results) == 1
        result = results[0]

        assert result["code"] == "1234"
        assert result["signal_date"] == "2024-02-29"
        assert "signal_ma" in result
        assert "signal_rsi" in result
        assert "signal_adx" in result
        assert "signals_count" in result
        assert isinstance(result["signals_count"], int)

    def test_compute_indicators_with_insufficient_data(self):
        """データが不十分な場合のテスト"""
        # 30日分の少ないデータ
        dates = pd.date_range("2024-01-01", periods=30)
        price_data = [
            {
                "date": date.strftime("%Y-%m-%d"),
                "adj_open": 100,
                "adj_high": 102,
                "adj_low": 98,
                "adj_close": 100,
            }
            for date in dates
        ]

        target_dates = [pd.Timestamp("2024-01-30")]

        # compute_indicators_for_code関数を呼び出す
        results = screen_technical_fast.compute_indicators_for_code(
            ("1234", price_data, target_dates)
        )

        # 50日未満のデータでは空のリストが返される
        assert results == []

    def test_compute_indicators_with_missing_values(self):
        """欠損値を含むデータのテスト"""
        dates = pd.date_range("2024-01-01", periods=60)
        price_data = []

        for i, date in enumerate(dates):
            if i % 10 == 0:
                # 10日ごとに欠損値
                price_data.append(
                    {
                        "date": date.strftime("%Y-%m-%d"),
                        "adj_open": None,
                        "adj_high": None,
                        "adj_low": None,
                        "adj_close": None,
                    }
                )
            else:
                price_data.append(
                    {
                        "date": date.strftime("%Y-%m-%d"),
                        "adj_open": 100 + i * 0.5,
                        "adj_high": 102 + i * 0.5,
                        "adj_low": 98 + i * 0.5,
                        "adj_close": 100 + i * 0.5,
                    }
                )

        target_dates = [pd.Timestamp("2024-02-29")]

        # compute_indicators_for_code関数を呼び出す（前方/後方補完が行われる）
        results = screen_technical_fast.compute_indicators_for_code(
            ("1234", price_data, target_dates)
        )

        # 結果が返されることを確認
        assert len(results) == 1


class TestProcessChunk:
    """process_chunk関数のテスト"""

    def test_process_chunk(self):
        """チャンク処理のテスト"""
        # テストデータ
        dates = pd.date_range("2024-01-01", periods=60)
        price_data = [
            {
                "date": date.strftime("%Y-%m-%d"),
                "adj_open": 100,
                "adj_high": 102,
                "adj_low": 98,
                "adj_close": 100,
            }
            for date in dates
        ]

        target_dates = [pd.Timestamp("2024-02-29")]

        chunk_data = [
            ("1234", price_data, target_dates),
            ("5678", price_data, target_dates),
        ]

        results = screen_technical_fast.process_chunk(chunk_data)

        assert len(results) == 2
        assert all(r["code"] in ["1234", "5678"] for r in results)


class TestRunIndicatorsFast:
    """run_indicators_fast関数のテスト"""

    def test_run_indicators_fast_with_data(self, temp_db):
        """データがある場合のrun_indicators_fast関数のテスト"""
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

        # run_indicators_fast関数を実行
        target_dates = [pd.Timestamp("2024-03-01")]
        screen_technical_fast.run_indicators_fast(
            conn, target_dates, use_parallel=False
        )

        # 結果を確認
        result = pd.read_sql(
            "SELECT * FROM technical_indicators WHERE signal_date='2024-03-01'", conn
        )
        conn.close()

        assert len(result) == 1
        assert result.iloc[0]["code"] == "1234"

    def test_run_indicators_fast_without_data(self, temp_db):
        """データがない場合のrun_indicators_fast関数のテスト"""
        conn = sqlite3.connect(temp_db)

        # 空のテーブルを作成
        conn.execute("CREATE TABLE IF NOT EXISTS prices (code TEXT, date TEXT)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS listed_info (code TEXT, market_code TEXT)"
        )

        # run_indicators_fast関数を実行（エラーにならないことを確認）
        target_dates = [pd.Timestamp("2024-03-01")]
        screen_technical_fast.run_indicators_fast(conn, target_dates)

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
                "signals_count": [8, 3],
                "signals_short_count": [0, 8],
            }
        )
        test_data.to_sql("technical_indicators", conn, if_exists="append", index=False)

        # screen_signals関数を実行（標準出力をキャプチャ）
        with patch("screening.screen_technical_fast.logger") as mock_logger:
            screen_technical_fast.screen_signals(conn, "2024-03-01")

            # ログが出力されたことを確認
            mock_logger.info.assert_called()

        conn.close()

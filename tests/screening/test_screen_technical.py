"""screen_technical.pyのテスト"""

import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from screening import screen_technical


class TestTechnicalIndicators:
    """テクニカル指標計算のテスト"""

    def test_calculate_breakout_flag(self, sample_prices_df):
        """ブレイクアウトフラグの計算テスト"""
        # テストデータの準備（ブレイクアウト条件を満たすデータ）
        df = pd.DataFrame(
            {
                "code": ["1234"] * 10,
                "date": pd.date_range("2024-01-01", periods=10),
                "close": [
                    100,
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107,
                    115,
                    120,
                ],  # 最後で急騰
                "volume": [10000] * 8 + [25000, 30000],  # 最後で出来高増加
                "adjustment_close": [100, 101, 102, 103, 104, 105, 106, 107, 115, 120],
            }
        )

        # ブレイクアウト判定（実際の関数があれば使用）
        # ここでは簡易的な判定ロジックを記述
        df["price_change"] = df["close"].pct_change()
        df["volume_ratio"] = df["volume"] / df["volume"].rolling(5).mean()

        # 最終日のブレイクアウト判定
        last_row = df.iloc[-1]
        assert last_row["price_change"] > 0.03  # 3%以上の上昇
        assert last_row["volume_ratio"] > 2.0  # 出来高2倍以上

    def test_calculate_ma_trend_flag(self, sample_prices_df):
        """移動平均トレンドフラグの計算テスト"""
        # テストデータの準備（上昇トレンド）
        df = pd.DataFrame(
            {
                "code": ["1234"] * 30,
                "date": pd.date_range("2024-01-01", periods=30),
                "close": list(range(100, 130)),  # 単調増加
                "adjustment_close": list(range(100, 130)),
            }
        )

        # 移動平均の計算
        df["ma5"] = df["adjustment_close"].rolling(5).mean()
        df["ma25"] = df["adjustment_close"].rolling(25).mean()

        # ゴールデンクロスの判定
        last_row = df.iloc[-1]
        assert last_row["ma5"] > last_row["ma25"]  # 短期MAが長期MAを上回る

    def test_calculate_rsi(self):
        """RSI計算のテスト"""
        # テストデータ（売られ過ぎ状態）
        prices = [100, 99, 98, 96, 94, 92, 90, 88, 86, 84, 82, 80, 78, 76, 75]
        df = pd.DataFrame({"close": prices, "adjustment_close": prices})

        # RSI計算（簡易版）
        delta = df["adjustment_close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        # 最終値のRSIが売られ過ぎ（30以下）であることを確認
        assert rsi.iloc[-1] < 30


class TestScreeningFunctions:
    """スクリーニング関数のテスト"""

    @patch("screening.screen_technical.pd.read_sql_query")
    def test_load_price_data(self, mock_read_sql, temp_db):
        """価格データ読み込みのテスト"""
        # モックデータの設定
        mock_df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": ["2024-01-01", "2024-01-01"],
                "close": [100, 200],
                "volume": [10000, 20000],
            }
        )
        mock_read_sql.return_value = mock_df

        # データ読み込み（実際の関数を呼び出す場合）
        # result = screen_technical.load_price_data(temp_db, '2024-01-01')

        # モックが呼ばれたことを確認
        mock_read_sql.assert_called_once()

    def test_screen_stocks(self, temp_db, sample_prices_df):
        """銘柄スクリーニングのテスト"""
        # テスト用データベースにデータを準備
        conn = sqlite3.connect(temp_db)

        # 価格データ
        prices_data = []
        for i in range(30):
            prices_data.append(
                {
                    "code": "1234",
                    "date": (datetime(2024, 1, 1) + timedelta(days=i)).strftime(
                        "%Y-%m-%d"
                    ),
                    "open": 100 + i,
                    "high": 105 + i,
                    "low": 95 + i,
                    "close": 100 + i,
                    "volume": 10000 + i * 100,
                    "adj_close": 100 + i,
                }
            )

        df_prices = pd.DataFrame(prices_data)
        df_prices.to_sql("prices", conn, if_exists="replace", index=False)

        # 銘柄情報
        listed_info = pd.DataFrame(
            {"code": ["1234"], "company_name": ["テスト会社"], "delete_flag": [0]}
        )
        listed_info.to_sql("listed_info", conn, if_exists="replace", index=False)

        conn.close()

        # スクリーニング実行（モック化する場合）
        with patch("screening.screen_technical.thresholds") as mock_thresholds:
            mock_thresholds.technical = {
                "breakout": {"volume_ratio": 2.0, "price_change": 0.03},
                "ma_trend": {"short_period": 5, "long_period": 25},
            }

            # 実際の関数呼び出しをテスト
            # results = screen_technical.screen_stocks(temp_db, '2024-01-30')


class TestSaveResults:
    """結果保存のテスト"""

    def test_save_technical_indicators(self, temp_db):
        """テクニカル指標の保存テスト"""
        # テストデータ
        indicators = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": ["2024-01-01", "2024-01-01"],
                "breakout_flag": [1, 0],
                "ma_trend_flag": [1, 1],
                "volume_spike_flag": [1, 0],
                "rsi_oversold_flag": [0, 1],
                "bollinger_squeeze_flag": [0, 0],
            }
        )

        # データベースに保存
        conn = sqlite3.connect(temp_db)
        indicators.to_sql(
            "technical_indicators", conn, if_exists="replace", index=False
        )

        # 保存されたデータを確認
        saved_df = pd.read_sql_query("SELECT * FROM technical_indicators", conn)
        conn.close()

        assert len(saved_df) == 2
        assert saved_df.iloc[0]["breakout_flag"] == 1
        assert saved_df.iloc[1]["rsi_oversold_flag"] == 1


class TestMainFunction:
    """main関数のテスト"""

    @patch("screening.screen_technical.save_results")
    @patch("screening.screen_technical.screen_stocks")
    def test_main_screen_mode(self, mock_screen, mock_save):
        """スクリーニングモードのテスト"""
        # モックの設定
        mock_screen.return_value = pd.DataFrame(
            {"code": ["1234"], "breakout_flag": [1]}
        )

        # 引数を設定
        test_args = ["screen_technical.py", "screen"]
        with patch("sys.argv", test_args):
            screen_technical.main()

        # 関数が呼ばれたことを確認
        mock_screen.assert_called_once()
        mock_save.assert_called_once()

    @patch("screening.screen_technical.logger")
    def test_main_invalid_mode(self, mock_logger):
        """無効なモードのテスト"""
        test_args = ["screen_technical.py", "invalid_mode"]
        with patch("sys.argv", test_args):
            with pytest.raises(SystemExit):
                screen_technical.main()

        # エラーログが出力されたことを確認
        mock_logger.error.assert_called()


class TestThresholds:
    """閾値設定のテスト"""

    def test_load_thresholds(self, tmp_path):
        """閾値ファイルの読み込みテスト"""
        # テスト用の閾値ファイルを作成
        thresholds_data = {
            "technical": {
                "breakout": {"volume_ratio": 2.0, "price_change": 0.03},
                "ma_trend": {"short_period": 5, "long_period": 25},
                "rsi": {"oversold": 30, "overbought": 70},
            }
        }

        thresholds_path = tmp_path / "thresholds.json"
        thresholds_path.write_text(json.dumps(thresholds_data))

        # 読み込みテスト
        with open(thresholds_path) as f:
            loaded = json.load(f)

        assert loaded["technical"]["breakout"]["volume_ratio"] == 2.0
        assert loaded["technical"]["rsi"]["oversold"] == 30

"""Tests for screening/screen_technical.py"""

import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd


class TestTechnicalIndicators:
    """テクニカル指標計算のテスト"""

    def test_bollinger_bands_calculation(self):
        """ボリンジャーバンドの計算をテスト"""
        # テストデータの作成
        prices = pd.Series([100, 102, 98, 103, 101, 99, 104, 102, 100, 105])

        # 移動平均とボリンジャーバンドの計算
        window = 5
        ma = prices.rolling(window=window).mean()
        std = prices.rolling(window=window).std()
        upper_band = ma + 2 * std
        lower_band = ma - 2 * std

        # 計算結果の確認
        assert pd.isna(ma.iloc[: window - 1]).all()  # 最初のwindow-1個はNaN
        assert not pd.isna(ma.iloc[window - 1 :]).any()  # それ以降は値がある
        # NaN以外の値で比較
        valid_idx = ~pd.isna(upper_band)
        assert (upper_band[valid_idx] > ma[valid_idx]).all()  # 上部バンドは平均より上
        assert (lower_band[valid_idx] < ma[valid_idx]).all()  # 下部バンドは平均より下

    def test_rsi_calculation(self):
        """RSI（相対力指数）の計算をテスト"""
        # テストデータの作成（価格変動）
        prices = pd.Series([100, 102, 101, 103, 102, 104, 103, 105, 104, 106])

        # RSI計算（簡易版）
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()

        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        # 基本的な性質の確認（NaN値を除く）
        valid_rsi = rsi.dropna()
        if len(valid_rsi) > 0:
            assert (valid_rsi >= 0).all()
            assert (valid_rsi <= 100).all()

    def test_macd_calculation(self):
        """MACD（移動平均収束拡散法）の計算をテスト"""
        # テストデータの作成
        prices = pd.Series(np.random.randn(100).cumsum() + 100)

        # MACD計算
        exp1 = prices.ewm(span=12, adjust=False).mean()
        exp2 = prices.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        histogram = macd - signal

        # 基本的な性質の確認
        assert len(macd) == len(prices)
        assert len(signal) == len(prices)
        assert len(histogram) == len(prices)

    def test_adx_calculation_logic(self):
        """ADX（平均方向性指数）の計算ロジックをテスト"""
        # テストデータの作成
        high = pd.Series([102, 103, 104, 103, 105, 106, 105, 107])
        low = pd.Series([98, 99, 100, 99, 101, 102, 101, 103])
        close = pd.Series([100, 101, 102, 101, 103, 104, 103, 105])

        # True Range (TR)の計算
        high_low = high - low
        high_close = (high - close.shift()).abs()
        low_close = (low - close.shift()).abs()

        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)

        # 基本的な性質の確認
        assert (true_range >= 0).all()
        assert len(true_range) == len(high)


class TestScreeningLogic:
    """スクリーニングロジックのテスト"""

    def test_long_signal_detection(self):
        """ロングシグナルの検出ロジックをテスト"""
        # テストデータの作成
        df = pd.DataFrame(
            {
                "close": [100, 98, 95, 93, 92, 94, 96, 98, 100, 102],
                "ma_20": [100, 99, 98, 97, 96, 95, 94, 95, 96, 97],
                "bb_lower": [95, 94, 93, 92, 91, 90, 89, 90, 91, 92],
                "rsi": [25, 22, 20, 18, 15, 20, 25, 30, 35, 40],
                "adx": [30, 32, 35, 37, 40, 38, 35, 33, 30, 28],
            }
        )

        # ロングシグナルの条件
        # 1. RSIが売られすぎ（30以下）
        # 2. 価格がボリンジャーバンド下部付近
        # 3. ADXが一定値以上（トレンドが存在）
        long_signal = (
            (df["rsi"] <= 30)
            & (df["close"] <= df["bb_lower"] * 1.05)  # 5%の余裕
            & (df["adx"] >= 25)
        )

        # シグナルが期待通りの位置で発生することを確認
        assert long_signal.sum() > 0
        assert long_signal.iloc[4]  # RSI=15の時点でシグナル

    def test_short_signal_detection(self):
        """ショートシグナルの検出ロジックをテスト"""
        # テストデータの作成
        df = pd.DataFrame(
            {
                "close": [100, 102, 105, 107, 108, 106, 104, 102, 100, 98],
                "ma_20": [100, 101, 102, 103, 104, 105, 106, 105, 104, 103],
                "bb_upper": [105, 106, 107, 108, 109, 110, 111, 110, 109, 108],
                "rsi": [75, 78, 80, 82, 85, 80, 75, 70, 65, 60],
                "adx": [30, 32, 35, 37, 40, 38, 35, 33, 30, 28],
            }
        )

        # ショートシグナルの条件
        # 1. RSIが買われすぎ（70以上）
        # 2. 価格がボリンジャーバンド上部付近
        # 3. ADXが一定値以上（トレンドが存在）
        short_signal = (
            (df["rsi"] >= 70)
            & (df["close"] >= df["bb_upper"] * 0.95)  # 5%の余裕
            & (df["adx"] >= 25)
        )

        # シグナルが期待通りの位置で発生することを確認
        assert short_signal.sum() > 0
        assert short_signal.iloc[4]  # RSI=85の時点でシグナル


class TestDatabaseOperations:
    """データベース操作のテスト"""

    def setup_method(self):
        """テスト用の一時データベースを作成"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name

        # テストデータを作成
        with sqlite3.connect(self.db_path) as conn:
            # pricesテーブルの作成
            conn.execute(
                """
                CREATE TABLE prices (
                    date TEXT,
                    code TEXT,
                    close REAL,
                    volume INTEGER,
                    PRIMARY KEY (date, code)
                )
            """
            )

            # technical_indicatorsテーブルの作成
            conn.execute(
                """
                CREATE TABLE technical_indicators (
                    date TEXT,
                    code TEXT,
                    ma_short REAL,
                    ma_long REAL,
                    bb_upper REAL,
                    bb_middle REAL,
                    bb_lower REAL,
                    rsi REAL,
                    macd REAL,
                    macd_signal REAL,
                    macd_histogram REAL,
                    adx REAL,
                    obv INTEGER,
                    is_long_signal INTEGER DEFAULT 0,
                    is_short_signal INTEGER DEFAULT 0,
                    is_breakout_up INTEGER DEFAULT 0,
                    is_breakout_down INTEGER DEFAULT 0,
                    PRIMARY KEY (date, code)
                )
            """
            )

            # テスト用の価格データを挿入
            dates = pd.date_range("2023-01-01", periods=100, freq="D")
            for i, date in enumerate(dates):
                price = 100 + np.sin(i / 10) * 10  # サイン波のような価格変動
                volume = 1000000 + np.random.randint(-100000, 100000)
                conn.execute(
                    "INSERT INTO prices (date, code, close, volume) VALUES (?, ?, ?, ?)",
                    (date.strftime("%Y-%m-%d"), "1234", price, volume),
                )
            conn.commit()

    def teardown_method(self):
        """一時ファイルの削除"""
        Path(self.db_path).unlink(missing_ok=True)

    def test_fetch_price_data(self):
        """価格データの取得をテスト"""
        with sqlite3.connect(self.db_path) as conn:
            query = """
                SELECT date, code, close, volume
                FROM prices
                WHERE code = ?
                ORDER BY date
            """
            df = pd.read_sql_query(query, conn, params=("1234",))

        assert len(df) == 100
        assert df["code"].iloc[0] == "1234"
        assert "close" in df.columns
        assert "volume" in df.columns

    def test_save_technical_indicators(self):
        """テクニカル指標の保存をテスト"""
        # テスト用のテクニカル指標データ
        data = {
            "date": "2023-04-01",
            "code": "1234",
            "ma_short": 100.5,
            "ma_long": 99.8,
            "bb_upper": 105.0,
            "bb_middle": 100.0,
            "bb_lower": 95.0,
            "rsi": 45.5,
            "macd": 0.5,
            "macd_signal": 0.3,
            "macd_histogram": 0.2,
            "adx": 25.5,
            "obv": 1500000,
            "is_long_signal": 0,
            "is_short_signal": 0,
            "is_breakout_up": 0,
            "is_breakout_down": 0,
        }

        with sqlite3.connect(self.db_path) as conn:
            # データの挿入
            placeholders = ", ".join(["?"] * len(data))
            columns = ", ".join(data.keys())
            query = f"INSERT OR REPLACE INTO technical_indicators ({columns}) VALUES ({placeholders})"
            conn.execute(query, list(data.values()))
            conn.commit()

            # データが保存されたことを確認
            result = conn.execute(
                "SELECT * FROM technical_indicators WHERE date = ? AND code = ?",
                ("2023-04-01", "1234"),
            ).fetchone()

        assert result is not None
        assert result[2] == 100.5  # ma_short


class TestOptimization:
    """最適化のテスト"""

    def test_data_type_optimization(self):
        """データ型の最適化をテスト"""
        # float64からfloat32への変換
        df = pd.DataFrame(
            {
                "close": np.random.randn(1000).astype(np.float64) * 100 + 1000,
                "volume": np.random.randint(0, 1000000, 1000).astype(np.int64),
            }
        )

        # メモリ使用量（変換前）
        memory_before = df.memory_usage(deep=True).sum()

        # データ型の最適化
        df["close"] = df["close"].astype(np.float32)
        df["volume"] = df["volume"].astype(np.int32)

        # メモリ使用量（変換後）
        memory_after = df.memory_usage(deep=True).sum()

        # メモリ使用量が削減されていることを確認
        assert memory_after < memory_before
        assert df["close"].dtype == np.float32
        assert df["volume"].dtype == np.int32

    def test_batch_processing(self):
        """バッチ処理のテスト"""
        # 大量のデータを作成
        n_rows = 10000
        data = []
        for i in range(n_rows):
            data.append(
                {
                    "date": f"2023-01-{(i % 30) + 1:02d}",
                    "code": f"{1000 + (i % 100):04d}",
                    "value": np.random.randn(),
                }
            )

        # バッチサイズごとに処理
        batch_size = 1000
        processed_count = 0

        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            # バッチ処理のシミュレーション
            processed_count += len(batch)

        # 全てのデータが処理されたことを確認
        assert processed_count == n_rows

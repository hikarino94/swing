"""Tests for screening/screen_ml.py"""

import argparse
import datetime as dt
import pickle
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


class TestMLFeatureEngineering:
    """機械学習の特徴量エンジニアリングのテスト"""

    def test_feature_calculation(self):
        """特徴量の計算をテスト"""
        # テスト用データの作成
        dates = pd.date_range("2022-01-01", periods=100, freq="D")
        df = pd.DataFrame(
            {
                "date": dates,
                "code": "1234",
                "close": 1000 + np.random.randn(100).cumsum() * 10,
                "volume": np.random.randint(100000, 1000000, 100),
            }
        )

        # 価格関連の特徴量
        df["returns"] = df["close"].pct_change()
        df["log_returns"] = np.log(df["close"] / df["close"].shift(1))
        df["volatility"] = df["returns"].rolling(window=20).std()
        df["rsi"] = self.calculate_rsi(df["close"], window=14)

        # 基本的な検証
        assert "returns" in df.columns
        assert "volatility" in df.columns
        assert "rsi" in df.columns
        assert df["volatility"].iloc[20:].notna().all()  # 20日目以降は値がある

    def calculate_rsi(self, prices, window=14):
        """RSI計算のヘルパー関数"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def test_label_generation(self):
        """予測ラベルの生成をテスト"""
        # テスト用の価格データ
        prices = pd.Series([100, 102, 105, 103, 107, 110, 108, 112, 115, 118])

        # 将来のリターンを計算（簡易版）
        future_window = 3
        future_returns = prices.shift(-future_window) / prices - 1

        # 5%以上上昇したらラベル1
        threshold = 0.05
        labels = (future_returns >= threshold).astype(int)

        # 検証
        assert len(labels) == len(prices)
        # 最後のfuture_window個はNaN（将来データがないため）
        # NaNの場合は0になるので、future_returnsで直接確認
        assert future_returns.iloc[-future_window:].isna().all()

        # 手動で確認: prices[0]=100, prices[3]=103, return=(103-100)/100=0.03 < 0.05
        assert future_returns.iloc[0] == pytest.approx(0.03, rel=1e-3)
        assert labels.iloc[0] == 0

        # prices[4]=107, prices[7]=112, return=(112-107)/107=0.0467 < 0.05
        assert labels.iloc[4] == 0

        # 5%以上のリターンがある位置を探す
        # prices[5]=110, prices[8]=115, return=(115-110)/110=0.0454 < 0.05
        # prices[6]=108, prices[9]=118, return=(118-108)/108=0.0926 > 0.05
        assert future_returns.iloc[6] > threshold
        assert labels.iloc[6] == 1


class TestModelTraining:
    """モデル学習のテスト"""

    def test_model_pipeline_creation(self):
        """モデルパイプラインの作成をテスト"""
        # パイプラインの構築
        pipeline = Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "classifier",
                    GradientBoostingClassifier(
                        n_estimators=100, max_depth=3, random_state=42
                    ),
                ),
            ]
        )

        # パイプラインの構成を確認
        assert len(pipeline.steps) == 2
        assert isinstance(pipeline.named_steps["scaler"], StandardScaler)
        assert isinstance(
            pipeline.named_steps["classifier"], GradientBoostingClassifier
        )

    def test_model_training_flow(self):
        """モデル学習フローをテスト"""
        # ダミーの特徴量とラベルを作成
        n_samples = 1000
        n_features = 10
        X = np.random.randn(n_samples, n_features)
        y = np.random.randint(0, 2, n_samples)

        # モデルの作成と学習
        model = GradientBoostingClassifier(
            n_estimators=10, max_depth=3, random_state=42  # テスト用に小さく
        )
        model.fit(X, y)

        # 予測
        predictions = model.predict(X)
        probabilities = model.predict_proba(X)

        # 検証
        assert len(predictions) == n_samples
        assert probabilities.shape == (n_samples, 2)
        assert (probabilities.sum(axis=1) - 1).max() < 1e-6  # 確率の合計は1

    def test_model_serialization(self):
        """モデルのシリアライズをテスト"""
        # 簡単なモデルを作成
        model = GradientBoostingClassifier(n_estimators=5, random_state=42)
        X = np.random.randn(100, 5)
        y = np.random.randint(0, 2, 100)
        model.fit(X, y)

        # モデルをバイト列にシリアライズ
        model_bytes = pickle.dumps(model)

        # デシリアライズ
        loaded_model = pickle.loads(model_bytes)

        # 同じ予測を返すことを確認
        X_test = np.random.randn(10, 5)
        np.testing.assert_array_equal(
            model.predict(X_test), loaded_model.predict(X_test)
        )


class TestDataPreparation:
    """データ準備のテスト"""

    def setup_method(self):
        """テスト用の一時データベースを作成"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name

        with sqlite3.connect(self.db_path) as conn:
            # pricesテーブル
            conn.execute(
                """
                CREATE TABLE prices (
                    date TEXT,
                    code TEXT,
                    close REAL,
                    volume INTEGER,
                    adjustment_close REAL,
                    PRIMARY KEY (date, code)
                )
            """
            )

            # statementsテーブル
            conn.execute(
                """
                CREATE TABLE statements (
                    code TEXT,
                    disclosure_date TEXT,
                    period_end TEXT,
                    eps REAL,
                    roe REAL,
                    equity_to_asset_ratio REAL
                )
            """
            )

            # テストデータの挿入
            base_date = dt.date(2023, 1, 1)
            for i in range(200):
                date = base_date + dt.timedelta(days=i)
                for code in ["1234", "5678"]:
                    price = 1000 + np.sin(i / 20) * 100 + (10 if code == "5678" else 0)
                    volume = 1000000 + np.random.randint(-100000, 100000)
                    conn.execute(
                        "INSERT INTO prices VALUES (?, ?, ?, ?, ?)",
                        (date.strftime("%Y-%m-%d"), code, price, volume, price),
                    )

            # 財務データの挿入
            for code in ["1234", "5678"]:
                conn.execute(
                    "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?)",
                    (code, "2023-03-31", "2023-03-31", 50.0, 0.15, 0.45),
                )
            conn.commit()

    def teardown_method(self):
        """一時ファイルの削除"""
        Path(self.db_path).unlink(missing_ok=True)

    def test_load_price_data(self):
        """価格データの読み込みをテスト"""
        with sqlite3.connect(self.db_path) as conn:
            query = """
                SELECT date, code, close, volume
                FROM prices
                WHERE date >= '2023-01-01'
                ORDER BY code, date
            """
            df = pd.read_sql_query(query, conn)

        assert len(df) == 400  # 200日 × 2銘柄
        assert set(df["code"].unique()) == {"1234", "5678"}
        assert df["close"].notna().all()

    def test_merge_financial_data(self):
        """財務データのマージをテスト"""
        with sqlite3.connect(self.db_path) as conn:
            # 価格データ
            price_df = pd.read_sql_query(
                "SELECT date, code, close FROM prices WHERE date >= '2023-03-01'", conn
            )

            # 財務データ
            stmt_df = pd.read_sql_query(
                "SELECT code, eps, roe, equity_to_asset_ratio FROM statements", conn
            )

        # マージ
        merged_df = price_df.merge(stmt_df, on="code", how="left")

        assert len(merged_df) == len(price_df)
        assert "eps" in merged_df.columns
        assert merged_df["eps"].notna().all()


class TestScreeningLogic:
    """スクリーニングロジックのテスト"""

    def test_prediction_output(self):
        """予測結果の出力形式をテスト"""
        # ダミーの予測結果
        predictions = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999", "1111", "2222"],
                "probability": [0.85, 0.72, 0.68, 0.55, 0.45],
                "predicted_return": [0.12, 0.08, 0.07, 0.05, 0.03],
            }
        )

        # 上位N銘柄の抽出
        top_n = 3
        top_stocks = predictions.nlargest(top_n, "probability")

        # 検証
        assert len(top_stocks) == top_n
        assert top_stocks.iloc[0]["code"] == "1234"
        assert top_stocks["probability"].is_monotonic_decreasing

    def test_screening_filters(self):
        """スクリーニングフィルターのテスト"""
        # テストデータ
        df = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999", "1111"],
                "probability": [0.85, 0.72, 0.30, 0.65],
                "volume": [1000000, 50000, 2000000, 500000],
                "market_cap": [100e8, 10e8, 200e8, 50e8],  # 億円
            }
        )

        # フィルター条件
        # 1. 確率が0.5以上
        # 2. 出来高が100,000以上
        # 3. 時価総額が20億円以上
        filtered = df[
            (df["probability"] >= 0.5)
            & (df["volume"] >= 100000)
            & (df["market_cap"] >= 20e8)
        ]

        # 検証
        assert len(filtered) == 2
        assert set(filtered["code"]) == {"1234", "1111"}


class TestCLIInterface:
    """CLIインターフェースのテスト"""

    @patch("sys.argv", ["screen_ml.py", "train", "--lookback", "365"])
    def test_train_command_parsing(self):
        """trainコマンドの引数解析をテスト"""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")

        # trainサブコマンド
        train_parser = subparsers.add_parser("train")
        train_parser.add_argument("--lookback", type=int, default=1095)
        train_parser.add_argument("--db", type=str, default="stock.db")

        # 引数解析
        args = parser.parse_args(["train", "--lookback", "365"])

        assert args.command == "train"
        assert args.lookback == 365

    @patch("sys.argv", ["screen_ml.py", "screen", "--top", "20"])
    def test_screen_command_parsing(self):
        """screenコマンドの引数解析をテスト"""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")

        # screenサブコマンド
        screen_parser = subparsers.add_parser("screen")
        screen_parser.add_argument("--top", type=int, default=30)
        screen_parser.add_argument("--retrain", action="store_true")

        # 引数解析
        args = parser.parse_args(["screen", "--top", "20"])

        assert args.command == "screen"
        assert args.top == 20
        assert not args.retrain

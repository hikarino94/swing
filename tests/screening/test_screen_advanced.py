"""Advanced tests for screening module"""

import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


class TestFundamentalScreening:
    """ファンダメンタルスクリーニングのテスト"""

    def test_financial_ratios_calculation(self):
        """財務比率計算のテスト"""
        # 財務データのサンプル
        financial_data = {
            "net_sales": 1_000_000_000,
            "operating_profit": 100_000_000,
            "ordinary_profit": 95_000_000,
            "net_profit": 70_000_000,
            "total_assets": 2_000_000_000,
            "equity": 1_000_000_000,
            "shares_outstanding": 10_000_000,
            "current_price": 1500,
        }

        # 各種比率の計算
        # 売上高営業利益率
        operating_margin = (
            financial_data["operating_profit"] / financial_data["net_sales"]
        )
        assert operating_margin == 0.1  # 10%

        # ROE (自己資本利益率)
        roe = financial_data["net_profit"] / financial_data["equity"]
        assert roe == 0.07  # 7%

        # ROA (総資産利益率)
        roa = financial_data["net_profit"] / financial_data["total_assets"]
        assert roa == 0.035  # 3.5%

        # PER (株価収益率)
        eps = financial_data["net_profit"] / financial_data["shares_outstanding"]
        per = financial_data["current_price"] / eps
        assert eps == 7.0
        assert per == pytest.approx(214.29, rel=1e-2)

        # PBR (株価純資産倍率)
        bps = financial_data["equity"] / financial_data["shares_outstanding"]
        pbr = financial_data["current_price"] / bps
        assert bps == 100.0
        assert pbr == 15.0

    def test_growth_rate_calculation(self):
        """成長率計算のテスト"""
        # 四半期データ
        quarterly_data = pd.DataFrame(
            {
                "quarter": [
                    "2022Q1",
                    "2022Q2",
                    "2022Q3",
                    "2022Q4",
                    "2023Q1",
                    "2023Q2",
                    "2023Q3",
                    "2023Q4",
                ],
                "net_sales": [900, 950, 980, 1000, 1050, 1100, 1150, 1200],
                "net_profit": [80, 85, 88, 90, 95, 100, 105, 110],
            }
        )

        # 前年同期比成長率（YoY）
        quarterly_data["sales_yoy"] = quarterly_data["net_sales"].pct_change(periods=4)
        quarterly_data["profit_yoy"] = quarterly_data["net_profit"].pct_change(
            periods=4
        )

        # 2023Q1の成長率を確認
        assert quarterly_data.iloc[4]["sales_yoy"] == pytest.approx(
            0.1667, rel=1e-3
        )  # 16.67%
        assert quarterly_data.iloc[4]["profit_yoy"] == pytest.approx(
            0.1875, rel=1e-3
        )  # 18.75%

    def test_screening_criteria_application(self):
        """スクリーニング条件の適用テスト"""
        # 複数銘柄のデータ
        stocks = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999", "1111", "2222"],
                "roe": [0.15, 0.08, 0.20, 0.05, 0.12],
                "per": [10, 25, 8, 30, 15],
                "pbr": [1.2, 3.0, 0.8, 4.5, 1.5],
                "operating_margin": [0.12, 0.08, 0.15, 0.05, 0.10],
                "sales_growth": [0.20, 0.05, 0.30, -0.10, 0.15],
            }
        )

        # スクリーニング条件
        criteria = {
            "roe_min": 0.10,  # ROE 10%以上
            "per_max": 20,  # PER 20倍以下
            "pbr_max": 2.0,  # PBR 2倍以下
            "operating_margin_min": 0.10,  # 営業利益率10%以上
            "sales_growth_min": 0.10,  # 売上成長率10%以上
        }

        # 条件を適用
        screened = stocks[
            (stocks["roe"] >= criteria["roe_min"])
            & (stocks["per"] <= criteria["per_max"])
            & (stocks["pbr"] <= criteria["pbr_max"])
            & (stocks["operating_margin"] >= criteria["operating_margin_min"])
            & (stocks["sales_growth"] >= criteria["sales_growth_min"])
        ]

        # 条件を満たす銘柄を確認
        assert len(screened) == 3
        assert set(screened["code"]) == {"1234", "9999", "2222"}


class TestTechnicalScreening:
    """テクニカルスクリーニングのテスト"""

    def test_moving_average_signals(self):
        """移動平均シグナルのテスト"""
        # 価格データ生成
        dates = pd.date_range("2023-01-01", periods=100, freq="D")
        np.random.seed(42)
        prices = pd.DataFrame(
            {"date": dates, "close": 1000 + np.cumsum(np.random.randn(100) * 10)}
        )

        # 移動平均計算
        prices["ma_20"] = prices["close"].rolling(20).mean()
        prices["ma_50"] = prices["close"].rolling(50).mean()

        # ゴールデンクロス・デッドクロスの検出
        prices["ma_signal"] = 0
        prices.loc[prices["ma_20"] > prices["ma_50"], "ma_signal"] = 1
        prices.loc[prices["ma_20"] < prices["ma_50"], "ma_signal"] = -1

        # シグナルの変化点を検出
        signal_changes = prices["ma_signal"].diff() != 0
        crossovers = prices[signal_changes & (prices["ma_signal"] != 0)]

        assert len(crossovers) > 0
        assert prices["ma_signal"].isin([1, 0, -1]).all()

    def test_momentum_indicators(self):
        """モメンタム指標のテスト"""
        # 価格データ
        prices = pd.Series([100, 102, 101, 103, 105, 104, 106, 108, 107, 109])

        # RSI計算（簡易版）
        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=14, min_periods=1).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=14, min_periods=1).mean()

        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        # RSIの範囲を確認
        assert (rsi.dropna() >= 0).all()
        assert (rsi.dropna() <= 100).all()

        # MACD計算
        exp12 = prices.ewm(span=12, adjust=False).mean()
        exp26 = prices.ewm(span=26, adjust=False).mean()
        macd = exp12 - exp26
        signal = macd.ewm(span=9, adjust=False).mean()
        histogram = macd - signal  # noqa: F841

        assert len(macd) == len(prices)
        assert len(signal) == len(prices)

    def test_volatility_indicators(self):
        """ボラティリティ指標のテスト"""
        # OHLCデータ
        data = pd.DataFrame(
            {
                "high": [105, 107, 106, 108, 110, 109, 111, 113, 112, 114],
                "low": [98, 99, 100, 101, 102, 103, 104, 105, 106, 107],
                "close": [102, 103, 104, 105, 106, 107, 108, 109, 110, 111],
            }
        )

        # ATR (Average True Range) 計算
        data["high_low"] = data["high"] - data["low"]
        data["high_close"] = abs(data["high"] - data["close"].shift())
        data["low_close"] = abs(data["low"] - data["close"].shift())

        data["true_range"] = data[["high_low", "high_close", "low_close"]].max(axis=1)
        data["atr"] = data["true_range"].rolling(14, min_periods=1).mean()

        assert (data["true_range"] >= 0).all()
        assert (data["atr"] >= 0).all()

        # ボリンジャーバンド
        data["bb_middle"] = data["close"].rolling(20, min_periods=1).mean()
        data["bb_std"] = data["close"].rolling(20, min_periods=1).std()
        data["bb_upper"] = data["bb_middle"] + 2 * data["bb_std"]
        data["bb_lower"] = data["bb_middle"] - 2 * data["bb_std"]

        # バンドの関係性を確認（NaNを除外）
        valid_data = data.dropna()
        assert (valid_data["bb_upper"] >= valid_data["bb_middle"]).all()
        assert (valid_data["bb_middle"] >= valid_data["bb_lower"]).all()


class TestMLScreening:
    """機械学習スクリーニングのテスト"""

    def test_feature_engineering(self):
        """特徴量エンジニアリングのテスト"""
        # 基本データ
        data = pd.DataFrame(
            {
                "close": [100, 102, 101, 103, 105, 104, 106, 108, 107, 109],
                "volume": [
                    1000000,
                    1100000,
                    900000,
                    1200000,
                    1300000,
                    1000000,
                    1400000,
                    1500000,
                    1100000,
                    1600000,
                ],
                "high": [101, 103, 102, 104, 106, 105, 107, 109, 108, 110],
                "low": [99, 101, 100, 102, 104, 103, 105, 107, 106, 108],
            }
        )

        # 技術的特徴量
        data["returns"] = data["close"].pct_change()
        data["volume_ratio"] = (
            data["volume"] / data["volume"].rolling(20, min_periods=1).mean()
        )
        data["price_range"] = (data["high"] - data["low"]) / data["close"]
        data["rsi"] = 50  # 簡略化

        # ファンダメンタル特徴量（モック）
        data["pe_ratio"] = 15
        data["roe"] = 0.12
        data["debt_ratio"] = 0.3

        # 特徴量が作成されたことを確認
        features = [
            "returns",
            "volume_ratio",
            "price_range",
            "rsi",
            "pe_ratio",
            "roe",
            "debt_ratio",
        ]
        for feature in features:
            assert feature in data.columns

    def test_model_prediction_format(self):
        """モデル予測フォーマットのテスト"""
        # 予測スコア
        predictions = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999", "1111", "2222"],
                "prediction_score": [0.85, 0.72, 0.91, 0.45, 0.68],
                "confidence": [0.90, 0.85, 0.95, 0.60, 0.80],
            }
        )

        # スコアでソートしてトップN選択
        top_n = 3
        top_stocks = predictions.nlargest(top_n, "prediction_score")

        assert len(top_stocks) == top_n
        assert top_stocks.iloc[0]["code"] == "9999"
        assert top_stocks.iloc[0]["prediction_score"] == 0.91

    def test_backtesting_ml_signals(self):
        """MLシグナルのバックテストテスト"""
        # 過去のMLシグナルと実際のリターン
        ml_results = pd.DataFrame(
            {
                "date": pd.date_range("2023-01-01", periods=20, freq="W"),
                "code": ["1234"] * 20,
                "ml_score": np.random.rand(20),
                "actual_return": np.random.randn(20) * 0.05,
            }
        )

        # 閾値以上のシグナルのみ取引
        threshold = 0.7
        ml_results["signal"] = ml_results["ml_score"] >= threshold

        # シグナルがある場合のリターンを計算
        signal_returns = ml_results[ml_results["signal"]]["actual_return"]

        if len(signal_returns) > 0:
            avg_return = signal_returns.mean()
            hit_rate = (signal_returns > 0).mean()

            assert isinstance(avg_return, float)
            assert 0 <= hit_rate <= 1


class TestScreeningDatabase:
    """スクリーニングデータベースのテスト"""

    def test_signal_storage_and_retrieval(self):
        """シグナルの保存と取得のテスト"""
        # 一時データベース
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            # テーブル作成
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE screening_signals (
                        date TEXT,
                        code TEXT,
                        signal_type TEXT,
                        signal_strength REAL,
                        metadata TEXT,
                        PRIMARY KEY (date, code, signal_type)
                    )
                """
                )

                # シグナルデータを挿入
                signals = [
                    (
                        "2023-01-01",
                        "1234",
                        "fundamental",
                        0.85,
                        '{"roe": 0.15, "per": 12}',
                    ),
                    (
                        "2023-01-01",
                        "5678",
                        "technical",
                        0.72,
                        '{"rsi": 30, "macd": "bullish"}',
                    ),
                    (
                        "2023-01-01",
                        "9999",
                        "ml",
                        0.91,
                        '{"model": "rf", "features": 50}',
                    ),
                ]

                conn.executemany(
                    "INSERT INTO screening_signals VALUES (?, ?, ?, ?, ?)", signals
                )
                conn.commit()

            # データの取得
            with sqlite3.connect(db_path) as conn:
                df = pd.read_sql_query(
                    "SELECT * FROM screening_signals WHERE date = '2023-01-01'", conn
                )

            assert len(df) == 3
            assert set(df["signal_type"]) == {"fundamental", "technical", "ml"}

            # メタデータのパース
            import json

            metadata = json.loads(df.iloc[0]["metadata"])
            assert "roe" in metadata

        finally:
            Path(db_path).unlink()

    def test_screening_history_tracking(self):
        """スクリーニング履歴追跡のテスト"""
        # 履歴データ
        history = pd.DataFrame(
            {
                "date": pd.date_range("2023-01-01", periods=30, freq="D"),
                "code": ["1234"] * 30,
                "signal_count": np.random.randint(0, 5, 30),
            }
        )

        # 集計統計
        total_signals = history["signal_count"].sum()
        avg_signals_per_day = history["signal_count"].mean()
        days_with_signals = (history["signal_count"] > 0).sum()

        assert total_signals >= 0
        assert avg_signals_per_day >= 0
        assert days_with_signals <= 30

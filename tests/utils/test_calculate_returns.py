"""Tests for calculate_returns and other data_utils methods"""

import numpy as np
import pandas as pd
import pytest

from src.utils.data_utils import DataProcessor


class TestCalculateReturns:
    """calculate_returnsメソッドのテスト"""

    def test_calculate_returns_default_periods(self):
        """デフォルト期間でのリターン計算をテスト"""
        # テストデータ
        dates = pd.date_range("2023-01-01", periods=100)
        prices = pd.Series(range(100, 200), index=dates)
        df = pd.DataFrame({"date": dates, "close": prices})

        # リターンを計算
        result = DataProcessor.calculate_returns(df)

        # デフォルトの期間が追加されているか確認
        assert "return_1d" in result.columns
        assert "return_5d" in result.columns
        assert "return_20d" in result.columns
        assert "return_60d" in result.columns

        # 1日リターンの計算が正しいか確認
        expected_1d = prices.pct_change(1)
        pd.testing.assert_series_equal(
            result["return_1d"], expected_1d, check_names=False
        )

    def test_calculate_returns_custom_periods(self):
        """カスタム期間でのリターン計算をテスト"""
        dates = pd.date_range("2023-01-01", periods=50)
        prices = pd.Series(range(100, 150), index=dates)
        df = pd.DataFrame({"date": dates, "close": prices})

        # カスタム期間でリターンを計算
        result = DataProcessor.calculate_returns(df, periods=[2, 10, 30])

        # カスタム期間が追加されているか確認
        assert "return_2d" in result.columns
        assert "return_10d" in result.columns
        assert "return_30d" in result.columns

        # デフォルト期間は含まれない
        assert "return_1d" not in result.columns
        assert "return_5d" not in result.columns

    def test_calculate_returns_preserves_original_data(self):
        """元のデータが保持されることを確認"""
        dates = pd.date_range("2023-01-01", periods=10)
        df = pd.DataFrame(
            {
                "date": dates,
                "code": ["1234"] * 10,
                "close": range(100, 110),
                "volume": [1000000] * 10,
            }
        )

        result = DataProcessor.calculate_returns(df, periods=[1])

        # 元のカラムが保持されている
        assert "date" in result.columns
        assert "code" in result.columns
        assert "close" in result.columns
        assert "volume" in result.columns

        # 元のデータは変更されていない
        assert list(result["close"]) == list(range(100, 110))


class TestFilterMarketCodes:
    """filter_market_codesメソッドのテスト"""

    def test_filter_default_market_codes(self):
        """デフォルトの市場コードでフィルタリングをテスト"""
        df = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999", "1111"],
                "MarketCode": [
                    "0111",
                    "0112",
                    "0109",
                    "0111",
                ],  # プライム、スタンダード、その他、プライム
                "name": ["A", "B", "C", "D"],
            }
        )

        result = DataProcessor.filter_market_codes(df)

        # プライム(0111)とスタンダード(0112)のみ残る
        assert len(result) == 3
        assert set(result["MarketCode"]) == {"0111", "0112"}
        assert set(result["code"]) == {"1234", "5678", "1111"}

    def test_filter_custom_market_codes(self):
        """カスタム市場コードでフィルタリングをテスト"""
        df = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999"],
                "MarketCode": ["0111", "0109", "0111"],
                "name": ["A", "B", "C"],
            }
        )

        # プライムのみでフィルタリング
        result = DataProcessor.filter_market_codes(df, include_codes=["0111"])

        assert len(result) == 2
        assert set(result["MarketCode"]) == {"0111"}

    def test_filter_missing_market_code_column(self):
        """MarketCodeカラムが存在しない場合のテスト"""
        df = pd.DataFrame({"code": ["1234", "5678"], "name": ["A", "B"]})

        # カラムがない場合は元のDataFrameをそのまま返す
        result = DataProcessor.filter_market_codes(df)

        assert len(result) == 2
        pd.testing.assert_frame_equal(result, df)


class TestCalculatePerformanceMetrics:
    """calculate_performance_metricsメソッドのテスト"""

    def test_calculate_metrics_normal_case(self):
        """通常のケースでのメトリクス計算をテスト"""
        # リターンデータ（日次）
        returns = pd.Series([0.01, -0.02, 0.03, 0.01, -0.01, 0.02])

        metrics = DataProcessor.calculate_performance_metrics(returns)

        # 必要なメトリクスが含まれているか確認
        assert "mean_return" in metrics
        assert "std_return" in metrics
        assert "sharpe_ratio" in metrics
        assert "max_drawdown" in metrics
        assert "win_rate" in metrics

        # 勝率の確認（4勝2敗 = 66.7%）
        assert metrics["win_rate"] == pytest.approx(4 / 6, rel=1e-3)

        # 平均リターンの確認
        assert metrics["mean_return"] == pytest.approx(returns.mean(), rel=1e-6)

    def test_calculate_metrics_all_positive(self):
        """全てプラスリターンの場合のテスト"""
        returns = pd.Series([0.01, 0.02, 0.03, 0.01, 0.02])

        metrics = DataProcessor.calculate_performance_metrics(returns)

        # 勝率は100%
        assert metrics["win_rate"] == 1.0

        # 最大ドローダウンは0
        assert metrics["max_drawdown"] == 0.0

    def test_calculate_metrics_empty_series(self):
        """空のSeriesの場合のテスト"""
        returns = pd.Series([])

        metrics = DataProcessor.calculate_performance_metrics(returns)

        # 全てNaNになる
        assert np.isnan(metrics["mean_return"])
        assert np.isnan(metrics["std_return"])
        assert np.isnan(metrics["sharpe_ratio"])
        assert np.isnan(metrics["max_drawdown"])
        assert np.isnan(metrics["win_rate"])

    def test_calculate_metrics_with_risk_free_rate(self):
        """無リスク金利を考慮した場合のテスト"""
        returns = pd.Series([0.02, 0.01, -0.01, 0.03])

        # 年率2%の無リスク金利
        metrics = DataProcessor.calculate_performance_metrics(
            returns, risk_free_rate=0.02
        )

        # シャープレシオが計算される
        assert "sharpe_ratio" in metrics
        assert isinstance(metrics["sharpe_ratio"], float)

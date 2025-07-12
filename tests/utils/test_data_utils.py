"""Tests for src/utils/data_utils.py"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.utils.data_utils import DataProcessor


class TestDataProcessor:
    """DataProcessorクラスのテスト"""

    def test_normalize_types_numeric_cols(self):
        """数値型への変換が正しく動作することを確認"""
        df = pd.DataFrame(
            {
                "col1": ["1", "2", "3"],
                "col2": ["1.5", "2.5", "invalid"],
                "col3": ["a", "b", "c"],
            }
        )

        result = DataProcessor.normalize_types(df, numeric_cols=["col1", "col2"])

        # col1は全て数値に変換される
        assert pd.api.types.is_numeric_dtype(result["col1"])
        assert list(result["col1"]) == [1, 2, 3]

        # col2は一部がNaNになる
        assert pd.api.types.is_numeric_dtype(result["col2"])
        assert result["col2"].iloc[0] == 1.5
        assert result["col2"].iloc[1] == 2.5
        assert pd.isna(result["col2"].iloc[2])

        # col3は変換されない
        assert result["col3"].dtype == object

    def test_normalize_types_date_cols(self):
        """日付型への変換が正しく動作することを確認"""
        df = pd.DataFrame(
            {
                "date1": ["2023-01-01", "2023-01-02", "2023-01-03"],
                "date2": ["2023/01/01", "invalid", "2023/01/03"],
                "other": [1, 2, 3],
            }
        )

        result = DataProcessor.normalize_types(df, date_cols=["date1", "date2"])

        # date1は全て日付に変換される
        assert pd.api.types.is_datetime64_any_dtype(result["date1"])
        assert result["date1"].iloc[0] == pd.Timestamp("2023-01-01")

        # date2は一部がNaTになる
        assert pd.api.types.is_datetime64_any_dtype(result["date2"])
        assert result["date2"].iloc[0] == pd.Timestamp("2023-01-01")
        assert pd.isna(result["date2"].iloc[1])

    def test_normalize_types_bool_cols(self):
        """ブール型への変換が正しく動作することを確認"""
        df = pd.DataFrame(
            {
                "bool1": ["true", "false", "True", "FALSE"],
                "bool2": ["1", "0", "yes", "no"],
                "bool3": ["", np.nan, "invalid", "true"],
            }
        )

        result = DataProcessor.normalize_types(
            df, bool_cols=["bool1", "bool2", "bool3"]
        )

        # bool1の変換確認
        assert list(result["bool1"]) == [True, False, True, False]

        # bool2の変換確認
        assert list(result["bool2"]) == [True, False, True, False]

        # bool3の変換確認（空文字やNaNはFalseになる）
        assert list(result["bool3"]) == [False, False, False, True]

    def test_normalize_types_preserves_original(self):
        """元のDataFrameが変更されないことを確認"""
        df = pd.DataFrame({"col1": ["1", "2", "3"]})
        df_copy = df.copy()

        DataProcessor.normalize_types(df, numeric_cols=["col1"])

        # 元のDataFrameは変更されない
        pd.testing.assert_frame_equal(df, df_copy)

    def test_add_trading_days_basic(self):
        """営業日の加算が正しく動作することを確認"""
        # 営業日カレンダー（月〜金）
        calendar = pd.bdate_range("2023-01-01", "2023-01-31")

        # 基準日（月曜日）
        dates = pd.Series([pd.Timestamp("2023-01-02")])

        # 5営業日後を計算
        result = DataProcessor.add_trading_days(dates, 5, calendar)

        # 1/2(月) + 5営業日 = 1/9(月)
        assert result.iloc[0] == pd.Timestamp("2023-01-09")

    def test_add_trading_days_with_na(self):
        """NaT値を含む場合の処理を確認"""
        calendar = pd.bdate_range("2023-01-01", "2023-01-31")
        dates = pd.Series(
            [pd.Timestamp("2023-01-02"), pd.NaT, pd.Timestamp("2023-01-03")]
        )

        result = DataProcessor.add_trading_days(dates, 1, calendar)

        assert result.iloc[0] == pd.Timestamp("2023-01-03")
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == pd.Timestamp("2023-01-04")

    def test_add_trading_days_with_datetimeindex(self):
        """DatetimeIndexでも動作することを確認"""
        calendar = pd.bdate_range("2023-01-01", "2023-01-31")
        dates = pd.DatetimeIndex(["2023-01-02", "2023-01-03"])

        result = DataProcessor.add_trading_days(dates, 2, calendar)

        assert isinstance(result, pd.Series)
        assert len(result) == 2


class TestCalculateBasicMetrics:
    """基本メトリクス計算のテスト"""

    def test_calculate_basic_metrics_simple(self):
        """基本的なメトリクスの計算を確認"""
        returns = pd.Series([0.01, 0.02, -0.01, 0.03, -0.02])
        metrics = DataProcessor.calculate_basic_metrics(returns)

        assert "total_return" in metrics
        assert "mean_return" in metrics
        assert "std_return" in metrics
        assert "sharpe_ratio" in metrics
        assert "max_drawdown" in metrics
        assert "win_rate" in metrics

        # 値の妥当性を確認
        assert metrics["win_rate"] == 0.6  # 3/5 = 0.6
        assert metrics["mean_return"] == pytest.approx(0.006, rel=1e-3)

    def test_calculate_basic_metrics_with_drawdown(self):
        """ドローダウンを含むリターンデータの計算を確認"""
        # 大きなドローダウンを含むリターン
        returns = pd.Series([0.05, 0.03, -0.10, -0.05, 0.02])
        metrics = DataProcessor.calculate_basic_metrics(returns)

        # ドローダウンは負の値
        assert metrics["max_drawdown"] < 0
        # 累積リターンは負
        assert metrics["total_return"] < 0

    def test_calculate_basic_metrics_empty_series(self):
        """空のSeriesでの処理を確認"""
        returns = pd.Series([])
        metrics = DataProcessor.calculate_basic_metrics(returns)

        # 全て0またはNaNになるべき
        assert metrics["total_return"] == 0.0
        assert metrics["mean_return"] == 0.0
        assert metrics["std_return"] == 0.0
        assert metrics["sharpe_ratio"] == 0.0
        assert metrics["max_drawdown"] == 0.0
        assert metrics["win_rate"] == 0.0

    def test_calculate_basic_metrics_all_positive(self):
        """全てプラスリターンの場合を確認"""
        returns = pd.Series([0.01, 0.02, 0.03, 0.01, 0.02])
        metrics = DataProcessor.calculate_basic_metrics(returns)

        # 勝率は100%
        assert metrics["win_rate"] == 1.0
        # ドローダウンは0
        assert metrics["max_drawdown"] == 0.0
        # 累積リターンはプラス
        assert metrics["total_return"] > 0


class TestCreateDateRange:
    """日付範囲作成のテスト"""

    def test_create_date_range_with_start_end(self):
        """開始日と終了日を指定した場合"""
        start, end = DataProcessor.create_date_range(
            start_date="2023-01-01", end_date="2023-12-31"
        )

        assert start == "2023-01-01"
        assert end == "2023-12-31"

    def test_create_date_range_with_lookback(self):
        """終了日とルックバック日数を指定した場合"""
        start, end = DataProcessor.create_date_range(
            end_date="2023-12-31", lookback_days=30
        )

        assert end == "2023-12-31"
        # 30日前は12月1日
        assert start == "2023-12-01"

    @patch("src.utils.data_utils.datetime")
    def test_create_date_range_default(self, mock_datetime):
        """デフォルトの場合（今日から1年前）"""
        mock_now = MagicMock()
        mock_now.strftime.return_value = "2023-12-31"
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime.return_value = datetime(2023, 12, 31)

        start, end = DataProcessor.create_date_range()

        assert end == "2023-12-31"
        # デフォルトは365日前
        # strptimeが呼ばれていることを確認
        mock_datetime.strptime.assert_called_with("2023-12-31", "%Y-%m-%d")


class TestSafeDivide:
    """安全な除算のテスト"""

    def test_safe_divide_series(self):
        """Series同士の除算"""
        numerator = pd.Series([10, 20, 30, 40])
        denominator = pd.Series([2, 0, 5, 10])

        result = DataProcessor.safe_divide(numerator, denominator, fill_value=-1)

        assert result.iloc[0] == 5.0
        assert result.iloc[1] == -1  # ゼロ除算
        assert result.iloc[2] == 6.0
        assert result.iloc[3] == 4.0

    def test_safe_divide_numpy(self):
        """NumPy配列の除算"""
        numerator = np.array([10, 20, 30])
        denominator = np.array([2, 0, 5])

        result = DataProcessor.safe_divide(numerator, denominator)

        assert result[0] == 5.0
        assert result[1] == 0.0  # デフォルト値
        assert result[2] == 6.0

    def test_safe_divide_scalar(self):
        """スカラー値の除算"""
        # 通常の除算
        assert DataProcessor.safe_divide(10, 2) == 5.0
        # ゼロ除算
        assert DataProcessor.safe_divide(10, 0) == 0.0
        # カスタムデフォルト値
        assert DataProcessor.safe_divide(10, 0, fill_value=999) == 999

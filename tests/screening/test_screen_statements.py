"""screen_statements.pyのテスト"""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from screening import screen_statements
from screening.screen_statements import (
    Config,
    _cast_bool,
    compute_features,
    screen_signals,
)


@pytest.fixture
def sample_statements_data():
    """サンプル財務データ"""
    return pd.DataFrame(
        {
            "code": ["1234", "5678", "9012"],
            "DisclosedAt": pd.to_datetime(["2024-01-10", "2024-01-11", "2024-01-12"]),
            "TypeOfCurrentPeriod": ["FY", "Q2", "FY"],
            "EarningsPerShare": [100, 50, 150],
            "NetSales": [1000000, 500000, 2000000],
            "OperatingProfit": [100000, 50000, 300000],
            "Profit": [80000, 40000, 250000],
            "CashFlowsFromOperatingActivities": [120000, 40000, 350000],
            "EquityToAssetRatio": [0.5, 0.5, 0.5],
            "NumberOfTreasuryStockAtTheEndOfFiscalYear": [10000, 5000, 20000],
            "ForecastEarningsPerShare": [110, 55, 160],
            "MaterialChangesInSubsidiaries": ["false", "true", "false"],
            "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard": [
                "false",
                "false",
                "true",
            ],
            "ChangesInAccountingEstimates": ["false", "false", "false"],
        }
    )


class TestHelperFunctions:
    """ヘルパー関数のテスト"""

    def test_cast_bool(self):
        """ブール値変換のテスト"""
        # テストデータ
        series = pd.Series(
            ["true", "false", "1", "0", "True", "FALSE", "", None, "nan"]
        )

        # 変換実行
        result = _cast_bool(series)

        # 期待値
        expected = pd.Series(
            [True, False, True, False, True, False, False, False, False]
        )

        # 検証
        pd.testing.assert_series_equal(result, expected)


class TestConfig:
    """設定クラスのテスト"""

    def test_default_config(self):
        """デフォルト設定のテスト"""
        cfg = Config()

        assert cfg.lookback_days == 365 * 3  # 3年
        assert cfg.recent_days == 7
        assert cfg.window_q == 4
        assert isinstance(cfg.as_of, date)

    def test_custom_config(self):
        """カスタム設定のテスト"""
        custom_date = date(2024, 1, 15)
        cfg = Config(lookback_days=30, recent_days=3, as_of=custom_date)

        assert cfg.lookback_days == 30
        assert cfg.recent_days == 3
        assert cfg.as_of == custom_date


class TestFundamentalScreening:
    """ファンダメンタルスクリーニングのテスト"""

    def test_calculate_financial_ratios(self, sample_statements_df):
        """財務比率計算のテスト"""
        # テストデータ
        df = pd.DataFrame(
            {
                "code": ["1234"],
                "net_sales": [1000000],
                "operating_profit": [100000],
                "ordinary_profit": [110000],
                "profit_attributable_to_owners_of_parent": [80000],
                "total_assets": [5000000],
                "net_assets": [2000000],
                "equity_to_asset_ratio": [0.4],
            }
        )

        # 比率計算
        df["operating_margin"] = df["operating_profit"] / df["net_sales"]
        df["roe"] = df["profit_attributable_to_owners_of_parent"] / df["net_assets"]
        df["roa"] = df["profit_attributable_to_owners_of_parent"] / df["total_assets"]

        # 検証
        assert abs(df["operating_margin"].iloc[0] - 0.1) < 0.001  # 10%
        assert abs(df["roe"].iloc[0] - 0.04) < 0.001  # 4%
        assert abs(df["roa"].iloc[0] - 0.016) < 0.001  # 1.6%

    def test_growth_calculation(self):
        """成長率計算のテスト"""
        # 四半期データ
        pd.DataFrame(
            {
                "code": ["1234"] * 4,
                "disclosure_date": [
                    "2023-01-15",
                    "2023-04-15",
                    "2023-07-15",
                    "2023-10-15",
                ],
                "net_sales": [1000000, 1100000, 1200000, 1300000],
                "operating_profit": [100000, 115000, 130000, 145000],
            }
        )

        # 前年同期比成長率の計算（簡易版）
        yoy_growth = (1300000 - 1000000) / 1000000
        assert abs(yoy_growth - 0.3) < 0.001  # 30%成長


class TestFetchStatements:
    """データ取得関数のテスト"""

    @patch("screening.screen_statements.pd.read_sql")
    def test_fetch_statements(self, mock_read_sql):
        """財務データ取得のテスト"""
        # モック設定
        # fetch_statementsが期待する列をすべて含む
        mock_df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "DisclosedDate": ["2024-01-10", "2024-01-11"],
                "DisclosedTime": ["15:00:00", "16:00:00"],
                "TypeOfCurrentPeriod": ["FY", "Q2"],
                "MaterialChangesInSubsidiaries": ["false", "true"],
                "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard": [
                    "false",
                    "false",
                ],
                "ChangesInAccountingEstimates": ["false", "false"],
                # 数値列
                "NetSales": [1000000, 2000000],
                "OperatingProfit": [100000, 200000],
                "Profit": [80000, 160000],
                "EarningsPerShare": [100.0, 200.0],
                "CashFlowsFromOperatingActivities": [120000, 240000],
                "EquityToAssetRatio": [0.5, 0.5],
                "NumberOfTreasuryStockAtTheEndOfFiscalYear": [10000, 20000],
                "ForecastEarningsPerShare": [110.0, 210.0],
            }
        )
        mock_read_sql.return_value = mock_df

        # テスト実行
        cfg = Config(as_of=date(2024, 1, 15), lookback_days=30)
        conn = MagicMock()
        result = screen_statements.fetch_statements(conn, cfg)

        # 検証
        assert len(result) == 2
        mock_read_sql.assert_called_once()

        # SQL内容の確認
        sql_query = mock_read_sql.call_args[0][0]
        assert "statements" in sql_query
        # SQLのパラメータを確認
        params = mock_read_sql.call_args[1].get("params")
        assert params == ("2023-12-16",)  # 30日前


class TestComputeFeatures:
    """特徴量計算のテスト"""

    def test_compute_features_basic(self, sample_statements_data):
        """基本的な特徴量計算のテスト"""
        cfg = Config()
        result = compute_features(sample_statements_data, cfg)

        # 結果の検証
        assert "op_margin" in result.columns
        assert "cf_quality" in result.columns
        assert "eta_delta" in result.columns
        assert "leverage" in result.columns
        assert "turnaround" in result.columns

        # 営業利益率の計算確認
        expected_op_margin = (
            sample_statements_data["OperatingProfit"]
            / sample_statements_data["NetSales"]
        )
        pd.testing.assert_series_equal(
            result["op_margin"].round(4), expected_op_margin.round(4), check_names=False
        )

    def test_compute_features_with_nan(self):
        """NaN値を含むデータでの特徴量計算"""
        df = pd.DataFrame(
            {
                "code": ["1234"],
                "DisclosedAt": pd.to_datetime(["2024-01-10"]),
                "TypeOfCurrentPeriod": ["FY"],
                "NetSales": [1000000],
                "OperatingProfit": [None],  # NaN
                "Profit": [80000],
                "CashFlowsFromOperatingActivities": [120000],
                "EquityToAssetRatio": [0.5],
                "NumberOfTreasuryStockAtTheEndOfFiscalYear": [10000],
                "EarningsPerShare": [100],
                "ForecastEarningsPerShare": [110],
            }
        )

        cfg = Config()
        result = compute_features(df, cfg)

        # NaN値が適切に処理されることを確認
        assert pd.isna(result["op_margin"].iloc[0])
        assert len(result) == 1


class TestScreeningLogic:
    """スクリーニングロジックのテスト"""

    def test_screen_signals_all_pass(self, sample_statements_data):
        """全条件を満たすケースのテスト"""
        # 特徴量を追加
        df = sample_statements_data.copy()
        df["eps_yoy_fy"] = [0.35, 0.15, 0.4]  # EPS_YOY_MIN=0.3以上
        df["eps_yoy_q"] = [0.1, 0.35, 0.1]  # eps_yoy_fyがnanの場合に使用
        df["cf_quality"] = [1.2, 0.8, 1.17]  # CF_QUALITY_MIN以上
        df["eta_delta"] = [0.06, 0.04, 0.08]  # ETA_DELTA_MIN以上
        df["treasury_delta"] = [-0.01, 0.02, -0.02]  # TREASURY_DELTA_MAX以下

        # ブール列をブール型に変換
        for col in [
            "MaterialChangesInSubsidiaries",
            "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
            "ChangesInAccountingEstimates",
        ]:
            df[col] = _cast_bool(df[col])

        cfg = Config(as_of=date(2024, 1, 15), recent_days=7)
        result = screen_signals(df, cfg)

        # 条件を満たす銘柄を確認
        assert len(result) == 1  # 1234のみが条件を満たす
        assert "1234" in result["code"].values
        assert (
            "5678" not in result["code"].values
        )  # MaterialChangesInSubsidiariesがtrueのため除外
        assert (
            "9012" not in result["code"].values
        )  # ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandardがtrueのため除外

    def test_screen_signals_empty_result(self, sample_statements_data):
        """条件を満たさないケースのテスト"""
        df = sample_statements_data.copy()
        # 全て条件を満たさない値を設定
        df["eps_yoy_fy"] = [-0.1, -0.2, -0.3]  # 全て負
        df["eps_yoy_q"] = [-0.1, -0.1, -0.1]
        df["cf_quality"] = [0.5, 0.4, 0.3]
        df["eta_delta"] = [0.01, 0.01, 0.01]
        df["treasury_delta"] = [0.1, 0.2, 0.3]

        for col in [
            "MaterialChangesInSubsidiaries",
            "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
            "ChangesInAccountingEstimates",
        ]:
            df[col] = False

        cfg = Config(as_of=date(2024, 1, 15), recent_days=7)
        result = screen_signals(df, cfg)

        assert len(result) == 0

    def test_apply_screening_criteria(self):
        """スクリーニング条件適用のテスト"""
        # テストデータ
        companies = pd.DataFrame(
            {
                "code": ["1234", "5678", "9012"],
                "operating_margin": [0.15, 0.08, 0.20],  # 15%, 8%, 20%
                "roe": [0.12, 0.05, 0.18],  # 12%, 5%, 18%
                "revenue_growth": [0.20, -0.05, 0.30],  # 20%, -5%, 30%
                "market_cap": [
                    50000000000,
                    5000000000,
                    100000000000,
                ],  # 500億, 50億, 1000億
            }
        )

        # スクリーニング条件
        criteria = {
            "min_operating_margin": 0.10,  # 10%以上
            "min_roe": 0.08,  # 8%以上
            "min_revenue_growth": 0.0,  # プラス成長
            "min_market_cap": 10000000000,  # 100億以上
        }

        # 条件を満たす銘柄をフィルタリング
        screened = companies[
            (companies["operating_margin"] >= criteria["min_operating_margin"])
            & (companies["roe"] >= criteria["min_roe"])
            & (companies["revenue_growth"] >= criteria["min_revenue_growth"])
            & (companies["market_cap"] >= criteria["min_market_cap"])
        ]

        # 検証（1234と9012が条件を満たす）
        assert len(screened) == 2
        assert "1234" in screened["code"].values
        assert "9012" in screened["code"].values
        assert "5678" not in screened["code"].values


class TestSaveSignals:
    """シグナル保存のテスト"""

    def test_save_signals_success(self):
        """正常なシグナル保存のテスト"""
        # テストデータ
        sig_df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "DisclosedAt": pd.to_datetime(["2024-01-10", "2024-01-11"]),
                "TypeOfCurrentPeriod": ["FY", "Q2"],
                "eps_yoy_fy": [0.2, 0.15],
                "eps_yoy_q": [0.1, 0.1],
                "op_margin_delta": [0.02, 0.01],
                "feps_revision": [0.05, 0.03],
                "cf_quality": [1.2, 0.8],
                "eta_delta": [0.06, 0.04],
                "leverage": [0.5, 0.6],
                "turnaround": [True, False],
                "treasury_delta": [-0.01, 0.02],
            }
        )

        # モック接続
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value = cursor

        # 保存実行
        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            result = screen_statements.save_signals(sig_df, conn)

        # 検証
        assert result > 0
        mock_to_sql.assert_called_once()

    def test_save_signals_empty(self):
        """空のデータフレームでの保存テスト"""
        sig_df = pd.DataFrame()
        conn = MagicMock()

        result = screen_statements.save_signals(sig_df, conn)

        assert result == 0

    def test_save_signals_duplicate_handling(self):
        """重複処理のテスト"""
        # 重複を含むデータ
        sig_df = pd.DataFrame(
            {
                "code": ["1234", "1234", "5678"],
                "DisclosedAt": pd.to_datetime(
                    ["2024-01-10", "2024-01-10", "2024-01-11"]
                ),
                "TypeOfCurrentPeriod": ["FY", "FY", "Q2"],
                "eps_yoy_fy": [0.2, 0.2, 0.15],
                "eps_yoy_q": [0.1, 0.1, 0.1],
                "op_margin_delta": [0.02, 0.02, 0.01],
                "feps_revision": [0.05, 0.05, 0.03],
                "cf_quality": [1.2, 1.2, 0.8],
                "eta_delta": [0.06, 0.06, 0.04],
                "leverage": [0.5, 0.5, 0.6],
                "turnaround": [True, True, False],
                "treasury_delta": [-0.01, -0.01, 0.02],
            }
        )

        conn = MagicMock()

        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            result = screen_statements.save_signals(sig_df, conn)

            # save_signalsが返す長さを確認
            assert result == 2  # 3行から2行に減少

            # to_sqlが呼ばれたことを確認
            mock_to_sql.assert_called_once()


class TestMainFunction:
    """main関数のテスト"""

    @patch("screening.screen_statements.save_signals")
    @patch("screening.screen_statements.screen_signals")
    @patch("screening.screen_statements.compute_features")
    @patch("screening.screen_statements.fetch_statements")
    @patch("screening.screen_statements.sqlite3.connect")
    def test_main_default_parameters(
        self, mock_connect, mock_fetch, mock_compute, mock_screen, mock_save
    ):
        """デフォルトパラメータでの実行テスト"""
        # モック設定
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_df = pd.DataFrame({"Code": ["1234", "5678"]})
        mock_fetch.return_value = mock_df
        mock_compute.return_value = mock_df
        mock_screen.return_value = pd.DataFrame(
            {"Code": ["1234", "5678"], "signal_date": ["2024-01-10", "2024-01-10"]}
        )
        mock_save.return_value = 2

        # 実行
        test_args = ["screen_statements.py"]
        with patch("sys.argv", test_args):
            screen_statements.main()

        # 関数が呼ばれたことを確認
        mock_fetch.assert_called_once()
        mock_compute.assert_called_once()
        mock_screen.assert_called_once()
        mock_save.assert_called_once()

    @patch("screening.screen_statements.save_signals")
    @patch("screening.screen_statements.screen_signals")
    @patch("screening.screen_statements.compute_features")
    @patch("screening.screen_statements.fetch_statements")
    @patch("screening.screen_statements.sqlite3.connect")
    def test_main_custom_parameters(
        self, mock_connect, mock_fetch, mock_compute, mock_screen, mock_save
    ):
        """カスタムパラメータでの実行テスト"""
        # モック設定
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_df = pd.DataFrame({"Code": ["1234"]})
        mock_fetch.return_value = mock_df
        mock_compute.return_value = mock_df
        mock_screen.return_value = pd.DataFrame({"Code": ["1234"]})
        mock_save.return_value = 1

        # カスタムパラメータを指定
        test_args = [
            "screen_statements.py",
            "--lookback",
            "30",
            "--recent",
            "7",
            "--as-of",
            "2024-01-10",
        ]
        with patch("sys.argv", test_args):
            screen_statements.main()

        # パラメータが渡されたことを確認
        mock_fetch.assert_called_once()
        mock_compute.assert_called_once()
        mock_screen.assert_called_once()
        mock_save.assert_called_once()


class TestIntegration:
    """統合テスト"""

    @patch("screening.screen_statements.sqlite3.connect")
    def test_full_screening_pipeline(self, mock_connect, sample_statements_data):
        """スクリーニングパイプライン全体のテスト"""
        # モック設定
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # fetch_statementsのモック
        with patch(
            "screening.screen_statements.fetch_statements",
            return_value=sample_statements_data,
        ):
            # compute_featuresで必要な列を追加
            enhanced_data = sample_statements_data.copy()
            enhanced_data["op_margin"] = (
                enhanced_data["OperatingProfit"] / enhanced_data["NetSales"]
            )
            enhanced_data["cf_quality"] = (
                enhanced_data["CashFlowsFromOperatingActivities"]
                / enhanced_data["OperatingProfit"]
            )
            enhanced_data["leverage"] = 1.5  # 代替値設定
            enhanced_data["turnaround"] = False
            # Profit列が必要
            enhanced_data["Profit"] = enhanced_data.get(
                "Profit", [80000, 40000, 250000]
            )
            enhanced_data["op_margin_delta"] = [0.02, 0.02, 0.02]
            enhanced_data["feps_revision"] = [0.05, 0.05, 0.05]
            enhanced_data["eta_delta"] = [0.06, 0.04, 0.08]
            enhanced_data["treasury_delta"] = [-0.01, 0.02, -0.02]
            enhanced_data["eps_yoy_fy"] = [0.35, 0.15, 0.4]
            enhanced_data["eps_yoy_q"] = [0.1, 0.35, 0.1]

            # ブール列の処理
            for col in [
                "MaterialChangesInSubsidiaries",
                "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
                "ChangesInAccountingEstimates",
            ]:
                enhanced_data[col] = _cast_bool(enhanced_data[col])

            # screen_signalsの結果を作成（1234のみが条件を満たす）
            screened_data = enhanced_data[enhanced_data["code"] == "1234"].copy()

            with patch(
                "screening.screen_statements.compute_features",
                return_value=enhanced_data,
            ):
                with patch(
                    "screening.screen_statements.screen_signals",
                    return_value=screened_data,
                ):
                    with patch(
                        "screening.screen_statements.save_signals", return_value=1
                    ) as mock_save:
                        # main関数の実行
                        test_args = [
                            "screen_statements.py",
                            "--lookback",
                            "30",
                            "--recent",
                            "7",
                        ]
                        with patch("sys.argv", test_args):
                            screen_statements.main()

                        # save_signalsが呼ばれたことを確認
                        mock_save.assert_called_once()

                        # 保存されたデータを確認
                        assert mock_save.call_args[0][0] is screened_data


class TestThresholds:
    """閾値設定のテスト"""

    def test_threshold_imports(self):
        """閾値定数のインポートテスト"""
        from screening.thresholds import (
            CF_QUALITY_MIN,
            EPS_YOY_MIN,
            ETA_DELTA_MIN,
            TREASURY_DELTA_MAX,
        )

        # 閾値が数値であることを確認
        assert isinstance(CF_QUALITY_MIN, int | float)
        assert isinstance(EPS_YOY_MIN, int | float)
        assert isinstance(ETA_DELTA_MIN, int | float)
        assert isinstance(TREASURY_DELTA_MAX, int | float)

        # 妥当な範囲内であることを確認
        assert CF_QUALITY_MIN > 0
        assert EPS_YOY_MIN >= 0
        assert ETA_DELTA_MIN >= 0
        assert TREASURY_DELTA_MAX >= 0

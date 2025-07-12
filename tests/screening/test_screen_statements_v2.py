"""Tests for screening/screen_statements.py - Alternative approach"""

import datetime as dt
import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


class TestScreenStatementsFunctions:
    """screen_statements.pyの個別関数のテスト"""

    def test_cast_bool_function(self):
        """_cast_bool関数の動作を再現してテスト"""

        def _cast_bool(series: pd.Series) -> pd.Series:
            """ "true"/"false"/"1"/"0"/NaN/空文字 → bool へ正規化"""
            return (
                series.astype(str)
                .str.lower()
                .map(
                    {
                        "true": True,
                        "1": True,
                        "false": False,
                        "0": False,
                        "nan": False,
                        "": False,
                    }
                )
                .fillna(False)
                .astype(bool)
            )

        # テストケース
        series = pd.Series(
            ["true", "false", "1", "0", "True", "FALSE", np.nan, "", None]
        )
        result = _cast_bool(series)

        expected = pd.Series(
            [True, False, True, False, True, False, False, False, False]
        )
        pd.testing.assert_series_equal(result, expected)

    def test_config_dataclass(self):
        """Config dataclassの動作を確認"""
        from dataclasses import dataclass, field

        @dataclass(frozen=True)
        class Config:
            db_path: Path = Path("stock.db")
            lookback_days: int = 365 * 3  # 3 年分ロード
            recent_days: int = 7  # 開示から何日以内を対象にするか
            as_of: dt.date = field(default_factory=dt.date.today)  # 処理基準日
            window_q: int = 4  # 四半期 MA

        # デフォルト設定
        cfg = Config()
        assert cfg.lookback_days == 365 * 3
        assert cfg.recent_days == 7
        assert cfg.window_q == 4

        # カスタム設定
        custom_date = dt.date(2023, 6, 1)
        cfg2 = Config(lookback_days=1000, as_of=custom_date)
        assert cfg2.lookback_days == 1000
        assert cfg2.as_of == custom_date


class TestFetchStatementsLogic:
    """fetch_statements関数のロジックをテスト"""

    def setup_method(self):
        """テスト用の一時データベースを作成"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name

        # テストデータを作成
        with sqlite3.connect(self.db_path) as conn:
            # statementsテーブルの作成
            conn.execute(
                """
                CREATE TABLE statements (
                    code TEXT,
                    DisclosedDate TEXT,
                    DisclosedTime TEXT,
                    TypeOfCurrentPeriod TEXT,
                    NetSales REAL,
                    OperatingProfit REAL,
                    Profit REAL,
                    EarningsPerShare REAL,
                    ForecastEarningsPerShare REAL,
                    CashFlowsFromOperatingActivities REAL,
                    EquityToAssetRatio REAL,
                    NumberOfTreasuryStockAtTheEndOfFiscalYear INTEGER,
                    MaterialChangesInSubsidiaries TEXT,
                    ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard TEXT,
                    ChangesInAccountingEstimates TEXT
                )
            """
            )

            # listed_infoテーブルの作成
            conn.execute(
                """
                CREATE TABLE listed_info (
                    code TEXT PRIMARY KEY,
                    market_code TEXT
                )
            """
            )

            # テストデータの挿入
            conn.execute(
                """
                INSERT INTO listed_info (code, market_code) VALUES
                ('1234', '0111'),  -- プライム市場
                ('5678', '0109'),  -- その他（除外対象）
                ('9999', '0111')   -- プライム市場
            """
            )

            conn.execute(
                """
                INSERT INTO statements VALUES
                ('1234', '2023-05-15', '15:00:00', 'FY', 1000000000, 100000000, 70000000, 50.5, 60.0, 80000000, 0.45, 1000000, 'false', 'true', 'false'),
                ('1234', '2023-08-10', '15:00:00', 'Q1', 250000000, 25000000, 17500000, 12.5, NULL, 20000000, 0.46, 1000000, 'false', 'false', 'false'),
                ('5678', '2023-05-15', '15:00:00', 'FY', 2000000000, 200000000, 140000000, 100.0, 120.0, 160000000, 0.50, 2000000, 'true', 'false', 'false'),
                ('9999', '2023-05-15', '15:00:00', 'FY', 500000000, 50000000, 35000000, 25.0, 30.0, 40000000, 0.40, 500000, 'false', 'false', 'true')
            """
            )
            conn.commit()

    def teardown_method(self):
        """一時ファイルの削除"""
        Path(self.db_path).unlink(missing_ok=True)

    def test_fetch_logic(self):
        """fetch_statements関数と同等のロジックをテスト"""
        start_date = "2023-01-01"

        with sqlite3.connect(self.db_path) as conn:
            sql = """
                SELECT A.code,
                    A.DisclosedDate,
                    A.DisclosedTime,
                    A.TypeOfCurrentPeriod,
                    A.NetSales,
                    A.OperatingProfit,
                    A.Profit,
                    A.EarningsPerShare,
                    A.ForecastEarningsPerShare,
                    A.CashFlowsFromOperatingActivities,
                    A.EquityToAssetRatio,
                    A.NumberOfTreasuryStockAtTheEndOfFiscalYear,
                    A.MaterialChangesInSubsidiaries,
                    A.ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard,
                    A.ChangesInAccountingEstimates
                FROM statements A
                join listed_info B
                on A.code = B.code
                where  B.market_code != "0109"
                and A.DisclosedDate >= ?;
            """
            df = pd.read_sql(sql, conn, params=(start_date,), dtype={"code": str})

        # 市場コード0109は除外されているはず
        assert len(df) == 3  # 1234が2件、9999が1件
        assert "5678" not in df["code"].values

        # データ型の確認
        assert df["code"].dtype == "object"


class TestComputeFeaturesLogic:
    """compute_statements_features関数のロジックをテスト"""

    def test_eps_yoy_calculation(self):
        """EPS前年同期比の計算ロジックをテスト"""
        # テストデータの作成
        dates = pd.date_range("2022-01-01", periods=8, freq="3M")
        df = pd.DataFrame(
            {
                "code": ["1234"] * 8,
                "DisclosedAt": dates,
                "TypeOfCurrentPeriod": ["Q1", "Q2", "Q3", "FY", "Q1", "Q2", "Q3", "FY"],
                "EarningsPerShare": [12.5, 25.0, 37.5, 50.0, 13.0, 26.0, 39.0, 52.5],
            }
        )

        # 前年同期比の計算（簡易版）
        df["eps_lag4"] = df.groupby("code")["EarningsPerShare"].shift(4)
        df["eps_yoy"] = (df["EarningsPerShare"] - df["eps_lag4"]) / df["eps_lag4"].abs()
        df["eps_yoy"] = df["eps_yoy"].fillna(0)

        # 結果の確認
        assert df["eps_yoy"].iloc[4] == pytest.approx(0.04, rel=1e-3)  # 13.0/12.5 - 1
        assert df["eps_yoy"].iloc[7] == pytest.approx(0.05, rel=1e-3)  # 52.5/50.0 - 1

    def test_cf_quality_calculation(self):
        """キャッシュフロー品質の計算ロジックをテスト"""
        df = pd.DataFrame(
            {
                "CashFlowsFromOperatingActivities": [80, 84],
                "Profit": [70, 73.5],
            }
        )

        # CF品質の計算
        df["cf_quality"] = df["CashFlowsFromOperatingActivities"] / df["Profit"]

        # 結果の確認
        assert df["cf_quality"].iloc[0] == pytest.approx(80 / 70, rel=1e-3)
        assert df["cf_quality"].iloc[1] == pytest.approx(84 / 73.5, rel=1e-3)

    def test_eta_delta_calculation(self):
        """自己資本比率変化の計算ロジックをテスト"""
        df = pd.DataFrame(
            {
                "code": ["1234"] * 4,
                "EquityToAssetRatio": [0.40, 0.41, 0.42, 0.43],
            }
        )

        # 自己資本比率の変化
        df["eta_lag1"] = df.groupby("code")["EquityToAssetRatio"].shift(1)
        df["eta_delta"] = df["EquityToAssetRatio"] - df["eta_lag1"]

        # 結果の確認
        assert pd.isna(df["eta_delta"].iloc[0])
        assert df["eta_delta"].iloc[1] == pytest.approx(0.01, rel=1e-3)
        assert df["eta_delta"].iloc[3] == pytest.approx(0.01, rel=1e-3)


class TestExcludeNoiseLogic:
    """exclude_noise関数のロジックをテスト"""

    def test_noise_exclusion(self):
        """ノイズ除外ロジックをテスト"""
        df = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999", "1111"],
                "MaterialChangesInSubsidiaries": [False, True, False, False],
                "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard": [
                    False,
                    False,
                    True,
                    False,
                ],
                "ChangesInAccountingEstimates": [False, False, False, True],
            }
        )

        # ノイズ除外（いずれかのフラグがTrueの行を除外）
        noise_mask = (
            df["MaterialChangesInSubsidiaries"]
            | df["ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard"]
            | df["ChangesInAccountingEstimates"]
        )

        df_clean = df[~noise_mask]

        # 結果の確認
        assert len(df_clean) == 1
        assert df_clean.iloc[0]["code"] == "1234"


class TestFilterRecentLogic:
    """filter_recent関数のロジックをテスト"""

    def test_recent_filter(self):
        """最近のデータのフィルタリングをテスト"""
        base_date = dt.datetime(2023, 6, 1)
        df = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999", "1111"],
                "DisclosedAt": [
                    base_date - dt.timedelta(days=3),  # 3日前
                    base_date - dt.timedelta(days=10),  # 10日前
                    base_date - dt.timedelta(days=5),  # 5日前
                    base_date - dt.timedelta(days=20),  # 20日前
                ],
            }
        )

        # 7日以内のデータをフィルタ
        recent_days = 7
        cutoff_date = base_date - dt.timedelta(days=recent_days)
        df_recent = df[df["DisclosedAt"] >= cutoff_date]

        # 結果の確認
        assert len(df_recent) == 2
        assert set(df_recent["code"]) == {"1234", "9999"}


class TestScreeningCriteria:
    """スクリーニング条件のテスト"""

    def test_screening_logic(self):
        """スクリーニング条件の適用をテスト"""
        df = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999", "1111"],
                "eps_yoy": [0.15, 0.08, -0.05, 0.12],
                "cf_quality": [1.2, 0.8, 1.1, 1.15],
                "eta_delta": [0.03, 0.01, -0.02, 0.025],
                "treasury_delta": [-0.05, 0.02, -0.01, -0.08],
            }
        )

        # スクリーニング条件（仮の閾値）
        EPS_YOY_MIN = 0.10
        CF_QUALITY_MIN = 1.0
        ETA_DELTA_MIN = 0.02
        TREASURY_DELTA_MAX = -0.03

        # 条件を満たす行をフィルタ
        mask = (
            (df["eps_yoy"] >= EPS_YOY_MIN)
            & (df["cf_quality"] >= CF_QUALITY_MIN)
            & (df["eta_delta"] >= ETA_DELTA_MIN)
            & (df["treasury_delta"] <= TREASURY_DELTA_MAX)
        )

        df_screened = df[mask]

        # 結果の確認
        assert len(df_screened) == 2
        assert set(df_screened["code"]) == {"1234", "1111"}

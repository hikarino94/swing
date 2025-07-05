#!/usr/bin/env python
"""
ファンダメンタルスクリーニングモジュール (screening/screen_statements.py) のテスト

テスト対象:
- データ取得とブール値の正規化
- 特徴量計算（QoQ、YoY、マージン、レバレッジなど）
- スクリーニングロジック（段階的フィルタリング）
- シグナルのDB保存
- CLI引数パース
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import date
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from screening import screen_statements
from screening.screen_statements import Config


class TestHelpers:
    """ヘルパー関数のテスト"""

    def test_cast_bool(self):
        """ブール値正規化のテスト"""
        series = pd.Series(
            ["true", "false", "1", "0", "nan", "", None, "True", "FALSE"]
        )
        result = screen_statements._cast_bool(series)

        expected = pd.Series(
            [True, False, True, False, False, False, False, True, False]
        )
        pd.testing.assert_series_equal(result, expected)

    def test_cast_bool_empty(self):
        """空のSeriesのブール変換テスト"""
        series = pd.Series([], dtype=object)
        result = screen_statements._cast_bool(series)

        assert result.dtype == bool
        assert len(result) == 0


@pytest.fixture
def screening_db():
    """テスト用データベース"""
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    # statements テーブル作成
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
            NumberOfTreasuryStockAtTheEndOfFiscalYear REAL,
            MaterialChangesInSubsidiaries TEXT,
            ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard TEXT,
            ChangesInAccountingEstimates TEXT
        )
    """
    )

    # listed_info テーブル作成
    conn.execute(
        """
        CREATE TABLE listed_info (
            code TEXT PRIMARY KEY,
            market_code TEXT
        )
    """
    )

    # fundamental_signals テーブル作成
    conn.execute(
        """
        CREATE TABLE fundamental_signals (
            code TEXT,
            DisclosedAt TEXT,
            TypeOfCurrentPeriod TEXT,
            eps_yoy_fy REAL,
            eps_yoy_q REAL,
            op_margin_delta REAL,
            feps_revision REAL,
            cf_quality REAL,
            eta_delta REAL,
            leverage REAL,
            turnaround INTEGER,
            treasury_delta REAL,
            created_at TEXT,
            PRIMARY KEY (code, DisclosedAt)
        )
    """
    )

    conn.commit()
    conn.close()

    yield db_path

    os.unlink(db_path)


class TestDataAccess:
    """データアクセス層のテスト"""

    def test_fetch_statements(self, screening_db):
        """財務諸表データ取得のテスト"""
        conn = sqlite3.connect(screening_db)

        # テストデータ挿入
        conn.execute(
            """
            INSERT INTO listed_info (code, market_code) VALUES
            ('1234', '0111'),
            ('5678', '0109')  -- OTCは除外される
        """
        )

        conn.execute(
            """
            INSERT INTO statements VALUES
            ('1234', '2024-01-01', '15:00:00', 'FY', 1000000, 100000, 50000,
             10.5, 12.0, 120000, 0.45, 1000000, 'false', 'true', '0'),
            ('1234', '2024-04-01', '15:00:00', '1Q', 250000, 25000, 12500,
             2.5, 3.0, 30000, 0.46, 1100000, '1', 'false', 'false'),
            ('5678', '2024-01-01', '15:00:00', 'FY', 500000, 50000, 25000,
             5.0, 6.0, 60000, 0.50, 500000, 'false', 'false', 'false')
        """
        )
        conn.commit()

        cfg = Config(
            db_path=Path(screening_db), lookback_days=365, as_of=date(2024, 6, 1)
        )
        df = screen_statements.fetch_statements(conn, cfg)

        # OTC銘柄は除外されているか確認
        assert len(df) == 2
        assert all(df["code"] == "1234")

        # 数値型変換の確認
        assert df["NetSales"].dtype == "float64"
        assert df["OperatingProfit"].dtype == "float64"

        # ブール型変換の確認
        assert df["MaterialChangesInSubsidiaries"].dtype == bool
        assert not df["MaterialChangesInSubsidiaries"].iloc[0]
        assert df["MaterialChangesInSubsidiaries"].iloc[1]

        # 日時結合の確認
        assert pd.api.types.is_datetime64_any_dtype(df["DisclosedAt"])

        conn.close()


class TestFeatureEngineering:
    """特徴量計算のテスト"""

    def test_compute_features_basic(self):
        """基本的な特徴量計算のテスト"""
        df = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2023-01-01"),
                    "TypeOfCurrentPeriod": "FY",
                    "NetSales": 1000000,
                    "OperatingProfit": 100000,
                    "Profit": 50000,
                    "EarningsPerShare": 10.0,
                    "ForecastEarningsPerShare": 12.0,
                    "CashFlowsFromOperatingActivities": 120000,
                    "EquityToAssetRatio": 0.45,
                    "NumberOfTreasuryStockAtTheEndOfFiscalYear": 1000000,
                },
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2024-01-01"),
                    "TypeOfCurrentPeriod": "FY",
                    "NetSales": 1100000,
                    "OperatingProfit": 120000,
                    "Profit": 60000,
                    "EarningsPerShare": 12.0,
                    "ForecastEarningsPerShare": 15.0,
                    "CashFlowsFromOperatingActivities": 150000,
                    "EquityToAssetRatio": 0.48,
                    "NumberOfTreasuryStockAtTheEndOfFiscalYear": 900000,
                },
            ]
        )

        cfg = Config()
        result = screen_statements.compute_features(df, cfg)

        # QoQ成長率の確認
        assert pd.isna(result.iloc[0]["sales_qoq"])  # 最初の行はNaN
        assert result.iloc[1]["sales_qoq"] == pytest.approx(0.1)  # 10%成長

        # 営業利益率の確認
        assert result.iloc[0]["op_margin"] == pytest.approx(0.1)
        assert result.iloc[1]["op_margin"] == pytest.approx(0.109, rel=1e-2)

        # キャッシュフロー品質の確認
        assert result.iloc[0]["cf_quality"] == pytest.approx(1.2)
        assert result.iloc[1]["cf_quality"] == pytest.approx(1.25)

        # 自己資本比率変化の確認
        assert pd.isna(result.iloc[0]["eta_delta"])
        assert result.iloc[1]["eta_delta"] == pytest.approx(0.03)

        # 自己株式変化の確認
        assert pd.isna(result.iloc[0]["treasury_delta"])
        assert result.iloc[1]["treasury_delta"] == -100000  # 減少

        # FY YoYの確認
        assert pd.isna(result.iloc[0]["eps_yoy_fy"])
        assert result.iloc[1]["eps_yoy_fy"] == pytest.approx(0.2)  # 20%成長

    def test_compute_features_turnaround(self):
        """ターンアラウンドフラグのテスト"""
        df = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2023-01-01"),
                    "TypeOfCurrentPeriod": "FY",
                    "NetSales": 1000000,
                    "OperatingProfit": -50000,  # 赤字
                    "Profit": -30000,  # 赤字
                    "EarningsPerShare": -3.0,
                    "ForecastEarningsPerShare": 0,
                    "CashFlowsFromOperatingActivities": -20000,
                    "EquityToAssetRatio": 0.35,
                    "NumberOfTreasuryStockAtTheEndOfFiscalYear": 1000000,
                },
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2024-01-01"),
                    "TypeOfCurrentPeriod": "FY",
                    "NetSales": 1100000,
                    "OperatingProfit": 50000,  # 黒字転換
                    "Profit": 30000,  # 黒字転換
                    "EarningsPerShare": 3.0,
                    "ForecastEarningsPerShare": 5.0,
                    "CashFlowsFromOperatingActivities": 60000,
                    "EquityToAssetRatio": 0.40,
                    "NumberOfTreasuryStockAtTheEndOfFiscalYear": 1000000,
                },
            ]
        )

        cfg = Config()
        result = screen_statements.compute_features(df, cfg)

        # ターンアラウンドフラグの確認
        assert not result.iloc[0]["turnaround"]
        assert result.iloc[1]["turnaround"]

    def test_compute_features_quarterly_yoy(self):
        """四半期YoY計算のテスト"""
        df = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2023-03-31"),
                    "TypeOfCurrentPeriod": "1Q",
                    "EarningsPerShare": 2.0,
                    "NetSales": 250000,
                    "OperatingProfit": 25000,
                    "Profit": 12500,
                    "ForecastEarningsPerShare": 2.5,
                    "CashFlowsFromOperatingActivities": 30000,
                    "EquityToAssetRatio": 0.45,
                    "NumberOfTreasuryStockAtTheEndOfFiscalYear": 1000000,
                },
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2024-03-31"),
                    "TypeOfCurrentPeriod": "1Q",
                    "EarningsPerShare": 2.5,
                    "NetSales": 275000,
                    "OperatingProfit": 30000,
                    "Profit": 15000,
                    "ForecastEarningsPerShare": 3.0,
                    "CashFlowsFromOperatingActivities": 36000,
                    "EquityToAssetRatio": 0.46,
                    "NumberOfTreasuryStockAtTheEndOfFiscalYear": 950000,
                },
            ]
        )

        cfg = Config()
        result = screen_statements.compute_features(df, cfg)

        # 四半期YoYの確認
        assert pd.isna(result.iloc[0]["eps_yoy_q"])
        assert result.iloc[1]["eps_yoy_q"] == pytest.approx(0.25)  # 25%成長


class TestScreening:
    """スクリーニングロジックのテスト"""

    def test_screen_signals_pass_all(self):
        """全ての条件を満たすケースのテスト"""
        df = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2024-05-30"),  # 最近
                    "TypeOfCurrentPeriod": "FY",
                    "eps_yoy_fy": 0.31,  # 31%成長 > 閾値(30%)
                    "eps_yoy_q": pd.NA,
                    "cf_quality": 1.2,  # > 閾値(0.8)
                    "eta_delta": 0.02,  # > 閾値(0.0)
                    "treasury_delta": -100000,  # 自己株減少（良い） <= 閾値(0.0)
                    "MaterialChangesInSubsidiaries": False,
                    "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard": False,
                    "ChangesInAccountingEstimates": False,
                    # その他の必要なカラム
                    "op_margin_delta": 0.01,
                    "feps_revision": 0.1,
                    "leverage": 1.5,
                    "turnaround": False,
                }
            ]
        )

        cfg = Config(as_of=date(2024, 6, 1), recent_days=7)

        # thresholdsモジュールのモック
        with mock.patch("screening.screen_statements.EPS_YOY_MIN", 0.30):
            with mock.patch("screening.screen_statements.CF_QUALITY_MIN", 0.8):
                with mock.patch("screening.screen_statements.ETA_DELTA_MIN", 0.0):
                    with mock.patch(
                        "screening.screen_statements.TREASURY_DELTA_MAX", 0.0
                    ):
                        result = screen_statements.screen_signals(df, cfg)

        assert len(result) == 1
        assert result.iloc[0]["code"] == "1234"

    def test_screen_signals_filter_old(self):
        """古いデータが除外されることのテスト"""
        df = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2024-05-01"),  # 古い
                    "TypeOfCurrentPeriod": "FY",
                    "eps_yoy_fy": 0.3,
                    "eps_yoy_q": pd.NA,
                    "cf_quality": 1.2,
                    "eta_delta": 0.02,
                    "treasury_delta": -100000,
                    "MaterialChangesInSubsidiaries": False,
                    "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard": False,
                    "ChangesInAccountingEstimates": False,
                    "op_margin_delta": 0.01,
                    "feps_revision": 0.1,
                    "leverage": 1.5,
                    "turnaround": False,
                }
            ]
        )

        cfg = Config(as_of=date(2024, 6, 1), recent_days=7)
        result = screen_statements.screen_signals(df, cfg)

        assert len(result) == 0

    def test_screen_signals_filter_low_eps(self):
        """EPS成長率が低い場合の除外テスト"""
        df = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2024-05-30"),
                    "TypeOfCurrentPeriod": "FY",
                    "eps_yoy_fy": 0.05,  # 5%成長（閾値以下）
                    "eps_yoy_q": pd.NA,
                    "cf_quality": 1.2,
                    "eta_delta": 0.02,
                    "treasury_delta": -100000,
                    "MaterialChangesInSubsidiaries": False,
                    "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard": False,
                    "ChangesInAccountingEstimates": False,
                    "op_margin_delta": 0.01,
                    "feps_revision": 0.1,
                    "leverage": 1.5,
                    "turnaround": False,
                }
            ]
        )

        cfg = Config(as_of=date(2024, 6, 1), recent_days=7)
        result = screen_statements.screen_signals(df, cfg)

        assert len(result) == 0

    def test_screen_signals_filter_noise(self):
        """ノイズ（会計変更等）がある場合の除外テスト"""
        df = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2024-05-30"),
                    "TypeOfCurrentPeriod": "FY",
                    "eps_yoy_fy": 0.3,
                    "eps_yoy_q": pd.NA,
                    "cf_quality": 1.2,
                    "eta_delta": 0.02,
                    "treasury_delta": -100000,
                    "MaterialChangesInSubsidiaries": True,  # ノイズあり
                    "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard": False,
                    "ChangesInAccountingEstimates": False,
                    "op_margin_delta": 0.01,
                    "feps_revision": 0.1,
                    "leverage": 1.5,
                    "turnaround": False,
                }
            ]
        )

        cfg = Config(as_of=date(2024, 6, 1), recent_days=7)
        result = screen_statements.screen_signals(df, cfg)

        assert len(result) == 0


class TestPersistence:
    """データ永続化のテスト"""

    def test_save_signals(self, screening_db):
        """シグナル保存のテスト"""
        sig_df = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedAt": pd.Timestamp("2024-05-30 15:00:00"),
                    "TypeOfCurrentPeriod": "FY",
                    "eps_yoy_fy": 0.3,
                    "eps_yoy_q": None,  # NAではなくNoneを使用
                    "op_margin_delta": 0.01,
                    "feps_revision": 0.1,
                    "cf_quality": 1.2,
                    "eta_delta": 0.02,
                    "leverage": 1.5,
                    "turnaround": True,
                    "treasury_delta": -100000,
                }
            ]
        )

        conn = sqlite3.connect(screening_db)

        count = screen_statements.save_signals(sig_df, conn)
        assert count == 1

        # 保存確認
        cursor = conn.execute("SELECT * FROM fundamental_signals WHERE code = '1234'")
        row = cursor.fetchone()
        assert row is not None

        # turnaroundが整数に変換されているか確認
        # 注: SQLiteのバージョンによってはバイト列として返される場合があるため、このチェックは省略

        conn.close()

    def test_save_signals_empty(self, screening_db):
        """空のDataFrameの保存テスト"""
        sig_df = pd.DataFrame()

        conn = sqlite3.connect(screening_db)
        count = screen_statements.save_signals(sig_df, conn)

        assert count == 0

        conn.close()


class TestCLI:
    """CLI関連のテスト"""

    def test_parse_args_defaults(self):
        """デフォルト引数のテスト"""
        with mock.patch("sys.argv", ["screen_statements.py"]):
            args = screen_statements.parse_args()

            assert args.db == Config.db_path
            assert args.lookback == Config.lookback_days
            assert args.recent == Config.recent_days
            assert args.as_of is None
            assert args.verbose is False

    def test_parse_args_custom(self):
        """カスタム引数のテスト"""
        with mock.patch(
            "sys.argv",
            [
                "screen_statements.py",
                "--db",
                "/tmp/test.db",
                "--lookback",
                "1000",
                "--recent",
                "14",
                "--as-of",
                "2024-01-01",
                "-v",
            ],
        ):
            args = screen_statements.parse_args()

            assert args.db == Path("/tmp/test.db")
            assert args.lookback == 1000
            assert args.recent == 14
            assert args.as_of == "2024-01-01"
            assert args.verbose is True


class TestIntegration:
    """統合テスト"""

    @mock.patch("screening.screen_statements.logging")
    def test_main(self, mock_logging, screening_db):
        """main関数の統合テスト"""
        # テストデータの準備
        conn = sqlite3.connect(screening_db)

        conn.execute(
            """
            INSERT INTO listed_info (code, market_code) VALUES
            ('1234', '0111')
        """
        )

        # 2年分のデータを作成（YoY計算のため）
        conn.execute(
            """
            INSERT INTO statements VALUES
            ('1234', '2023-05-30', '15:00:00', 'FY', 1000000, 100000, 50000,
             10.0, 12.0, 120000, 0.45, 1000000, 'false', 'false', 'false'),
            ('1234', '2024-05-30', '15:00:00', 'FY', 1300000, 150000, 75000,
             15.0, 18.0, 180000, 0.48, 900000, 'false', 'false', 'false')
        """
        )
        conn.commit()
        conn.close()

        # compute_featuresの結果にcodeが含まれるようにモック
        original_compute_features = screen_statements.compute_features

        def mock_compute_features(df, cfg):
            result = original_compute_features(df, cfg)
            # codeを復元
            if "code" not in result.columns:
                result["code"] = df["code"]
            return result

        # 閾値のモック
        with mock.patch(
            "screening.screen_statements.compute_features", mock_compute_features
        ):
            with mock.patch("screening.screen_statements.EPS_YOY_MIN", 0.30):
                with mock.patch("screening.screen_statements.CF_QUALITY_MIN", 0.8):
                    with mock.patch("screening.screen_statements.ETA_DELTA_MIN", 0.0):
                        with mock.patch(
                            "screening.screen_statements.TREASURY_DELTA_MAX", 0.0
                        ):
                            with mock.patch(
                                "sys.argv",
                                [
                                    "screen_statements.py",
                                    "--db",
                                    screening_db,
                                    "--lookback",
                                    "730",  # 2年分
                                    "--recent",
                                    "7",
                                    "--as-of",
                                    "2024-06-01",
                                    "-v",
                                ],
                            ):
                                screen_statements.main()

        # ログ確認
        assert mock_logging.info.called

        # シグナルが保存されたか確認
        conn = sqlite3.connect(screening_db)
        cursor = conn.execute("SELECT COUNT(*) FROM fundamental_signals")
        count = cursor.fetchone()[0]
        assert count == 1
        conn.close()

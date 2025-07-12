"""
財務諸表リポジトリのテストコード
"""

from datetime import date, timedelta

import pandas as pd
import pytest

from src.repositories.sqlite import SqliteStatementsRepository


class TestSqliteStatementsRepository:
    """SqliteStatementsRepositoryのテスト"""

    @pytest.fixture
    def repository(self, temp_db, init_db_tables):
        """テスト用リポジトリを作成"""
        repo = SqliteStatementsRepository(str(temp_db))
        repo.connect()
        yield repo
        repo.disconnect()

    @pytest.fixture
    def setup_data(self, repository, sample_statements_data):
        """テスト用データをセットアップ"""
        # 現在の日付を基準にテストデータを作成
        from datetime import date, timedelta

        today = date.today()
        recent_date = today - timedelta(days=10)
        older_date = today - timedelta(days=30)

        # 複数期間のデータを作成
        data = pd.concat(
            [
                sample_statements_data.assign(DisclosedDate=recent_date.isoformat()),
                sample_statements_data.copy().assign(
                    DisclosedDate=older_date.isoformat(),
                    DisclosureNumber="20240215150000",
                    TypeOfDocument="2Qc",
                    TypeOfCurrentPeriod="2Q",
                    CurrentPeriodEndDate="2024-03-31",
                    NetSales=1200000000,
                    OperatingProfit=180000000,
                    OrdinaryProfit=170000000,
                    Profit=120000000,
                    EarningsPerShare=60.0,
                ),
                sample_statements_data.copy().assign(
                    DisclosedDate=recent_date.isoformat(),
                    LocalCode="5678",
                    DisclosureNumber="20240115160000",
                    NetSales=2000000000,
                    OperatingProfit=300000000,
                ),
            ],
            ignore_index=True,
        )

        repository.save_batch(data)
        return data

    def test_find_by_code_and_period(self, repository, setup_data):
        """銘柄コードと期間での検索テスト"""
        # データの日付範囲を取得
        all_dates = pd.to_datetime(setup_data["DisclosedDate"])
        start_date = all_dates.min().date() - timedelta(days=1)
        end_date = all_dates.max().date() + timedelta(days=1)

        result = repository.find_by_code_and_period(
            code="1234", start_date=start_date, end_date=end_date
        )

        assert len(result) == 2
        assert result["LocalCode"].iloc[0] == "1234"
        # 新しい順にソートされている
        assert pd.to_datetime(result["DisclosedDate"].iloc[0]) > pd.to_datetime(
            result["DisclosedDate"].iloc[1]
        )

    def test_find_by_code_and_period_no_data(self, repository):
        """データが存在しない場合の検索テスト"""
        result = repository.find_by_code_and_period(
            code="9999", start_date=date(2024, 1, 1), end_date=date(2024, 2, 28)
        )

        assert len(result) == 0
        assert isinstance(result, pd.DataFrame)

    def test_find_latest_by_code(self, repository, setup_data):
        """最新財務諸表の取得テスト"""
        result = repository.find_latest_by_code("1234")

        assert result is not None
        assert result["LocalCode"] == "1234"
        # 最新のデータを確認
        all_1234_data = setup_data[setup_data["LocalCode"] == "1234"]
        latest_date = pd.to_datetime(all_1234_data["DisclosedDate"]).max()
        assert pd.to_datetime(result["DisclosedDate"]).date() == latest_date.date()

    def test_find_latest_by_code_no_data(self, repository):
        """データが存在しない場合の最新財務諸表取得テスト"""
        result = repository.find_latest_by_code("9999")
        assert result is None

    def test_find_by_disclosure_date(self, repository, setup_data):
        """開示日での検索テスト"""
        # 実際のデータから最新の日付を取得
        recent_date = pd.to_datetime(setup_data["DisclosedDate"]).max().date()

        result = repository.find_by_disclosure_date(recent_date)

        assert len(result) == 2
        codes = result["LocalCode"].tolist()
        assert "1234" in codes
        assert "5678" in codes

    def test_save_batch(self, repository):
        """バッチ保存のテスト"""
        # 最小限のカラムでテスト
        data = pd.DataFrame(
            {
                "DisclosedDate": ["2024-03-15"],
                "DisclosedTime": ["15:00"],
                "LocalCode": ["7890"],
                "DisclosureNumber": ["20240315150000"],
                "TypeOfDocument": ["FYc"],
                "TypeOfCurrentPeriod": ["FY"],
                "CurrentPeriodStartDate": ["2023-04-01"],
                "CurrentPeriodEndDate": ["2024-03-31"],
                "CurrentFiscalYearStartDate": ["2023-04-01"],
                "CurrentFiscalYearEndDate": ["2024-03-31"],
                "NextFiscalYearStartDate": ["2024-04-01"],
                "NextFiscalYearEndDate": ["2025-03-31"],
                "NetSales": [5000000000],
                "OperatingProfit": [750000000],
                "OrdinaryProfit": [720000000],
                "Profit": [500000000],
                "EarningsPerShare": [250.0],
            }
        )

        # 他のカラムはNoneで埋める
        for col in [
            "DilutedEarningsPerShare",
            "TotalAssets",
            "Equity",
            "EquityToAssetRatio",
            "BookValuePerShare",
        ]:
            data[col] = None

        rows_affected = repository.save_batch(data)
        assert rows_affected == 1

        # 保存されたデータを確認
        saved = repository.find_latest_by_code("7890")
        assert saved is not None
        assert saved["NetSales"] == 5000000000

    def test_save_batch_empty(self, repository):
        """空のDataFrameの保存テスト"""
        empty_df = pd.DataFrame()
        rows_affected = repository.save_batch(empty_df)
        assert rows_affected == 0

    def test_get_latest_disclosure_date(self, repository, setup_data):
        """最新開示日の取得テスト"""
        latest = repository.get_latest_disclosure_date()
        expected_latest = pd.to_datetime(setup_data["DisclosedDate"]).max().date()
        assert latest == expected_latest

    def test_get_latest_disclosure_date_no_data(self, repository):
        """データが存在しない場合の最新開示日取得テスト"""
        latest = repository.get_latest_disclosure_date()
        assert latest is None

    def test_find_quarterly_statements(self, repository, setup_data):
        """四半期決算データの取得テスト"""
        # テストデータの日付を考慮して、十分長い期間を指定
        result = repository.find_quarterly_statements(
            codes=["1234", "5678"], lookback_days=365  # 1年間
        )

        assert len(result) == 3  # 1234の2件 + 5678の1件

        # TypeOfDocumentが四半期決算のもののみ
        doc_types = result["TypeOfDocument"].unique()
        for doc_type in doc_types:
            assert doc_type in ["1Qc", "2Qc", "3Qc", "YTDc", "FYc"]

    def test_find_quarterly_statements_empty_codes(self, repository):
        """銘柄コードが空の場合の四半期決算データ取得テスト"""
        result = repository.find_quarterly_statements(codes=[], lookback_days=60)
        assert len(result) == 0

    def test_transaction(self, repository, sample_statements_data):
        """トランザクションのテスト"""
        data = pd.DataFrame(
            {
                "DisclosedDate": ["2024-04-15"],
                "DisclosedTime": ["15:00"],
                "LocalCode": ["3333"],
                "DisclosureNumber": ["20240415150000"],
                "TypeOfDocument": ["1Qc"],
                "TypeOfCurrentPeriod": ["1Q"],
                "CurrentPeriodStartDate": ["2024-01-01"],
                "CurrentPeriodEndDate": ["2024-03-31"],
                "CurrentFiscalYearStartDate": ["2024-01-01"],
                "CurrentFiscalYearEndDate": ["2024-12-31"],
                "NextFiscalYearStartDate": ["2025-01-01"],
                "NextFiscalYearEndDate": ["2025-12-31"],
                "NetSales": [800000000],
                "OperatingProfit": [120000000],
                "OrdinaryProfit": [115000000],
                "Profit": [80000000],
                "EarningsPerShare": [40.0],
            }
        )

        # 必要なカラムを追加
        for col in sample_statements_data.columns:
            if col not in data.columns:
                data[col] = None

        # ロールバックのテスト
        repository.begin_transaction()
        repository.save_batch(data)
        repository.rollback()

        result = repository.find_latest_by_code("3333")
        assert result is None

        # コミットのテスト
        repository.begin_transaction()
        repository.save_batch(data)
        repository.commit()

        result = repository.find_latest_by_code("3333")
        assert result is not None
        assert result["NetSales"] == 800000000

"""
株価リポジトリのテストコード
"""

from datetime import date

import pandas as pd
import pytest

from src.repositories.sqlite import SqlitePriceRepository


class TestSqlitePriceRepository:
    """SqlitePriceRepositoryのテスト"""

    @pytest.fixture
    def repository(self, temp_db, init_db_tables):
        """テスト用リポジトリを作成"""
        repo = SqlitePriceRepository(str(temp_db))
        repo.connect()
        yield repo
        repo.disconnect()

    @pytest.fixture
    def setup_data(self, repository, sample_stock_data):
        """テスト用データをセットアップ"""
        repository.save_batch(sample_stock_data)
        return sample_stock_data

    def test_find_by_code_and_date_range(self, repository, setup_data):
        """銘柄コードと期間での検索テスト"""
        # 全期間を検索
        result = repository.find_by_code_and_date_range(
            code="1234", start_date=date(2024, 1, 1), end_date=date(2024, 1, 5)
        )

        assert len(result) == 5
        assert result["Code"].iloc[0] == "1234"
        assert result["Close"].iloc[0] == 1010

        # 部分期間を検索
        result = repository.find_by_code_and_date_range(
            code="1234", start_date=date(2024, 1, 2), end_date=date(2024, 1, 3)
        )

        assert len(result) == 2
        assert result["Date"].iloc[0] == pd.Timestamp("2024-01-02")
        assert result["Date"].iloc[1] == pd.Timestamp("2024-01-03")

    def test_find_by_code_and_date_range_no_data(self, repository):
        """データが存在しない場合の検索テスト"""
        result = repository.find_by_code_and_date_range(
            code="9999", start_date=date(2024, 1, 1), end_date=date(2024, 1, 5)
        )

        assert len(result) == 0
        assert isinstance(result, pd.DataFrame)

    def test_find_latest_by_code(self, repository, setup_data):
        """最新データの取得テスト"""
        result = repository.find_latest_by_code("1234")

        assert result is not None
        assert result["Code"] == "1234"
        assert result["Date"] == "2024-01-05"
        assert result["Close"] == 1030

    def test_find_latest_by_code_no_data(self, repository):
        """データが存在しない場合の最新データ取得テスト"""
        result = repository.find_latest_by_code("9999")
        assert result is None

    def test_find_all_by_date(self, repository, setup_data):
        """日付での全銘柄検索テスト"""
        # 追加データを準備
        additional_data = pd.DataFrame(
            {
                "Date": [pd.Timestamp("2024-01-03")],
                "Code": ["5678"],
                "Open": [2000],
                "High": [2050],
                "Low": [1980],
                "Close": [2030],
                "Volume": [200000],
                "TurnoverValue": [406000000],
                "AdjustmentFactor": [1.0],
                "AdjustmentOpen": [2000],
                "AdjustmentHigh": [2050],
                "AdjustmentLow": [1980],
                "AdjustmentClose": [2030],
                "AdjustmentVolume": [200000],
            }
        )
        repository.save_batch(additional_data)

        # 特定日の全データを取得
        result = repository.find_all_by_date(date(2024, 1, 3))

        assert len(result) == 2
        assert set(result["Code"]) == {"1234", "5678"}

    def test_save_batch(self, repository):
        """バッチ保存のテスト"""
        data = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-10", periods=3),
                "Code": ["7890"] * 3,
                "Open": [3000, 3010, 3020],
                "High": [3050, 3060, 3070],
                "Low": [2980, 2990, 3000],
                "Close": [3040, 3050, 3060],
                "Volume": [300000, 310000, 320000],
                "TurnoverValue": [912000000, 945500000, 979200000],
                "AdjustmentFactor": [1.0] * 3,
                "AdjustmentOpen": [3000, 3010, 3020],
                "AdjustmentHigh": [3050, 3060, 3070],
                "AdjustmentLow": [2980, 2990, 3000],
                "AdjustmentClose": [3040, 3050, 3060],
                "AdjustmentVolume": [300000, 310000, 320000],
            }
        )

        rows_affected = repository.save_batch(data)
        assert rows_affected == 3

        # 保存されたデータを確認
        saved = repository.find_by_code_and_date_range(
            code="7890", start_date=date(2024, 1, 10), end_date=date(2024, 1, 12)
        )
        assert len(saved) == 3
        assert saved["Close"].tolist() == [3040, 3050, 3060]

    def test_save_batch_empty(self, repository):
        """空のDataFrameの保存テスト"""
        empty_df = pd.DataFrame()
        rows_affected = repository.save_batch(empty_df)
        assert rows_affected == 0

    def test_delete_by_date_range(self, repository, setup_data):
        """日付範囲でのデータ削除テスト"""
        # 一部期間を削除
        rows_deleted = repository.delete_by_date_range(
            start_date=date(2024, 1, 2), end_date=date(2024, 1, 4)
        )
        assert rows_deleted == 3

        # 残りのデータを確認
        remaining = repository.find_by_code_and_date_range(
            code="1234", start_date=date(2024, 1, 1), end_date=date(2024, 1, 5)
        )
        assert len(remaining) == 2
        assert remaining["Date"].iloc[0] == pd.Timestamp("2024-01-01")
        assert remaining["Date"].iloc[1] == pd.Timestamp("2024-01-05")

    def test_get_latest_date(self, repository, setup_data):
        """最新日付の取得テスト"""
        latest = repository.get_latest_date()
        assert latest == date(2024, 1, 5)

    def test_get_latest_date_no_data(self, repository):
        """データが存在しない場合の最新日付取得テスト"""
        latest = repository.get_latest_date()
        assert latest is None

    def test_transaction_commit(self, repository, sample_stock_data):
        """トランザクションのコミットテスト"""
        repository.begin_transaction()
        repository.save_batch(sample_stock_data)
        repository.commit()

        # データが保存されていることを確認
        result = repository.find_latest_by_code("1234")
        assert result is not None

    def test_transaction_rollback(self, repository, sample_stock_data):
        """トランザクションのロールバックテスト"""
        repository.begin_transaction()
        repository.save_batch(sample_stock_data)
        repository.rollback()

        # データが保存されていないことを確認
        result = repository.find_latest_by_code("1234")
        assert result is None

    def test_context_manager(self, temp_db, init_db_tables, sample_stock_data):
        """コンテキストマネージャーのテスト"""
        with SqlitePriceRepository(str(temp_db)) as repo:
            repo.save_batch(sample_stock_data)
            result = repo.find_latest_by_code("1234")
            assert result is not None

        # 接続が切断されていることを確認
        assert repo.connection is None

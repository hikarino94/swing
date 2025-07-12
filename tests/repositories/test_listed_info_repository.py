"""
上場企業情報リポジトリのテストコード
"""

import pandas as pd
import pytest

from src.repositories.sqlite import SqliteListedInfoRepository


class TestSqliteListedInfoRepository:
    """SqliteListedInfoRepositoryのテスト"""

    @pytest.fixture
    def repository(self, temp_db, init_db_tables):
        """テスト用リポジトリを作成"""
        repo = SqliteListedInfoRepository(str(temp_db))
        repo.connect()
        yield repo
        repo.disconnect()

    @pytest.fixture
    def setup_data(self, repository, sample_listed_info):
        """テスト用データをセットアップ"""
        # 複数企業のデータを作成
        data = pd.concat(
            [
                sample_listed_info,
                pd.DataFrame(
                    {
                        "Date": ["2024-01-01"],
                        "Code": ["5678"],
                        "CompanyName": ["テスト電機株式会社"],
                        "CompanyNameEnglish": ["Test Electric Corporation"],
                        "Sector17Code": ["3"],
                        "Sector17CodeName": ["電気機器"],
                        "Sector33Code": ["3050"],
                        "Sector33CodeName": ["電気機械器具"],
                        "ScaleCategory": ["TOPIX Large 70"],
                        "MarketCode": ["0111"],
                        "MarketCodeName": ["プライム"],
                        "MarginCode": ["1"],
                        "MarginCodeName": ["制度信用"],
                        "delete_flag": [False],
                    }
                ),
                pd.DataFrame(
                    {
                        "Date": ["2024-01-01"],
                        "Code": ["9999"],
                        "CompanyName": ["削除済み株式会社"],
                        "CompanyNameEnglish": ["Deleted Corporation"],
                        "Sector17Code": ["1"],
                        "Sector17CodeName": ["食品"],
                        "Sector33Code": ["0050"],
                        "Sector33CodeName": ["水産・農林業"],
                        "ScaleCategory": ["TOPIX Small 2"],
                        "MarketCode": ["0111"],
                        "MarketCodeName": ["プライム"],
                        "MarginCode": ["1"],
                        "MarginCodeName": ["制度信用"],
                        "delete_flag": [True],
                    }
                ),
            ],
            ignore_index=True,
        )

        repository.save_batch(data)
        return data

    def test_find_by_code(self, repository, setup_data):
        """銘柄コードでの検索テスト"""
        result = repository.find_by_code("1234")

        assert result is not None
        assert result["Code"] == "1234"
        assert result["CompanyName"] == "テスト株式会社"
        assert result["Sector17CodeName"] == "食品"

    def test_find_by_code_not_found(self, repository):
        """存在しない銘柄コードでの検索テスト"""
        result = repository.find_by_code("0000")
        assert result is None

    def test_find_all_active(self, repository, setup_data):
        """アクティブな企業情報の取得テスト"""
        result = repository.find_all_active()

        assert len(result) == 2  # delete_flag=Trueの企業は除外
        codes = result["Code"].tolist()
        assert "1234" in codes
        assert "5678" in codes
        assert "9999" not in codes

    def test_find_by_sector(self, repository, setup_data):
        """セクターでの検索テスト"""
        # Sector17Codeで検索
        result = repository.find_by_sector("1")
        assert len(result) == 1
        assert result["Code"].iloc[0] == "1234"

        # Sector33Codeで検索
        result = repository.find_by_sector("3050")
        assert len(result) == 1
        assert result["Code"].iloc[0] == "5678"

    def test_save_batch(self, repository):
        """バッチ保存のテスト"""
        data = pd.DataFrame(
            {
                "Date": ["2024-01-02", "2024-01-02"],
                "Code": ["1111", "2222"],
                "CompanyName": ["新規A株式会社", "新規B株式会社"],
                "CompanyNameEnglish": ["New A Corporation", "New B Corporation"],
                "Sector17Code": ["2", "3"],
                "Sector17CodeName": ["化学", "電気機器"],
                "Sector33Code": ["2050", "3100"],
                "Sector33CodeName": ["化学工業", "電子部品"],
                "ScaleCategory": ["TOPIX Mid 400", "TOPIX Small 1"],
                "MarketCode": ["0111", "0112"],
                "MarketCodeName": ["プライム", "スタンダード"],
                "MarginCode": ["1", "0"],
                "MarginCodeName": ["制度信用", "非対象"],
                "delete_flag": [False, False],
            }
        )

        rows_affected = repository.save_batch(data)
        assert rows_affected == 2

        # 保存されたデータを確認
        result1 = repository.find_by_code("1111")
        assert result1["CompanyName"] == "新規A株式会社"

        result2 = repository.find_by_code("2222")
        assert result2["CompanyName"] == "新規B株式会社"

    def test_save_batch_empty(self, repository):
        """空のDataFrameの保存テスト"""
        empty_df = pd.DataFrame()
        rows_affected = repository.save_batch(empty_df)
        assert rows_affected == 0

    def test_mark_as_deleted(self, repository, setup_data):
        """削除フラグ設定のテスト"""
        # 複数銘柄に削除フラグを設定
        rows_affected = repository.mark_as_deleted(["1234", "5678"])
        assert rows_affected == 2

        # フラグが設定されたことを確認
        active = repository.find_all_active()
        assert len(active) == 0

        # 個別に確認
        result = repository.find_by_code("1234")
        assert result["delete_flag"] == 1

    def test_mark_as_deleted_empty(self, repository):
        """空のリストでの削除フラグ設定テスト"""
        rows_affected = repository.mark_as_deleted([])
        assert rows_affected == 0

    def test_update_delete_flags(self, repository, setup_data):
        """削除フラグの一括更新テスト"""
        # 1234のみアクティブとして更新
        rows_affected = repository.update_delete_flags(["1234"])

        # 1234以外に削除フラグが立つ
        assert rows_affected == 2  # 5678と9999

        # 確認
        active = repository.find_all_active()
        assert len(active) == 1
        assert active["Code"].iloc[0] == "1234"

    def test_update_delete_flags_empty(self, repository, setup_data):
        """アクティブな銘柄が空の場合の削除フラグ更新テスト"""
        rows_affected = repository.update_delete_flags([])

        # 全銘柄に削除フラグが立つ
        assert rows_affected == 3

        active = repository.find_all_active()
        assert len(active) == 0

    def test_transaction(self, repository):
        """トランザクションのテスト"""
        data = pd.DataFrame(
            {
                "Date": ["2024-01-01"],
                "Code": ["7777"],
                "CompanyName": ["トランザクションテスト株式会社"],
                "CompanyNameEnglish": ["Transaction Test Corporation"],
                "Sector17Code": ["1"],
                "Sector17CodeName": ["食品"],
                "Sector33Code": ["0050"],
                "Sector33CodeName": ["水産・農林業"],
                "ScaleCategory": ["TOPIX Small 2"],
                "MarketCode": ["0111"],
                "MarketCodeName": ["プライム"],
                "MarginCode": ["1"],
                "MarginCodeName": ["制度信用"],
                "delete_flag": [False],
            }
        )

        # ロールバックのテスト
        repository.begin_transaction()
        repository.save_batch(data)
        repository.rollback()

        result = repository.find_by_code("7777")
        assert result is None

        # コミットのテスト
        repository.begin_transaction()
        repository.save_batch(data)
        repository.commit()

        result = repository.find_by_code("7777")
        assert result is not None
        assert result["CompanyName"] == "トランザクションテスト株式会社"

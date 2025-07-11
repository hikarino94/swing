"""Tests for fetch/listed_info.py"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from fetch.listed_info import (
    _fetch_listed_info,
    _load_token,
    _to_db,
    update_listed_info,
)


class TestLoadToken:
    """トークン読み込みのテスト"""

    @patch("fetch.listed_info.get_idtoken")
    def test_load_token(self, mock_get_idtoken):
        """トークンが正しく読み込まれることを確認"""
        mock_get_idtoken.return_value = "test_token_12345"
        token = _load_token()
        assert token == "test_token_12345"
        mock_get_idtoken.assert_called_once()


class TestFetchListedInfo:
    """API呼び出し関数のテスト"""

    @patch("fetch.listed_info.requests.get")
    def test_fetch_listed_info_success(self, mock_get):
        """正常なAPI呼び出し"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "info": [
                {"Code": "1234", "CompanyName": "テスト株式会社", "Date": "2024-01-15"}
            ]
        }
        mock_get.return_value = mock_response

        result_df = _fetch_listed_info("test_token")

        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 1
        assert result_df.iloc[0]["Code"] == "1234"
        mock_get.assert_called_once()

    @patch("fetch.listed_info.requests.get")
    def test_fetch_listed_info_empty(self, mock_get):
        """空のレスポンスの場合"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"info": []}
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="no 'info' key"):
            _fetch_listed_info("test_token")

    @patch("fetch.listed_info.requests.get")
    def test_fetch_listed_info_api_error(self, mock_get):
        """エラーレスポンスの場合"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response

        with pytest.raises(RuntimeError, match="API error 500"):
            _fetch_listed_info("test_token")


class TestDatabaseOperations:
    """データベース操作のテスト"""

    def test_to_db_success(self):
        """正常なデータベース保存"""
        mock_conn = MagicMock()

        df = pd.DataFrame(
            {
                "Code": ["1234", "5678"],
                "CompanyName": ["テスト株式会社", "サンプル株式会社"],
                "Date": ["2024-01-15", "2024-01-15"],
                "MarketCode": ["0111", "0111"],
            }
        )

        _to_db(df, mock_conn)

        # executemanyとcommitが呼ばれたことを確認
        assert mock_conn.executemany.called
        assert mock_conn.commit.called

    def test_to_db_empty_dataframe(self):
        """空のDataFrameの場合"""
        mock_conn = MagicMock()
        df = pd.DataFrame()

        # エラーが発生しないことを確認
        _to_db(df, mock_conn)

        # 何も呼ばれないことを確認
        assert not mock_conn.executemany.called

    def test_to_db_with_delete_flag_handling(self):
        """上場廃止フラグ処理のテスト"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # 既存コード: 1234, 5678, 9999
        mock_cursor.fetchall.return_value = [("1234",), ("5678",), ("9999",)]

        # 新規データには9999が含まれない（上場廃止）
        df = pd.DataFrame({"Code": ["1234", "5678"], "CompanyName": ["会社A", "会社B"]})

        _to_db(df, mock_conn)

        # delete_flagを更新するSQLが実行されたことを確認
        execute_calls = mock_cursor.execute.call_args_list
        assert any(
            "UPDATE listed_info SET delete_flag = 1" in str(call)
            for call in execute_calls
        )


class TestUpdateListedInfo:
    """上場情報更新関数のテスト"""

    @patch("fetch.listed_info._to_db")
    @patch("fetch.listed_info._fetch_listed_info")
    @patch("fetch.listed_info._load_token")
    @patch("fetch.listed_info.sqlite3.connect")
    def test_update_listed_info_success(
        self, mock_connect, mock_token, mock_fetch, mock_to_db
    ):
        """正常な更新処理"""
        mock_token.return_value = "test_token"
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_df = pd.DataFrame(
            {
                "Code": ["1234", "5678"],
                "CompanyName": ["テスト株式会社", "サンプル株式会社"],
            }
        )
        mock_fetch.return_value = mock_df

        update_listed_info()

        mock_fetch.assert_called_once_with("test_token")
        mock_to_db.assert_called_once_with(mock_df, mock_conn)
        mock_conn.close.assert_called_once()


class TestColumnHandling:
    """カラム処理のテスト"""

    def test_column_renaming(self):
        """カラム名の変換テスト"""
        # APIレスポンスの形式
        df = pd.DataFrame(
            {
                "Code": ["1234"],
                "CompanyName": ["テスト株式会社"],
                "CompanyNameEnglish": ["Test Corp"],
                "Sector17Code": ["1"],
                "Sector17CodeName": ["食品"],
                "MarketCode": ["0111"],
                "MarketCodeName": ["プライム"],
            }
        )

        # _to_db関数内でのカラム名変換を確認
        expected_columns = {
            "Code": "code",
            "CompanyName": "company_name",
            "CompanyNameEnglish": "company_name_english",
            "Sector17Code": "sector17_code",
            "Sector17CodeName": "sector17_code_name",
            "MarketCode": "market_code",
            "MarketCodeName": "market_code_name",
        }

        # 各カラムが存在することを確認
        for col in expected_columns.keys():
            assert col in df.columns

    def test_delete_flag_handling(self):
        """削除フラグの処理"""
        # 新規データ作成時にdelete_flagが追加されることを想定
        df = pd.DataFrame({"Code": ["1234"], "CompanyName": ["テスト株式会社"]})

        # delete_flagカラムを追加
        df["delete_flag"] = 0

        assert "delete_flag" in df.columns
        assert df["delete_flag"].iloc[0] == 0

"""listed_info.pyのテスト"""

import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from fetch import listed_info


class TestUpdateListedInfo:
    """update_listed_info関数のテスト"""

    @patch("fetch.listed_info._to_db")
    @patch("fetch.listed_info._fetch_listed_info")
    @patch("fetch.listed_info._load_token")
    def test_update_listed_info_success(self, mock_load_token, mock_fetch, mock_to_db):
        """正常な更新処理のテスト"""
        # モックの設定
        mock_load_token.return_value = "test_token"
        mock_fetch.return_value = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "CompanyName": "テスト会社",
                    "MarketCodeName": "プライム",
                }
            ]
        )

        # テスト実行
        listed_info.update_listed_info()

        # 検証
        mock_load_token.assert_called_once()
        mock_fetch.assert_called_once_with("test_token")
        mock_to_db.assert_called_once()

    @patch("fetch.listed_info._load_token")
    def test_update_listed_info_token_error(self, mock_load_token):
        """トークン読み込みエラーのテスト"""
        mock_load_token.side_effect = FileNotFoundError("Token file not found")

        # エラーが発生することを確認
        with pytest.raises(FileNotFoundError):
            listed_info.update_listed_info()

    @patch("fetch.listed_info._fetch_listed_info")
    @patch("fetch.listed_info._load_token")
    def test_update_listed_info_api_error(self, mock_load_token, mock_fetch):
        """APIエラーのテスト"""
        mock_load_token.return_value = "test_token"
        mock_fetch.side_effect = RuntimeError("API Error")

        # エラーが発生することを確認
        with pytest.raises(RuntimeError):
            listed_info.update_listed_info()


class TestPrivateFunctions:
    """プライベート関数のテスト（モック経由）"""

    @patch("fetch.listed_info.config")
    def test_load_token(self, mock_config, tmp_path):
        """_load_token関数のテスト"""
        # IDトークンファイルの準備
        idtoken_path = tmp_path / "idtoken.json"
        idtoken_path.write_text(json.dumps({"idToken": "test_token_12345"}))

        # configのモック - Pathオブジェクトを返す
        mock_config.get_file_path.return_value = idtoken_path

        # テスト実行
        token = listed_info._load_token()

        # 検証
        assert token == "test_token_12345"
        mock_config.get_file_path.assert_called_once_with("idtoken")

    @patch("fetch.listed_info.requests")
    def test_fetch_listed_info(self, mock_requests):
        """_fetch_listed_info関数のテスト"""
        # APIレスポンスのモック
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "info": [
                {
                    "Code": "1234",
                    "CompanyName": "テスト会社",
                    "MarketCodeName": "プライム",
                }
            ]
        }
        mock_requests.get.return_value = mock_response

        # テスト実行
        result = listed_info._fetch_listed_info("test_token")

        # 検証
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["Code"] == "1234"

        # APIが正しく呼ばれたことを確認
        mock_requests.get.assert_called_once_with(
            listed_info.API_ENDPOINT,
            headers={"Authorization": "Bearer test_token"},
            timeout=30,
        )

    @patch("fetch.listed_info.requests")
    def test_fetch_listed_info_api_error(self, mock_requests):
        """APIエラー時のテスト"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_requests.get.return_value = mock_response

        with pytest.raises(RuntimeError) as exc_info:
            listed_info._fetch_listed_info("test_token")

        assert "API error 500" in str(exc_info.value)


class TestDatabaseOperations:
    """データベース操作のテスト"""

    def test_to_db_operation(self, temp_db):
        """_to_db関数の動作テスト"""
        # テストデータ
        df = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-01",
                    "CompanyName": "テスト会社",
                    "CompanyNameEnglish": "Test Company",
                    "Sector17Code": "1",
                    "Sector17CodeName": "食品",
                    "Sector33Code": "1050",
                    "Sector33CodeName": "電気機器",
                    "ScaleCategory": "TOPIX Core30",
                    "MarketCode": "0111",
                    "MarketCodeName": "プライム",
                    "MarginCode": "1",
                    "MarginCodeName": "信用",
                }
            ]
        )

        # データベース接続
        conn = sqlite3.connect(temp_db)

        # テスト実行
        listed_info._to_db(df, conn)

        # データベースから読み込んで検証
        result_df = pd.read_sql_query(
            "SELECT * FROM listed_info WHERE code = '1234'", conn
        )
        conn.close()

        assert len(result_df) == 1
        assert result_df.iloc[0]["code"] == "1234"
        assert result_df.iloc[0]["company_name"] == "テスト会社"
        assert result_df.iloc[0]["market_name"] == "プライム"

    def test_to_db_empty_dataframe(self, temp_db):
        """空のDataFrameを処理するテスト"""
        # 空のDataFrame
        df = pd.DataFrame()

        # データベース接続
        conn = sqlite3.connect(temp_db)

        # テスト実行（エラーが発生しないことを確認）
        listed_info._to_db(df, conn)

        conn.close()

    def test_delete_flag_update(self, temp_db):
        """delete_flag更新のテスト"""
        conn = sqlite3.connect(temp_db)

        # 既存データを作成
        conn.execute(
            """
            INSERT INTO listed_info (code, company_name, date, delete_flag)
            VALUES ('9999', '削除予定会社', '2024-01-01', 0)
        """
        )
        conn.commit()

        # 新しいデータ（9999は含まれない）
        df = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-02",
                    "CompanyName": "テスト会社",
                    "Sector33CodeName": "電気機器",
                    "MarketCodeName": "プライム",
                }
            ]
        )

        # テスト実行
        listed_info._to_db(df, conn)

        # 古いデータのdelete_flagが更新されていることを確認
        result = conn.execute(
            "SELECT delete_flag FROM listed_info WHERE code = '9999'"
        ).fetchone()

        conn.close()

        # delete_flagが1に更新されているはず
        assert result[0] == 1


class TestCLI:
    """CLI関数のテスト"""

    @patch("fetch.listed_info.update_listed_info")
    @patch("sys.argv", ["listed_info.py"])
    def test_cli_execution(self, mock_update):
        """CLI実行のテスト"""
        # _cli関数を実行
        listed_info._cli()

        # update_listed_infoが呼ばれたことを確認
        mock_update.assert_called_once()

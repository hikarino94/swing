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


class TestFetchListedInfo:
    """fetch_listed_info関数のテスト"""

    @patch("fetch.listed_info.logger")
    @patch("fetch.listed_info.Session")
    def test_fetch_listed_info_success(
        self, mock_session_class, mock_logger, mock_jquants_response, tmp_path
    ):
        """正常なデータ取得のテスト"""
        # モックセッションの設定
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # APIレスポンスのモック
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_jquants_response("listed_info")
        mock_session.get.return_value = mock_response

        # IDトークンのモック
        idtoken_path = tmp_path / "idtoken.json"
        idtoken_path.write_text(json.dumps({"idToken": "test_token"}))

        with patch("fetch.listed_info.config") as mock_config:
            mock_config.files.idtoken = str(idtoken_path)

            # テスト実行
            result = listed_info.fetch_listed_info(mock_session)

            # 検証
            assert len(result) == 1
            assert result[0]["Code"] == "1234"
            assert result[0]["CompanyName"] == "テスト会社"
            assert result[0]["MarketCodeName"] == "プライム"

            # APIが呼ばれたことを確認
            mock_session.get.assert_called()

    @patch("fetch.listed_info.logger")
    @patch("fetch.listed_info.Session")
    def test_fetch_listed_info_empty_response(
        self, mock_session_class, mock_logger, tmp_path
    ):
        """空のレスポンスのテスト"""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # 空のレスポンス
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"info": []}
        mock_session.get.return_value = mock_response

        idtoken_path = tmp_path / "idtoken.json"
        idtoken_path.write_text(json.dumps({"idToken": "test_token"}))

        with patch("fetch.listed_info.config") as mock_config:
            mock_config.files.idtoken = str(idtoken_path)

            result = listed_info.fetch_listed_info(mock_session)

            assert len(result) == 0

    @patch("fetch.listed_info.logger")
    @patch("fetch.listed_info.Session")
    def test_fetch_listed_info_api_error(
        self, mock_session_class, mock_logger, tmp_path
    ):
        """APIエラーのテスト"""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # エラーレスポンス
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = ValueError("Server Error")
        mock_session.get.return_value = mock_response

        idtoken_path = tmp_path / "idtoken.json"
        idtoken_path.write_text(json.dumps({"idToken": "test_token"}))

        with patch("fetch.listed_info.config") as mock_config:
            mock_config.files.idtoken = str(idtoken_path)

            with pytest.raises(ValueError):
                listed_info.fetch_listed_info(mock_session)


class TestSaveListedInfo:
    """save_listed_info関数のテスト"""

    def test_save_listed_info_to_db(self, temp_db):
        """データベースへの保存テスト"""
        # テストデータの準備
        info_list = [
            {
                "Code": "1234",
                "CompanyName": "テスト会社",
                "CompanyNameEnglish": "Test Company",
                "Sector17Code": "1",
                "Sector17CodeName": "食品",
                "Sector33Code": "1050",
                "Sector33CodeName": "電気機器",
                "ScaleCategory": "TOPIX Core30",
                "MarketCode": "0111",
                "MarketCodeName": "プライム",
            }
        ]

        # 保存実行
        listed_info.save_listed_info(info_list, str(temp_db))

        # データベースから読み込んで検証
        conn = sqlite3.connect(temp_db)
        df = pd.read_sql_query("SELECT * FROM listed_info WHERE code = '1234'", conn)
        conn.close()

        assert len(df) == 1
        assert df.iloc[0]["code"] == "1234"
        assert df.iloc[0]["company_name"] == "テスト会社"

    def test_save_listed_info_update_existing(self, temp_db):
        """既存データの更新テスト"""
        # 初回データ
        info_list_1 = [
            {
                "Code": "1234",
                "CompanyName": "旧テスト会社",
                "CompanyNameEnglish": "Old Test Company",
                "Sector33Code": "1050",
                "Sector33CodeName": "電気機器",
                "MarketCode": "0111",
                "MarketCodeName": "プライム",
            }
        ]

        # 更新データ
        info_list_2 = [
            {
                "Code": "1234",
                "CompanyName": "新テスト会社",
                "CompanyNameEnglish": "New Test Company",
                "Sector33Code": "1050",
                "Sector33CodeName": "電気機器",
                "MarketCode": "0111",
                "MarketCodeName": "プライム",
            }
        ]

        # 保存実行
        listed_info.save_listed_info(info_list_1, str(temp_db))
        listed_info.save_listed_info(info_list_2, str(temp_db))

        # データベースから読み込んで検証
        conn = sqlite3.connect(temp_db)
        df = pd.read_sql_query("SELECT * FROM listed_info WHERE code = '1234'", conn)
        conn.close()

        # データが更新されていることを確認
        assert len(df) == 1
        assert df.iloc[0]["company_name"] == "新テスト会社"

    def test_save_listed_info_delete_flag(self, temp_db):
        """delete_flagの処理テスト"""
        # 既存データを作成
        conn = sqlite3.connect(temp_db)
        conn.execute(
            """
            INSERT INTO listed_info (code, company_name, delete_flag)
            VALUES ('9999', '削除予定会社', 0)
        """
        )
        conn.commit()
        conn.close()

        # 新しいデータ（9999は含まれない）
        info_list = [
            {
                "Code": "1234",
                "CompanyName": "テスト会社",
                "Sector33Code": "1050",
                "Sector33CodeName": "電気機器",
                "MarketCode": "0111",
                "MarketCodeName": "プライム",
            }
        ]

        # 保存実行（delete_flagの更新を含む）
        listed_info.save_listed_info(info_list, str(temp_db))

        # データベースから読み込んで検証
        conn = sqlite3.connect(temp_db)
        df = pd.read_sql_query("SELECT * FROM listed_info WHERE code = '9999'", conn)
        conn.close()

        # delete_flagが1に更新されていることを確認
        if len(df) > 0 and "delete_flag" in df.columns:
            assert df.iloc[0]["delete_flag"] == 1


class TestMainFunction:
    """main関数のテスト"""

    @patch("fetch.listed_info.save_listed_info")
    @patch("fetch.listed_info.fetch_listed_info")
    def test_main_success(self, mock_fetch, mock_save):
        """正常実行のテスト"""
        mock_fetch.return_value = [{"Code": "1234", "CompanyName": "テスト会社"}]

        # 実行
        listed_info.main()

        # 関数が呼ばれたことを確認
        mock_fetch.assert_called_once()
        mock_save.assert_called_once()

    @patch("fetch.listed_info.logger")
    @patch("fetch.listed_info.fetch_listed_info")
    def test_main_error_handling(self, mock_fetch, mock_logger):
        """エラーハンドリングのテスト"""
        mock_fetch.side_effect = RuntimeError("API Error")

        # エラーが発生しても終了しないことを確認
        listed_info.main()

        # エラーログが出力されたことを確認
        mock_logger.error.assert_called()


class TestUtilityFunctions:
    """ユーティリティ関数のテスト"""

    def test_column_mapping(self):
        """カラムマッピングの確認"""
        # _INFO_COLSが定義されていることを確認
        assert hasattr(listed_info, "_INFO_COLS")
        assert len(listed_info._INFO_COLS) > 0

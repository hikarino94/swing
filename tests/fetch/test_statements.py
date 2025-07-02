"""statements.pyのテスト"""

import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))

from fetch import statements


class TestMainFunction:
    """main関数のテスト"""

    @patch("fetch.statements._upsert")
    @patch("fetch.statements._fetch_multiple_codes")
    @patch("fetch.statements._load_token")
    @patch("fetch.statements.sqlite3.connect")
    def test_main_mode1(self, mock_connect, mock_load_token, mock_fetch, mock_upsert):
        """モード1（銘柄ごと一括取得）のテスト"""
        # モックの設定
        mock_load_token.return_value = "test_token"
        mock_fetch.return_value = [
            {"Code": "1234", "DisclosureDate": "2024-01-10", "NetSales": 1000000}
        ]

        # データベースモック
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("1234",), ("5678",)]
        mock_conn.execute.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        statements.main("1", None, None)

        # 検証
        mock_load_token.assert_called_once()
        mock_fetch.assert_called_once_with("test_token", ["1234", "5678"])
        mock_upsert.assert_called_once()

    @patch("fetch.statements._upsert")
    @patch("fetch.statements._fetch_statements_by_period")
    @patch("fetch.statements._load_token")
    @patch("fetch.statements.sqlite3.connect")
    @patch("requests.Session")
    def test_main_mode2_with_dates(
        self, mock_session_class, mock_connect, mock_load_token, mock_fetch, mock_upsert
    ):
        """モード2（日付指定）のテスト"""
        mock_load_token.return_value = "test_token"
        # _fetch_statements_by_periodはリストを返す
        mock_fetch.return_value = [
            {"Code": "1234", "DisclosureDate": "2024-01-10", "NetSales": 1000000},
            {"Code": "1234", "DisclosureDate": "2024-01-11", "NetSales": 1100000},
            {"Code": "5678", "DisclosureDate": "2024-01-10", "NetSales": 2000000},
        ]

        # セッションモック
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        # データベースモック
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # 実行
        statements.main("2", "2024-01-01", "2024-01-03")

        # _fetch_statements_by_periodが1回呼ばれたことを確認
        assert mock_fetch.call_count == 1
        mock_fetch.assert_called_once_with(
            mock_session, "test_token", "2024-01-01", "2024-01-03"
        )

        # _upsertが1回呼ばれたことを確認
        assert mock_upsert.call_count == 1

    @patch("fetch.statements._upsert")
    @patch("fetch.statements._fetch_statements_by_date")
    @patch("fetch.statements._load_token")
    @patch("fetch.statements.sqlite3.connect")
    @patch("requests.Session")
    def test_main_mode2_no_dates(
        self, mock_session_class, mock_connect, mock_load_token, mock_fetch, mock_upsert
    ):
        """モード2（日付指定なし、当日のみ）のテスト"""
        mock_load_token.return_value = "test_token"
        mock_fetch.return_value = []

        # セッションモック
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        # データベースモック
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # 実行
        statements.main("2", None, None)

        # 当日分のみ呼ばれたことを確認
        assert mock_fetch.call_count == 1

    @patch("fetch.statements.logger")
    @patch("fetch.statements._load_token")
    @patch("fetch.statements.sqlite3.connect")
    def test_main_invalid_mode(self, mock_connect, mock_load_token, mock_logger):
        """無効なモードのテスト"""
        mock_load_token.return_value = "test_token"
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # 実行
        statements.main("3", None, None)

        # エラーログが出力されたことを確認
        mock_logger.error.assert_called_with(
            "無効なモードです: %s。'1' または '2' を指定してください", "3"
        )


class TestPrivateFunctions:
    """プライベート関数のテスト"""

    @patch("fetch.statements.config")
    def test_load_token(self, mock_config, tmp_path):
        """_load_token関数のテスト"""
        # IDトークンファイルの準備
        idtoken_path = tmp_path / "idtoken.json"
        idtoken_path.write_text(json.dumps({"idToken": "test_token_12345"}))

        # configのモック - Pathオブジェクトを返す
        mock_config.get_file_path.return_value = idtoken_path

        # テスト実行
        token = statements._load_token()

        # 検証
        assert token == "test_token_12345"
        mock_config.get_file_path.assert_called_once_with("idtoken")

    def test_fetch_statements_by_date(self):
        """_fetch_statements_by_date関数のテスト"""
        # モックセッションの作成
        mock_session = MagicMock()

        # APIレスポンスのモック
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "statements": [
                {
                    "Code": "1234",
                    "DisclosureDate": "2024-01-10",
                    "NetSales": 1000000,
                    "OperatingProfit": 100000,
                }
            ]
        }
        mock_session.get.return_value = mock_response

        # テスト実行
        result = statements._fetch_statements_by_date(
            mock_session, "test_token", "2024-01-10"
        )

        # 検証
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["Code"] == "1234"

        # APIが正しく呼ばれたことを確認
        mock_session.get.assert_called_with(
            statements.API_ENDPOINT,
            headers={"Authorization": "Bearer test_token"},
            params={"date": "2024-01-10"},
            timeout=60,
        )

    def test_fetch_statements_by_date_with_pagination(self):
        """ページネーション処理のテスト"""
        # モックセッションの作成
        mock_session = MagicMock()

        # 2ページ分のレスポンスを設定
        responses = []

        # 1ページ目
        mock_resp1 = MagicMock()
        mock_resp1.status_code = 200
        mock_resp1.json.return_value = {
            "statements": [{"Code": "1234", "NetSales": 1000000}],
            "pagination_key": "next_page_key",
        }
        responses.append(mock_resp1)

        # 2ページ目
        mock_resp2 = MagicMock()
        mock_resp2.status_code = 200
        mock_resp2.json.return_value = {
            "statements": [{"Code": "5678", "NetSales": 2000000}]
            # pagination_keyがない = 最終ページ
        }
        responses.append(mock_resp2)

        mock_session.get.side_effect = responses

        # テスト実行
        result = statements._fetch_statements_by_date(
            mock_session, "test_token", "2024-01-10"
        )

        # 検証
        assert len(result) == 2
        assert result[0]["Code"] == "1234"
        assert result[1]["Code"] == "5678"
        assert mock_session.get.call_count == 2

    def test_fetch_statements_by_code(self):
        """_fetch_statements_by_code関数のテスト"""
        # モックセッションの作成
        mock_session = MagicMock()

        # APIレスポンスのモック
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "statements": [
                {"Code": "1234", "DisclosureDate": "2024-01-10", "NetSales": 1000000},
                {"Code": "1234", "DisclosureDate": "2023-10-10", "NetSales": 900000},
            ]
        }
        mock_session.get.return_value = mock_response

        # テスト実行
        result = statements._fetch_statements_by_code(
            mock_session, "test_token", "1234"
        )

        # 検証
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(item["Code"] == "1234" for item in result)

    def test_fetch_api_error(self):
        """APIエラー時のテスト"""
        # モックセッションの作成
        mock_session = MagicMock()

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "500 Server Error"
        )
        mock_session.get.return_value = mock_response

        # HTTPErrorが発生することを確認
        with pytest.raises(requests.HTTPError):
            statements._fetch_statements_by_date(
                mock_session, "test_token", "2024-01-10"
            )


class TestDatabaseOperations:
    """データベース操作のテスト"""

    @patch(
        "fetch.statements.SCHEMA_COLUMNS",
        [
            "LocalCode",
            "DisclosureNumber",
            "DisclosedDate",
            "NetSales",
            "OperatingProfit",
            "OrdinaryProfit",
            "Profit",
        ],
    )
    def test_upsert_operation(self, temp_db):
        """_upsert関数の動作テスト"""
        # テスト用の簡略化されたスキーマでテーブルを作成
        conn = sqlite3.connect(temp_db)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS statements (
                LocalCode TEXT,
                DisclosureNumber TEXT PRIMARY KEY,
                DisclosedDate TEXT,
                NetSales REAL,
                OperatingProfit REAL,
                OrdinaryProfit REAL,
                Profit REAL
            )
            """
        )
        conn.commit()

        # テストデータ（リスト形式）
        records = [
            {
                "LocalCode": "1234",
                "DisclosureNumber": "12345678901234567890",
                "DisclosedDate": "2024-01-10",
                "NetSales": 1000000,
                "OperatingProfit": 100000,
                "OrdinaryProfit": 110000,
                "Profit": 80000,
            }
        ]

        # テスト実行
        statements._upsert(conn, records)

        # データベースから読み込んで検証
        result_df = pd.read_sql_query(
            "SELECT * FROM statements WHERE LocalCode = '1234'", conn
        )
        conn.close()

        assert len(result_df) == 1
        assert result_df.iloc[0]["LocalCode"] == "1234"
        assert result_df.iloc[0]["DisclosedDate"] == "2024-01-10"
        assert result_df.iloc[0]["NetSales"] == 1000000

    @patch(
        "fetch.statements.SCHEMA_COLUMNS",
        [
            "LocalCode",
            "DisclosureNumber",
            "DisclosedDate",
            "NetSales",
            "OperatingProfit",
            "OrdinaryProfit",
            "Profit",
        ],
    )
    def test_upsert_empty_list(self, temp_db):
        """空のリストを処理するテスト"""
        # テスト用の簡略化されたスキーマでテーブルを作成
        conn = sqlite3.connect(temp_db)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS statements (
                LocalCode TEXT,
                DisclosureNumber TEXT PRIMARY KEY,
                DisclosedDate TEXT,
                NetSales REAL,
                OperatingProfit REAL,
                OrdinaryProfit REAL,
                Profit REAL
            )
            """
        )
        conn.commit()

        # 空のリスト
        records = []

        # テスト実行（エラーが発生しないことを確認）
        statements._upsert(conn, records)

        conn.close()

    @patch(
        "fetch.statements.SCHEMA_COLUMNS",
        [
            "LocalCode",
            "DisclosureNumber",
            "DisclosedDate",
            "NetSales",
            "OperatingProfit",
            "OrdinaryProfit",
            "Profit",
        ],
    )
    def test_upsert_duplicate_handling(self, temp_db):
        """重複データのハンドリングテスト"""
        # テスト用の簡略化されたスキーマでテーブルを作成
        conn = sqlite3.connect(temp_db)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS statements (
                LocalCode TEXT,
                DisclosureNumber TEXT PRIMARY KEY,
                DisclosedDate TEXT,
                NetSales REAL,
                OperatingProfit REAL,
                OrdinaryProfit REAL,
                Profit REAL
            )
            """
        )
        conn.commit()

        # 同じデータを作成
        records1 = [
            {
                "LocalCode": "1234",
                "DisclosureNumber": "12345678901234567890",
                "DisclosedDate": "2024-01-10",
                "NetSales": 1000000,
            }
        ]

        # 更新されたデータ
        records2 = [
            {
                "LocalCode": "1234",
                "DisclosureNumber": "12345678901234567890",
                "DisclosedDate": "2024-01-10",
                "NetSales": 1100000,  # 売上が更新された
            }
        ]

        conn = sqlite3.connect(temp_db)

        # 1回目の挿入
        statements._upsert(conn, records1)

        # 2回目の挿入（更新）
        statements._upsert(conn, records2)

        # データベースから読み込んで検証
        result_df = pd.read_sql_query(
            "SELECT * FROM statements WHERE LocalCode = '1234'", conn
        )
        conn.close()

        # データが更新されていることを確認
        assert len(result_df) == 1
        assert result_df.iloc[0]["NetSales"] == 1100000


class TestUtilityConstants:
    """定数のテスト"""

    def test_api_endpoint(self):
        """API_ENDPOINT定数が定義されていることを確認"""
        assert hasattr(statements, "API_ENDPOINT")
        assert isinstance(statements.API_ENDPOINT, str)

    def test_schema_columns(self):
        """SCHEMA_COLUMNS定数が定義されていることを確認"""
        assert hasattr(statements, "SCHEMA_COLUMNS")
        assert isinstance(statements.SCHEMA_COLUMNS, list)
        assert len(statements.SCHEMA_COLUMNS) > 0

        # 主要なカラムが含まれていることを確認
        expected_columns = ["LocalCode", "DisclosedDate", "NetSales"]
        for col in expected_columns:
            assert col in statements.SCHEMA_COLUMNS


class TestHelperFunctions:
    """ヘルパー関数のテスト"""

    def test_daterange(self):
        """_daterange関数のテスト"""
        from datetime import date

        start = date(2024, 1, 1)
        end = date(2024, 1, 3)

        result = statements._daterange(start, end)

        assert len(result) == 3
        assert result[0] == date(2024, 1, 1)
        assert result[1] == date(2024, 1, 2)
        assert result[2] == date(2024, 1, 3)

    @patch("fetch.statements._normalize")
    def test_normalize_function(self, mock_normalize):
        """_normalize関数が正しく呼ばれることを確認"""
        # DataFrameを作成
        df = pd.DataFrame([{"Code": "1234", "NetSales": 1000000}])

        # _normalizeがDataFrameを返すようにモック
        mock_normalize.return_value = df

        # _upsertの中で_normalizeが呼ばれることを確認
        conn = MagicMock()
        statements._upsert(conn, [{"Code": "1234", "NetSales": 1000000}])

        mock_normalize.assert_called_once()

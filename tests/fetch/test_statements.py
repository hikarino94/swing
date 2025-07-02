"""statements.pyのテスト"""

import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

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
    @patch("fetch.statements._fetch_statements_by_date")
    @patch("fetch.statements._load_token")
    @patch("fetch.statements.sqlite3.connect")
    def test_main_mode2_with_dates(
        self, mock_connect, mock_load_token, mock_fetch, mock_upsert
    ):
        """モード2（日付指定）のテスト"""
        mock_load_token.return_value = "test_token"
        mock_fetch.return_value = pd.DataFrame(
            [{"Code": "1234", "DisclosureDate": "2024-01-10", "NetSales": 1000000}]
        )

        # データベースモック
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # 実行
        statements.main("2", "2024-01-01", "2024-01-03")

        # 3日分呼ばれたことを確認
        assert mock_fetch.call_count == 3
        assert mock_upsert.call_count == 3

    @patch("fetch.statements._upsert")
    @patch("fetch.statements._fetch_statements_by_date")
    @patch("fetch.statements._load_token")
    @patch("fetch.statements.sqlite3.connect")
    def test_main_mode2_no_dates(
        self, mock_connect, mock_load_token, mock_fetch, mock_upsert
    ):
        """モード2（日付指定なし、当日のみ）のテスト"""
        mock_load_token.return_value = "test_token"
        mock_fetch.return_value = pd.DataFrame([])

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
    """プライベート関数のテスト（モック経由）"""

    @patch("fetch.statements.config")
    def test_load_token(self, mock_config, tmp_path):
        """_load_token関数のテスト"""
        # IDトークンファイルの準備
        idtoken_path = tmp_path / "idtoken.json"
        idtoken_path.write_text(json.dumps({"idToken": "test_token_12345"}))

        mock_config.files.idtoken = str(idtoken_path)

        # テスト実行
        token = statements._load_token()

        # 検証
        assert token == "test_token_12345"

    @patch("fetch.statements.requests")
    def test_fetch_statements_by_date(self, mock_requests):
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
        expected_url = f"{statements.API_BASE}/fins/statements"
        mock_session.get.assert_called_with(
            expected_url,
            headers={"Authorization": "Bearer test_token"},
            params={"date": "2024-01-10"},
            timeout=30,
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

    @patch("fetch.statements.requests")
    def test_fetch_statements_by_code(self, mock_requests):
        """_fetch_statements_by_code関数のテスト"""
        # APIレスポンスのモック
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "statements": [
                {"Code": "1234", "DisclosureDate": "2024-01-10", "NetSales": 1000000},
                {"Code": "1234", "DisclosureDate": "2023-10-10", "NetSales": 900000},
            ]
        }
        mock_requests.get.return_value = mock_response

        # テスト実行
        result = statements._fetch_statements_by_code("test_token", "1234")

        # 検証
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert all(result["Code"] == "1234")

    @patch("fetch.statements.requests")
    def test_fetch_api_error(self, mock_requests):
        """APIエラー時のテスト"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_requests.get.return_value = mock_response

        with pytest.raises(RuntimeError) as exc_info:
            statements._fetch_statements_by_date("test_token", "2024-01-10")

        assert "API error 500" in str(exc_info.value)


class TestDatabaseOperations:
    """データベース操作のテスト"""

    def test_upsert_operation(self, temp_db):
        """_upsert関数の動作テスト"""
        # テストデータ
        df = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "TypeOfDocument": "1Q",
                    "DisclosureDate": "2024-01-10",
                    "NetSales": 1000000,
                    "OperatingProfit": 100000,
                    "OrdinaryProfit": 110000,
                    "ProfitAttributableToOwnersOfParent": 80000,
                    "TotalAssets": 5000000,
                    "NetAssets": 2000000,
                    "EquityToAssetRatio": 0.4,
                    "BookValuePerShare": 200.0,
                }
            ]
        )

        # データベース接続
        conn = sqlite3.connect(temp_db)

        # テスト実行
        statements._upsert(df, conn)

        # データベースから読み込んで検証
        result_df = pd.read_sql_query(
            "SELECT * FROM statements WHERE LocalCode = '1234'", conn
        )
        conn.close()

        assert len(result_df) == 1
        assert result_df.iloc[0]["LocalCode"] == "1234"
        assert result_df.iloc[0]["DisclosedDate"] == "2024-01-10"
        assert result_df.iloc[0]["NetSales"] == 1000000

    def test_upsert_empty_dataframe(self, temp_db):
        """空のDataFrameを処理するテスト"""
        # 空のDataFrame
        df = pd.DataFrame()

        # データベース接続
        conn = sqlite3.connect(temp_db)

        # テスト実行（エラーが発生しないことを確認）
        statements._upsert(df, conn)

        conn.close()

    def test_upsert_duplicate_handling(self, temp_db):
        """重複データのハンドリングテスト"""
        # 同じデータを作成
        df1 = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "TypeOfDocument": "1Q",
                    "DisclosureDate": "2024-01-10",
                    "NetSales": 1000000,
                }
            ]
        )

        # 更新されたデータ
        df2 = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "TypeOfDocument": "1Q",
                    "DisclosureDate": "2024-01-10",
                    "NetSales": 1100000,  # 売上が更新された
                }
            ]
        )

        conn = sqlite3.connect(temp_db)

        # 1回目の挿入
        statements._upsert(df1, conn)

        # 2回目の挿入（更新）
        statements._upsert(df2, conn)

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

    def test_api_constants(self):
        """API関連の定数が定義されていることを確認"""
        assert hasattr(statements, "API_BASE")
        assert statements.API_BASE == "https://api.jquants.com/v1"

    def test_column_mapping(self):
        """カラムマッピングの確認"""
        # _STMT_COLSが定義されていることを確認
        assert hasattr(statements, "_STMT_COLS")
        assert isinstance(statements._STMT_COLS, dict)
        assert len(statements._STMT_COLS) > 0

        # 主要なカラムが含まれていることを確認
        expected_keys = ["Code", "DisclosureDate", "NetSales"]
        for key in expected_keys:
            assert key in statements._STMT_COLS

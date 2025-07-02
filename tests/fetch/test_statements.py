"""statements.pyのテスト"""

import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from fetch import statements


class TestFetchStatements:
    """fetch_statements関数のテスト"""

    @patch("fetch.statements.logger")
    @patch("fetch.statements.Session")
    def test_fetch_statements_success(
        self, mock_session_class, mock_logger, mock_jquants_response, tmp_path
    ):
        """正常なデータ取得のテスト"""
        # モックセッションの設定
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # APIレスポンスのモック
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_jquants_response("statements")
        mock_session.get.return_value = mock_response

        # IDトークンのモック
        idtoken_path = tmp_path / "idtoken.json"
        idtoken_path.write_text(json.dumps({"idToken": "test_token"}))

        with patch("fetch.statements.config") as mock_config:
            mock_config.files.idtoken = str(idtoken_path)

            # テスト実行
            result = statements.fetch_statements("2024-01-01", mock_session)

            # 検証
            assert len(result) == 1
            assert result[0]["Code"] == "1234"
            assert result[0]["DisclosureDate"] == "2024-01-10"
            assert result[0]["NetSales"] == 1000000

            # APIが呼ばれたことを確認
            mock_session.get.assert_called()

    @patch("fetch.statements.logger")
    @patch("fetch.statements.Session")
    def test_fetch_statements_pagination(
        self, mock_session_class, mock_logger, tmp_path
    ):
        """ページネーション処理のテスト"""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # 2ページ分のレスポンスを設定
        responses = [
            {
                "statements": [
                    {
                        "Code": "1234",
                        "DisclosureDate": "2024-01-10",
                        "NetSales": 1000000,
                    }
                ],
                "pagination_key": "next_page_key",
            },
            {
                "statements": [
                    {
                        "Code": "5678",
                        "DisclosureDate": "2024-01-10",
                        "NetSales": 2000000,
                    }
                ],
                "pagination_key": None,
            },
        ]

        mock_responses = []
        for resp_data in responses:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = resp_data
            mock_responses.append(mock_resp)

        mock_session.get.side_effect = mock_responses

        # IDトークンのモック
        idtoken_path = tmp_path / "idtoken.json"
        idtoken_path.write_text(json.dumps({"idToken": "test_token"}))

        with patch("fetch.statements.config") as mock_config:
            mock_config.files.idtoken = str(idtoken_path)

            # テスト実行
            result = statements.fetch_statements("2024-01-01", mock_session)

            # 検証
            assert len(result) == 2
            assert result[0]["Code"] == "1234"
            assert result[1]["Code"] == "5678"
            assert mock_session.get.call_count == 2


class TestSaveStatements:
    """save_statements関数のテスト"""

    def test_save_statements_to_db(self, tmp_db):
        """データベースへの保存テスト"""
        # テストデータの準備
        stmt_list = [
            {
                "Code": "1234",
                "DisclosureDate": "2024-01-10",
                "TypeOfDocument": "1Q",
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

        # 保存実行
        statements.save_statements(stmt_list, str(tmp_db))

        # データベースから読み込んで検証
        conn = sqlite3.connect(tmp_db)
        df = pd.read_sql_query(
            "SELECT * FROM statements WHERE LocalCode = '1234'", conn
        )
        conn.close()

        assert len(df) == 1
        assert df.iloc[0]["LocalCode"] == "1234"
        assert df.iloc[0]["DisclosedDate"] == "2024-01-10"
        assert df.iloc[0]["NetSales"] == 1000000

    def test_save_statements_duplicate_handling(self, tmp_db):
        """重複データのハンドリングテスト"""
        stmt_list = [
            {
                "Code": "1234",
                "DisclosureDate": "2024-01-10",
                "TypeOfDocument": "1Q",
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

        # 同じデータを2回保存
        statements.save_statements(stmt_list, str(tmp_db))

        # 更新されたデータで再度保存
        stmt_list[0]["NetSales"] = 1100000
        statements.save_statements(stmt_list, str(tmp_db))

        # データベースから読み込んで検証
        conn = sqlite3.connect(tmp_db)
        df = pd.read_sql_query(
            "SELECT * FROM statements WHERE LocalCode = '1234'", conn
        )
        conn.close()

        # データが更新されていることを確認
        assert len(df) == 1
        assert df.iloc[0]["NetSales"] == 1100000


class TestMainFunction:
    """main関数のテスト"""

    @patch("fetch.statements.save_statements")
    @patch("fetch.statements.fetch_statements")
    def test_main_default_days(self, mock_fetch, mock_save):
        """デフォルト日数での実行テスト"""
        mock_fetch.return_value = [{"Code": "1234", "DisclosureDate": "2024-01-10"}]

        # 引数なしで実行
        test_args = ["statements.py"]
        with patch("sys.argv", test_args):
            statements.main()

        # デフォルトの日数分呼ばれたことを確認
        assert mock_fetch.call_count > 0
        assert mock_save.call_count > 0

    @patch("fetch.statements.save_statements")
    @patch("fetch.statements.fetch_statements")
    def test_main_custom_days(self, mock_fetch, mock_save):
        """カスタム日数での実行テスト"""
        mock_fetch.return_value = [{"Code": "1234", "DisclosureDate": "2024-01-10"}]

        # 5日分を指定
        test_args = ["statements.py", "5"]
        with patch("sys.argv", test_args):
            statements.main()

        # 5日分呼ばれたことを確認
        assert mock_fetch.call_count == 5
        assert mock_save.call_count == 5

    @patch("fetch.statements.logger")
    @patch("fetch.statements.fetch_statements")
    def test_main_error_handling(self, mock_fetch, mock_logger):
        """エラーハンドリングのテスト"""
        mock_fetch.side_effect = Exception("API Error")

        test_args = ["statements.py"]
        with patch("sys.argv", test_args):
            # エラーが発生しても継続することを確認
            statements.main()

        # エラーログが出力されたことを確認
        mock_logger.error.assert_called()


class TestUtilityFunctions:
    """ユーティリティ関数のテスト"""

    def test_column_mapping(self):
        """カラムマッピングの確認"""
        # _STMT_COLSが定義されていることを確認
        assert hasattr(statements, "_STMT_COLS")
        assert len(statements._STMT_COLS) > 0

    def test_date_generation(self):
        """日付生成のテスト"""
        # 今日から過去7日分の日付が生成できることを確認
        today = datetime.now().date()
        dates = []
        for i in range(7):
            date = today - timedelta(days=i)
            dates.append(date.strftime("%Y-%m-%d"))

        assert len(dates) == 7
        assert dates[0] == today.strftime("%Y-%m-%d")

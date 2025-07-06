"""ポートフォリオ管理のWeb APIテスト"""

import json
import unittest
from unittest.mock import MagicMock, patch

from src.auth.models import User
from src.ui.web import app


class TestPortfolioAPI(unittest.TestCase):
    """ポートフォリオ管理APIのテスト"""

    def setUp(self):
        """テスト前の初期設定"""
        self.app = app
        self.client = self.app.test_client()
        self.app_context = self.app.app_context()
        self.app_context.push()

        # テストユーザーの作成
        self.test_user = User()
        self.test_user.id = 1
        self.test_user.username = "test_user"

        # セッション認証のモック
        self.session_patcher = patch("src.auth.models.Session.find_by_id")
        self.mock_session = self.session_patcher.start()

        # ログイン状態をシミュレート
        mock_session_obj = MagicMock()
        mock_session_obj.user_id = self.test_user.id
        mock_session_obj.is_valid.return_value = True
        self.mock_session.return_value = mock_session_obj

        # ユーザー取得のモック
        self.user_patcher = patch("src.auth.models.User.find_by_id")
        self.mock_user = self.user_patcher.start()
        self.mock_user.return_value = self.test_user

        # セッションクリーンアップのモック
        self.cleanup_patcher = patch("src.auth.models.Session.cleanup_expired")
        self.mock_cleanup = self.cleanup_patcher.start()
        self.mock_cleanup.return_value = 0

        # セッションクッキーを設定
        with self.client.session_transaction() as sess:
            sess["session_id"] = "test_session_id"

    def tearDown(self):
        """テスト後のクリーンアップ"""
        self.session_patcher.stop()
        self.user_patcher.stop()
        self.cleanup_patcher.stop()
        self.app_context.pop()

    @patch("src.ui.web.sqlite3.connect")
    def test_search_stocks(self, mock_connect):
        """銘柄検索APIのテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                "12340",
                "テスト株式会社",
                "0001",
                "プライム市場",
                "01",
                "食品",
                "0100",
                "水産・農林業",
            ),
            (
                "12350",
                "テスト2株式会社",
                "0002",
                "スタンダード市場",
                "02",
                "エネルギー資源",
                "0200",
                "鉱業",
            ),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # リクエスト
        response = self.client.get("/api/portfolio/stocks/search?q=1234")
        data = json.loads(response.data)

        # 検証
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["success"])
        self.assertEqual(len(data["stocks"]), 2)
        self.assertEqual(data["stocks"][0]["code"], "1234")
        self.assertEqual(data["stocks"][0]["full_code"], "12340")
        self.assertEqual(data["stocks"][0]["company_name"], "テスト株式会社")

    @patch("src.portfolio.models.Holding")
    @patch("src.ui.web.PortfolioManager")
    def test_add_holding(self, mock_manager, mock_holding):
        """保有銘柄追加APIのテスト"""
        # モックの設定
        mock_holding_instance = MagicMock()
        mock_holding_instance.save.return_value = True
        mock_holding.find_by_user_code_and_account.return_value = None
        mock_holding.return_value = mock_holding_instance

        # リクエストデータ
        request_data = {
            "code": "1234",
            "account_name": "test_account",
            "quantity": 100,
            "average_price": 1500.0,
        }

        # リクエスト
        response = self.client.post(
            "/api/portfolio/holdings/add",
            data=json.dumps(request_data),
            content_type="application/json",
        )
        data = json.loads(response.data)

        # 検証
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["success"])
        self.assertEqual(data["message"], "保有銘柄を追加しました")
        mock_holding_instance.save.assert_called_once()

    @patch("src.portfolio.models.Holding")
    @patch("src.ui.web.PortfolioManager")
    def test_update_holding(self, mock_manager, mock_holding):
        """保有銘柄更新APIのテスト"""
        # モックの設定
        mock_holding_instance = MagicMock()
        mock_holding_instance.save.return_value = True
        mock_holding.find_by_user_code_and_account.return_value = mock_holding_instance

        # リクエストデータ
        request_data = {
            "code": "1234",
            "account_name": "test_account",
            "quantity": 200,
            "average_price": 1600.0,
        }

        # リクエスト
        response = self.client.post(
            "/api/portfolio/holdings/update",
            data=json.dumps(request_data),
            content_type="application/json",
        )
        data = json.loads(response.data)

        # 検証
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["success"])
        self.assertEqual(data["message"], "保有銘柄を更新しました")

    @patch("src.ui.web.sqlite3.connect")
    def test_delete_single_holding(self, mock_connect):
        """保有銘柄削除APIのテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # リクエスト
        response = self.client.delete(
            "/api/portfolio/holdings/delete/1234/test_account"
        )
        data = json.loads(response.data)

        # 検証
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["success"])
        self.assertEqual(data["message"], "保有銘柄を削除しました")

    @patch("src.portfolio.models.Transaction")
    def test_add_transaction(self, mock_transaction):
        """取引履歴追加APIのテスト"""
        # モックの設定
        mock_trans_instance = MagicMock()
        mock_trans_instance.save.return_value = True
        mock_transaction.return_value = mock_trans_instance

        # リクエストデータ
        request_data = {
            "code": "1234",
            "transaction_date": "2024-01-01",
            "transaction_type": "buy",
            "quantity": 100,
            "price": 1500.0,
            "commission": 100,
            "tax": 0,
            "remarks": "テスト取引",
        }

        # リクエスト
        response = self.client.post(
            "/api/portfolio/transactions/add",
            data=json.dumps(request_data),
            content_type="application/json",
        )
        data = json.loads(response.data)

        # 検証
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["success"])
        self.assertEqual(data["message"], "取引を追加しました")
        mock_trans_instance.save.assert_called_once()

    @patch("src.ui.web.sqlite3.connect")
    def test_update_transaction(self, mock_connect):
        """取引履歴更新APIのテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            (1,),
            (100, 1500),
        ]  # user_id, quantity/price
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # リクエストデータ
        request_data = {"quantity": 200, "price": 1600.0}

        # リクエスト
        response = self.client.post(
            "/api/portfolio/transactions/update/1",
            data=json.dumps(request_data),
            content_type="application/json",
        )
        data = json.loads(response.data)

        # 検証
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["success"])
        self.assertEqual(data["message"], "取引を更新しました")

    @patch("src.ui.web.sqlite3.connect")
    def test_delete_transaction(self, mock_connect):
        """取引履歴削除APIのテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # リクエスト
        response = self.client.delete("/api/portfolio/transactions/delete/1")
        data = json.loads(response.data)

        # 検証
        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["success"])
        self.assertEqual(data["message"], "取引を削除しました")

    def test_add_holding_validation(self):
        """保有銘柄追加のバリデーションテスト"""
        # 銘柄コードなし
        response = self.client.post(
            "/api/portfolio/holdings/add",
            data=json.dumps({"quantity": 100, "average_price": 1500}),
            content_type="application/json",
        )
        data = json.loads(response.data)
        self.assertFalse(data["success"])
        self.assertEqual(data["error"], "銘柄コードは必須です")

        # 数量が0以下
        response = self.client.post(
            "/api/portfolio/holdings/add",
            data=json.dumps({"code": "1234", "quantity": 0, "average_price": 1500}),
            content_type="application/json",
        )
        data = json.loads(response.data)
        self.assertFalse(data["success"])
        self.assertEqual(data["error"], "数量は正の数を入力してください")

    def test_add_transaction_validation(self):
        """取引履歴追加のバリデーションテスト"""
        # 取引種別が不正
        response = self.client.post(
            "/api/portfolio/transactions/add",
            data=json.dumps(
                {
                    "code": "1234",
                    "transaction_date": "2024-01-01",
                    "transaction_type": "invalid",
                    "quantity": 100,
                    "price": 1500,
                }
            ),
            content_type="application/json",
        )
        data = json.loads(response.data)
        self.assertFalse(data["success"])
        self.assertEqual(data["error"], "取引種別は buy または sell を指定してください")


if __name__ == "__main__":
    unittest.main()

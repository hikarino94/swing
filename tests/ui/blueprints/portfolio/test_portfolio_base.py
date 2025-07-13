"""ui.blueprints.portfolio.baseのテスト"""

import json
import time
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

from src.auth.models import User
from src.ui.blueprints.portfolio.base import (
    get_cached_search,
    portfolio_base_bp,
    set_cached_search,
)


@pytest.fixture
def app():
    """テスト用のFlaskアプリケーション"""
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-secret-key"
    app.config["TESTING"] = True
    app.register_blueprint(portfolio_base_bp)

    return app


@pytest.fixture
def client(app):
    """テスト用のクライアント"""
    return app.test_client()


@pytest.fixture
def user():
    """テスト用ユーザー"""
    user = MagicMock(spec=User)
    user.id = 1
    user.username = "testuser"
    return user


class TestCacheFunctions:
    """キャッシュ関数のテスト"""

    def test_get_cached_search_not_found(self):
        """キャッシュにない場合"""
        # キャッシュをクリア
        import src.ui.blueprints.portfolio.base as base_module

        base_module._search_cache.clear()

        result = get_cached_search("test_query")
        assert result is None

    def test_set_and_get_cached_search(self):
        """キャッシュの設定と取得"""
        import src.ui.blueprints.portfolio.base as base_module

        base_module._search_cache.clear()

        test_result = [{"code": "1234", "name": "テスト"}]
        set_cached_search("test_query", test_result)

        result = get_cached_search("test_query")
        assert result == test_result

    def test_cached_search_expiry(self):
        """キャッシュの有効期限"""
        import src.ui.blueprints.portfolio.base as base_module

        base_module._search_cache.clear()

        test_result = [{"code": "1234", "name": "テスト"}]

        # 古いタイムスタンプでキャッシュを設定
        base_module._search_cache["test_query"] = (
            time.time() - 20,
            test_result,
        )  # 20秒前

        # 期限切れなのでNoneが返る
        result = get_cached_search("test_query")
        assert result is None

    def test_cache_size_limit(self):
        """キャッシュサイズ制限のテスト"""
        import src.ui.blueprints.portfolio.base as base_module

        base_module._search_cache.clear()

        # 100件までキャッシュを追加
        for i in range(100):
            set_cached_search(f"query_{i}", [{"code": str(i)}])

        assert len(base_module._search_cache) == 100

        # 101件目を追加すると最も古いものが削除される
        set_cached_search("query_100", [{"code": "100"}])
        assert len(base_module._search_cache) == 100
        assert "query_100" in base_module._search_cache


class TestGetFunds:
    """get_funds関数のテスト"""

    @patch("src.ui.blueprints.portfolio.base.sqlite3.connect")
    def test_get_funds_success(self, mock_connect, client):
        """投資信託一覧の正常取得"""
        # モックデータ
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                "JP90C0001234",  # fund_id
                "テスト投信A",  # fund_name
                "特定口座",  # account_name
                "特定",  # account_type
                10000,  # quantity
                12000,  # average_price
                13000,  # market_value
                1000,  # profit_loss
                8.33,  # profit_loss_ratio
                "再投資",  # dividend_method
                "2024-01-15",  # updated_at
                13500,  # current_nav
                "2024-01-14",  # nav_date
            ),
            (
                "JP90C0005678",
                "テスト投信B",
                "NISA口座",
                "NISA",
                5000,
                15000,
                14000,
                -500,
                -3.33,
                "受取",
                "2024-01-15",
                14000,
                "2024-01-14",
            ),
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        response = client.get("/funds")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["funds"]) == 2

        # 最初の投信の確認
        fund1 = data["funds"][0]
        assert fund1["fund_id"] == "JP90C0001234"
        assert fund1["fund_name"] == "テスト投信A"
        assert fund1["account_name"] == "特定口座"
        assert fund1["quantity"] == 10000

        # 再計算された値の確認
        # market_value = 10000 * 13500 / 10000 = 13500
        assert fund1["market_value"] == 13500

        # 集計情報の確認
        aggregate = data["aggregated"]
        assert aggregate["total_funds"] == 2
        # fund2の再計算: 5000 * 14000 / 10000 = 7000
        assert aggregate["total_value"] == 20500  # 13500 + 7000

    @patch("src.ui.blueprints.portfolio.base.sqlite3.connect")
    def test_get_funds_empty(self, mock_connect, client):
        """投資信託がない場合"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        response = client.get("/funds")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["funds"]) == 0
        assert data["aggregated"]["total_funds"] == 0
        assert data["aggregated"]["total_value"] == 0

    @patch("src.ui.blueprints.portfolio.base.sqlite3.connect")
    @patch("src.ui.blueprints.portfolio.base.logger")
    def test_get_funds_error(self, mock_logger, mock_connect, client):
        """エラー発生時"""
        mock_connect.side_effect = Exception("Database error")

        response = client.get("/funds")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is False
        assert "Database error" in data["error"]

        # エラーログの確認
        mock_logger.error.assert_called()


class TestGetPortfolioSummary:
    """get_portfolio_summary関数のテスト"""

    @patch("src.ui.blueprints.portfolio.base.PortfolioManager")
    def test_get_portfolio_summary_success(self, mock_manager, client):
        """ポートフォリオサマリーの正常取得"""
        mock_summary = {
            "total_value": 1000000,
            "total_profit_loss": 50000,
            "total_profit_loss_ratio": 5.26,
            "stock_count": 10,
        }
        mock_manager.get_portfolio_summary.return_value = mock_summary

        response = client.get("/summary")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert data["summary"] == mock_summary

        # 正しいユーザーIDで呼ばれたことを確認
        mock_manager.get_portfolio_summary.assert_called_once_with(1)

    @patch("src.ui.blueprints.portfolio.base.PortfolioManager")
    @patch("src.ui.blueprints.portfolio.base.logger")
    def test_get_portfolio_summary_error(self, mock_logger, mock_manager, client):
        """エラー発生時"""
        mock_manager.get_portfolio_summary.side_effect = Exception("Calculation error")

        response = client.get("/summary")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is False
        assert "Calculation error" in data["error"]

        # エラーログの確認
        mock_logger.error.assert_called()


class TestGetAccounts:
    """get_accounts関数のテスト"""

    @patch("src.ui.blueprints.portfolio.base.sqlite3.connect")
    def test_get_accounts_success(self, mock_connect, client):
        """口座一覧の正常取得"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("特定口座", "特定", 5),
            ("NISA口座", "NISA", 3),
            ("特定口座2", "特定", 2),
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        response = client.get("/accounts")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["accounts"]) == 3

        # 最初の口座の確認
        account1 = data["accounts"][0]
        assert account1["account_name"] == "特定口座"
        assert account1["account_type"] == "特定"
        assert account1["holdings_count"] == 5
        assert account1["display_name"] == "特定口座 (特定)"

        # プルダウン用リストの確認
        assert len(data["account_list"]) == 3
        assert data["account_list"][0] == "特定口座 (特定)"

    @patch("src.ui.blueprints.portfolio.base.sqlite3.connect")
    def test_get_accounts_empty(self, mock_connect, client):
        """口座がない場合"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        response = client.get("/accounts")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["accounts"]) == 0
        assert len(data["account_list"]) == 0


class TestSearchStocks:
    """search_stocks関数のテスト"""

    @patch("src.ui.blueprints.portfolio.base.sqlite3.connect")
    def test_search_stocks_by_code(self, mock_connect, client):
        """コードでの検索"""
        # キャッシュをクリア
        import src.ui.blueprints.portfolio.base as base_module

        base_module._search_cache.clear()

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("7203", "トヨタ自動車", "16"),
            ("7201", "日産自動車", "16"),
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        response = client.get("/stocks/search?q=720")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["stocks"]) == 2

        # 最初の銘柄の確認
        stock1 = data["stocks"][0]
        assert stock1["code"] == "7203"
        assert stock1["company_name"] == "トヨタ自動車"
        assert stock1["market"] == "16"

    @patch("src.ui.blueprints.portfolio.base.sqlite3.connect")
    def test_search_stocks_by_name(self, mock_connect, client):
        """会社名での検索"""
        # キャッシュをクリア
        import src.ui.blueprints.portfolio.base as base_module

        base_module._search_cache.clear()

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("6758", "ソニーグループ", "16"),
            ("6857", "アドバンテスト", "16"),
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        response = client.get("/stocks/search?q=テスト")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["stocks"]) == 2

    def test_search_stocks_empty_query(self, client):
        """空のクエリ"""
        response = client.get("/stocks/search?q=")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["stocks"]) == 0

    def test_search_stocks_from_cache(self, client):
        """キャッシュからの取得"""
        # キャッシュをクリア
        import src.ui.blueprints.portfolio.base as base_module

        base_module._search_cache.clear()

        # キャッシュに事前設定
        cached_data = [
            {"code": "1234", "company_name": "キャッシュテスト", "market": ""}
        ]
        set_cached_search("cache_test", cached_data)

        response = client.get("/stocks/search?q=cache_test")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["stocks"]) == 1
        assert data["stocks"][0]["company_name"] == "キャッシュテスト"

    @patch("src.ui.blueprints.portfolio.base.sqlite3.connect")
    @patch("src.ui.blueprints.portfolio.base.logger")
    def test_search_stocks_error(self, mock_logger, mock_connect, client):
        """エラー発生時"""
        # キャッシュをクリア
        import src.ui.blueprints.portfolio.base as base_module

        base_module._search_cache.clear()

        mock_connect.side_effect = Exception("Search error")

        response = client.get("/stocks/search?q=test")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is False
        assert "Search error" in data["error"]

        # エラーログの確認
        mock_logger.error.assert_called()


class TestPortfolioBaseIntegration:
    """portfolio_base_bpの統合テスト"""

    def test_all_endpoints_require_login(self, app, client):
        """全エンドポイントがログイン必須であることを確認"""
        # TESTINGモードを一時的に無効化
        app.config["TESTING"] = False

        endpoints = [
            "/funds",
            "/summary",
            "/accounts",
            "/stocks/search?q=test",
        ]

        with patch(
            "src.auth.decorators.AuthManager.get_user_by_session"
        ) as mock_get_user:
            # ユーザーが存在しない（ログインしていない）状態
            mock_get_user.return_value = None

            # portfolio関連エンドポイントは/api/で始まらないため、redirectが呼ばれる
            # url_forとredirectをモック
            with patch("src.auth.decorators.url_for") as mock_url_for:
                mock_url_for.return_value = "/login"  # loginエンドポイントのURL

                with patch("src.auth.decorators.redirect") as mock_redirect:
                    from werkzeug.wrappers import Response

                    # redirectの戻り値をモック（401レスポンスを返す）
                    mock_response = Response("Unauthorized", status=401)
                    mock_redirect.return_value = mock_response

                    for endpoint in endpoints:
                        response = client.get(endpoint)
                        assert response.status_code == 401

                    # 各エンドポイントでredirectが呼ばれたことを確認
                    assert mock_redirect.call_count == len(endpoints)

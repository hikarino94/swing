"""src.ui.web モジュールのテスト"""

from unittest.mock import MagicMock, patch

import pytest

from src.auth.models import User
from src.ui.web import app, init_database


class TestWebApp:
    """Webアプリケーション全体のテスト"""

    @pytest.fixture
    def client(self):
        """テスト用クライアント"""
        app.config["TESTING"] = True
        app.config["SECRET_KEY"] = "test-secret-key"
        with app.test_client() as client:
            yield client

    @pytest.fixture
    def authenticated_client(self, client):
        """認証済みクライアント"""
        with client.session_transaction() as sess:
            sess["session_id"] = "test-session-id"
        yield client

    def test_app_configuration(self):
        """アプリケーション設定のテスト"""
        assert app.config["SESSION_COOKIE_HTTPONLY"] is True
        assert app.config["SESSION_COOKIE_SAMESITE"] == "Lax"
        assert app.config["SESSION_COOKIE_NAME"] == "swing_session"
        assert app.config["MAX_CONTENT_LENGTH"] == 16 * 1024 * 1024
        assert app.config["SEND_FILE_MAX_AGE_DEFAULT"] == 0

    def test_after_request_handler(self, client):
        """after_requestハンドラのテスト"""
        response = client.get("/")
        assert response.direct_passthrough is False

    @patch("src.ui.web.Path")
    @patch("db.db_schema.init_schema")
    @patch("src.auth.admin_setup.create_admin_from_env")
    @patch("src.ui.web.sqlite3.connect")
    def test_init_database_creates_new_db(
        self, mock_connect, mock_create_admin, mock_init_schema, mock_path
    ):
        """データベース初期化（新規作成）のテスト"""
        # データベースが存在しない場合
        mock_db_path = MagicMock()
        mock_db_path.exists.return_value = False
        mock_db_path.parent.mkdir = MagicMock()
        mock_path.return_value = mock_db_path

        init_database()

        mock_db_path.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_init_schema.assert_called_once_with(mock_db_path)
        mock_create_admin.assert_called_once()

    @patch("src.ui.web.Path")
    @patch("db.db_schema.init_schema")
    @patch("src.auth.admin_setup.create_admin_from_env")
    @patch("src.ui.web.sqlite3.connect")
    def test_init_database_checks_existing_db(
        self, mock_connect, mock_create_admin, mock_init_schema, mock_path
    ):
        """データベース初期化（既存DB確認）のテスト"""
        # データベースが存在する場合
        mock_db_path = MagicMock()
        mock_db_path.exists.return_value = True
        mock_path.return_value = mock_db_path

        # usersテーブルが存在する場合
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("users",)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connect.return_value = mock_conn

        init_database()

        mock_cursor.execute.assert_called_once()
        mock_init_schema.assert_not_called()
        mock_create_admin.assert_called_once()

    @patch("src.ui.web.Path")
    @patch("db.db_schema.init_schema")
    @patch("src.auth.admin_setup.create_admin_from_env")
    @patch("src.ui.web.sqlite3.connect")
    def test_init_database_recreates_schema(
        self, mock_connect, mock_create_admin, mock_init_schema, mock_path
    ):
        """データベース初期化（スキーマ再作成）のテスト"""
        # データベースは存在するがusersテーブルがない場合
        mock_db_path = MagicMock()
        mock_db_path.exists.return_value = True
        mock_path.return_value = mock_db_path

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # usersテーブルなし
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connect.return_value = mock_conn

        init_database()

        mock_init_schema.assert_called_once_with(mock_db_path)
        mock_create_admin.assert_called_once()


class TestIndexRoute:
    """メインページルートのテスト"""

    @pytest.fixture
    def client(self):
        """テスト用クライアント"""
        app.config["TESTING"] = True
        app.config["SECRET_KEY"] = "test-secret-key"
        with app.test_client() as client:
            yield client

    def test_index_with_testing_config(self, client):
        """テスト環境でのインデックスページアクセス"""
        response = client.get("/")
        assert response.status_code == 200
        assert b"testuser" in response.data

    @patch("src.ui.web.AuthManager.get_user_by_session")
    def test_index_without_session(self, mock_get_user, client):
        """セッションなしでのアクセス"""
        app.config["TESTING"] = False
        response = client.get("/")
        assert response.status_code == 302
        assert response.location.endswith("/login")

    @patch("src.ui.web.AuthManager.get_user_by_session")
    def test_index_with_invalid_session(self, mock_get_user, client):
        """無効なセッションでのアクセス"""
        app.config["TESTING"] = False
        mock_get_user.return_value = None

        with client.session_transaction() as sess:
            sess["session_id"] = "invalid-session"

        response = client.get("/")
        assert response.status_code == 302
        assert response.location.endswith("/login")

    @patch("src.ui.web.AuthManager.get_user_by_session")
    def test_index_with_valid_session(self, mock_get_user, client):
        """有効なセッションでのアクセス"""
        app.config["TESTING"] = False
        mock_user = User(
            id=1,
            username="user1",
            email="user@example.com",
            password_hash="hash",
            role="user",
        )
        mock_get_user.return_value = mock_user

        with client.session_transaction() as sess:
            sess["session_id"] = "valid-session"

        response = client.get("/")
        assert response.status_code == 200
        assert b"user1" in response.data

    @patch("src.ui.web.AuthManager.get_user_by_session")
    def test_index_with_portfolio_only_user(self, mock_get_user, client):
        """ポートフォリオ専用ユーザーでのアクセス"""
        app.config["TESTING"] = False
        mock_user = User(
            id=2,
            username="portfolio_user",
            email="portfolio@example.com",
            password_hash="hash",
            role="portfolio_only",
        )
        mock_get_user.return_value = mock_user

        with client.session_transaction() as sess:
            sess["session_id"] = "portfolio-session"

        response = client.get("/")
        assert response.status_code == 200
        assert b"portfolio_user" in response.data

    @patch("src.ui.web.AuthManager.get_user_by_session")
    def test_index_with_tab_parameter(self, mock_get_user, client):
        """タブパラメータ付きアクセス"""
        app.config["TESTING"] = False
        mock_user = User(
            id=1,
            username="user1",
            email="user@example.com",
            password_hash="hash",
            role="user",
        )
        mock_get_user.return_value = mock_user

        with client.session_transaction() as sess:
            sess["session_id"] = "valid-session"

        response = client.get("/?tab=backtest")
        assert response.status_code == 200
        assert b"backtest" in response.data


class TestScreeningRoute:
    """スクリーニングページルートのテスト"""

    @pytest.fixture
    def client(self):
        """テスト用クライアント"""
        app.config["TESTING"] = True
        app.config["SECRET_KEY"] = "test-secret-key"
        with app.test_client() as client:
            yield client

    def test_screening_without_session(self, client):
        """セッションなしでのアクセス"""
        app.config["TESTING"] = False
        response = client.get("/screening")
        assert response.status_code == 302
        assert response.location.endswith("/login")

    @patch("src.ui.web.AuthManager.get_user_by_session")
    def test_screening_with_invalid_session(self, mock_get_user, client):
        """無効なセッションでのアクセス"""
        app.config["TESTING"] = False
        mock_get_user.return_value = None

        with client.session_transaction() as sess:
            sess["session_id"] = "invalid-session"

        response = client.get("/screening")
        assert response.status_code == 302
        assert response.location.endswith("/login")

    @patch("src.ui.web.AuthManager.get_user_by_session")
    def test_screening_with_valid_session(self, mock_get_user, client):
        """有効なセッションでのアクセス"""
        app.config["TESTING"] = False
        mock_user = User(
            id=1,
            username="user1",
            email="user@example.com",
            password_hash="hash",
            role="user",
        )
        mock_get_user.return_value = mock_user

        with client.session_transaction() as sess:
            sess["session_id"] = "valid-session"

        response = client.get("/screening")
        assert response.status_code == 302
        assert response.location.endswith("/?tab=screening")


class TestBacktestRoute:
    """バックテストページルートのテスト"""

    @pytest.fixture
    def client(self):
        """テスト用クライアント"""
        app.config["TESTING"] = True
        app.config["SECRET_KEY"] = "test-secret-key"
        with app.test_client() as client:
            yield client

    @patch("src.ui.web.AuthManager.get_user_by_session")
    def test_backtest_with_valid_session(self, mock_get_user, client):
        """有効なセッションでのアクセス"""
        app.config["TESTING"] = False
        mock_user = User(
            id=1,
            username="user1",
            email="user@example.com",
            password_hash="hash",
            role="user",
        )
        mock_get_user.return_value = mock_user

        with client.session_transaction() as sess:
            sess["session_id"] = "valid-session"

        response = client.get("/backtest")
        assert response.status_code == 302
        assert response.location.endswith("/?tab=backtest")


class TestImportRoute:
    """インポートページルートのテスト"""

    @pytest.fixture
    def client(self):
        """テスト用クライアント"""
        app.config["TESTING"] = True
        app.config["SECRET_KEY"] = "test-secret-key"
        with app.test_client() as client:
            yield client

    @patch("src.ui.web.AuthManager.get_user_by_session")
    def test_import_with_valid_session(self, mock_get_user, client):
        """有効なセッションでのアクセス"""
        app.config["TESTING"] = False
        mock_user = User(
            id=1,
            username="user1",
            email="user@example.com",
            password_hash="hash",
            role="user",
        )
        mock_get_user.return_value = mock_user

        with client.session_transaction() as sess:
            sess["session_id"] = "valid-session"

        response = client.get("/import")
        assert response.status_code == 302
        assert response.location.endswith("/?tab=import")


class TestBlueprints:
    """Blueprint登録のテスト"""

    def test_blueprints_registered(self):
        """全てのBlueprintが登録されているか"""
        blueprint_names = [bp.name for bp in app.blueprints.values()]

        assert "auth" in blueprint_names
        assert "fetch" in blueprint_names
        assert "screening" in blueprint_names
        assert "backtest" in blueprint_names
        assert "utils" in blueprint_names
        assert "results" in blueprint_names
        assert "holdings" in blueprint_names
        assert "daytrade" in blueprint_names

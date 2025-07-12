"""ui.blueprints.auth.routesのテスト"""

from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

from src.ui.blueprints.auth.routes import auth_bp


@pytest.fixture
def app():
    """テスト用のFlaskアプリケーション"""
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-secret-key"
    app.config["TESTING"] = True
    app.register_blueprint(auth_bp)

    # ダミーのindexルートを追加
    @app.route("/")
    def index():
        return "Index Page"

    return app


@pytest.fixture
def client(app):
    """テスト用のクライアント"""
    return app.test_client()


class TestLoginRoute:
    """ログインルートのテスト"""

    @patch("src.ui.blueprints.auth.routes.render_template")
    def test_login_get(self, mock_render, client):
        """GETリクエストでログインページが表示されることを確認"""
        mock_render.return_value = "Login Page"

        response = client.get("/login")
        assert response.status_code == 200
        mock_render.assert_called_once_with("login.html", error=None)

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    def test_login_get_already_logged_in(self, mock_auth_manager, client, app):
        """既にログイン済みの場合のリダイレクトテスト"""
        with app.test_request_context():
            # セッションにsession_idを設定
            with client.session_transaction() as sess:
                sess["session_id"] = "test-session-id"

            # ユーザーが存在する
            mock_user = MagicMock()
            mock_auth_manager.get_user_by_session.return_value = mock_user

            response = client.get("/login")
            assert response.status_code == 302
            assert response.location == "/"

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    @patch("src.ui.blueprints.auth.routes.render_template")
    def test_login_post_success(self, mock_render, mock_auth_manager, client, app):
        """ログイン成功のテスト"""
        # モックの設定
        mock_user = MagicMock()
        mock_auth_manager.login.return_value = (mock_user, "new-session-id", None)

        # POSTリクエスト
        response = client.post(
            "/login",
            data={
                "username": "testuser",
                "password": "password123",
                "remember_me": "on",
            },
        )

        # 検証
        assert response.status_code == 302
        assert response.location == "/"

        # セッションの確認
        with client.session_transaction() as sess:
            assert sess.get("session_id") == "new-session-id"

        mock_auth_manager.login.assert_called_once_with("testuser", "password123", True)

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    @patch("src.ui.blueprints.auth.routes.render_template")
    def test_login_post_failure(self, mock_render, mock_auth_manager, client):
        """ログイン失敗のテスト"""
        # モックの設定
        mock_auth_manager.login.return_value = (None, None, "Invalid credentials")
        mock_render.return_value = "Error Page"

        # POSTリクエスト
        client.post(
            "/login", data={"username": "testuser", "password": "wrongpassword"}
        )

        # render_templateが呼ばれたことを確認
        mock_render.assert_called_once_with("login.html", error="Invalid credentials")

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    def test_login_post_with_next_url(self, mock_auth_manager, client, app):
        """next_urlがある場合のリダイレクトテスト"""
        # モックの設定
        mock_user = MagicMock()
        mock_auth_manager.login.return_value = (mock_user, "session-id", None)

        # セッションにnext_urlを設定
        with client.session_transaction() as sess:
            sess["next_url"] = "/portfolio"

        # POSTリクエスト
        response = client.post(
            "/login", data={"username": "testuser", "password": "password123"}
        )

        # 検証
        assert response.status_code == 302
        assert response.location == "/portfolio"

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    def test_login_remember_me_disabled(self, mock_auth_manager, client, app):
        """Remember Meが無効の場合のテスト"""
        # モックの設定
        mock_user = MagicMock()
        mock_auth_manager.login.return_value = (mock_user, "session-id", None)

        # POSTリクエスト（remember_meなし）
        client.post("/login", data={"username": "testuser", "password": "password123"})

        # AuthManager.loginの呼び出しを確認
        mock_auth_manager.login.assert_called_once_with(
            "testuser", "password123", False
        )


class TestRegisterRoute:
    """新規登録ルートのテスト"""

    @patch("src.ui.blueprints.auth.routes.render_template")
    def test_register_get(self, mock_render, client):
        """GETリクエストで登録ページが表示されることを確認"""
        mock_render.return_value = "Register Page"

        response = client.get("/register")
        assert response.status_code == 200
        mock_render.assert_called_once_with("register.html", error=None)

    @patch("src.ui.blueprints.auth.routes.render_template")
    def test_register_post_password_mismatch(self, mock_render, client):
        """パスワード不一致のテスト"""
        mock_render.return_value = "Error Page"

        client.post(
            "/register",
            data={
                "username": "newuser",
                "email": "new@example.com",
                "password": "password123",
                "password_confirm": "password456",
            },
        )

        mock_render.assert_called_once_with(
            "register.html", error="パスワードが一致しません"
        )

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    def test_register_post_success(self, mock_auth_manager, client, app):
        """登録成功のテスト"""
        # モックの設定
        mock_auth_manager.register_user.return_value = (True, "Success")
        mock_user = MagicMock()
        mock_auth_manager.login.return_value = (mock_user, "session-id", None)

        # POSTリクエスト
        response = client.post(
            "/register",
            data={
                "username": "newuser",
                "email": "new@example.com",
                "password": "password123",
                "password_confirm": "password123",
            },
        )

        # 検証
        assert response.status_code == 302
        assert response.location == "/"

        # register_userの呼び出しを確認
        mock_auth_manager.register_user.assert_called_once_with(
            "newuser", "new@example.com", "password123", role="portfolio_only"
        )

        # 自動ログインの確認
        mock_auth_manager.login.assert_called_once_with("newuser", "password123")

        # セッションの確認
        with client.session_transaction() as sess:
            assert sess.get("session_id") == "session-id"

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    @patch("src.ui.blueprints.auth.routes.render_template")
    def test_register_post_failure(self, mock_render, mock_auth_manager, client):
        """登録失敗のテスト"""
        # モックの設定
        mock_auth_manager.register_user.return_value = (
            False,
            "Username already exists",
        )
        mock_render.return_value = "Error Page"

        # POSTリクエスト
        client.post(
            "/register",
            data={
                "username": "existinguser",
                "email": "existing@example.com",
                "password": "password123",
                "password_confirm": "password123",
            },
        )

        # render_templateが呼ばれたことを確認
        mock_render.assert_called_once_with(
            "register.html", error="Username already exists"
        )

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    @patch("src.ui.blueprints.auth.routes.render_template")
    def test_register_post_login_failure(self, mock_render, mock_auth_manager, client):
        """登録成功後の自動ログイン失敗のテスト"""
        # モックの設定
        mock_auth_manager.register_user.return_value = (True, "Success")
        mock_auth_manager.login.return_value = (None, None, "Login failed")
        mock_render.return_value = "Error Page"

        # POSTリクエスト
        client.post(
            "/register",
            data={
                "username": "newuser",
                "email": "new@example.com",
                "password": "password123",
                "password_confirm": "password123",
            },
        )

        # render_templateが呼ばれたことを確認
        mock_render.assert_called_once_with("register.html", error="Success")

    def test_register_post_whitespace_handling(self, client):
        """空白文字の処理テスト"""
        with patch("src.ui.blueprints.auth.routes.AuthManager") as mock_auth:
            mock_auth.register_user.return_value = (True, "Success")
            mock_auth.login.return_value = (MagicMock(), "session-id", None)

            # 前後に空白を含むデータ
            client.post(
                "/register",
                data={
                    "username": "  newuser  ",
                    "email": "  new@example.com  ",
                    "password": "password123",
                    "password_confirm": "password123",
                },
            )

            # strip()されていることを確認
            mock_auth.register_user.assert_called_once_with(
                "newuser", "new@example.com", "password123", role="portfolio_only"
            )


class TestLogoutRoute:
    """ログアウトルートのテスト"""

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    def test_logout_with_session(self, mock_auth_manager, client):
        """セッションがある場合のログアウトテスト"""
        # セッションを設定
        with client.session_transaction() as sess:
            sess["session_id"] = "test-session-id"
            sess["other_data"] = "some value"

        # ログアウト実行
        response = client.get("/logout")

        # 検証
        assert response.status_code == 302
        assert response.location == "/login"

        # AuthManager.logoutが呼ばれたことを確認
        mock_auth_manager.logout.assert_called_once_with("test-session-id")

        # セッションがクリアされたことを確認
        with client.session_transaction() as sess:
            assert "session_id" not in sess
            assert "other_data" not in sess

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    def test_logout_without_session(self, mock_auth_manager, client):
        """セッションがない場合のログアウトテスト"""
        # ログアウト実行
        response = client.get("/logout")

        # 検証
        assert response.status_code == 302
        assert response.location == "/login"

        # AuthManager.logoutが呼ばれていないことを確認
        mock_auth_manager.logout.assert_not_called()


class TestFormValueHandling:
    """フォーム値の処理に関するテスト"""

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    @patch("src.ui.blueprints.auth.routes.render_template")
    def test_empty_form_values(self, mock_render, mock_auth_manager, client):
        """空のフォーム値の処理テスト"""
        mock_auth_manager.login.return_value = (None, None, "Empty credentials")
        mock_render.return_value = "Error Page"

        # 空のデータでPOST
        client.post("/login", data={})

        # 空文字列として処理されることを確認
        mock_auth_manager.login.assert_called_once_with("", "", False)

    @patch("src.ui.blueprints.auth.routes.AuthManager")
    @patch("src.ui.blueprints.auth.routes.render_template")
    def test_missing_form_fields(self, mock_render, mock_auth_manager, client):
        """フォームフィールドが欠けている場合のテスト"""
        mock_auth_manager.login.return_value = (None, None, "Invalid")
        mock_render.return_value = "Error Page"

        # パスワードフィールドなし
        client.post("/login", data={"username": "testuser"})

        # デフォルト値が使用されることを確認
        mock_auth_manager.login.assert_called_once_with("testuser", "", False)

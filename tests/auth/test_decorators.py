"""auth.decoratorsのテスト"""

from unittest.mock import MagicMock, patch

from flask import Flask, jsonify

from src.auth.decorators import admin_required, login_required
from src.auth.models import User


class TestLoginRequired:
    """login_requiredデコレータのテスト"""

    def test_login_required_test_mode(self):
        """テスト環境での認証スキップテスト"""
        app = Flask(__name__)
        app.config["TESTING"] = True

        @app.route("/protected")
        @login_required
        def protected_view():
            from flask import request

            return f"Hello {request.current_user.username}"

        with app.test_client() as client:
            response = client.get("/protected")
            assert response.status_code == 200
            assert b"Hello testuser" in response.data

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_login_required_authenticated_user(self, mock_get_user):
        """認証済みユーザーのアクセステスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # モックユーザー
        mock_user = MagicMock(spec=User)
        mock_user.username = "realuser"
        mock_user.id = 10
        mock_get_user.return_value = mock_user

        @app.route("/protected")
        @login_required
        def protected_view():
            from flask import request

            return f"Hello {request.current_user.username}"

        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_id"] = "valid-session-id"

            response = client.get("/protected")
            assert response.status_code == 200
            assert b"Hello realuser" in response.data
            mock_get_user.assert_called_once_with("valid-session-id")

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_login_required_unauthenticated_redirect(self, mock_get_user):
        """未認証ユーザーのリダイレクトテスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # 未認証
        mock_get_user.return_value = None

        @app.route("/login")
        def login():
            return "Login Page"

        @app.route("/protected")
        @login_required
        def protected_view():
            return "Protected Content"

        with app.test_client() as client:
            response = client.get("/protected")
            assert response.status_code == 302
            assert response.location.endswith("/login")

            # セッションにnext_urlが保存される
            with client.session_transaction() as sess:
                assert sess.get("next_url") == "http://localhost/protected"

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_login_required_api_unauthenticated(self, mock_get_user):
        """API未認証アクセスのJSONレスポンステスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # 未認証
        mock_get_user.return_value = None

        @app.route("/api/data")
        @login_required
        def api_data():
            return jsonify({"data": "secret"})

        with app.test_client() as client:
            response = client.get("/api/data")
            assert response.status_code == 401
            data = response.get_json()
            assert data["success"] is False
            assert data["error"] == "ログインが必要です"
            assert data["code"] == "UNAUTHORIZED"

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_login_required_no_session_id(self, mock_get_user):
        """セッションIDなしのアクセステスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # 認証されていないことを明示的に設定
        mock_get_user.return_value = None

        @app.route("/login")
        def login():
            return "Login Page"

        @app.route("/protected")
        @login_required
        def protected_view():
            return "Protected Content"

        with app.test_client() as client:
            # セッションIDなし
            response = client.get("/protected")
            assert response.status_code == 302
            assert response.location.endswith("/login")
            # get_user_by_sessionはNoneで呼ばれる
            mock_get_user.assert_called_once_with(None)


class TestAdminRequired:
    """admin_requiredデコレータのテスト"""

    def test_admin_required_test_mode(self):
        """テスト環境での認証スキップテスト"""
        app = Flask(__name__)
        app.config["TESTING"] = True

        @app.route("/admin")
        @admin_required
        def admin_view():
            from flask import request

            return f"Admin: {request.current_user.username}"

        with app.test_client() as client:
            response = client.get("/admin")
            assert response.status_code == 200
            assert b"Admin: testuser" in response.data

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_admin_required_admin_user(self, mock_get_user):
        """管理者ユーザーのアクセステスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # 管理者ユーザー
        mock_user = MagicMock(spec=User)
        mock_user.username = "admin"
        mock_user.role = "admin"
        mock_get_user.return_value = mock_user

        @app.route("/admin")
        @admin_required
        def admin_view():
            from flask import request

            return f"Admin: {request.current_user.username}"

        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_id"] = "admin-session-id"

            response = client.get("/admin")
            assert response.status_code == 200
            assert b"Admin: admin" in response.data

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_admin_required_portfolio_only_user(self, mock_get_user):
        """ポートフォリオ専用ユーザーのアクセス拒否テスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # ポートフォリオ専用ユーザー
        mock_user = MagicMock(spec=User)
        mock_user.username = "portfolio_user"
        mock_user.role = "portfolio_only"
        mock_get_user.return_value = mock_user

        @app.route("/admin")
        @admin_required
        def admin_view():
            return "Admin Content"

        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_id"] = "portfolio-session-id"

            response = client.get("/admin")
            assert response.status_code == 403
            assert "アクセス権限がありません" in response.get_data(as_text=True)

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_admin_required_portfolio_only_api(self, mock_get_user):
        """ポートフォリオ専用ユーザーのAPIアクセス拒否テスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # ポートフォリオ専用ユーザー
        mock_user = MagicMock(spec=User)
        mock_user.username = "portfolio_user"
        mock_user.role = "portfolio_only"
        mock_get_user.return_value = mock_user

        @app.route("/api/admin")
        @admin_required
        def api_admin():
            return jsonify({"admin": "data"})

        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_id"] = "portfolio-session-id"

            response = client.get("/api/admin")
            assert response.status_code == 403
            data = response.get_json()
            assert data["success"] is False
            assert data["error"] == "管理者権限が必要です"
            assert data["code"] == "FORBIDDEN"

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_admin_required_unauthenticated(self, mock_get_user):
        """未認証ユーザーのアクセステスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # 未認証
        mock_get_user.return_value = None

        @app.route("/login")
        def login():
            return "Login Page"

        @app.route("/admin")
        @admin_required
        def admin_view():
            return "Admin Content"

        with app.test_client() as client:
            response = client.get("/admin")
            assert response.status_code == 302
            assert response.location.endswith("/login")

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_admin_required_api_unauthenticated(self, mock_get_user):
        """API未認証アクセスのJSONレスポンステスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # 未認証
        mock_get_user.return_value = None

        @app.route("/api/admin")
        @admin_required
        def api_admin():
            return jsonify({"admin": "data"})

        with app.test_client() as client:
            response = client.get("/api/admin")
            assert response.status_code == 401
            data = response.get_json()
            assert data["success"] is False
            assert data["error"] == "ログインが必要です"
            assert data["code"] == "UNAUTHORIZED"

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    def test_admin_required_user_role(self, mock_get_user):
        """通常ユーザー（user role）のアクセステスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # 通常ユーザー（adminでもportfolio_onlyでもない）
        mock_user = MagicMock(spec=User)
        mock_user.username = "normal_user"
        mock_user.role = "user"
        mock_get_user.return_value = mock_user

        @app.route("/login")
        def login():
            return "Login Page"

        @app.route("/admin")
        @admin_required
        def admin_view():
            from flask import request

            return f"Admin: {request.current_user.username}"

        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_id"] = "user-session-id"

            response = client.get("/admin")
            # userロールは管理者権限がないため、403エラーを返す
            assert response.status_code == 403
            assert "アクセス権限がありません" in response.get_data(as_text=True)


class TestDecoratorsIntegration:
    """デコレータの統合テスト"""

    def test_decorated_function_preserves_metadata(self):
        """デコレートされた関数のメタデータ保持テスト"""

        @login_required
        def my_view():
            """My view documentation"""
            return "content"

        assert my_view.__name__ == "my_view"
        assert my_view.__doc__ == "My view documentation"

        @admin_required
        def admin_view():
            """Admin view documentation"""
            return "admin content"

        assert admin_view.__name__ == "admin_view"
        assert admin_view.__doc__ == "Admin view documentation"

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    @patch("src.auth.decorators.logger")
    def test_logging_on_unauthorized_access(self, mock_logger, mock_get_user):
        """未認証アクセス時のログ出力テスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # 未認証
        mock_get_user.return_value = None

        @app.route("/login")
        def login():
            return "Login"

        @app.route("/protected")
        @login_required
        def protected_view():
            return "Protected"

        @app.route("/api/data")
        @login_required
        def api_data():
            return jsonify({"data": "value"})

        with app.test_client() as client:
            # 通常のビュー
            client.get("/protected")
            mock_logger.warning.assert_called_with("未認証アクセス: /protected")

            # APIエンドポイント
            client.get("/api/data")
            mock_logger.warning.assert_called_with("未認証アクセス (API): /api/data")

    @patch("src.auth.decorators.AuthManager.get_user_by_session")
    @patch("src.auth.decorators.logger")
    def test_logging_on_forbidden_access(self, mock_logger, mock_get_user):
        """権限なしアクセス時のログ出力テスト"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"  # nosec B106

        # ポートフォリオ専用ユーザー
        mock_user = MagicMock(spec=User)
        mock_user.username = "portfolio_user"
        mock_user.role = "portfolio_only"
        mock_get_user.return_value = mock_user

        @app.route("/admin")
        @admin_required
        def admin_view():
            return "Admin"

        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_id"] = "portfolio-session"

            client.get("/admin")
            mock_logger.warning.assert_called_with(
                "管理者権限なしアクセス: /admin by portfolio_user"
            )

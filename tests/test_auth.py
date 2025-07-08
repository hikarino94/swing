"""認証機能のテストスイート"""

import os
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from werkzeug.security import generate_password_hash

from src.auth import AuthManager, login_required
from src.auth.models import Session, User


class TestUserModel:
    """Userモデルのテスト"""

    @pytest.fixture
    def temp_db(self):
        """テスト用の一時データベース"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        # データベースを初期化
        import sqlite3

        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'admin',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
        """
        )
        conn.close()

        yield db_path

        # クリーンアップ
        os.unlink(db_path)

    def test_user_creation(self):
        """ユーザーインスタンスの作成"""
        user = User(
            id=1, username="testuser", email="test@example.com", password_hash="hash"
        )
        assert user.id == 1
        assert user.username == "testuser"
        assert user.email == "test@example.com"
        assert user.password_hash == "hash"

    def test_find_by_username(self, temp_db):
        """ユーザー名での検索"""
        with patch("src.auth.models.get_db_path", return_value=temp_db):
            # テストユーザーを作成
            import sqlite3

            conn = sqlite3.connect(temp_db)
            conn.execute(
                """
                INSERT INTO users (username, email, password_hash)
                VALUES (?, ?, ?)
            """,
                ("testuser", "test@example.com", "hash"),
            )
            conn.commit()
            conn.close()

            # 検索テスト
            user = User.find_by_username("testuser")
            assert user is not None
            assert user.username == "testuser"
            assert user.email == "test@example.com"

            # 存在しないユーザー
            user = User.find_by_username("nonexistent")
            assert user is None

    def test_find_by_email(self, temp_db):
        """メールアドレスでの検索"""
        with patch("src.auth.models.get_db_path", return_value=temp_db):
            # テストユーザーを作成
            import sqlite3

            conn = sqlite3.connect(temp_db)
            conn.execute(
                """
                INSERT INTO users (username, email, password_hash)
                VALUES (?, ?, ?)
            """,
                ("testuser", "test@example.com", "hash"),
            )
            conn.commit()
            conn.close()

            # 検索テスト
            user = User.find_by_email("test@example.com")
            assert user is not None
            assert user.username == "testuser"

            # 存在しないメール
            user = User.find_by_email("none@example.com")
            assert user is None

    def test_save_new_user(self, temp_db):
        """新規ユーザーの保存"""
        with patch("src.auth.models.get_db_path", return_value=temp_db):
            user = User(
                username="newuser", email="new@example.com", password_hash="hash"
            )
            result = user.save()

            assert result is True
            assert user.id is not None

            # 保存されたことを確認
            saved_user = User.find_by_username("newuser")
            assert saved_user is not None
            assert saved_user.email == "new@example.com"

    def test_save_duplicate_user(self, temp_db):
        """重複ユーザーの保存（エラー）"""
        with patch("src.auth.models.get_db_path", return_value=temp_db):
            # 最初のユーザー
            user1 = User(
                username="testuser", email="test@example.com", password_hash="hash"
            )
            user1.save()

            # 同じユーザー名で保存を試みる
            user2 = User(
                username="testuser", email="test2@example.com", password_hash="hash"
            )
            result = user2.save()

            assert result is False


class TestSessionModel:
    """Sessionモデルのテスト"""

    @pytest.fixture
    def temp_db(self):
        """テスト用の一時データベース"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        # データベースを初期化
        import sqlite3

        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                remember_me INTEGER DEFAULT 0
            );
        """
        )
        conn.close()

        yield db_path

        # クリーンアップ
        os.unlink(db_path)

    def test_session_creation(self):
        """セッションインスタンスの作成"""
        expires_at = (datetime.now() + timedelta(hours=24)).isoformat()
        session = Session("session123", 1, expires_at)
        assert session.id == "session123"
        assert session.user_id == 1
        assert session.expires_at == expires_at

    def test_find_valid_session(self, temp_db):
        """有効なセッションの検索"""
        with patch("src.auth.models.get_db_path", return_value=temp_db):
            # 有効なセッションを作成
            import sqlite3

            conn = sqlite3.connect(temp_db)
            expires_at = (datetime.now() + timedelta(hours=1)).isoformat()
            conn.execute(
                """
                INSERT INTO sessions (id, user_id, expires_at)
                VALUES (?, ?, ?)
            """,
                ("session123", 1, expires_at),
            )
            conn.commit()
            conn.close()

            # 検索テスト
            session = Session.find_by_id("session123")
            assert session is not None
            assert session.user_id == 1

    def test_find_expired_session(self, temp_db):
        """期限切れセッションの検索"""
        with patch("src.auth.models.get_db_path", return_value=temp_db):
            # 期限切れセッションを作成
            import sqlite3

            conn = sqlite3.connect(temp_db)
            expires_at = (datetime.now() - timedelta(hours=1)).isoformat()
            conn.execute(
                """
                INSERT INTO sessions (id, user_id, expires_at)
                VALUES (?, ?, ?)
            """,
                ("expired123", 1, expires_at),
            )
            conn.commit()
            conn.close()

            # 検索テスト（期限切れは見つからない）
            session = Session.find_by_id("expired123")
            assert session is None

    def test_save_session(self, temp_db):
        """セッションの保存"""
        with patch("src.auth.models.get_db_path", return_value=temp_db):
            expires_at = (datetime.now() + timedelta(hours=24)).isoformat()
            session = Session("newsession", 1, expires_at)
            result = session.save()

            assert result is True

            # 保存されたことを確認
            saved_session = Session.find_by_id("newsession")
            assert saved_session is not None
            assert saved_session.user_id == 1

    def test_delete_session(self, temp_db):
        """セッションの削除"""
        with patch("src.auth.models.get_db_path", return_value=temp_db):
            # セッションを作成
            expires_at = (datetime.now() + timedelta(hours=24)).isoformat()
            session = Session("deleteme", 1, expires_at)
            session.save()

            # 削除
            result = session.delete()
            assert result is True

            # 削除されたことを確認
            deleted_session = Session.find_by_id("deleteme")
            assert deleted_session is None

    def test_cleanup_expired_sessions(self, temp_db):
        """期限切れセッションのクリーンアップ"""
        with patch("src.auth.models.get_db_path", return_value=temp_db):
            import sqlite3

            conn = sqlite3.connect(temp_db)

            # 有効なセッション
            valid_expires = (datetime.now() + timedelta(hours=1)).isoformat()
            conn.execute(
                """
                INSERT INTO sessions (id, user_id, expires_at)
                VALUES (?, ?, ?)
            """,
                ("valid", 1, valid_expires),
            )

            # 期限切れセッション
            expired_expires = (datetime.now() - timedelta(hours=1)).isoformat()
            conn.execute(
                """
                INSERT INTO sessions (id, user_id, expires_at)
                VALUES (?, ?, ?)
            """,
                ("expired", 1, expired_expires),
            )

            conn.commit()
            conn.close()

            # クリーンアップ実行
            deleted_count = Session.cleanup_expired()
            assert deleted_count == 1

            # 有効なセッションは残っている
            valid = Session.find_by_id("valid")
            assert valid is not None

            # 期限切れは削除されている
            expired = Session.find_by_id("expired")
            assert expired is None


class TestAuthManager:
    """AuthManagerのテスト"""

    @patch("src.auth.auth.User")
    def test_register_user_success(self, mock_user_class):
        """ユーザー登録成功"""
        # モックの設定
        mock_user_class.find_by_username.return_value = None
        mock_user_class.find_by_email.return_value = None
        mock_user_instance = MagicMock()
        mock_user_instance.save.return_value = True
        mock_user_class.return_value = mock_user_instance

        # 登録実行
        success, message = AuthManager.register_user(
            "newuser", "new@example.com", "password123"
        )

        assert success is True
        assert message == "ユーザー登録が完了しました"
        mock_user_instance.save.assert_called_once()

    def test_register_user_validation_errors(self):
        """ユーザー登録の入力検証"""
        # ユーザー名が短い
        success, message = AuthManager.register_user(
            "ab", "test@example.com", "password123"
        )
        assert success is False
        assert "3文字以上" in message

        # メールアドレスが無効
        success, message = AuthManager.register_user(
            "testuser", "invalid-email", "password123"
        )
        assert success is False
        assert "有効なメールアドレス" in message

        # パスワードが短い
        success, message = AuthManager.register_user(
            "testuser", "test@example.com", "short"
        )
        assert success is False
        assert "8文字以上" in message

    @patch("src.auth.auth.User")
    def test_register_user_duplicate(self, mock_user_class):
        """重複ユーザーの登録"""
        # ユーザー名が既に存在
        mock_user_class.find_by_username.return_value = MagicMock()
        mock_user_class.find_by_email.return_value = None

        success, message = AuthManager.register_user(
            "existing", "new@example.com", "password123"
        )
        assert success is False
        assert "既に使用されています" in message

    @patch("src.auth.auth.User")
    @patch("src.auth.auth.Session")
    def test_login_success(self, mock_session_class, mock_user_class):
        """ログイン成功"""
        # モックユーザー
        mock_user = MagicMock()
        mock_user.id = 1
        mock_user.username = "testuser"
        mock_user.password_hash = generate_password_hash("password123")
        mock_user_class.find_by_username.return_value = mock_user

        # モックセッション
        mock_session_instance = MagicMock()
        mock_session_instance.save.return_value = True
        mock_session_class.return_value = mock_session_instance

        # ログイン実行
        user, session_id, error = AuthManager.login("testuser", "password123")

        assert user is not None
        assert session_id is not None
        assert error == ""
        mock_session_instance.save.assert_called_once()

    @patch("src.auth.auth.User")
    def test_login_invalid_credentials(self, mock_user_class):
        """無効な認証情報でのログイン"""
        # ユーザーが見つからない
        mock_user_class.find_by_username.return_value = None
        mock_user_class.find_by_email.return_value = None

        user, session_id, error = AuthManager.login("nonexistent", "password")
        assert user is None
        assert session_id is None
        assert "正しくありません" in error

        # パスワードが間違っている
        mock_user = MagicMock()
        mock_user.password_hash = generate_password_hash("correct_password")
        mock_user_class.find_by_username.return_value = mock_user

        user, session_id, error = AuthManager.login("testuser", "wrong_password")
        assert user is None
        assert session_id is None
        assert "正しくありません" in error

    @patch("src.auth.auth.Session")
    def test_logout_success(self, mock_session_class):
        """ログアウト成功"""
        mock_session = MagicMock()
        mock_session.delete.return_value = True
        mock_session_class.find_by_id.return_value = mock_session

        result = AuthManager.logout("session123")
        assert result is True
        mock_session.delete.assert_called_once()

    @patch("src.auth.auth.Session")
    def test_logout_invalid_session(self, mock_session_class):
        """無効なセッションでのログアウト"""
        mock_session_class.find_by_id.return_value = None

        result = AuthManager.logout("invalid_session")
        assert result is False

    @patch("src.auth.auth.Session")
    @patch("src.auth.auth.User")
    def test_get_user_by_session(self, mock_user_class, mock_session_class):
        """セッションからユーザー取得"""
        # モックセッション
        mock_session = MagicMock()
        mock_session.user_id = 1
        mock_session_class.find_by_id.return_value = mock_session

        # モックユーザー
        mock_user = MagicMock()
        mock_user_class.find_by_id.return_value = mock_user

        # クリーンアップも呼ばれることを確認
        mock_session_class.cleanup_expired.return_value = 0

        user = AuthManager.get_user_by_session("session123")
        assert user is not None
        mock_session_class.cleanup_expired.assert_called_once()
        mock_user_class.find_by_id.assert_called_with(1)

    @patch("src.auth.auth.User")
    def test_change_password_success(self, mock_user_class):
        """パスワード変更成功"""
        # モックユーザー
        mock_user = MagicMock()
        mock_user.username = "testuser"
        mock_user.password_hash = generate_password_hash("old_password")
        mock_user.save.return_value = True
        mock_user_class.find_by_id.return_value = mock_user

        success, message = AuthManager.change_password(
            1, "old_password", "new_password123"
        )

        assert success is True
        assert "変更しました" in message
        mock_user.save.assert_called_once()

        # パスワードハッシュが更新されたことを確認
        assert mock_user.password_hash != generate_password_hash("old_password")

    @patch("src.auth.auth.User")
    def test_change_password_wrong_current(self, mock_user_class):
        """現在のパスワードが間違っている場合"""
        mock_user = MagicMock()
        mock_user.password_hash = generate_password_hash("correct_password")
        mock_user_class.find_by_id.return_value = mock_user

        success, message = AuthManager.change_password(
            1, "wrong_password", "new_password123"
        )

        assert success is False
        assert "現在のパスワードが正しくありません" in message


class TestLoginDecorator:
    """ログイン必須デコレータのテスト"""

    def test_login_required_authenticated(self):
        """認証済みユーザーのアクセス"""
        from flask import Flask, request

        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test"

        with app.test_request_context():
            # セッションとユーザーをモック
            with patch("src.auth.decorators.session", {"session_id": "test123"}):
                with patch(
                    "src.auth.decorators.AuthManager.get_user_by_session"
                ) as mock_get_user:
                    mock_user = MagicMock()
                    mock_get_user.return_value = mock_user

                    @login_required
                    def protected_view():
                        return "success"

                    result = protected_view()
                    assert result == "success"
                    assert request.current_user == mock_user

    def test_login_required_unauthenticated_api(self):
        """未認証ユーザーのAPIアクセス"""
        from flask import Flask

        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test"

        with app.test_request_context("/api/test"):
            with patch("src.auth.decorators.session", {}):
                with patch(
                    "src.auth.decorators.AuthManager.get_user_by_session"
                ) as mock_get_user:
                    mock_get_user.return_value = None

                    @login_required
                    def api_view():
                        return "should not reach here"

                    response, status_code = api_view()
                    assert status_code == 401
                    assert response.json["success"] is False
                    assert response.json["code"] == "UNAUTHORIZED"

    def test_login_required_unauthenticated_web(self):
        """未認証ユーザーのWebアクセス"""
        from flask import Flask

        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test"

        # ダミーのloginエンドポイントを追加
        @app.route("/login")
        def login():
            return "login page"

        with app.test_request_context("/protected"):
            with patch("src.auth.decorators.session", {}):
                with patch(
                    "src.auth.decorators.AuthManager.get_user_by_session"
                ) as mock_get_user:
                    mock_get_user.return_value = None

                    @login_required
                    def web_view():
                        return "should not reach here"

                    with app.app_context():
                        response = web_view()
                        assert response.status_code == 302  # リダイレクト
                        assert "/login" in response.location

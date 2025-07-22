"""src.auth.auth モジュールのテスト"""

from unittest.mock import MagicMock, patch

from src.auth.auth import AuthManager
from src.auth.models import Session, User


class TestAuthManagerRegister:
    """AuthManager.register_user のテスト"""

    @patch("src.auth.models.User.find_by_username")
    @patch("src.auth.models.User.find_by_email")
    @patch("src.auth.models.User.save")
    @patch("src.auth.auth.generate_password_hash")
    def test_register_user_success(
        self, mock_generate_hash, mock_save, mock_find_email, mock_find_username
    ):
        """ユーザー登録成功のテスト"""
        mock_find_username.return_value = None
        mock_find_email.return_value = None
        mock_generate_hash.return_value = "hashed_password"
        mock_save.return_value = True

        success, message = AuthManager.register_user(
            "testuser", "test@example.com", "password123", "admin"
        )

        assert success is True, f"Failed: {message}"
        assert message == "ユーザー登録が完了しました"
        mock_save.assert_called_once()

    def test_register_user_short_username(self):
        """短いユーザー名のテスト"""
        success, message = AuthManager.register_user(
            "ab", "test@example.com", "password123"
        )
        assert success is False
        assert "ユーザー名は3文字以上" in message

    def test_register_user_invalid_email(self):
        """無効なメールアドレスのテスト（メールバリデーションは削除されたため成功するはず）"""
        with (
            patch("src.auth.auth.User.find_by_username") as mock_find_username,
            patch("src.auth.auth.User.find_by_email") as mock_find_email,
            patch("src.auth.auth.User.save") as mock_save,
        ):
            mock_find_username.return_value = None
            mock_find_email.return_value = None
            mock_save.return_value = True

            success, message = AuthManager.register_user(
                "testuser", "invalid-email", "password123"
            )
            assert success is True
            assert message == "ユーザー登録が完了しました"

    def test_register_user_short_password(self):
        """短いパスワードのテスト"""
        success, message = AuthManager.register_user(
            "testuser", "test@example.com", "pass123"
        )
        assert success is False
        assert "パスワードは8文字以上" in message

    @patch("src.auth.auth.User.find_by_username")
    def test_register_user_duplicate_username(self, mock_find_username):
        """重複するユーザー名のテスト"""
        mock_find_username.return_value = MagicMock(spec=User)

        success, message = AuthManager.register_user(
            "testuser", "test@example.com", "password123"
        )
        assert success is False
        assert "このユーザー名は既に使用されています" in message

    @patch("src.auth.auth.User.find_by_username")
    @patch("src.auth.auth.User.find_by_email")
    def test_register_user_duplicate_email(self, mock_find_email, mock_find_username):
        """重複するメールアドレスのテスト"""
        mock_find_username.return_value = None
        mock_find_email.return_value = MagicMock(spec=User)

        success, message = AuthManager.register_user(
            "testuser", "test@example.com", "password123"
        )
        assert success is False
        assert "このメールアドレスは既に登録されています" in message

    @patch("src.auth.models.User.find_by_username")
    @patch("src.auth.models.User.find_by_email")
    @patch("src.auth.models.User.save")
    @patch("src.auth.auth.generate_password_hash")
    def test_register_user_save_failure(
        self, mock_generate_hash, mock_save, mock_find_email, mock_find_username
    ):
        """ユーザー保存失敗のテスト"""
        mock_find_username.return_value = None
        mock_find_email.return_value = None
        mock_generate_hash.return_value = "hashed_password"
        mock_save.return_value = False

        success, message = AuthManager.register_user(
            "testuser", "test@example.com", "password123"
        )

        assert success is False
        assert "ユーザー登録に失敗しました" in message


class TestAuthManagerLogin:
    """AuthManager.login のテスト"""

    @patch("src.auth.auth.User.find_by_username")
    @patch("src.auth.auth.User.find_by_email")
    @patch("src.auth.auth.Session")
    @patch("src.auth.auth.check_password_hash")
    def test_login_success_with_username(
        self,
        mock_check_password,
        mock_session_class,
        mock_find_email,
        mock_find_username,
    ):
        """ユーザー名でのログイン成功テスト"""
        mock_user = MagicMock(spec=User)
        mock_user.id = 1
        mock_user.username = "testuser"
        mock_user.password_hash = "hashed_password"

        mock_find_username.return_value = mock_user
        mock_find_email.return_value = None
        mock_check_password.return_value = True

        mock_session = MagicMock(spec=Session)
        mock_session.save.return_value = True
        mock_session_class.return_value = mock_session

        user, session_id, error = AuthManager.login("testuser", "password123", False)

        assert user == mock_user
        assert session_id is not None
        assert error == ""
        mock_session.save.assert_called_once()

    @patch("src.auth.auth.User.find_by_username")
    @patch("src.auth.auth.User.find_by_email")
    @patch("src.auth.auth.Session")
    @patch("src.auth.auth.check_password_hash")
    def test_login_success_with_email(
        self,
        mock_check_password,
        mock_session_class,
        mock_find_email,
        mock_find_username,
    ):
        """メールアドレスでのログイン成功テスト"""
        mock_user = MagicMock(spec=User)
        mock_user.id = 1
        mock_user.username = "testuser"
        mock_user.password_hash = "hashed_password"

        mock_find_username.return_value = None
        mock_find_email.return_value = mock_user
        mock_check_password.return_value = True

        mock_session = MagicMock(spec=Session)
        mock_session.save.return_value = True
        mock_session_class.return_value = mock_session

        user, session_id, error = AuthManager.login(
            "test@example.com", "password123", True
        )

        assert user == mock_user
        assert session_id is not None
        assert error == ""
        assert mock_session.remember_me is True

    @patch("src.auth.auth.User.find_by_username")
    @patch("src.auth.auth.User.find_by_email")
    def test_login_user_not_found(self, mock_find_email, mock_find_username):
        """存在しないユーザーでのログインテスト"""
        mock_find_username.return_value = None
        mock_find_email.return_value = None

        user, session_id, error = AuthManager.login("nonexistent", "password123")

        assert user is None
        assert session_id is None
        assert "ユーザー名またはパスワードが正しくありません" in error

    @patch("src.auth.auth.User.find_by_username")
    @patch("src.auth.auth.check_password_hash")
    def test_login_wrong_password(self, mock_check_password, mock_find_username):
        """間違ったパスワードでのログインテスト"""
        mock_user = MagicMock(spec=User)
        mock_user.password_hash = "hashed_password"

        mock_find_username.return_value = mock_user
        mock_check_password.return_value = False

        user, session_id, error = AuthManager.login("testuser", "wrongpassword")

        assert user is None
        assert session_id is None
        assert "ユーザー名またはパスワードが正しくありません" in error

    @patch("src.auth.auth.User.find_by_username")
    @patch("src.auth.auth.check_password_hash")
    def test_login_user_without_id(self, mock_check_password, mock_find_username):
        """IDが設定されていないユーザーのログインテスト"""
        mock_user = MagicMock(spec=User)
        mock_user.id = None
        mock_user.password_hash = "hashed_password"

        mock_find_username.return_value = mock_user
        mock_check_password.return_value = True

        user, session_id, error = AuthManager.login("testuser", "password123")

        assert user is None
        assert session_id is None
        assert "システムエラーが発生しました" in error

    @patch("src.auth.auth.User.find_by_username")
    @patch("src.auth.auth.Session")
    @patch("src.auth.auth.check_password_hash")
    def test_login_session_save_failure(
        self, mock_check_password, mock_session_class, mock_find_username
    ):
        """セッション保存失敗のテスト"""
        mock_user = MagicMock(spec=User)
        mock_user.id = 1
        mock_user.password_hash = "hashed_password"

        mock_find_username.return_value = mock_user
        mock_check_password.return_value = True

        mock_session = MagicMock(spec=Session)
        mock_session.save.return_value = False
        mock_session_class.return_value = mock_session

        user, session_id, error = AuthManager.login("testuser", "password123")

        assert user is None
        assert session_id is None
        assert "セッション作成に失敗しました" in error


class TestAuthManagerLogout:
    """AuthManager.logout のテスト"""

    @patch("src.auth.auth.Session.find_by_id")
    def test_logout_success(self, mock_find_by_id):
        """ログアウト成功のテスト"""
        mock_session = MagicMock(spec=Session)
        mock_session.delete.return_value = True
        mock_find_by_id.return_value = mock_session

        result = AuthManager.logout("test_session_id")

        assert result is True
        mock_session.delete.assert_called_once()

    @patch("src.auth.auth.Session.find_by_id")
    def test_logout_session_not_found(self, mock_find_by_id):
        """セッションが見つからない場合のテスト"""
        mock_find_by_id.return_value = None

        result = AuthManager.logout("nonexistent_session_id")

        assert result is False

    @patch("src.auth.auth.Session.find_by_id")
    def test_logout_delete_failure(self, mock_find_by_id):
        """セッション削除失敗のテスト"""
        mock_session = MagicMock(spec=Session)
        mock_session.delete.return_value = False
        mock_find_by_id.return_value = mock_session

        result = AuthManager.logout("test_session_id")

        assert result is False


class TestAuthManagerGetUserBySession:
    """AuthManager.get_user_by_session のテスト"""

    @patch("src.auth.auth.Session.cleanup_expired")
    @patch("src.auth.auth.Session.find_by_id")
    @patch("src.auth.auth.User.find_by_id")
    def test_get_user_by_session_success(
        self, mock_user_find, mock_session_find, mock_cleanup
    ):
        """セッションからユーザー取得成功のテスト"""
        mock_session = MagicMock(spec=Session)
        mock_session.user_id = 1
        mock_session_find.return_value = mock_session

        mock_user = MagicMock(spec=User)
        mock_user_find.return_value = mock_user

        user = AuthManager.get_user_by_session("test_session_id")

        assert user == mock_user
        mock_cleanup.assert_called_once()
        mock_user_find.assert_called_once_with(1)

    def test_get_user_by_session_empty_id(self):
        """空のセッションIDのテスト"""
        user = AuthManager.get_user_by_session("")
        assert user is None

    @patch("src.auth.auth.Session.cleanup_expired")
    @patch("src.auth.auth.Session.find_by_id")
    def test_get_user_by_session_not_found(self, mock_session_find, mock_cleanup):
        """セッションが見つからない場合のテスト"""
        mock_session_find.return_value = None

        user = AuthManager.get_user_by_session("nonexistent_session_id")

        assert user is None
        mock_cleanup.assert_called_once()


class TestAuthManagerChangePassword:
    """AuthManager.change_password のテスト"""

    @patch("src.auth.auth.User.find_by_id")
    @patch("src.auth.auth.check_password_hash")
    def test_change_password_success(self, mock_check_password, mock_find_user):
        """パスワード変更成功のテスト"""
        mock_user = MagicMock(spec=User)
        mock_user.username = "testuser"
        mock_user.password_hash = "old_hash"
        mock_user.save.return_value = True
        mock_find_user.return_value = mock_user
        mock_check_password.return_value = True

        success, message = AuthManager.change_password(
            1, "old_password", "new_password123"
        )

        assert success is True
        assert "パスワードを変更しました" in message
        assert mock_user.password_hash != "old_hash"
        mock_user.save.assert_called_once()

    @patch("src.auth.auth.User.find_by_id")
    def test_change_password_user_not_found(self, mock_find_user):
        """ユーザーが見つからない場合のテスト"""
        mock_find_user.return_value = None

        success, message = AuthManager.change_password(
            1, "old_password", "new_password123"
        )

        assert success is False
        assert "ユーザーが見つかりません" in message

    @patch("src.auth.auth.User.find_by_id")
    @patch("src.auth.auth.check_password_hash")
    def test_change_password_wrong_current(self, mock_check_password, mock_find_user):
        """現在のパスワードが間違っている場合のテスト"""
        mock_user = MagicMock(spec=User)
        mock_user.password_hash = "old_hash"
        mock_find_user.return_value = mock_user
        mock_check_password.return_value = False

        success, message = AuthManager.change_password(
            1, "wrong_password", "new_password123"
        )

        assert success is False
        assert "現在のパスワードが正しくありません" in message

    @patch("src.auth.auth.User.find_by_id")
    @patch("src.auth.auth.check_password_hash")
    def test_change_password_short_new_password(
        self, mock_check_password, mock_find_user
    ):
        """新しいパスワードが短い場合のテスト"""
        mock_user = MagicMock(spec=User)
        mock_user.password_hash = "old_hash"
        mock_find_user.return_value = mock_user
        mock_check_password.return_value = True

        success, message = AuthManager.change_password(1, "old_password", "short")

        assert success is False
        assert "新しいパスワードは8文字以上" in message

    @patch("src.auth.auth.User.find_by_id")
    @patch("src.auth.auth.check_password_hash")
    def test_change_password_save_failure(self, mock_check_password, mock_find_user):
        """パスワード保存失敗のテスト"""
        mock_user = MagicMock(spec=User)
        mock_user.username = "testuser"
        mock_user.password_hash = "old_hash"
        mock_user.save.return_value = False
        mock_find_user.return_value = mock_user
        mock_check_password.return_value = True

        success, message = AuthManager.change_password(
            1, "old_password", "new_password123"
        )

        assert success is False
        assert "パスワード変更に失敗しました" in message

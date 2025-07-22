"""auth.modelsのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.auth.models import Session, User


class TestUser:
    """Userモデルのテスト"""

    def test_user_initialization(self):
        """ユーザー初期化のテスト"""
        user = User(
            id=1,
            username="testuser",
            email="test@example.com",
            password_hash="hashed_password",
            role="admin",
        )

        assert user.id == 1
        assert user.username == "testuser"
        assert user.email == "test@example.com"
        assert user.password_hash == "hashed_password"
        assert user.role == "admin"
        assert user.created_at is None
        assert user.updated_at is None

    def test_user_default_values(self):
        """デフォルト値のテスト"""
        user = User()
        assert user.id is None
        assert user.username == ""
        assert user.email == ""
        assert user.password_hash == ""
        assert user.role == "admin"

    @patch("src.auth.models.sqlite3.connect")
    def test_find_by_username_found(self, mock_connect):
        """ユーザー名での検索（見つかった場合）のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            1,
            "testuser",
            "test@example.com",
            "hashed_password",
            "2024-01-01 00:00:00",
            "2024-01-02 00:00:00",
            "admin",
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        user = User.find_by_username("testuser")

        # 検証
        assert user is not None
        assert user.id == 1
        assert user.username == "testuser"
        assert user.email == "test@example.com"
        assert user.role == "admin"
        assert user.created_at == "2024-01-01 00:00:00"
        assert user.updated_at == "2024-01-02 00:00:00"

    @patch("src.auth.models.sqlite3.connect")
    def test_find_by_username_not_found(self, mock_connect):
        """ユーザー名での検索（見つからなかった場合）のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        user = User.find_by_username("nonexistent")

        # 検証
        assert user is None

    @patch("src.auth.models.sqlite3.connect")
    def test_find_by_email_found(self, mock_connect):
        """メールアドレスでの検索（見つかった場合）のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            2,
            "emailuser",
            "email@example.com",
            "hashed_password",
            "2024-01-01 00:00:00",
            "2024-01-02 00:00:00",
            "user",
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        user = User.find_by_email("email@example.com")

        # 検証
        assert user is not None
        assert user.id == 2
        assert user.username == "emailuser"
        assert user.email == "email@example.com"
        assert user.role == "user"

    @patch("src.auth.models.sqlite3.connect")
    def test_find_by_email_not_found(self, mock_connect):
        """メールアドレスでの検索（見つからなかった場合）のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        user = User.find_by_email("notfound@example.com")

        # 検証
        assert user is None

    @patch("src.auth.models.sqlite3.connect")
    def test_save_new_user(self, mock_connect):
        """新規ユーザーの保存テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 10
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        user = User(username="newuser", email="new@example.com", password_hash="hash")
        result = user.save()

        # 検証
        assert result is True
        assert user.id == 10
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch("src.auth.models.sqlite3.connect")
    def test_save_existing_user(self, mock_connect):
        """既存ユーザーの更新テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        user = User(
            id=5,
            username="existinguser",
            email="existing@example.com",
            password_hash="hash",
        )
        result = user.save()

        # 検証
        assert result is True
        # UPDATEクエリが実行されることを確認
        update_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "UPDATE users" in call[0][0]
        ]
        assert len(update_calls) == 1
        mock_conn.commit.assert_called_once()

    @patch("src.auth.models.sqlite3.connect")
    def test_save_database_error(self, mock_connect):
        """保存時のデータベースエラーテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = sqlite3.Error("Database error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行 - IntegrityError以外のエラーは処理されないので例外が発生する
        user = User(username="erroruser")
        with pytest.raises(sqlite3.Error):
            user.save()

    @patch("src.auth.models.sqlite3.connect")
    def test_save_integrity_error(self, mock_connect):
        """保存時の整合性エラーテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = sqlite3.IntegrityError(
            "UNIQUE constraint failed"
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        user = User(username="duplicate")
        result = user.save()

        # 検証
        assert result is False
        # IntegrityErrorの場合、rollbackは呼ばれない
        mock_conn.rollback.assert_not_called()


class TestSession:
    """Sessionモデルのテスト"""

    def test_session_initialization(self):
        """セッション初期化のテスト"""
        session = Session(
            session_id="test-session-id", user_id=10, expires_at="2024-12-31 23:59:59"
        )

        assert session.id == "test-session-id"
        assert session.user_id == 10
        assert session.expires_at == "2024-12-31 23:59:59"
        assert session.created_at is None
        assert session.remember_me is False

    @patch("src.auth.models.sqlite3.connect")
    def test_find_by_id_found(self, mock_connect):
        """セッションIDでの検索（見つかった場合）のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            "session123",
            10,
            "2024-12-31 23:59:59",
            "2024-01-01 00:00:00",
            1,  # remember_me
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        session = Session.find_by_id("session123")

        # 検証
        assert session is not None
        assert session.id == "session123"
        assert session.user_id == 10
        assert session.expires_at == "2024-12-31 23:59:59"
        assert session.created_at == "2024-01-01 00:00:00"
        assert session.remember_me is True

    @patch("src.auth.models.sqlite3.connect")
    def test_find_by_id_not_found(self, mock_connect):
        """セッションIDでの検索（見つからなかった場合）のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        session = Session.find_by_id("nonexistent")

        # 検証
        assert session is None

    @patch("src.auth.models.sqlite3.connect")
    def test_save_new_session_with_remember_me(self, mock_connect):
        """Remember Me付き新規セッションの保存テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        # テーブル構造を返す
        mock_cursor.fetchone.return_value = (
            "CREATE TABLE sessions (id TEXT, user_id INTEGER, expires_at TEXT, remember_me INTEGER)",
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        session = Session("new-session-id", 10, "2024-12-31 23:59:59")
        session.remember_me = True
        result = session.save()

        # 検証
        assert result is True
        # remember_meカラムありのINSERTが実行されることを確認
        insert_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "INSERT INTO sessions" in str(call)
        ]
        assert len(insert_calls) == 1
        assert "remember_me" in str(insert_calls[0])
        mock_conn.commit.assert_called_once()

    @patch("src.auth.models.sqlite3.connect")
    def test_delete_session_success(self, mock_connect):
        """セッション削除の成功テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        session = Session("delete-session-id", 10, "2024-12-31")
        result = session.delete()

        # 検証
        assert result is True
        mock_cursor.execute.assert_called_with(
            "DELETE FROM sessions WHERE id = ?", ("delete-session-id",)
        )
        mock_conn.commit.assert_called_once()

    @patch("src.auth.models.sqlite3.connect")
    def test_save_without_remember_me_column(self, mock_connect):
        """remember_meカラムがない場合の保存テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        # remember_meカラムがないテーブル構造
        mock_cursor.fetchone.return_value = (
            "CREATE TABLE sessions (id TEXT, user_id INTEGER, expires_at TEXT)",
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        session = Session("no-remember-session", 10, "2024-12-31 23:59:59")
        result = session.save()

        # 検証
        assert result is True
        # remember_meカラムなしのINSERTが実行されることを確認
        insert_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "INSERT INTO sessions" in str(call) and "remember_me" not in str(call)
        ]
        assert len(insert_calls) == 1
        mock_conn.commit.assert_called_once()

    @patch("src.auth.models.sqlite3.connect")
    def test_cleanup_expired_sessions(self, mock_connect):
        """期限切れセッションのクリーンアップテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5  # 5件削除
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        deleted_count = Session.cleanup_expired()

        # 検証
        assert deleted_count == 5
        # 現在時刻より前の期限切れセッションを削除
        delete_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "DELETE FROM sessions WHERE expires_at" in str(call)
        ]
        assert len(delete_calls) == 1
        mock_conn.commit.assert_called_once()

    @patch("src.auth.models.sqlite3.connect")
    def test_find_by_id_success(self, mock_connect):
        """IDでのユーザー検索（成功）のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            3,
            "iduser",
            "id@example.com",
            "hashed_password",
            "2024-01-01 00:00:00",
            "2024-01-02 00:00:00",
            "user",
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        user = User.find_by_id(3)

        # 検証
        assert user is not None
        assert user.id == 3
        assert user.username == "iduser"
        assert user.email == "id@example.com"
        assert user.role == "user"

    @patch("src.auth.models.sqlite3.connect")
    def test_save_database_integrity_error(self, mock_connect):
        """保存時の整合性エラーテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = sqlite3.IntegrityError(
            "UNIQUE constraint failed"
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        user = User(username="duplicate")
        result = user.save()

        # 検証
        assert result is False
        # IntegrityErrorの場合、rollbackは呼ばれない
        mock_conn.rollback.assert_not_called()

    @patch("src.auth.models.sqlite3.connect")
    def test_session_save_database_error(self, mock_connect):
        """セッション保存時のデータベースエラーテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        # テーブル構造取得は成功
        mock_cursor.fetchone.return_value = (
            "CREATE TABLE sessions (id TEXT, user_id INTEGER, expires_at TEXT)",
        )
        # INSERT時にエラー
        mock_cursor.execute.side_effect = [None, sqlite3.Error("Database error")]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        session = Session("error-session", 10, "2024-12-31")
        result = session.save()

        # 検証
        assert result is False

    @patch("src.auth.models.sqlite3.connect")
    def test_session_delete_database_error(self, mock_connect):
        """セッション削除時のデータベースエラーテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = sqlite3.Error("Database error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        session = Session("error-delete", 10, "2024-12-31")
        result = session.delete()

        # 検証
        assert result is False

    @patch("src.auth.models.sqlite3.connect")
    def test_cleanup_expired_database_error(self, mock_connect):
        """期限切れセッションクリーンアップ時のエラーテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = sqlite3.Error("Database error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        deleted_count = Session.cleanup_expired()

        # 検証
        assert deleted_count == 0

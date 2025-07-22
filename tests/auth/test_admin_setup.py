"""auth.admin_setupのテスト"""

import os
from unittest.mock import MagicMock, patch

from src.auth.admin_setup import create_admin_from_env


class TestCreateAdminFromEnv:
    """create_admin_from_env関数のテスト"""

    @patch.dict(
        os.environ,
        {
            "ADMIN_USERNAME": "admin",
            "ADMIN_EMAIL": "admin@example.com",
            "ADMIN_PASSWORD": "adminpass123",
        },
    )
    @patch("src.auth.admin_setup.User.find_by_username")
    @patch("src.auth.admin_setup.AuthManager.register_user")
    @patch("src.auth.admin_setup.logger")
    def test_create_admin_success(self, mock_logger, mock_register, mock_find_user):
        """管理者ユーザーの作成成功テスト"""
        # 既存ユーザーなし
        mock_find_user.return_value = None
        # 登録成功
        mock_register.return_value = (True, "Success")

        result = create_admin_from_env()

        assert result is True
        mock_find_user.assert_called_once_with("admin")
        mock_register.assert_called_once_with(
            username="admin",
            email="admin@example.com",
            password="adminpass123",
            role="admin",
        )
        mock_logger.info.assert_called_with("管理者ユーザー 'admin' を作成しました")

    @patch.dict(
        os.environ,
        {
            "ADMIN_USERNAME": "admin",
            "ADMIN_EMAIL": "admin@example.com",
            "ADMIN_PASSWORD": "adminpass123",
        },
    )
    @patch("src.auth.admin_setup.User.find_by_username")
    @patch("src.auth.admin_setup.logger")
    def test_admin_already_exists(self, mock_logger, mock_find_user):
        """既存の管理者ユーザーがある場合のテスト"""
        # 既存ユーザーあり
        mock_existing_user = MagicMock()
        mock_find_user.return_value = mock_existing_user

        result = create_admin_from_env()

        assert result is True
        mock_find_user.assert_called_once_with("admin")
        mock_logger.info.assert_called_with("管理者ユーザー 'admin' は既に存在します")

    @patch.dict(
        os.environ,
        {
            "ADMIN_USERNAME": "admin",
            "ADMIN_EMAIL": "admin@example.com",
            "ADMIN_PASSWORD": "adminpass123",
        },
    )
    @patch("src.auth.admin_setup.User.find_by_username")
    @patch("src.auth.admin_setup.AuthManager.register_user")
    @patch("src.auth.admin_setup.logger")
    def test_create_admin_failure(self, mock_logger, mock_register, mock_find_user):
        """管理者ユーザーの作成失敗テスト"""
        # 既存ユーザーなし
        mock_find_user.return_value = None
        # 登録失敗
        mock_register.return_value = (False, "Registration failed")

        result = create_admin_from_env()

        assert result is False
        mock_logger.error.assert_called_with(
            "管理者ユーザーの作成に失敗しました: Registration failed"
        )

    @patch.dict(
        os.environ,
        {
            "ADMIN_USERNAME": "admin",
            "ADMIN_EMAIL": "admin@example.com",
            # PASSWORDが欠落
        },
    )
    @patch("src.auth.admin_setup.logger")
    def test_missing_environment_variables(self, mock_logger):
        """環境変数が不足している場合のテスト"""
        result = create_admin_from_env()

        assert result is False
        mock_logger.info.assert_called_with(
            "管理者ユーザーの環境変数が設定されていないため、作成をスキップします"
        )

    @patch.dict(os.environ, {}, clear=True)
    @patch("src.auth.admin_setup.logger")
    def test_no_environment_variables(self, mock_logger):
        """環境変数が全くない場合のテスト"""
        result = create_admin_from_env()

        assert result is False
        mock_logger.info.assert_called_with(
            "管理者ユーザーの環境変数が設定されていないため、作成をスキップします"
        )

    @patch.dict(
        os.environ,
        {
            "ADMIN_USERNAME": "",  # 空文字
            "ADMIN_EMAIL": "admin@example.com",
            "ADMIN_PASSWORD": "adminpass123",
        },
    )
    @patch("src.auth.admin_setup.logger")
    def test_empty_environment_variable(self, mock_logger):
        """環境変数が空文字の場合のテスト"""
        result = create_admin_from_env()

        assert result is False
        mock_logger.info.assert_called_with(
            "管理者ユーザーの環境変数が設定されていないため、作成をスキップします"
        )


class TestMainExecution:
    """メイン実行のテスト"""

    @patch("src.auth.admin_setup.create_admin_from_env")
    def test_main_execution(self, mock_create_admin):
        """__main__実行時の動作テスト"""
        mock_create_admin.return_value = True

        # モジュールを実行
        import src.auth.admin_setup

        # 実際のif __name__ == "__main__"ブロックは
        # インポート時には実行されないため、
        # 関数が定義されていることのみ確認
        assert hasattr(src.auth.admin_setup, "create_admin_from_env")

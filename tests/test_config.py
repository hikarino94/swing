"""Tests for src/config.py"""

import os
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from src.config import (
    API_BASE_URL,
    DB_PATH,
    LOG_DIR,
    MODEL_DIR,
    OUTPUT_BASE_DIR,
    Config,
    get_account_credentials,
    get_db_path,
    get_idtoken,
)


class TestAccountCredentials:
    """アカウント認証情報関連のテスト"""

    @patch("src.config.config.get_file_path")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"mailaddress": "test@example.com", "password": "test_password"}',
    )
    def test_get_account_credentials_success(self, mock_file, mock_get_path):
        """正常に認証情報を取得できることを確認"""
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.open.return_value = mock_file.return_value
        mock_get_path.return_value = mock_path

        credentials = get_account_credentials()
        assert credentials["email"] == "test@example.com"
        assert credentials["password"] == "test_password"

    @patch("src.config.config.get_file_path")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"mail": "test@example.com", "password": "test_password"}',
    )
    def test_get_account_credentials_mail_key(self, mock_file, mock_get_path):
        """mailキーでも認証情報を取得できることを確認"""
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.open.return_value = mock_file.return_value
        mock_get_path.return_value = mock_path

        credentials = get_account_credentials()
        assert credentials["email"] == "test@example.com"

    @patch("src.config.config.get_file_path")
    def test_get_account_credentials_file_not_found(self, mock_get_path):
        """ファイルが存在しない場合の処理を確認"""
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_get_path.return_value = mock_path

        with pytest.raises(FileNotFoundError):
            get_account_credentials()


class TestConfig:
    """Configクラスのテスト"""

    @patch("src.config.Path.exists")
    @patch(
        "builtins.open", new_callable=mock_open, read_data='{"test_key": "test_value"}'
    )
    def test_config_load_from_file(self, mock_file, mock_exists):
        """設定ファイルから読み込めることを確認"""
        mock_exists.return_value = True
        config = Config()
        assert "test_key" in config._config

    @patch("src.config.Path.exists")
    def test_config_default_values(self, mock_exists):
        """ファイルが存在しない場合はデフォルト値を使用することを確認"""
        mock_exists.return_value = False
        config = Config()
        assert "database" in config._config
        assert "api" in config._config

    def test_config_directories(self):
        """ディレクトリプロパティが正しく設定されることを確認"""
        config = Config()
        assert config.base_dir.name == "swing"
        assert config.config_path.parent.name == "config"
        assert config.output_base_dir.name == "output"
        assert config.log_dir.name == "logs"
        assert config.model_dir.name == "models"

    def test_get_file_path(self):
        """get_file_pathメソッドが正しく動作することを確認"""
        config = Config()
        # get_file_pathは存在するか確認
        if hasattr(config, "get_file_path"):
            account_path = config.get_file_path("account")
            assert account_path.name == "account.json"
            assert account_path.parent.name == "config"
        else:
            # file_pathsから直接取得
            account_path = config.file_paths.get("account")
            assert account_path is not None

    def test_api_configuration(self):
        """API設定が正しく取得できることを確認"""
        config = Config()
        assert hasattr(config, "api_base_url")
        assert config.api_base_url.startswith("https://")
        assert hasattr(config, "api_rate_limit_sleep")
        assert config.api_rate_limit_sleep > 0


class TestGetDbPath:
    """データベースパス取得関数のテスト"""

    def test_get_db_path_default(self):
        """デフォルトのDBパスを取得できることを確認"""
        db_path = get_db_path()
        # get_db_pathは文字列を返すので、Pathに変換
        db_path_obj = Path(db_path)
        # テスト環境ではtest_stock.dbになることがある
        assert db_path_obj.name in ["stock.db", "test_stock.db"]
        assert "db" in str(db_path)

    @patch.dict(os.environ, {"DATABASE_PATH": "/custom/path/test.db"})
    def test_get_db_path_from_env(self):
        """環境変数からDBパスを取得できることを確認"""
        db_path = get_db_path()
        # 環境変数が優先される
        assert str(db_path) == "/custom/path/test.db"


class TestIdToken:
    """IDトークン関連のテスト"""

    @patch("src.config.config.get_file_path")
    @patch(
        "builtins.open", new_callable=mock_open, read_data='{"idToken": "test_token"}'
    )
    def test_get_idtoken_success(self, mock_file, mock_get_path):
        """正常にIDトークンを取得できることを確認"""
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.open.return_value = mock_file.return_value
        mock_get_path.return_value = mock_path

        token = get_idtoken()
        assert token == "test_token"

    @patch("src.config.config.get_file_path")
    def test_get_idtoken_file_not_found(self, mock_get_path):
        """ファイルが存在しない場合はFileNotFoundErrorを発生させることを確認"""
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_get_path.return_value = mock_path

        with pytest.raises(FileNotFoundError):
            get_idtoken()

    @patch("src.config.config.get_file_path")
    @patch("builtins.open", new_callable=mock_open, read_data="{}")
    def test_get_idtoken_no_token_in_file(self, mock_file, mock_get_path):
        """ファイルにidTokenがない場合はRuntimeErrorを発生させることを確認"""
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path.open.return_value = mock_file.return_value
        mock_get_path.return_value = mock_path

        with pytest.raises(RuntimeError, match="idToken not found"):
            get_idtoken()


class TestModuleLevelConstants:
    """モジュールレベルの定数のテスト"""

    def test_db_path(self):
        """DB_PATHが正しく設定されることを確認"""
        # DB_PATHは文字列として定義されている
        assert isinstance(DB_PATH, str)
        db_path_obj = Path(DB_PATH)
        # テスト環境ではtest_stock.dbになることがある
        assert db_path_obj.name in ["stock.db", "test_stock.db"]
        assert "db" in DB_PATH

    def test_api_base_url(self):
        """API_BASE_URLが設定されていることを確認"""
        assert isinstance(API_BASE_URL, str)
        assert API_BASE_URL.startswith("https://")

    def test_output_base_dir(self):
        """OUTPUT_BASE_DIRが正しく設定されることを確認"""
        assert isinstance(OUTPUT_BASE_DIR, Path)
        assert OUTPUT_BASE_DIR.name == "output"
        assert OUTPUT_BASE_DIR.parent.name == "data"

    def test_log_dir(self):
        """LOG_DIRが正しく設定されることを確認"""
        assert isinstance(LOG_DIR, Path)
        assert LOG_DIR.name == "logs"

    def test_model_dir(self):
        """MODEL_DIRが正しく設定されることを確認"""
        assert isinstance(MODEL_DIR, Path)
        assert MODEL_DIR.name == "models"
        assert MODEL_DIR.parent.name == "db"


class TestApiEndpoints:
    """APIエンドポイント設定のテスト"""

    def test_get_api_endpoint(self):
        """APIエンドポイントが正しく取得できることを確認"""
        config = Config()
        auth_endpoint = config.get_api_endpoint("auth")
        assert auth_endpoint == "https://api.jquants.com/v1/token/auth_user"

        quotes_endpoint = config.get_api_endpoint("daily_quotes")
        assert quotes_endpoint == "https://api.jquants.com/v1/prices/daily_quotes"

    def test_get_api_endpoint_invalid(self):
        """無効なエンドポイント名でValueErrorが発生することを確認"""
        config = Config()
        with pytest.raises(ValueError, match="Unknown endpoint"):
            config.get_api_endpoint("invalid_endpoint")

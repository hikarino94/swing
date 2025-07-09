"""Test suite for src/config.py module."""

import json
import os
import sys
from pathlib import Path
from unittest import mock

import pytest

# プロジェクトルートをPYTHONPATHに追加
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.config import API_BASE_URL, DB_PATH, Config, get_db_path


class TestConfig:
    """Test Config class."""

    @pytest.fixture
    def temp_config_file(self, tmp_path):
        """一時的な設定ファイルを作成"""
        config_data = {
            "database": {"path": "/custom/path/to/db.sqlite"},
            "api": {
                "base_url": "https://custom.api.com",
                "endpoints": {
                    "auth": "/custom/auth",
                    "refresh": "/custom/refresh",
                    "daily_quotes": "/custom/quotes",
                },
                "rate_limit": {"sleep_seconds": 1.0},
            },
            "scheduler": {
                "tasks": {"custom_task": {"time": "12:00", "frequency": "hourly"}}
            },
            "files": {
                "account": "custom_account.json",
                "idtoken": "custom_idtoken.json",
                "custom_file": "path/to/custom.json",
            },
            "logging": {
                "level": "DEBUG",
                "format": "%(message)s",
            },
            "custom_section": {"nested": {"value": "test_value"}},
        }

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config_data))
        return config_file

    def test_default_config(self, tmp_path):
        """デフォルト設定のテスト"""
        # 存在しない設定ファイルパスを指定
        config = Config(config_path=tmp_path / "nonexistent.json")

        # デフォルト値の確認
        assert config.api_base_url == "https://api.jquants.com/v1"
        assert config.api_rate_limit_sleep == 0.35
        assert config.log_level == "INFO"
        assert (
            config.log_format == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # エンドポイントの確認
        assert (
            config.get_api_endpoint("auth")
            == "https://api.jquants.com/v1/token/auth_user"
        )
        assert (
            config.get_api_endpoint("daily_quotes")
            == "https://api.jquants.com/v1/prices/daily_quotes"
        )

    def test_custom_config(self, temp_config_file):
        """カスタム設定ファイルの読み込みテスト"""
        config = Config(config_path=temp_config_file)

        # カスタム値の確認
        assert config.db_path == "/custom/path/to/db.sqlite"
        assert config.api_base_url == "https://custom.api.com"
        assert config.api_rate_limit_sleep == 1.0
        assert config.log_level == "DEBUG"
        assert config.log_format == "%(message)s"

        # カスタムエンドポイント
        assert config.get_api_endpoint("auth") == "https://custom.api.com/custom/auth"

    def test_get_api_endpoint_invalid(self, tmp_path):
        """無効なエンドポイント名でのエラー"""
        config = Config(config_path=tmp_path / "nonexistent.json")

        with pytest.raises(ValueError, match="Unknown endpoint: invalid_endpoint"):
            config.get_api_endpoint("invalid_endpoint")

    def test_get_scheduler_config(self, temp_config_file):
        """スケジューラ設定の取得テスト"""
        config = Config(config_path=temp_config_file)

        # カスタムタスクの確認
        task_config = config.get_scheduler_config("custom_task")
        assert task_config == {"time": "12:00", "frequency": "hourly"}

        # 存在しないタスクは空の辞書を返す
        assert config.get_scheduler_config("nonexistent_task") == {}

    def test_get_file_path(self, temp_config_file):
        """ファイルパスの取得テスト"""
        config = Config(config_path=temp_config_file)

        # カスタムファイルパス
        account_path = config.get_file_path("account")
        assert account_path.name == "custom_account.json"
        assert account_path.parent == config.base_dir

        custom_file_path = config.get_file_path("custom_file")
        assert str(custom_file_path).endswith("path/to/custom.json")

    def test_get_file_path_invalid(self, tmp_path):
        """無効なファイルタイプでのエラー"""
        config = Config(config_path=tmp_path / "nonexistent.json")

        with pytest.raises(ValueError, match="Unknown file type: invalid_type"):
            config.get_file_path("invalid_type")

    def test_get_method(self, temp_config_file):
        """getメソッドのテスト（ネストしたキーのアクセス）"""
        config = Config(config_path=temp_config_file)

        # 単一レベル
        assert config.get("database") == {"path": "/custom/path/to/db.sqlite"}

        # ネストしたレベル
        assert config.get("database.path") == "/custom/path/to/db.sqlite"
        assert config.get("api.rate_limit.sleep_seconds") == 1.0
        assert config.get("custom_section.nested.value") == "test_value"

        # 存在しないキー（デフォルト値なし）
        assert config.get("nonexistent.key") is None

        # 存在しないキー（デフォルト値あり）
        assert config.get("nonexistent.key", "default") == "default"

    def test_property_paths(self, tmp_path):
        """各種パスプロパティのテスト"""
        config = Config(config_path=tmp_path / "nonexistent.json")

        # 出力ディレクトリ
        assert config.output_base_dir == config.base_dir / "data" / "output"
        assert config.log_dir == config.base_dir / "data" / "logs"
        assert config.model_dir == config.base_dir / "db" / "models"

    def test_base_dir_calculation(self):
        """base_dirの計算が正しいことを確認"""
        config = Config()

        # config.pyから2階層上がプロジェクトルート
        expected_base = Path(__file__).resolve().parent.parent
        assert config.base_dir == expected_base

    def test_config_file_encoding(self, tmp_path):
        """UTF-8エンコーディングでの設定ファイル読み込み"""
        config_data = {
            "database": {"path": "/パス/データベース.db"},
            "api": {"base_url": "https://api.例.com"},
            "logging": {"level": "情報", "format": "ログフォーマット"},
        }

        config_file = tmp_path / "unicode_config.json"
        config_file.write_text(
            json.dumps(config_data, ensure_ascii=False), encoding="utf-8"
        )

        config = Config(config_path=config_file)
        assert config.db_path == "/パス/データベース.db"
        assert config.api_base_url == "https://api.例.com"
        assert config.log_level == "情報"


class TestGlobalFunctions:
    """Test module-level functions and variables."""

    def test_get_db_path_default(self):
        """環境変数なしでのget_db_path"""
        # DATABASE_PATH環境変数を削除
        with mock.patch.dict(os.environ, {}, clear=True):
            # configインスタンスのdb_pathプロパティをモック
            with mock.patch.object(
                type(Config()), "db_path", new_callable=mock.PropertyMock
            ) as mock_db_path:
                mock_db_path.return_value = "/default/db/path"
                # src.configモジュールのconfigインスタンスを置き換え
                with mock.patch("src.config.config", Config()):
                    assert get_db_path() == "/default/db/path"

    def test_get_db_path_with_env(self):
        """環境変数DATABASE_PATHが設定されている場合"""
        with mock.patch.dict(os.environ, {"DATABASE_PATH": "/env/db/path"}):
            assert get_db_path() == "/env/db/path"

    def test_module_constants(self):
        """モジュールレベルの定数が正しく設定されていることを確認"""
        # これらの値はモジュールインポート時に設定される
        assert isinstance(DB_PATH, str)
        assert isinstance(API_BASE_URL, str)
        assert API_BASE_URL.startswith("http")

    def test_global_config_instance(self):
        """グローバルconfigインスタンスの確認"""
        # src.configモジュールのグローバル変数を確認
        from src.config import config

        # configがConfigクラスのインスタンスであることを確認
        assert isinstance(config, Config)

        # グローバル変数が正しく設定されていることを確認
        assert hasattr(config, "base_dir")
        assert hasattr(config, "db_path")
        assert hasattr(config, "api_base_url")


class TestIntegration:
    """Integration tests for config module."""

    def test_real_world_config_scenario(self, tmp_path):
        """実際の使用シナリオのテスト"""
        # プロダクション風の設定ファイル
        config_data = {
            "database": {"path": str(tmp_path / "production.db")},
            "api": {
                "base_url": "https://api.production.com/v2",
                "endpoints": {
                    "auth": "/auth/token",
                    "refresh": "/auth/refresh",
                    "daily_quotes": "/market/quotes",
                    "listed_info": "/market/listings",
                    "statements": "/financial/statements",
                },
                "rate_limit": {"sleep_seconds": 0.5},
            },
            "scheduler": {
                "tasks": {
                    "fetch_quotes": {"time": "19:00", "frequency": "daily"},
                    "fetch_statements": {"time": "19:30", "frequency": "daily"},
                    "update_listed_info": {"time": "05:00", "frequency": "weekly"},
                }
            },
            "files": {
                "account": "config/prod_account.json",
                "idtoken": "config/prod_token.json",
                "thresholds": "config/prod_thresholds.json",
            },
            "logging": {
                "level": "WARNING",
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            },
        }

        config_file = tmp_path / "production_config.json"
        config_file.write_text(json.dumps(config_data))

        config = Config(config_path=config_file)

        # APIエンドポイントの完全なURL生成
        endpoints = ["auth", "refresh", "daily_quotes", "listed_info", "statements"]
        for endpoint in endpoints:
            url = config.get_api_endpoint(endpoint)
            assert url.startswith("https://api.production.com/v2/")
            assert endpoint in config_data["api"]["endpoints"]

        # スケジューラタスクの確認
        tasks = ["fetch_quotes", "fetch_statements", "update_listed_info"]
        for task in tasks:
            task_config = config.get_scheduler_config(task)
            assert "time" in task_config
            assert "frequency" in task_config

        # ファイルパスの解決
        file_types = ["account", "idtoken", "thresholds"]
        for file_type in file_types:
            path = config.get_file_path(file_type)
            assert isinstance(path, Path)
            assert str(path).endswith(config_data["files"][file_type])

    def test_environment_override(self, tmp_path):
        """環境変数によるデータベースパスのオーバーライド"""
        config_data = {
            "database": {"path": str(tmp_path / "config_db.db")},
            "api": {
                "base_url": "https://api.test.com",
                "endpoints": {},
                "rate_limit": {"sleep_seconds": 1},
            },
            "files": {},
            "logging": {"level": "INFO", "format": "%(message)s"},
        }

        config_file = tmp_path / "test_config.json"
        config_file.write_text(json.dumps(config_data))

        # 新しいConfigインスタンスを作成
        config = Config(config_path=config_file)

        # 設定ファイルからのパス
        assert config.db_path == str(tmp_path / "config_db.db")

        # 環境変数でオーバーライド
        env_db_path = str(tmp_path / "env_db.db")
        with mock.patch.dict(os.environ, {"DATABASE_PATH": env_db_path}):
            # get_db_path関数は環境変数を優先
            from src.config import get_db_path as get_db_path_func

            assert get_db_path_func() == env_db_path

            # configインスタンスのdb_pathは変わらない
            assert config.db_path == str(tmp_path / "config_db.db")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""Additional tests for src/config.py"""

import json
import tempfile
from pathlib import Path

import pytest

from src.config import Config


class TestConfigGet:
    """Config.getメソッドの詳細なテスト"""

    def test_get_nested_value(self):
        """ネストした値の取得をテスト"""
        # テスト用の設定を作成
        test_config = Config()
        test_config._config = {"level1": {"level2": {"level3": "value"}}}

        # ネストした値を取得
        assert test_config.get("level1.level2.level3") == "value"
        assert test_config.get("level1.level2") == {"level3": "value"}

    def test_get_with_default(self):
        """デフォルト値の取得をテスト"""
        test_config = Config()
        test_config._config = {"existing": "value"}

        # 存在しないキーでデフォルト値を返す
        assert test_config.get("nonexistent", "default") == "default"
        assert test_config.get("existing", "default") == "value"

    def test_get_partial_path(self):
        """部分的なパスでの取得をテスト"""
        test_config = Config()
        test_config._config = {"api": {"endpoints": {"auth": "/token/auth_user"}}}

        # 存在しない部分パス
        assert test_config.get("api.endpoints.nonexistent", None) is None
        assert test_config.get("api.nonexistent.auth", "default") == "default"


class TestConfigProperties:
    """Configプロパティの詳細なテスト"""

    def test_log_level_property(self):
        """log_levelプロパティのテスト"""
        test_config = Config()
        assert test_config.log_level == "INFO"

    def test_log_format_property(self):
        """log_formatプロパティのテスト"""
        test_config = Config()
        expected_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        assert test_config.log_format == expected_format

    def test_file_paths_property(self):
        """file_pathsプロパティが存在することを確認"""
        test_config = Config()

        # get_file_pathメソッドで各ファイルパスを取得
        account_path = test_config.get_file_path("account")
        idtoken_path = test_config.get_file_path("idtoken")
        thresholds_path = test_config.get_file_path("thresholds")

        assert account_path.name == "account.json"
        assert idtoken_path.name == "idtoken.json"
        assert thresholds_path.name == "thresholds.json"

    def test_get_file_path_invalid(self):
        """無効なファイルタイプでValueErrorが発生することを確認"""
        test_config = Config()

        with pytest.raises(ValueError, match="Unknown file type"):
            test_config.get_file_path("invalid_type")


class TestSchedulerConfig:
    """スケジューラー設定のテスト"""

    def test_get_scheduler_config_valid(self):
        """有効なタスクのスケジューラー設定を取得"""
        test_config = Config()

        # fetch_quotesの設定を取得
        quotes_config = test_config.get_scheduler_config("fetch_quotes")
        assert quotes_config["time"] == "20:00"
        assert quotes_config["frequency"] == "daily"

        # fetch_statementsの設定を取得
        statements_config = test_config.get_scheduler_config("fetch_statements")
        assert statements_config["time"] == "20:30"
        assert statements_config["frequency"] == "daily"

        # update_listed_infoの設定を取得
        listed_config = test_config.get_scheduler_config("update_listed_info")
        assert listed_config["time"] == "06:00"
        assert listed_config["frequency"] == "monday"

    def test_get_scheduler_config_invalid(self):
        """無効なタスクの場合は空の辞書を返す"""
        test_config = Config()

        result = test_config.get_scheduler_config("invalid_task")
        assert result == {}


class TestConfigInitialization:
    """Config初期化の詳細なテスト"""

    def test_init_with_custom_path(self):
        """カスタム設定ファイルパスでの初期化をテスト"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            custom_config = {
                "custom_key": "custom_value",
                "database": {"path": "/custom/db/path.db"},
                "api": {
                    "base_url": "https://custom.api.com",
                    "endpoints": {"test": "/test"},
                    "rate_limit": {"sleep_seconds": 1.0},
                },
                "logging": {"level": "DEBUG", "format": "custom format"},
            }
            json.dump(custom_config, f)
            temp_path = Path(f.name)

        try:
            test_config = Config(config_path=temp_path)

            assert "custom_key" in test_config._config
            assert test_config._config["custom_key"] == "custom_value"
            assert test_config.db_path == "/custom/db/path.db"
            assert test_config.api_base_url == "https://custom.api.com"

        finally:
            temp_path.unlink()

    def test_base_dir_calculation(self):
        """base_dirが正しく計算されることを確認"""
        test_config = Config()

        # プロジェクトルートを指していることを確認
        assert test_config.base_dir.name == "swing"
        assert (test_config.base_dir / "src").exists()
        assert (test_config.base_dir / "db").exists()


class TestGlobalConfigInstance:
    """グローバル設定インスタンスのテスト"""

    def test_global_config_is_singleton(self):
        """グローバルconfigインスタンスが同一であることを確認"""
        from src.config import config as config1
        from src.config import config as config2

        assert config1 is config2

    def test_global_exports(self):
        """グローバルエクスポートが正しく設定されていることを確認"""
        from src.config import (
            API_BASE_URL,
            API_RATE_LIMIT_SLEEP,
            DB_PATH,
            LOG_DIR,
            MODEL_DIR,
            OUTPUT_BASE_DIR,
        )

        assert isinstance(DB_PATH, str)
        assert "stock.db" in DB_PATH

        assert isinstance(API_BASE_URL, str)
        assert API_BASE_URL.startswith("https://")

        assert isinstance(API_RATE_LIMIT_SLEEP, float)
        assert API_RATE_LIMIT_SLEEP > 0

        assert isinstance(OUTPUT_BASE_DIR, Path)
        assert OUTPUT_BASE_DIR.name == "output"

        assert isinstance(LOG_DIR, Path)
        assert LOG_DIR.name == "logs"

        assert isinstance(MODEL_DIR, Path)
        assert MODEL_DIR.name == "models"

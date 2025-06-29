"""config.pyのテスト"""
import json
from pathlib import Path

import pytest

from config import Config


class TestConfig:
    """Configクラスのテスト"""
    
    def test_load_config_file(self, sample_config: Path):
        """設定ファイルの読み込みテスト"""
        config = Config(sample_config)
        assert config.db_path == "test.db"
        assert config.api_base_url == "https://api.example.com/v1"
        assert config.api_rate_limit_sleep == 0.1
    
    def test_default_config(self, tmp_path: Path):
        """デフォルト設定のテスト"""
        # 存在しない設定ファイルを指定
        config = Config(tmp_path / "nonexistent.json")
        
        # デフォルト値が使用されることを確認
        assert "stock.db" in config.db_path
        assert config.api_base_url == "https://api.jquants.com/v1"
        assert config.api_rate_limit_sleep == 0.35
    
    def test_get_api_endpoint(self, sample_config: Path):
        """APIエンドポイント取得のテスト"""
        config = Config(sample_config)
        
        assert config.get_api_endpoint("auth") == "https://api.example.com/v1/auth"
        assert config.get_api_endpoint("daily_quotes") == "https://api.example.com/v1/quotes"
        
        # 存在しないエンドポイント
        with pytest.raises(ValueError, match="Unknown endpoint"):
            config.get_api_endpoint("invalid")
    
    def test_get_nested_config(self, sample_config: Path):
        """ネストした設定値の取得テスト"""
        config = Config(sample_config)
        
        assert config.get("api.base_url") == "https://api.example.com/v1"
        assert config.get("api.rate_limit.sleep_seconds") == 0.1
        assert config.get("nonexistent.key", "default") == "default"
    
    def test_file_paths(self, sample_config: Path):
        """ファイルパス取得のテスト"""
        config = Config(sample_config)
        
        account_path = config.get_file_path("account")
        assert account_path.name == "account.json"
        
        # 存在しないファイルタイプ
        with pytest.raises(ValueError, match="Unknown file type"):
            config.get_file_path("invalid")
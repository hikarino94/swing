"""ConfigManagerのテスト"""
import json

import pytest

from src.utils.config import ConfigManager


class TestConfigManager:
    """ConfigManagerのテストクラス"""

    def test_load_json_success(self, tmp_path):
        """正常なJSON読み込みテスト"""
        test_file = tmp_path / "test.json"
        test_data = {"key": "value", "number": 42}

        with open(test_file, "w") as f:
            json.dump(test_data, f)

        config = ConfigManager(tmp_path)
        result = config.load_json("test.json")

        assert result == test_data

    def test_load_json_file_not_found(self, tmp_path):
        """存在しないファイルのテスト"""
        config = ConfigManager(tmp_path)

        with pytest.raises(Exception):
            config.load_json("nonexistent.json")

    def test_load_json_invalid_format(self, tmp_path):
        """無効なJSON形式のテスト"""
        test_file = tmp_path / "invalid.json"

        with open(test_file, "w") as f:
            f.write("invalid json content")

        config = ConfigManager(tmp_path)

        with pytest.raises(Exception):
            config.load_json("invalid.json")

    def test_load_idtoken(self, tmp_path):
        """idtoken.json読み込みテスト"""
        idtoken_file = tmp_path / "idtoken.json"
        token_data = {"idToken": "test_token_12345"}

        with open(idtoken_file, "w") as f:
            json.dump(token_data, f)

        config = ConfigManager(tmp_path)
        result = config.load_idtoken()

        assert result["idToken"] == "test_token_12345"

    def test_load_account(self, tmp_path):
        """account.json読み込みテスト"""
        account_file = tmp_path / "account.json"
        account_data = {"mailaddress": "test@example.com", "password": "test_password"}

        with open(account_file, "w") as f:
            json.dump(account_data, f)

        config = ConfigManager(tmp_path)
        result = config.load_account()

        assert result["mailaddress"] == "test@example.com"
        assert result["password"] == "test_password"

    def test_singleton_behavior(self, tmp_path):
        """シングルトン的な動作のテスト"""
        from src.utils.config import get_config_manager

        # 同じインスタンスが返されることを確認
        manager1 = get_config_manager()
        manager2 = get_config_manager()

        assert manager1 is manager2

    def test_config_validation(self, tmp_path):
        """設定ファイルの検証テスト"""
        config = ConfigManager(tmp_path)

        # 空のJSONファイル
        empty_file = tmp_path / "empty.json"
        with open(empty_file, "w") as f:
            json.dump({}, f)

        result = config.load_json("empty.json")
        assert result == {}

    @pytest.mark.parametrize(
        "filename,expected_key",
        [
            ("idtoken.json", "idToken"),
            ("account.json", "mailaddress"),
            ("login.json", "mailaddress"),
        ],
    )
    def test_required_config_files(self, tmp_path, filename, expected_key):
        """必要な設定ファイルのテスト"""
        config_file = tmp_path / filename
        test_data = {expected_key: "test_value"}

        with open(config_file, "w") as f:
            json.dump(test_data, f)

        config = ConfigManager(tmp_path)
        result = config.load_json(filename)

        assert expected_key in result
        assert result[expected_key] == "test_value"

"""ユーティリティモジュールのテスト"""
import pytest
import tempfile
import sqlite3
from pathlib import Path
from datetime import date, datetime
import pandas as pd
import json

from utils.config import ConfigManager
from utils.db_utils import DatabaseManager
from utils.logging_config import get_logger
from utils.common import generate_timestamped_filename, parse_date_string


class TestConfigManager:
    """ConfigManagerのテスト"""
    
    def test_load_json_success(self, tmp_path):
        """JSONファイルの正常読み込みテスト"""
        # テスト用JSONファイルを作成
        test_file = tmp_path / "test.json"
        test_data = {"test_key": "test_value"}
        with open(test_file, "w") as f:
            json.dump(test_data, f)
        
        # ConfigManagerでテスト
        config = ConfigManager(tmp_path)
        result = config.load_json("test.json")
        
        assert result == test_data
    
    def test_load_json_file_not_found(self, tmp_path):
        """存在しないファイルのテスト"""
        config = ConfigManager(tmp_path)
        
        with pytest.raises(Exception):
            config.load_json("nonexistent.json")
    
    def test_load_json_invalid_format(self, tmp_path):
        """不正なJSON形式のテスト"""
        # 不正なJSONファイルを作成
        test_file = tmp_path / "invalid.json"
        with open(test_file, "w") as f:
            f.write("invalid json content")
        
        config = ConfigManager(tmp_path)
        
        with pytest.raises(Exception):
            config.load_json("invalid.json")


class TestDatabaseManager:
    """DatabaseManagerのテスト"""
    
    def test_get_connection(self):
        """データベース接続のテスト"""
        # 一時的なSQLiteデータベースを使用
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = Path(tmp.name)
        
        try:
            db_manager = DatabaseManager(db_path)
            
            with db_manager.get_connection() as conn:
                # テーブル作成
                conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
                conn.execute("INSERT INTO test VALUES (1, 'test')")
                
                # データ確認
                cursor = conn.execute("SELECT * FROM test")
                result = cursor.fetchone()
                assert result["id"] == 1
                assert result["name"] == "test"
        
        finally:
            # 一時ファイルを削除
            if db_path.exists():
                db_path.unlink()
    
    def test_transaction_commit(self):
        """トランザクション成功のテスト"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = Path(tmp.name)
        
        try:
            db_manager = DatabaseManager(db_path)
            
            # トランザクション内でデータ挿入
            with db_manager.transaction() as conn:
                conn.execute("CREATE TABLE test (id INTEGER)")
                conn.execute("INSERT INTO test VALUES (1)")
            
            # データが永続化されているか確認
            with db_manager.get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM test")
                count = cursor.fetchone()[0]
                assert count == 1
        
        finally:
            if db_path.exists():
                db_path.unlink()
    
    def test_transaction_rollback(self):
        """トランザクション失敗のテスト"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = Path(tmp.name)
        
        try:
            db_manager = DatabaseManager(db_path)
            
            # テーブル作成
            with db_manager.transaction() as conn:
                conn.execute("CREATE TABLE test (id INTEGER)")
            
            # 意図的にエラーを発生させる
            with pytest.raises(Exception):
                with db_manager.transaction() as conn:
                    conn.execute("INSERT INTO test VALUES (1)")
                    # 不正なSQL文でエラーを発生
                    conn.execute("INVALID SQL")
            
            # ロールバックされているか確認
            with db_manager.get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM test")
                count = cursor.fetchone()[0]
                assert count == 0  # データが挿入されていない
        
        finally:
            if db_path.exists():
                db_path.unlink()


class TestCommonUtils:
    """共通ユーティリティのテスト"""
    
    def test_generate_timestamped_filename(self, tmp_path):
        """タイムスタンプ付きファイル名生成のテスト"""
        filename = generate_timestamped_filename("test", ".txt", tmp_path)
        
        # パスが正しいディレクトリを指している
        assert filename.parent == tmp_path
        
        # ファイル名がtest_で始まり.txtで終わる
        assert filename.name.startswith("test_")
        assert filename.name.endswith(".txt")
        
        # タイムスタンプが含まれている（長さチェック）
        assert len(filename.stem) > 4  # "test"より長い
    
    def test_parse_date_string_valid(self):
        """日付文字列解析の正常ケース"""
        # YYYY-MM-DD形式
        result = parse_date_string("2023-12-25")
        assert result == date(2023, 12, 25)
        
        # YYYYMMDD形式
        result = parse_date_string("20231225")
        assert result == date(2023, 12, 25)
    
    def test_parse_date_string_invalid(self):
        """日付文字列解析の異常ケース"""
        with pytest.raises(ValueError):
            parse_date_string("invalid-date")
        
        with pytest.raises(ValueError):
            parse_date_string("2023-13-01")  # 無効な月


class TestLogging:
    """ロギング機能のテスト"""
    
    def test_get_logger(self):
        """ロガー取得のテスト"""
        logger = get_logger("test_logger")
        
        assert logger.name == "test_logger"
        assert logger.level <= 20  # INFO以下
        assert len(logger.handlers) > 0  # ハンドラーが設定されている


# Pytestフィクスチャ
@pytest.fixture
def sample_dataframe():
    """テスト用DataFrame"""
    return pd.DataFrame({
        "code": ["1301", "1332", "1333"],
        "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "close": [100.0, 200.0, 300.0]
    })


@pytest.fixture
def temp_database():
    """テスト用一時データベース"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = Path(tmp.name)
    
    yield db_path
    
    # クリーンアップ
    if db_path.exists():
        db_path.unlink()


# インテグレーションテスト
class TestIntegration:
    """統合テスト"""
    
    def test_config_and_database_integration(self, tmp_path):
        """設定とデータベースの統合テスト"""
        # 設定ファイルを作成
        config_file = tmp_path / "config.json"
        config_data = {"database": "test.db"}
        with open(config_file, "w") as f:
            json.dump(config_data, f)
        
        # ConfigManagerでデータベースパスを取得
        config = ConfigManager(tmp_path)
        db_config = config.load_json("config.json")
        
        # DatabaseManagerでデータベース操作
        db_path = tmp_path / db_config["database"]
        db_manager = DatabaseManager(db_path)
        
        with db_manager.transaction() as conn:
            conn.execute("CREATE TABLE integration_test (id INTEGER, value TEXT)")
            conn.execute("INSERT INTO integration_test VALUES (1, 'success')")
        
        # データが正常に保存されているか確認
        with db_manager.get_connection() as conn:
            cursor = conn.execute("SELECT value FROM integration_test WHERE id = 1")
            result = cursor.fetchone()
            assert result["value"] == "success"


if __name__ == "__main__":
    pytest.main([__file__])
"""Tests for src/utils/db_utils.py"""

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from src.config import get_db_path
from src.utils.db_utils import get_db_connection


class TestDbConfig:
    """DB設定関連のテスト"""

    def test_get_db_path_returns_correct_path(self):
        """正しいデータベースパスを返すことを確認"""
        # src.configからインポートした関数をテスト
        db_path = get_db_path()
        # get_db_pathは文字列を返す
        assert isinstance(db_path, str)
        assert db_path.endswith("stock.db")
        assert "db" in db_path

    def test_db_path_contains_stock_db(self):
        """パスにstock.dbまたはtest_stock.dbが含まれることを確認"""
        db_path = get_db_path()
        # 文字列をPathに変換して確認
        db_path_obj = Path(db_path)
        # テスト環境ではtest_stock.dbになることがある
        assert db_path_obj.name in ["stock.db", "test_stock.db"]
        assert "db" in str(db_path)


class TestGetDbConnection:
    """get_db_connection関数のテスト"""

    @patch("src.utils.db_utils.sqlite3.connect")
    @patch("src.utils.db_utils.get_db_path")
    def test_creates_connection_context_manager(self, mock_get_path, mock_connect):
        """コンテキストマネージャーとして動作することを確認"""
        mock_path = Path("/test/db/stock.db")
        mock_get_path.return_value = mock_path
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with get_db_connection() as conn:
            assert conn == mock_conn

        mock_connect.assert_called_once_with(mock_path)
        mock_conn.close.assert_called_once()

    @patch("src.utils.db_utils.sqlite3.connect")
    def test_uses_custom_db_path(self, mock_connect):
        """カスタムDBパスが使用できることを確認"""
        custom_path = "/custom/path/test.db"
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with get_db_connection(db_path=custom_path):
            pass

        mock_connect.assert_called_once_with(custom_path)

    @patch("src.utils.db_utils.sqlite3.connect")
    @patch("src.utils.db_utils.get_db_path")
    def test_applies_optimizations(self, mock_get_path, mock_connect):
        """最適化設定が適用されることを確認"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with get_db_connection(optimize=True):
            pass

        # PRAGMA設定が実行されたことを確認
        expected_calls = [
            call("PRAGMA cache_size = -64000"),
            call("PRAGMA temp_store = MEMORY"),
            call("PRAGMA mmap_size = 268435456"),
            call("PRAGMA synchronous = NORMAL"),
            call("PRAGMA journal_mode = WAL"),
        ]
        mock_conn.execute.assert_has_calls(expected_calls)

    @patch("src.utils.db_utils.sqlite3.connect")
    @patch("src.utils.db_utils.get_db_path")
    def test_no_optimizations_when_disabled(self, mock_get_path, mock_connect):
        """optimize=False時に最適化がスキップされることを確認"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with get_db_connection(optimize=False):
            pass

        # PRAGMA設定が実行されていないことを確認
        mock_conn.execute.assert_not_called()

    @patch("src.utils.db_utils.logger")
    @patch("src.utils.db_utils.sqlite3.connect")
    @patch("src.utils.db_utils.get_db_path")
    def test_handles_exception_with_rollback(
        self, mock_get_path, mock_connect, mock_logger
    ):
        """例外時にロールバックされることを確認"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # コンテキスト内で例外を発生させる
        with pytest.raises(ValueError):
            with get_db_connection():
                raise ValueError("Test error")

        # ロールバックされたことを確認
        mock_conn.rollback.assert_called_once()
        mock_logger.error.assert_called_once()


class TestDatabaseUtilities:
    """データベースユーティリティの統合テスト"""

    @patch("src.utils.db_utils.sqlite3.connect")
    def test_database_operations_flow(self, mock_connect):
        """データベース操作の一連の流れをテスト"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.execute.return_value = MagicMock()
        mock_connect.return_value = mock_conn

        # コンテキストマネージャーでデータベース操作を実行
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")

        # コネクションが適切に閉じられたことを確認
        mock_conn.close.assert_called_once()


class TestTableExists:
    """table_exists関数のテスト"""

    def test_table_exists_true(self):
        """テーブルが存在する場合のテスト"""
        from src.utils.db_utils import table_exists

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("users",)
        mock_conn.cursor.return_value = mock_cursor

        result = table_exists(mock_conn, "users")

        assert result is True
        mock_cursor.execute.assert_called_once()
        # SQL文にテーブル名が含まれることを確認
        sql_call = mock_cursor.execute.call_args[0][0]
        assert "sqlite_master" in sql_call
        assert mock_cursor.execute.call_args[0][1] == ("users",)

    def test_table_exists_false(self):
        """テーブルが存在しない場合のテスト"""
        from src.utils.db_utils import table_exists

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor

        result = table_exists(mock_conn, "nonexistent")

        assert result is False


class TestGetTableColumns:
    """get_table_columns関数のテスト"""

    def test_get_table_columns(self):
        """テーブルのカラム名取得テスト"""
        from src.utils.db_utils import get_table_columns

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # PRAGMA table_infoの戻り値形式
        mock_cursor.fetchall.return_value = [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "name", "TEXT", 0, None, 0),
            (2, "email", "TEXT", 0, None, 0),
        ]
        mock_conn.cursor.return_value = mock_cursor

        result = get_table_columns(mock_conn, "users")

        assert result == ["id", "name", "email"]
        mock_cursor.execute.assert_called_once_with("PRAGMA table_info(users)")

    def test_get_table_columns_empty(self):
        """存在しないテーブルの場合のテスト"""
        from src.utils.db_utils import get_table_columns

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        result = get_table_columns(mock_conn, "nonexistent")

        assert result == []


class TestVacuumDatabase:
    """vacuum_database関数のテスト"""

    @patch("src.utils.db_utils.sqlite3.connect")
    @patch("src.utils.db_utils.get_db_path")
    @patch("src.utils.db_utils.logger")
    def test_vacuum_database_default_path(
        self, mock_logger, mock_get_path, mock_connect
    ):
        """デフォルトパスでのVACUUM実行テスト"""
        from src.utils.db_utils import vacuum_database

        mock_get_path.return_value = "/test/db/stock.db"
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        vacuum_database()

        mock_get_path.assert_called_once()
        mock_connect.assert_called_once_with("/test/db/stock.db")
        mock_conn.execute.assert_called_once_with("VACUUM")
        mock_conn.close.assert_called_once()
        mock_logger.info.assert_called_once_with("Database vacuum completed")

    @patch("src.utils.db_utils.sqlite3.connect")
    @patch("src.utils.db_utils.logger")
    def test_vacuum_database_custom_path(self, mock_logger, mock_connect):
        """カスタムパスでのVACUUM実行テスト"""
        from src.utils.db_utils import vacuum_database

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        vacuum_database("/custom/path/test.db")

        mock_connect.assert_called_once_with("/custom/path/test.db")
        mock_conn.execute.assert_called_once_with("VACUUM")
        mock_conn.close.assert_called_once()

    @patch("src.utils.db_utils.sqlite3.connect")
    def test_vacuum_database_exception(self, mock_connect):
        """VACUUM実行時の例外処理テスト"""
        from src.utils.db_utils import vacuum_database

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("VACUUM failed")
        mock_connect.return_value = mock_conn

        with pytest.raises(Exception, match="VACUUM failed"):
            vacuum_database("/test/db/stock.db")

        # 例外が発生してもcloseが呼ばれることを確認
        mock_conn.close.assert_called_once()

"""Additional tests for src/utils/db_utils.py"""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from src.utils.db_utils import (
    execute_many,
    execute_query,
    upsert_dataframe,
)


class TestExecuteQuery:
    """execute_query関数のテスト"""

    def setup_method(self):
        """テスト用の一時データベースを作成"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name

        # テストテーブルを作成
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value INTEGER
                )
            """
            )
            conn.execute("INSERT INTO test_table VALUES (1, 'test1', 100)")
            conn.execute("INSERT INTO test_table VALUES (2, 'test2', 200)")
            conn.commit()

    def teardown_method(self):
        """一時ファイルの削除"""
        Path(self.db_path).unlink(missing_ok=True)

    def test_execute_query_select(self):
        """SELECT文の実行テスト"""
        results = execute_query(
            "SELECT * FROM test_table WHERE value > ?", (150,), db_path=self.db_path
        )

        assert len(results) == 1
        assert results[0][1] == "test2"
        assert results[0][2] == 200

    def test_execute_query_no_params(self):
        """パラメータなしのクエリ実行テスト"""
        results = execute_query("SELECT COUNT(*) FROM test_table", db_path=self.db_path)

        assert results[0][0] == 2

    @patch("src.utils.db_utils.get_db_connection")
    def test_execute_query_with_default_db(self, mock_get_connection):
        """デフォルトDBパスでの実行テスト"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(1, "test")]
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value.__enter__.return_value = mock_conn

        results = execute_query("SELECT * FROM test")

        assert results == [(1, "test")]
        mock_get_connection.assert_called_once_with(None)


class TestExecuteMany:
    """execute_many関数のテスト"""

    def setup_method(self):
        """テスト用の一時データベースを作成"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE test_batch (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                )
            """
            )

    def teardown_method(self):
        """一時ファイルの削除"""
        Path(self.db_path).unlink(missing_ok=True)

    def test_execute_many_insert(self):
        """複数行のINSERTテスト"""
        params_list = [(i, f"value_{i}") for i in range(1, 6)]

        execute_many(
            "INSERT INTO test_batch VALUES (?, ?)", params_list, db_path=self.db_path
        )

        # 結果を確認
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM test_batch")
            count = cursor.fetchone()[0]
            assert count == 5

    def test_execute_many_with_batch_size(self):
        """バッチサイズ指定のテスト"""
        params_list = [(i, f"value_{i}") for i in range(1, 11)]

        with patch("src.utils.db_utils.logger") as mock_logger:
            execute_many(
                "INSERT INTO test_batch VALUES (?, ?)",
                params_list,
                db_path=self.db_path,
                batch_size=3,
            )

            # 4バッチ処理されることを確認
            assert mock_logger.debug.call_count == 4

        # データが全て挿入されたことを確認
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM test_batch")
            count = cursor.fetchone()[0]
            assert count == 10


class TestUpsertDataFrame:
    """upsert_dataframe関数のテスト"""

    def setup_method(self):
        """テスト用の一時データベースを作成"""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name

    def teardown_method(self):
        """一時ファイルの削除"""
        Path(self.db_path).unlink(missing_ok=True)

    def test_upsert_new_data(self):
        """新規データのUPSERTテスト"""
        with sqlite3.connect(self.db_path) as conn:
            # テーブル作成
            conn.execute(
                """
                CREATE TABLE stocks (
                    code TEXT,
                    date TEXT,
                    price INTEGER,
                    PRIMARY KEY (code, date)
                )
            """
            )

            # DataFrameを作成
            df = pd.DataFrame(
                {
                    "code": ["1234", "5678"],
                    "date": ["2023-01-01", "2023-01-01"],
                    "price": [1000, 2000],
                }
            )

            # UPSERT実行
            upsert_dataframe(conn, df, "stocks", ["code", "date"])

            # 結果を確認
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM stocks ORDER BY code")
            results = cursor.fetchall()

            assert len(results) == 2
            assert results[0] == ("1234", "2023-01-01", 1000)
            assert results[1] == ("5678", "2023-01-01", 2000)

    def test_upsert_update_existing(self):
        """既存データの更新テスト"""
        with sqlite3.connect(self.db_path) as conn:
            # テーブル作成と初期データ
            conn.execute(
                """
                CREATE TABLE stocks (
                    code TEXT PRIMARY KEY,
                    price INTEGER
                )
            """
            )
            conn.execute("INSERT INTO stocks VALUES ('1234', 1000)")
            conn.commit()

            # 更新データ
            df = pd.DataFrame({"code": ["1234", "5678"], "price": [1500, 2000]})

            # UPSERT実行
            upsert_dataframe(conn, df, "stocks", ["code"])

            # 結果を確認
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM stocks ORDER BY code")
            results = cursor.fetchall()

            assert len(results) == 2
            assert results[0] == ("1234", 1500)  # 更新された
            assert results[1] == ("5678", 2000)  # 新規追加

    def test_upsert_empty_dataframe(self):
        """空のDataFrameのテスト"""
        with sqlite3.connect(self.db_path) as conn:
            # テーブル作成
            conn.execute(
                """
                CREATE TABLE test_empty (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                )
            """
            )

            # 空のDataFrame
            df = pd.DataFrame(columns=["id", "value"])

            # UPSERT実行（エラーにならない）
            upsert_dataframe(conn, df, "test_empty", ["id"])

            # データが追加されていないことを確認
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM test_empty")
            count = cursor.fetchone()[0]
            assert count == 0

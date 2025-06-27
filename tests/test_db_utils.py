"""DatabaseManagerのテスト"""
import tempfile
from pathlib import Path

import pytest

from src.utils.db_utils import DatabaseManager, get_db_manager
from src.utils.exceptions import DatabaseError


class TestDatabaseManager:
    """DatabaseManagerのテストクラス"""

    @pytest.fixture
    def temp_db(self):
        """一時データベースフィクスチャ"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = Path(tmp.name)

        yield db_path

        if db_path.exists():
            db_path.unlink()

    def test_get_connection(self, temp_db):
        """基本的な接続テスト"""
        db_manager = DatabaseManager(temp_db)

        with db_manager.get_connection() as conn:
            conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO test VALUES (1, 'test')")
            conn.commit()

            cursor = conn.execute("SELECT * FROM test")
            result = cursor.fetchone()
            assert result["id"] == 1
            assert result["name"] == "test"

    def test_transaction_commit(self, temp_db):
        """トランザクション成功テスト"""
        db_manager = DatabaseManager(temp_db)

        # テーブル作成
        with db_manager.transaction() as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")

        # データ挿入
        with db_manager.transaction() as conn:
            conn.execute("INSERT INTO test VALUES (1)")
            conn.execute("INSERT INTO test VALUES (2)")

        # データ確認
        with db_manager.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM test")
            count = cursor.fetchone()[0]
            assert count == 2

    def test_transaction_rollback(self, temp_db):
        """トランザクション失敗・ロールバックテスト"""
        db_manager = DatabaseManager(temp_db)

        # テーブル作成
        with db_manager.transaction() as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.execute("INSERT INTO test VALUES (1)")

        # 失敗するトランザクション
        with pytest.raises(DatabaseError):
            with db_manager.transaction() as conn:
                conn.execute("INSERT INTO test VALUES (2)")
                # 無効なSQLでエラーを発生
                conn.execute("INVALID SQL SYNTAX")

        # ロールバック確認
        with db_manager.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM test")
            count = cursor.fetchone()[0]
            assert count == 1  # 最初の1レコードのみ

    def test_execute_query(self, temp_db):
        """execute_queryメソッドテスト"""
        db_manager = DatabaseManager(temp_db)

        # テストデータ準備
        with db_manager.transaction() as conn:
            conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO test VALUES (1, 'alice')")
            conn.execute("INSERT INTO test VALUES (2, 'bob')")

        # クエリ実行
        results = db_manager.execute_query("SELECT * FROM test WHERE id = ?", (1,))
        assert len(results) == 1
        assert results[0]["name"] == "alice"

        # 全件取得
        all_results = db_manager.execute_query("SELECT * FROM test ORDER BY id")
        assert len(all_results) == 2
        assert all_results[0]["name"] == "alice"
        assert all_results[1]["name"] == "bob"

    def test_execute_scalar(self, temp_db):
        """execute_scalarメソッドテスト"""
        db_manager = DatabaseManager(temp_db)

        # テストデータ準備
        with db_manager.transaction() as conn:
            conn.execute("CREATE TABLE test (id INTEGER, value REAL)")
            conn.execute("INSERT INTO test VALUES (1, 10.5)")
            conn.execute("INSERT INTO test VALUES (2, 20.5)")

        # スカラー値取得
        count = db_manager.execute_scalar("SELECT COUNT(*) FROM test")
        assert count == 2

        total = db_manager.execute_scalar("SELECT SUM(value) FROM test")
        assert total == 31.0

        # 存在しない結果
        result = db_manager.execute_scalar("SELECT id FROM test WHERE id = 999")
        assert result is None

    def test_table_exists(self, temp_db):
        """table_existsメソッドテスト"""
        db_manager = DatabaseManager(temp_db)

        # テーブルが存在しない
        assert not db_manager.table_exists("nonexistent")

        # テーブル作成
        with db_manager.transaction() as conn:
            conn.execute("CREATE TABLE test_table (id INTEGER)")

        # テーブルが存在する
        assert db_manager.table_exists("test_table")

    def test_execute_many(self, temp_db):
        """execute_manyメソッドテスト"""
        db_manager = DatabaseManager(temp_db)

        # テーブル作成
        with db_manager.transaction() as conn:
            conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")

        # バッチ挿入
        data = [(1, "alice"), (2, "bob"), (3, "charlie")]
        rows_affected = db_manager.execute_many("INSERT INTO test VALUES (?, ?)", data)

        # 結果確認
        count = db_manager.execute_scalar("SELECT COUNT(*) FROM test")
        assert count == 3

        # 名前確認
        names = db_manager.execute_query("SELECT name FROM test ORDER BY id")
        expected_names = ["alice", "bob", "charlie"]
        actual_names = [row["name"] for row in names]
        assert actual_names == expected_names

    def test_row_factory(self, temp_db):
        """Row factoryの動作テスト"""
        db_manager = DatabaseManager(temp_db)

        with db_manager.get_connection() as conn:
            conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO test VALUES (1, 'test')")

            cursor = conn.execute("SELECT id, name FROM test")
            row = cursor.fetchone()

            # 辞書形式でアクセス
            assert row["id"] == 1
            assert row["name"] == "test"

            # インデックスでもアクセス可能
            assert row[0] == 1
            assert row[1] == "test"

    def test_database_error_handling(self, temp_db):
        """データベースエラーハンドリングテスト"""
        db_manager = DatabaseManager(temp_db)

        # 存在しないテーブルへのクエリ
        with pytest.raises(DatabaseError):
            db_manager.execute_query("SELECT * FROM nonexistent_table")

        # 無効なSQL
        with pytest.raises(DatabaseError):
            db_manager.execute_scalar("INVALID SQL")

    def test_get_db_manager_singleton(self):
        """get_db_manager関数のシングルトン動作テスト"""
        manager1 = get_db_manager()
        manager2 = get_db_manager()

        # 同じインスタンスが返される
        assert manager1 is manager2

    @pytest.mark.parametrize("batch_size", [1, 10, 100])
    def test_execute_many_batch_sizes(self, temp_db, batch_size):
        """異なるバッチサイズでのexecute_manyテスト"""
        db_manager = DatabaseManager(temp_db)

        with db_manager.transaction() as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")

        # 50件のデータ
        data = [(i,) for i in range(50)]

        rows_affected = db_manager.execute_many("INSERT INTO test VALUES (?)", data, batch_size=batch_size)

        count = db_manager.execute_scalar("SELECT COUNT(*) FROM test")
        assert count == 50

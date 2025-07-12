"""Tests for db/db_schema.py"""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from db.db_schema import DDL, init_schema, main


class TestInitSchema:
    """init_schema 関数のテスト"""

    def test_init_schema_creates_tables(self):
        """全てのテーブルが作成されることを確認"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            # スキーマを初期化
            init_schema(db_path)

            # テーブルが作成されたことを確認
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # 主要なテーブルの存在確認
                expected_tables = [
                    "prices",
                    "listed_info",
                    "statements",
                    "fundamental_signals",
                    "technical_indicators",
                    "users",
                    "sessions",
                    "holdings",
                    "transactions",
                    "fund_master",
                    "fund_prices",
                    "fund_holdings",
                    "fund_transactions",
                ]

                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )
                tables = [row[0] for row in cursor.fetchall()]

                for table in expected_tables:
                    assert table in tables, f"テーブル {table} が作成されていません"

                # インデックスの存在確認
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
                )
                indexes = [row[0] for row in cursor.fetchall()]
                assert len(indexes) > 0, "インデックスが作成されていません"

                # WALモードの確認
                cursor.execute("PRAGMA journal_mode")
                journal_mode = cursor.fetchone()[0]
                assert journal_mode == "wal", "WALモードが設定されていません"

        finally:
            # テンポラリファイルを削除
            Path(db_path).unlink(missing_ok=True)

    def test_init_schema_with_path_object(self):
        """Path オブジェクトを渡した場合"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = Path(tmp.name)

        try:
            # Path オブジェクトで初期化
            init_schema(db_path)

            # テーブルが作成されたことを確認
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                table_count = cursor.fetchone()[0]
                assert table_count > 0

        finally:
            db_path.unlink(missing_ok=True)

    def test_init_schema_idempotent(self):
        """複数回実行しても問題ないことを確認"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            # 2回実行
            init_schema(db_path)
            init_schema(db_path)

            # エラーなく実行できることを確認
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                table_count = cursor.fetchone()[0]
                assert table_count > 0

        finally:
            Path(db_path).unlink(missing_ok=True)

    @patch("db.db_schema.logger")
    def test_init_schema_migration_account_type(self, mock_logger):
        """account_typeカラムのマイグレーション"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            # 古いスキーマでテーブルを作成（deleted_atを含む）
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE holdings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        code TEXT NOT NULL,
                        account_name TEXT NOT NULL DEFAULT 'default',
                        quantity INTEGER NOT NULL,
                        average_price REAL NOT NULL,
                        market_value REAL,
                        profit_loss REAL,
                        profit_loss_ratio REAL,
                        expected_per REAL,
                        actual_pbr REAL,
                        dividend_yield REAL,
                        expected_eps REAL,
                        actual_bps REAL,
                        expected_dividend REAL,
                        lending_type TEXT,
                        updated_at TEXT DEFAULT (datetime('now')),
                        deleted_at TEXT DEFAULT NULL
                    )
                """
                )

            # スキーマを初期化（マイグレーションが実行される）
            init_schema(db_path)

            # account_typeカラムが追加されたことを確認
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(holdings)")
                columns = {row[1]: row for row in cursor.fetchall()}
                assert "account_type" in columns

            # ログが記録されたことを確認
            mock_logger.info.assert_called()

        finally:
            Path(db_path).unlink(missing_ok=True)

    @patch("db.db_schema.logger")
    def test_init_schema_unique_constraint_update(self, mock_logger):
        """UNIQUE制約の更新テスト"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            # 古いUNIQUE制約を持つテーブルを作成（必要なフィールドを全て含む）
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE holdings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        code TEXT NOT NULL,
                        account_name TEXT NOT NULL DEFAULT 'default',
                        quantity INTEGER NOT NULL,
                        average_price REAL NOT NULL,
                        market_value REAL,
                        profit_loss REAL,
                        profit_loss_ratio REAL,
                        expected_per REAL,
                        actual_pbr REAL,
                        dividend_yield REAL,
                        expected_eps REAL,
                        actual_bps REAL,
                        expected_dividend REAL,
                        lending_type TEXT,
                        updated_at TEXT DEFAULT (datetime('now')),
                        deleted_at TEXT DEFAULT NULL,
                        UNIQUE(user_id, code, account_name)
                    )
                """
                )

                # テストデータを挿入（必要なフィールドをすべて指定）
                conn.execute(
                    """
                    INSERT INTO holdings (user_id, code, account_name, quantity, average_price,
                                      market_value, profit_loss, profit_loss_ratio,
                                      expected_per, actual_pbr, dividend_yield,
                                      expected_eps, actual_bps, expected_dividend,
                                      lending_type)
                    VALUES (1, '1234', 'default', 100, 1000.0,
                            100000.0, 0.0, 0.0,
                            15.0, 1.2, 2.5,
                            100.0, 1000.0, 50.0,
                            NULL)
                """
                )
                conn.commit()

            # スキーマを初期化
            init_schema(db_path)

            # 新しいUNIQUE制約が適用されていることを確認
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # データが保持されていることを確認
                cursor.execute("SELECT * FROM holdings")
                rows = cursor.fetchall()
                assert len(rows) == 1

                # 新しいUNIQUE制約でのテスト（同じuser_id, code, account_nameで異なるaccount_type）
                cursor.execute(
                    """
                    INSERT INTO holdings (user_id, code, account_name, account_type, quantity, average_price)
                    VALUES (1, '1234', 'default', 'NISA', 50, 1100.0)
                """
                )
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM holdings")
                count = cursor.fetchone()[0]
                assert count == 2  # 2レコード存在できる

        finally:
            Path(db_path).unlink(missing_ok=True)


class TestDDL:
    """DDL定数のテスト"""

    def test_ddl_is_valid_sql(self):
        """DDLが有効なSQLであることを確認"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            with sqlite3.connect(db_path) as conn:
                # DDLを実行してエラーがないことを確認
                conn.executescript(DDL)

        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_ddl_contains_all_tables(self):
        """DDLに必要なテーブル定義が含まれていることを確認"""
        expected_tables = [
            "prices",
            "listed_info",
            "statements",
            "fundamental_signals",
            "technical_indicators",
            "users",
            "sessions",
            "holdings",
            "transactions",
            "fund_master",
            "fund_prices",
            "fund_holdings",
            "fund_transactions",
        ]

        for table in expected_tables:
            assert f"CREATE TABLE IF NOT EXISTS {table}" in DDL

    def test_ddl_contains_indexes(self):
        """DDLにインデックス定義が含まれていることを確認"""
        # 主要なインデックスの存在確認
        expected_indexes = [
            "idx_prices_date",
            "idx_prices_code",
            "idx_listed_date",
            "idx_statements_code",
            "idx_fsignals_code",
            "idx_tindicators_code",
            "idx_users_username",
            "idx_sessions_user_id",
            "idx_holdings_user_id",
            "idx_transactions_user_id",
        ]

        for index in expected_indexes:
            assert f"CREATE INDEX IF NOT EXISTS {index}" in DDL


class TestMain:
    """main 関数のテスト"""

    @patch("db.db_schema.get_db_path")
    @patch("db.db_schema.init_schema")
    @patch("db.db_schema.logger")
    def test_main_function(self, mock_logger, mock_init_schema, mock_get_db_path):
        """main関数の正常実行"""
        mock_db_path = "/tmp/test.db"
        mock_get_db_path.return_value = mock_db_path

        # main関数を実行
        main()

        # init_schemaが呼ばれたことを確認
        mock_init_schema.assert_called_once_with(mock_db_path)

        # ログが出力されたことを確認
        mock_logger.info.assert_called_once_with(
            "Schema created or verified at %s", mock_db_path
        )


class TestSchemaIntegrity:
    """スキーマの整合性テスト"""

    def test_foreign_key_constraints(self):
        """外部キー制約が正しく機能することを確認"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            init_schema(db_path)

            with sqlite3.connect(db_path) as conn:
                # 外部キー制約を有効化
                conn.execute("PRAGMA foreign_keys = ON")

                # ユーザーを作成
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO users (username, email, password_hash)
                    VALUES ('test_user', 'test@example.com', 'hash')
                """
                )
                user_id = cursor.lastrowid

                # 存在しないユーザーIDでholdingsに挿入しようとする
                with pytest.raises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO holdings (user_id, code, quantity, average_price)
                        VALUES (9999, '1234', 100, 1000.0)
                    """
                    )

                # 正しいユーザーIDなら成功
                conn.execute(
                    """
                    INSERT INTO holdings (user_id, code, quantity, average_price)
                    VALUES (?, '1234', 100, 1000.0)
                """,
                    (user_id,),
                )
                conn.commit()

        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_unique_constraints(self):
        """UNIQUE制約が正しく機能することを確認"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            init_schema(db_path)

            with sqlite3.connect(db_path) as conn:
                # prices テーブルの主キー制約（code, date）
                conn.execute(
                    """
                    INSERT INTO prices (code, date, adj_close)
                    VALUES ('1234', '2024-01-15', 1000.0)
                """
                )

                # 同じ主キーで挿入しようとするとエラー
                with pytest.raises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO prices (code, date, adj_close)
                        VALUES ('1234', '2024-01-15', 1100.0)
                    """
                    )

        finally:
            Path(db_path).unlink(missing_ok=True)

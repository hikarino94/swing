"""Tests for db_schema module"""

from unittest.mock import patch


class TestDbSchema:
    """Tests for db_schema module"""

    @patch("db.db_schema.get_db_path")
    def test_initialization(self, mock_get_db_path):
        """データベース初期化のテスト"""
        mock_get_db_path.return_value = ":memory:"

        # この時点でのテストは簡単なimport確認のみ
        from db import db_schema

        assert hasattr(db_schema, "init_schema")
        assert hasattr(db_schema, "DDL")

    def test_init_schema_creates_tables(self):
        """init_schemaがテーブルを作成することを確認"""
        import sqlite3
        import tempfile

        from db.db_schema import init_schema

        # 一時データベースファイルを作成
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            # スキーマを初期化
            init_schema(db_path)

            # テーブルが作成されたことを確認
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = {row[0] for row in cursor.fetchall()}

                expected_tables = {
                    "prices",
                    "listed_info",
                    "statements",
                    "fundamental_signals",
                    "technical_indicators",
                    "users",
                    "holdings",
                    "transactions",
                }

                # 全ての期待されるテーブルが存在することを確認
                for table in expected_tables:
                    assert table in tables, f"Table {table} was not created"

        finally:
            # クリーンアップ
            import os

            if os.path.exists(db_path):
                os.unlink(db_path)

    def test_holdings_table_migration(self):
        """holdingsテーブルのマイグレーションテスト"""
        import sqlite3
        import tempfile

        from db.db_schema import init_schema

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            # 古い形式のholdingsテーブルを作成（必要なフィールドを追加）
            with sqlite3.connect(db_path) as conn:
                # まずusersテーブルを作成（外部キー制約のため）
                conn.execute(
                    """
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL UNIQUE,
                        email TEXT NOT NULL UNIQUE,
                        password_hash TEXT NOT NULL,
                        role TEXT NOT NULL DEFAULT 'portfolio_only'
                    )
                """
                )

                conn.execute(
                    """
                    INSERT INTO users (id, username, email, password_hash, role)
                    VALUES (1, 'testuser', 'test@example.com', 'hash', 'admin')
                """
                )

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

                # テストデータを挿入（すべての必要なフィールドを指定）
                conn.execute(
                    """
                    INSERT INTO holdings (user_id, code, account_name, quantity, average_price,
                                      market_value, profit_loss, profit_loss_ratio, expected_per,
                                      actual_pbr, dividend_yield, expected_eps, actual_bps,
                                      expected_dividend, lending_type)
                    VALUES (1, '1234', 'default', 100, 1500.0,
                            150000.0, 5000.0, 0.033, 15.0,
                            1.2, 2.5, 100.0, 1250.0,
                            50.0, NULL)
                """
                )
                conn.commit()

            # init_schemaを実行（マイグレーションが行われる）
            init_schema(db_path)

            # account_typeカラムが追加されたことを確認
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(holdings)")
                columns = {row[1] for row in cursor.fetchall()}

                assert "account_type" in columns, "account_type column was not added"

                # データが保持されていることを確認
                cursor.execute("SELECT * FROM holdings WHERE user_id = 1")
                row = cursor.fetchone()
                assert row is not None, "Data was lost during migration"

        finally:
            import os

            if os.path.exists(db_path):
                os.unlink(db_path)

    def test_create_indexes(self):
        """インデックスが作成されることを確認"""
        import sqlite3
        import tempfile

        from db.db_schema import init_schema

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            # スキーマを初期化
            init_schema(db_path)

            # インデックスが作成されたことを確認
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
                indexes = {row[0] for row in cursor.fetchall()}

                # 主要なインデックスが存在することを確認
                expected_indexes = {
                    "idx_prices_date_code",
                    "idx_listed_date",  # idx_listed_info_codeではなくidx_listed_date
                    "idx_statements_code",
                    "idx_fsignals_created",  # fundamental_signalsの実際のインデックス
                    "idx_tindicators_date",  # technical_indicatorsの実際のインデックス
                }

                for idx in expected_indexes:
                    assert idx in indexes, f"Index {idx} was not created"

        finally:
            import os

            if os.path.exists(db_path):
                os.unlink(db_path)

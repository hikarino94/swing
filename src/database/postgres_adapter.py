"""PostgreSQL用データベースアダプター"""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    psycopg2 = None

from .base import DatabaseAdapter


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL用のデータベースアダプター"""

    def __init__(self, connection_params: dict[str, Any]):
        """アダプターの初期化"""
        super().__init__(connection_params)
        if psycopg2 is None:
            raise ImportError(
                "psycopg2がインストールされていません。"
                "pip install psycopg2-binaryでインストールしてください。"
            )

    def connect(self) -> Any:
        """PostgreSQLデータベースに接続"""
        if self._connection is not None:
            return self._connection

        # 環境変数またはconnection_paramsから接続情報を取得
        conn_params = {
            "host": os.environ.get(
                "POSTGRES_HOST", self.connection_params.get("host", "localhost")
            ),
            "port": int(
                os.environ.get(
                    "POSTGRES_PORT", self.connection_params.get("port", 5432)
                )
            ),
            "database": os.environ.get(
                "POSTGRES_DB", self.connection_params.get("database", "swing")
            ),
            "user": os.environ.get(
                "POSTGRES_USER", self.connection_params.get("user", "postgres")
            ),
            "password": os.environ.get(
                "POSTGRES_PASSWORD", self.connection_params.get("password", "")
            ),
        }

        # DATABASE_URLが設定されている場合はそれを優先（Fly.io標準）
        database_url = os.environ.get("DATABASE_URL")
        if database_url:
            self._connection = psycopg2.connect(database_url)
        else:
            self._connection = psycopg2.connect(**conn_params)

        # カーソルファクトリの設定（辞書形式でも取得可能に）
        self._connection.cursor_factory = psycopg2.extras.RealDictCursor

        return self._connection

    def disconnect(self) -> None:
        """データベース接続を切断"""
        if self._connection:
            self._connection.close()
            self._connection = None

    def execute(self, query: str, params: tuple | dict | None = None) -> Any:
        """クエリを実行"""
        if not self._connection:
            self.connect()

        cursor = self._connection.cursor()
        # SQLiteの?プレースホルダーをPostgreSQLの%sに変換
        converted_query = self.convert_placeholder(query)

        if params:
            cursor.execute(converted_query, params)
        else:
            cursor.execute(converted_query)
        return cursor

    def executemany(self, query: str, params_list: list[tuple | dict]) -> Any:
        """複数のパラメータでクエリを実行"""
        if not self._connection:
            self.connect()

        cursor = self._connection.cursor()
        converted_query = self.convert_placeholder(query)
        cursor.executemany(converted_query, params_list)
        return cursor

    def fetchone(self, cursor: Any) -> tuple | None:
        """1行取得"""
        return cursor.fetchone()

    def fetchall(self, cursor: Any) -> list[tuple]:
        """全行取得"""
        return cursor.fetchall()

    def fetchmany(self, cursor: Any, size: int) -> list[tuple]:
        """指定行数取得"""
        return cursor.fetchmany(size)

    def commit(self) -> None:
        """トランザクションをコミット"""
        if self._connection:
            self._connection.commit()

    def rollback(self) -> None:
        """トランザクションをロールバック"""
        if self._connection:
            self._connection.rollback()

    def begin_transaction(self) -> None:
        """トランザクションを開始"""
        if self._connection:
            # PostgreSQLでは明示的にBEGINを実行
            cursor = self._connection.cursor()
            cursor.execute("BEGIN")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """トランザクションコンテキストマネージャー"""
        self.begin_transaction()
        try:
            yield
            self.commit()
        except Exception:
            self.rollback()
            raise

    def create_tables(self, schema_sql: str) -> None:
        """テーブルを作成"""
        if not self._connection:
            self.connect()

        # 自動コミットモードで実行
        old_isolation_level = self._connection.isolation_level
        self._connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        try:
            cursor = self._connection.cursor()
            # SQLiteのPRAGMA文を除去
            cleaned_sql = self._clean_sqlite_pragmas(schema_sql)
            cursor.execute(cleaned_sql)
        finally:
            self._connection.set_isolation_level(old_isolation_level)

    def table_exists(self, table_name: str) -> bool:
        """テーブルの存在確認"""
        cursor = self.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            )
            """,
            (table_name,),
        )
        return cursor.fetchone()["exists"]

    def get_table_columns(self, table_name: str) -> list[dict[str, Any]]:
        """テーブルのカラム情報を取得"""
        cursor = self.execute(
            """
            SELECT
                column_name as name,
                data_type as type,
                is_nullable = 'NO' as notnull,
                column_default as default,
                FALSE as primary_key
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )

        columns = []
        for row in cursor.fetchall():
            columns.append(
                {
                    "name": row["name"],
                    "type": row["type"],
                    "notnull": row["notnull"],
                    "default": row["default"],
                    "primary_key": row["primary_key"],
                }
            )

        # プライマリキー情報を取得
        pk_cursor = self.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            """,
            (table_name,),
        )

        pk_columns = {row["attname"] for row in pk_cursor.fetchall()}
        for col in columns:
            if col["name"] in pk_columns:
                col["primary_key"] = True

        return columns

    def convert_placeholder(self, query: str) -> str:
        """SQLプレースホルダーを変換（PostgreSQLは%sを使用）"""
        # SQLiteの?をPostgreSQLの%sに変換
        # ただし、文字列リテラル内の?は変換しない

        # 簡易的な実装（より複雑なSQLには対応が必要）
        parts = []
        in_string = False
        quote_char = None
        i = 0

        while i < len(query):
            char = query[i]

            if char in ("'", '"') and (i == 0 or query[i - 1] != "\\"):
                if not in_string:
                    in_string = True
                    quote_char = char
                elif char == quote_char:
                    in_string = False
                    quote_char = None

            if char == "?" and not in_string:
                parts.append("%s")
            else:
                parts.append(char)

            i += 1

        return "".join(parts)

    def last_insert_id(self) -> int | None:
        """最後に挿入されたIDを取得"""
        # PostgreSQLではRETURNING句を使用するか、
        # currval('sequence_name')を使用する必要がある
        # この実装では簡易的にNoneを返す
        return None

    def _clean_sqlite_pragmas(self, sql: str) -> str:
        """SQLiteのPRAGMA文を除去"""
        lines = sql.split("\n")
        cleaned_lines = []

        for line in lines:
            # PRAGMA文をスキップ
            if line.strip().upper().startswith("PRAGMA"):
                continue
            # SQLite特有の構文を変換
            line = line.replace("AUTOINCREMENT", "")
            line = line.replace("datetime('now')", "CURRENT_TIMESTAMP")
            line = line.replace("date('now')", "CURRENT_DATE")

            cleaned_lines.append(line)

        return "\n".join(cleaned_lines)

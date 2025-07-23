"""SQLite用データベースアダプター"""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .base import DatabaseAdapter


class SQLiteAdapter(DatabaseAdapter):
    """SQLite用のデータベースアダプター"""

    def connect(self) -> sqlite3.Connection:
        """SQLiteデータベースに接続"""
        if self._connection is not None:
            return self._connection

        db_path = self.connection_params.get("database", "db/stock.db")
        db_file = Path(db_path)

        # ディレクトリが存在しない場合は作成
        db_file.parent.mkdir(parents=True, exist_ok=True)

        self._connection = sqlite3.connect(
            db_path, check_same_thread=False, timeout=30.0
        )

        # パフォーマンス最適化の設定
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = NORMAL")
        self._connection.execute("PRAGMA cache_size = -64000")  # 64MB
        self._connection.execute("PRAGMA temp_store = MEMORY")
        self._connection.execute("PRAGMA mmap_size = 268435456")  # 256MB

        # Row Factoryの設定（辞書形式でも取得可能に）
        self._connection.row_factory = sqlite3.Row

        return self._connection

    def disconnect(self) -> None:
        """データベース接続を切断"""
        if self._connection:
            self._connection.close()
            self._connection = None

    def execute(self, query: str, params: tuple | dict | None = None) -> sqlite3.Cursor:
        """クエリを実行"""
        if not self._connection:
            self.connect()

        cursor = self._connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor

    def executemany(
        self, query: str, params_list: list[tuple | dict]
    ) -> sqlite3.Cursor:
        """複数のパラメータでクエリを実行"""
        if not self._connection:
            self.connect()

        cursor = self._connection.cursor()
        cursor.executemany(query, params_list)
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
        """トランザクションを開始（SQLiteでは明示的な開始は不要）"""
        pass

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """トランザクションコンテキストマネージャー"""
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

        # 複数のSQL文を実行
        self._connection.executescript(schema_sql)
        self.commit()

    def table_exists(self, table_name: str) -> bool:
        """テーブルの存在確認"""
        cursor = self.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    def get_table_columns(self, table_name: str) -> list[dict[str, Any]]:
        """テーブルのカラム情報を取得"""
        cursor = self.execute(f"PRAGMA table_info({table_name})")
        columns = []
        for row in cursor.fetchall():
            columns.append(
                {
                    "name": row["name"],
                    "type": row["type"],
                    "notnull": bool(row["notnull"]),
                    "default": row["dflt_value"],
                    "primary_key": bool(row["pk"]),
                }
            )
        return columns

    def convert_placeholder(self, query: str) -> str:
        """SQLプレースホルダーを変換（SQLiteは?を使用）"""
        # PostgreSQLの%sをSQLiteの?に変換
        return query.replace("%s", "?")

    def last_insert_id(self) -> int | None:
        """最後に挿入されたIDを取得"""
        if self._connection:
            return self._connection.lastrowid
        return None

    def execute_script(self, script: str) -> None:
        """SQLスクリプトを実行（SQLite専用）"""
        if not self._connection:
            self.connect()
        self._connection.executescript(script)

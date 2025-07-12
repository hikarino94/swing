"""
SQLiteリポジトリの基底クラス
"""

import logging
import sqlite3
from pathlib import Path

from ..interfaces import BaseRepository


class SqliteBaseRepository(BaseRepository):
    """SQLiteリポジトリの基底クラス"""

    def __init__(self, db_path: str = "db/stock.db"):
        """
        Args:
            db_path: データベースファイルのパス
        """
        self.db_path = Path(db_path)
        self.connection: sqlite3.Connection | None = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._in_transaction = False

    def connect(self) -> None:
        """データベースに接続"""
        if self.connection is None:
            self.connection = sqlite3.connect(
                self.db_path, isolation_level=None  # 自動コミットモード
            )
            self.connection.row_factory = sqlite3.Row
            # WALモードを有効化
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=NORMAL")
            self.logger.debug(f"Connected to database: {self.db_path}")

    def disconnect(self) -> None:
        """データベース接続を切断"""
        if self.connection:
            if self._in_transaction:
                self.rollback()
            self.connection.close()
            self.connection = None
            self.logger.debug("Disconnected from database")

    def begin_transaction(self) -> None:
        """トランザクションを開始"""
        if not self.connection:
            raise RuntimeError("Not connected to database")
        if not self._in_transaction:
            self.connection.execute("BEGIN")
            self._in_transaction = True
            self.logger.debug("Transaction started")

    def commit(self) -> None:
        """トランザクションをコミット"""
        if not self.connection:
            raise RuntimeError("Not connected to database")
        if self._in_transaction:
            self.connection.execute("COMMIT")
            self._in_transaction = False
            self.logger.debug("Transaction committed")

    def rollback(self) -> None:
        """トランザクションをロールバック"""
        if not self.connection:
            raise RuntimeError("Not connected to database")
        if self._in_transaction:
            self.connection.execute("ROLLBACK")
            self._in_transaction = False
            self.logger.debug("Transaction rolled back")

    def execute(self, query: str, params: tuple = ()):
        """SQLクエリを実行"""
        if not self.connection:
            raise RuntimeError("Not connected to database")
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor

    def executemany(self, query: str, params_list: list):
        """複数のパラメータでSQLクエリを実行"""
        if not self.connection:
            raise RuntimeError("Not connected to database")
        cursor = self.connection.cursor()
        cursor.executemany(query, params_list)
        return cursor

    def __enter__(self):
        """コンテキストマネージャーのエントリーポイント"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャーのエグジットポイント"""
        if exc_type is not None and self._in_transaction:
            self.rollback()
        self.disconnect()

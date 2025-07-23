"""データベースアダプターの抽象基底クラス"""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


class DatabaseAdapter(ABC):
    """データベースアダプターの抽象基底クラス

    SQLiteとPostgreSQLの両方で共通のインターフェースを提供します。
    """

    def __init__(self, connection_params: dict[str, Any]):
        """アダプターの初期化

        Args:
            connection_params: データベース接続に必要なパラメータ
        """
        self.connection_params = connection_params
        self._connection = None

    @abstractmethod
    def connect(self) -> Any:
        """データベースに接続"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """データベース接続を切断"""
        pass

    @abstractmethod
    def execute(self, query: str, params: tuple | dict | None = None) -> Any:
        """クエリを実行

        Args:
            query: SQL文
            params: パラメータ（タプルまたは辞書）

        Returns:
            実行結果のカーソル
        """
        pass

    @abstractmethod
    def executemany(self, query: str, params_list: list[tuple | dict]) -> Any:
        """複数のパラメータでクエリを実行

        Args:
            query: SQL文
            params_list: パラメータのリスト

        Returns:
            実行結果のカーソル
        """
        pass

    @abstractmethod
    def fetchone(self, cursor: Any) -> tuple | None:
        """1行取得"""
        pass

    @abstractmethod
    def fetchall(self, cursor: Any) -> list[tuple]:
        """全行取得"""
        pass

    @abstractmethod
    def fetchmany(self, cursor: Any, size: int) -> list[tuple]:
        """指定行数取得"""
        pass

    @abstractmethod
    def commit(self) -> None:
        """トランザクションをコミット"""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """トランザクションをロールバック"""
        pass

    @abstractmethod
    def begin_transaction(self) -> None:
        """トランザクションを開始"""
        pass

    @abstractmethod
    @contextmanager
    def transaction(self) -> Iterator[None]:
        """トランザクションコンテキストマネージャー"""
        pass

    @abstractmethod
    def create_tables(self, schema_sql: str) -> None:
        """テーブルを作成"""
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """テーブルの存在確認"""
        pass

    @abstractmethod
    def get_table_columns(self, table_name: str) -> list[dict[str, Any]]:
        """テーブルのカラム情報を取得"""
        pass

    @abstractmethod
    def convert_placeholder(self, query: str) -> str:
        """SQLプレースホルダーを変換

        SQLiteの?形式とPostgreSQLの%s形式を相互変換します。
        """
        pass

    @abstractmethod
    def last_insert_id(self) -> int | None:
        """最後に挿入されたIDを取得"""
        pass

    @contextmanager
    def connection_context(self) -> Iterator["DatabaseAdapter"]:
        """接続コンテキストマネージャー"""
        try:
            self.connect()
            yield self
        finally:
            self.disconnect()

    def __enter__(self) -> "DatabaseAdapter":
        """コンテキストマネージャーのエントリ"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """コンテキストマネージャーのエクジット"""
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.disconnect()

    @property
    def is_connected(self) -> bool:
        """接続状態を確認"""
        return self._connection is not None

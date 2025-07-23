"""データベースアダプターのファクトリー"""

import os
from typing import Any

from .base import DatabaseAdapter
from .postgres_adapter import PostgreSQLAdapter
from .sqlite_adapter import SQLiteAdapter


def get_database_adapter(
    database_type: str | None = None, connection_params: dict[str, Any] | None = None
) -> DatabaseAdapter:
    """データベースアダプターを取得

    Args:
        database_type: データベースタイプ（sqlite/postgres）。
                      指定がない場合は環境変数DATABASE_TYPEを参照
        connection_params: 接続パラメータ。指定がない場合はデフォルト値を使用

    Returns:
        DatabaseAdapter: データベースアダプターのインスタンス

    Raises:
        ValueError: サポートされていないデータベースタイプの場合
    """
    # データベースタイプの決定
    if database_type is None:
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    # 接続パラメータのデフォルト値
    if connection_params is None:
        connection_params = {}

    # SQLiteの場合
    if database_type == "sqlite":
        # DATABASE_PATH環境変数があれば優先
        db_path = os.environ.get("DATABASE_PATH")
        if db_path:
            connection_params["database"] = db_path
        elif "database" not in connection_params:
            # デフォルトパスを設定
            from src.config import get_db_path

            connection_params["database"] = get_db_path()

        return SQLiteAdapter(connection_params)

    # PostgreSQLの場合
    elif database_type in ("postgres", "postgresql"):
        return PostgreSQLAdapter(connection_params)

    else:
        raise ValueError(
            f"サポートされていないデータベースタイプ: {database_type}。"
            "sqliteまたはpostgresを指定してください。"
        )


# 便利なヘルパー関数
def get_default_adapter() -> DatabaseAdapter:
    """デフォルトのデータベースアダプターを取得"""
    return get_database_adapter()


def is_sqlite() -> bool:
    """現在の設定がSQLiteかどうかを確認"""
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()
    return database_type == "sqlite"


def is_postgres() -> bool:
    """現在の設定がPostgreSQLかどうかを確認"""
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()
    return database_type in ("postgres", "postgresql")

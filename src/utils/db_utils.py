"""データベース操作の共通ユーティリティ

SQLiteとPostgreSQLの両方をサポートする共通インターフェースを提供します。
"""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.config import get_database_type, get_db_path
from src.database import get_database_adapter
from src.database.base import DatabaseAdapter
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


@contextmanager
def get_db_connection(
    db_path: str | Path | None = None, optimize: bool = True
) -> Iterator[sqlite3.Connection | DatabaseAdapter]:
    """データベース接続のコンテキストマネージャー

    Args:
        db_path: データベースファイルパス（SQLiteの場合のみ使用）
        optimize: パフォーマンス最適化を行うかどうか（SQLiteの場合のみ）

    Yields:
        Union[sqlite3.Connection, DatabaseAdapter]: データベース接続

    Example:
        with get_db_connection() as conn:
            # SQLiteの場合
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM prices")
            # PostgreSQLの場合（アダプター経由）
            cursor = conn.execute("SELECT * FROM prices")
    """
    database_type = get_database_type()

    # PostgreSQLの場合はアダプターを使用
    if database_type in ("postgres", "postgresql"):
        adapter = get_database_adapter()
        try:
            adapter.connect()
            yield adapter
        except Exception as e:
            logger.error(f"Database error: {e}")
            adapter.rollback()
            raise
        finally:
            adapter.disconnect()
    else:
        # SQLiteの場合は従来通り
        if db_path is None:
            db_path = get_db_path()

        conn = sqlite3.connect(db_path)
        try:
            if optimize:
                # パフォーマンス最適化設定
                conn.execute("PRAGMA cache_size = -64000")  # 64MB
                conn.execute("PRAGMA temp_store = MEMORY")
                conn.execute("PRAGMA mmap_size = 268435456")  # 256MB
                conn.execute("PRAGMA synchronous = NORMAL")
                conn.execute("PRAGMA journal_mode = WAL")

            yield conn
        except Exception as e:
            logger.error(f"Database error: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()


def execute_query(
    query: str,
    params: tuple[Any, ...] = (),
    db_path: str | Path | None = None,
) -> list[tuple]:
    """単一のクエリを実行して結果を返す

    Args:
        query: 実行するSQL文
        params: クエリパラメータ
        db_path: データベースファイルパス

    Returns:
        クエリ結果のリスト
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()


def execute_many(
    query: str,
    params_list: list[tuple[Any, ...]],
    db_path: str | Path | None = None,
    batch_size: int = 1000,
) -> None:
    """複数のパラメータで同じクエリを実行

    Args:
        query: 実行するSQL文
        params_list: パラメータのリスト
        db_path: データベースファイルパス
        batch_size: バッチサイズ
    """
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()

        # バッチ処理
        for i in range(0, len(params_list), batch_size):
            batch = params_list[i : i + batch_size]
            cursor.executemany(query, batch)
            conn.commit()
            logger.debug(f"Processed batch {i // batch_size + 1}")


def upsert_dataframe(
    conn: sqlite3.Connection,
    df: Any,  # DataFrame
    table_name: str,
    unique_columns: list[str],
) -> None:
    """DataFrameの内容をUPSERT（INSERT OR REPLACE）する

    Args:
        conn: データベース接続
        df: 保存するDataFrame
        table_name: テーブル名
        unique_columns: ユニーク制約のカラム名リスト
    """
    if df.empty:
        return

    columns = df.columns.tolist()
    placeholders = ", ".join(["?" for _ in columns])
    column_names = ", ".join(columns)

    # INSERT OR REPLACE文を構築
    query = f"""
        INSERT OR REPLACE INTO {table_name} ({column_names})
        VALUES ({placeholders})
    """

    # DataFrameをタプルのリストに変換
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]

    cursor = conn.cursor()
    cursor.executemany(query, values)
    conn.commit()

    logger.info(f"Upserted {len(values)} records to {table_name}")


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """テーブルが存在するかチェック

    Args:
        conn: データベース接続
        table_name: テーブル名

    Returns:
        テーブルが存在する場合True
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """,
        (table_name,),
    )
    return cursor.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    """テーブルのカラム名リストを取得

    Args:
        conn: データベース接続
        table_name: テーブル名

    Returns:
        カラム名のリスト
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


def vacuum_database(db_path: str | Path | None = None) -> None:
    """データベースをVACUUM（最適化）する

    Args:
        db_path: データベースファイルパス
    """
    if db_path is None:
        db_path = get_db_path()

    # VACUUMはトランザクション内で実行できないため、別途接続
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("VACUUM")
        logger.info("Database vacuum completed")
    finally:
        conn.close()


# 新しいアダプター互換の関数
@contextmanager
def get_db_adapter() -> Iterator[DatabaseAdapter]:
    """データベースアダプターを取得するコンテキストマネージャー

    PostgreSQLとSQLiteの両方で統一的なインターフェースを提供します。

    Yields:
        DatabaseAdapter: データベースアダプター

    Example:
        with get_db_adapter() as db:
            cursor = db.execute("SELECT * FROM prices WHERE code = ?", ("1234",))
            rows = db.fetchall(cursor)
    """
    adapter = get_database_adapter()
    try:
        adapter.connect()
        yield adapter
        adapter.commit()
    except Exception as e:
        logger.error(f"Database error: {e}")
        adapter.rollback()
        raise
    finally:
        adapter.disconnect()


def execute_adapter_query(query: str, params: tuple | dict | None = None) -> list[dict]:
    """アダプター経由でクエリを実行して結果を辞書形式で返す

    Args:
        query: 実行するSQL文
        params: クエリパラメータ

    Returns:
        クエリ結果の辞書のリスト
    """
    with get_db_adapter() as db:
        cursor = db.execute(query, params)
        rows = db.fetchall(cursor)

        # 結果を辞書形式に変換
        if rows and hasattr(rows[0], "keys"):
            # PostgreSQLの場合（RealDictCursor）
            return [dict(row) for row in rows]
        elif rows and hasattr(cursor, "description"):
            # SQLiteの場合
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row, strict=False)) for row in rows]
        else:
            return []

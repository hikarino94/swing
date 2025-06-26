"""データベース関連のユーティリティ"""
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, List, Tuple, Any, Iterator
import logging
from .exceptions import DatabaseError

logger = logging.getLogger(__name__)


class DatabaseManager:
    """データベース管理クラス"""
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Args:
            db_path: データベースファイルパス。Noneの場合はデフォルトパス
        """
        if db_path is None:
            self.db_path = Path(__file__).resolve().parents[1] / "db" / "stock.db"
        else:
            self.db_path = Path(db_path)
        
        # ディレクトリが存在しない場合は作成
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"DatabaseManager initialized with path: {self.db_path}")
    
    @contextmanager
    def get_connection(self) -> Iterator[sqlite3.Connection]:
        """データベース接続のコンテキストマネージャー
        
        Yields:
            データベース接続オブジェクト
            
        Raises:
            DatabaseError: 接続エラーが発生した場合
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # 辞書形式でアクセス可能に
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            raise DatabaseError(f"データベース接続エラー: {e}")
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """トランザクション管理付き接続
        
        Yields:
            データベース接続オブジェクト
            
        Raises:
            DatabaseError: トランザクションエラーが発生した場合
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            if conn:
                conn.rollback()
                logger.warning("Transaction rolled back due to error")
            logger.error(f"Transaction error: {e}")
            raise DatabaseError(f"トランザクションエラー: {e}")
        finally:
            if conn:
                conn.close()
    
    def execute_many(
        self,
        sql: str,
        data: List[Tuple[Any, ...]],
        batch_size: int = 1000
    ) -> int:
        """バッチ処理の共通実装
        
        Args:
            sql: 実行するSQL文
            data: パラメータのリスト
            batch_size: バッチサイズ
            
        Returns:
            影響を受けた行数
            
        Raises:
            DatabaseError: 実行エラーが発生した場合
        """
        total_rows = 0
        try:
            with self.transaction() as conn:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    conn.executemany(sql, batch)
                    total_rows += conn.total_changes
                    logger.debug(f"Processed batch {i//batch_size + 1}: {len(batch)} rows")
            
            logger.info(f"Executed batch insert: {total_rows} rows affected")
            return total_rows
        except sqlite3.Error as e:
            logger.error(f"Batch execution error: {e}")
            raise DatabaseError(f"バッチ実行エラー: {e}")
    
    def execute_query(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[sqlite3.Row]:
        """クエリを実行して結果を返す
        
        Args:
            sql: 実行するSQL文
            params: SQLパラメータ
            
        Returns:
            クエリ結果のリスト
            
        Raises:
            DatabaseError: クエリ実行エラーが発生した場合
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Query execution error: {e}")
            raise DatabaseError(f"クエリ実行エラー: {e}")
    
    def execute_scalar(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> Any:
        """単一の値を返すクエリを実行
        
        Args:
            sql: 実行するSQL文
            params: SQLパラメータ
            
        Returns:
            クエリ結果の最初の値
            
        Raises:
            DatabaseError: クエリ実行エラーが発生した場合
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                result = cursor.fetchone()
                return result[0] if result else None
        except sqlite3.Error as e:
            logger.error(f"Scalar query error: {e}")
            raise DatabaseError(f"スカラークエリエラー: {e}")
    
    def table_exists(self, table_name: str) -> bool:
        """テーブルの存在確認
        
        Args:
            table_name: テーブル名
            
        Returns:
            テーブルが存在する場合True
        """
        sql = """
        SELECT COUNT(*) FROM sqlite_master 
        WHERE type='table' AND name=?
        """
        count = self.execute_scalar(sql, (table_name,))
        return count > 0


# デフォルトのデータベースマネージャー
_default_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """デフォルトのDatabaseManagerインスタンスを取得"""
    global _default_db_manager
    if _default_db_manager is None:
        _default_db_manager = DatabaseManager()
    return _default_db_manager


@contextmanager
def get_db_connection(db_path: Optional[Path] = None) -> Iterator[sqlite3.Connection]:
    """データベース接続の簡易取得関数
    
    Args:
        db_path: データベースファイルパス
        
    Yields:
        データベース接続オブジェクト
    """
    if db_path:
        manager = DatabaseManager(db_path)
    else:
        manager = get_db_manager()
    
    with manager.get_connection() as conn:
        yield conn
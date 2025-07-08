"""取引テーブルに詳細タイプと決済損益フィールドを追加するマイグレーション"""

import sqlite3

from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def migrate():
    """取引テーブルに新しいフィールドを追加"""
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    try:
        # 既存のカラムを確認
        cursor.execute("PRAGMA table_info(transactions)")
        columns = [col[1] for col in cursor.fetchall()]

        # detailed_typeカラムの追加
        if "detailed_type" not in columns:
            cursor.execute(
                """
                ALTER TABLE transactions
                ADD COLUMN detailed_type TEXT
            """
            )
            logger.info("detailed_typeカラムを追加しました")

        # realized_profitカラムの追加
        if "realized_profit" not in columns:
            cursor.execute(
                """
                ALTER TABLE transactions
                ADD COLUMN realized_profit REAL
            """
            )
            logger.info("realized_profitカラムを追加しました")

        conn.commit()
        logger.info("マイグレーション完了")

    except sqlite3.Error as e:
        logger.error(f"マイグレーションエラー: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()

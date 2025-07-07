"""保有銘柄テーブルにaccount_typeカラムを追加するマイグレーション"""

import sqlite3

from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("db.migrate")


def add_account_type_column():
    """holdingsテーブルにaccount_typeカラムを追加"""
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    try:
        # テーブル情報を取得
        cursor.execute("PRAGMA table_info(holdings)")
        columns = [col[1] for col in cursor.fetchall()]

        # account_typeカラムが存在しない場合のみ追加
        if "account_type" not in columns:
            logger.info("account_typeカラムを追加します")
            cursor.execute(
                """
                ALTER TABLE holdings
                ADD COLUMN account_type TEXT DEFAULT '特定'
            """
            )

            # 既存レコードのaccount_typeを設定
            cursor.execute(
                """
                UPDATE holdings
                SET account_type = '特定'
                WHERE account_type IS NULL
            """
            )

            conn.commit()
            logger.info("account_typeカラムの追加が完了しました")
        else:
            logger.info("account_typeカラムは既に存在します")

    except Exception as e:
        logger.error(f"マイグレーションエラー: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    add_account_type_column()

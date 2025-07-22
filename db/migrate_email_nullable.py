#!/usr/bin/env python
"""
メールアドレスカラムをNULL許可に変更するマイグレーションスクリプト
"""

import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("migrate_email_nullable")


def migrate():
    """メールアドレスカラムをNULL許可に変更"""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # 現在のテーブル構造を確認
        cursor.execute("PRAGMA table_info(users)")
        columns = cursor.fetchall()

        # emailカラムが存在するか確認
        email_column = None
        for col in columns:
            if col[1] == "email":
                email_column = col
                break

        if not email_column:
            logger.error("emailカラムが見つかりません")
            return False

        # NOT NULL制約があるか確認（col[3]が1ならNOT NULL）
        if email_column[3] == 0:
            logger.info("emailカラムは既にNULL許可されています")
            return True

        logger.info("emailカラムをNULL許可に変更します...")

        # SQLiteではALTER TABLEで制約を直接変更できないため、
        # テーブルを再作成する必要があります

        # トランザクション開始
        conn.execute("BEGIN TRANSACTION")

        # 新しいテーブルを作成
        cursor.execute(
            """
            CREATE TABLE users_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'trader',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """
        )

        # データをコピー
        cursor.execute(
            """
            INSERT INTO users_new (id, username, email, password_hash, role, created_at, updated_at)
            SELECT id, username, email, password_hash, role, created_at, updated_at
            FROM users
        """
        )

        # 古いテーブルを削除
        cursor.execute("DROP TABLE users")

        # 新しいテーブルをリネーム
        cursor.execute("ALTER TABLE users_new RENAME TO users")

        # インデックスを再作成
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")

        # コミット
        conn.commit()
        logger.info("マイグレーション完了")
        return True

    except Exception as e:
        logger.error(f"マイグレーションエラー: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    if migrate():
        print("マイグレーションが正常に完了しました")
        sys.exit(0)
    else:
        print("マイグレーションに失敗しました")
        sys.exit(1)

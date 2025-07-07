#!/usr/bin/env python
"""
sessionsテーブルにremember_meカラムを追加するマイグレーションスクリプト
"""

import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("db_migration")


def add_remember_me_column():
    """既存のsessionsテーブルにremember_meカラムを追加"""
    try:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # カラムが既に存在するか確認
        cursor.execute("PRAGMA table_info(sessions)")
        columns = [column[1] for column in cursor.fetchall()]

        if "remember_me" not in columns:
            logger.info("remember_meカラムを追加します...")
            cursor.execute(
                """
                ALTER TABLE sessions
                ADD COLUMN remember_me INTEGER DEFAULT 0
            """
            )
            conn.commit()
            logger.info("remember_meカラムの追加が完了しました")
        else:
            logger.info("remember_meカラムは既に存在します")

        conn.close()

    except Exception as e:
        logger.error(f"マイグレーションエラー: {str(e)}")
        raise


if __name__ == "__main__":
    add_remember_me_column()

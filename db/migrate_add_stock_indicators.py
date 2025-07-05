#!/usr/bin/env python
"""
holdingsテーブルに株価指標カラムを追加するマイグレーションスクリプト

Usage:
    python db/migrate_add_stock_indicators.py
"""

import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import DB_PATH
from src.utils.logging_config import get_logger

logger = get_logger("migrate_stock_indicators")


def add_stock_indicator_columns():
    """holdingsテーブルに株価指標カラムを追加"""

    # 追加するカラムの定義
    new_columns = [
        ("expected_per", "REAL"),
        ("actual_pbr", "REAL"),
        ("dividend_yield", "REAL"),
        ("expected_eps", "REAL"),
        ("actual_bps", "REAL"),
        ("expected_dividend", "REAL"),
        ("lending_type", "TEXT"),
    ]

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # 既存のカラムを確認
            cursor.execute("PRAGMA table_info(holdings)")
            existing_columns = {row[1] for row in cursor.fetchall()}

            # 新しいカラムを追加
            for column_name, column_type in new_columns:
                if column_name not in existing_columns:
                    sql = f"ALTER TABLE holdings ADD COLUMN {column_name} {column_type}"
                    cursor.execute(sql)
                    logger.info(f"Added column: {column_name} ({column_type})")
                else:
                    logger.info(f"Column already exists: {column_name}")

            conn.commit()
            logger.info("Migration completed successfully")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def verify_schema():
    """スキーマが正しく更新されたか確認"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(holdings)")
            columns = cursor.fetchall()

            logger.info("\nCurrent holdings table schema:")
            for col in columns:
                logger.info(f"  {col[1]:20} {col[2]:10}")

    except Exception as e:
        logger.error(f"Schema verification failed: {e}")


if __name__ == "__main__":
    logger.info("Starting holdings table migration...")
    add_stock_indicator_columns()
    verify_schema()

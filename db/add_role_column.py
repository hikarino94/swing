#!/usr/bin/env python3
"""
usersテーブルにroleカラムを追加するマイグレーションスクリプト
"""

import sqlite3
import sys
from pathlib import Path

# プロジェクトルートをPYTHONPATHに追加
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.config import get_db_path


def add_role_column():
    """usersテーブルにroleカラムを追加"""
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    try:
        # roleカラムが既に存在するか確認
        cursor.execute("PRAGMA table_info(users)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]

        if "role" in column_names:
            print("roleカラムは既に存在します")
            return

        # roleカラムを追加（デフォルトは'admin'）
        cursor.execute(
            """
            ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'admin'
        """
        )

        conn.commit()
        print("roleカラムを正常に追加しました")

        # 既存のユーザーのロールを設定
        cursor.execute("UPDATE users SET role = 'admin' WHERE role IS NULL")
        conn.commit()
        print("既存ユーザーのロールを'admin'に設定しました")

    except sqlite3.Error as e:
        print(f"エラーが発生しました: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    add_role_column()

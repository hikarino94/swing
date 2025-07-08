#!/usr/bin/env python
"""
インデックスの最適化とデータベースのVACUUM/ANALYZEを実行
"""
import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import DB_PATH


def optimize_database(db_path):
    """データベースの最適化を実行"""
    conn = sqlite3.connect(db_path)

    print("データベースの最適化を開始します...")

    # ANALYZE実行（統計情報の更新）
    print("1. 統計情報を更新中...")
    conn.execute("ANALYZE")

    # VACUUM実行（データベースの再構築）
    print("2. データベースを再構築中...")
    conn.execute("VACUUM")

    # インデックスの再構築
    print("3. インデックスを再構築中...")
    indexes = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()

    for idx_name, _ in indexes:
        print(f"   - {idx_name}")
        conn.execute(f"REINDEX {idx_name}")

    conn.close()
    print("データベースの最適化が完了しました。")


if __name__ == "__main__":
    optimize_database(DB_PATH)

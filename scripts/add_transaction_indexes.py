#!/usr/bin/env python3
"""
取引履歴テーブルに最適化用のインデックスを追加するスクリプト
"""

import os
import sqlite3
import sys
from pathlib import Path

# プロジェクトのルートディレクトリをPythonパスに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# db/stock.db のパスを取得
db_path = os.path.join(project_root, "db", "stock.db")


def add_indexes():
    """取引履歴テーブルにインデックスを追加"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # 既存のインデックスを確認
        cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'index' AND tbl_name = 'transactions'
        """
        )
        existing_indexes = {row[0] for row in cursor.fetchall()}
        print(f"既存のインデックス: {existing_indexes}")

        # インデックスの定義
        indexes = [
            # ユーザーと日付でのクエリ最適化
            (
                "idx_transactions_user_date",
                "CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, transaction_date DESC)",
            ),
            # ユーザーと銘柄でのクエリ最適化
            (
                "idx_transactions_user_code",
                "CREATE INDEX IF NOT EXISTS idx_transactions_user_code ON transactions(user_id, code)",
            ),
            # ユーザー、銘柄、日付の複合インデックス
            (
                "idx_transactions_user_code_date",
                "CREATE INDEX IF NOT EXISTS idx_transactions_user_code_date ON transactions(user_id, code, transaction_date DESC)",
            ),
        ]

        # インデックスを作成
        created = 0
        for index_name, create_sql in indexes:
            if index_name not in existing_indexes:
                print(f"インデックスを作成中: {index_name}")
                cursor.execute(create_sql)
                created += 1
                print(f"インデックスを作成しました: {index_name}")
            else:
                print(f"インデックスは既に存在します: {index_name}")

        conn.commit()
        print(f"インデックス作成完了: {created}個のインデックスを作成しました")

        # ANALYZE を実行して統計情報を更新
        print("データベース統計情報を更新中...")
        cursor.execute("ANALYZE")
        conn.commit()
        print("データベース統計情報を更新しました")

    except Exception as e:
        print(f"インデックス作成エラー: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        add_indexes()
        print("インデックスの追加が完了しました")
    except Exception as e:
        print(f"エラー: {str(e)}")
        sys.exit(1)

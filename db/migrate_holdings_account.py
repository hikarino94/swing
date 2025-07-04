#!/usr/bin/env python3
"""
保有銘柄テーブルにaccount_nameカラムを追加するマイグレーションスクリプト
"""

import sqlite3
from pathlib import Path

# データベースパスを取得
DB_PATH = Path(__file__).parent.parent / "db" / "stock.db"


def migrate():
    """既存のholdingsテーブルにaccount_nameカラムを追加"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # 現在のテーブル構造を確認
        cursor.execute("PRAGMA table_info(holdings)")
        columns = [row[1] for row in cursor.fetchall()]

        if "account_name" not in columns:
            print("account_nameカラムを追加します...")

            # 一時テーブルを作成
            cursor.execute(
                """
                CREATE TABLE holdings_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    account_name TEXT NOT NULL DEFAULT 'default',
                    quantity INTEGER NOT NULL,
                    average_price REAL NOT NULL,
                    market_value REAL,
                    profit_loss REAL,
                    profit_loss_ratio REAL,
                    updated_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE(user_id, code, account_name)
                )
            """
            )

            # データを移行（既存データにはデフォルト口座名を設定）
            cursor.execute(
                """
                INSERT INTO holdings_new (id, user_id, code, account_name, quantity,
                                        average_price, market_value, profit_loss,
                                        profit_loss_ratio, updated_at)
                SELECT id, user_id, code, 'default', quantity, average_price,
                       market_value, profit_loss, profit_loss_ratio, updated_at
                FROM holdings
            """
            )

            # 古いテーブルを削除
            cursor.execute("DROP TABLE holdings")

            # 新しいテーブルをリネーム
            cursor.execute("ALTER TABLE holdings_new RENAME TO holdings")

            # インデックスを再作成
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_holdings_user_id ON holdings(user_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_holdings_code ON holdings(code)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_holdings_account ON holdings(account_name)"
            )

            conn.commit()
            print("マイグレーションが完了しました。")
        else:
            print("account_nameカラムは既に存在します。")

    except Exception as e:
        conn.rollback()
        print(f"エラーが発生しました: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    if DB_PATH.exists():
        migrate()
    else:
        print(f"データベースファイルが見つかりません: {DB_PATH}")

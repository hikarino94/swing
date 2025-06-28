#!/usr/bin/env python3
"""データベースロック解除ツール"""

import os
import sqlite3
import sys


def unlock_database():
    """データベースのロックを解除"""
    db_path = "data/db/stock.db"

    print("データベースロック解除処理を開始します...")

    # WALファイルの存在確認
    wal_path = f"{db_path}-wal"
    shm_path = f"{db_path}-shm"

    wal_exists = os.path.exists(wal_path)
    shm_exists = os.path.exists(shm_path)

    print(f"WALファイル: {'存在' if wal_exists else 'なし'}")
    print(f"SHMファイル: {'存在' if shm_exists else 'なし'}")

    if wal_exists:
        # WALファイルのサイズ確認
        wal_size = os.path.getsize(wal_path) / 1024 / 1024
        print(f"WALファイルサイズ: {wal_size:.2f} MB")

    try:
        # データベースに接続してチェックポイントを実行
        print("チェックポイント実行中...")
        conn = sqlite3.connect(db_path, timeout=5.0)

        # WALモードの確認
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        print(f"現在のジャーナルモード: {mode}")

        # チェックポイント実行
        result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        print(f"チェックポイント結果: {result}")

        # 接続を閉じる
        conn.close()
        print("✅ データベース接続をクローズしました")

        # WALファイルが削除されたか確認
        if not os.path.exists(wal_path):
            print("✅ WALファイルが正常に削除されました")
        else:
            print("⚠️  WALファイルがまだ存在します")

            # 手動削除を試みる
            response = input("WALファイルを手動で削除しますか？ (y/N): ")
            if response.lower() == "y":
                try:
                    if os.path.exists(wal_path):
                        os.remove(wal_path)
                        print(f"✅ {wal_path} を削除しました")
                    if os.path.exists(shm_path):
                        os.remove(shm_path)
                        print(f"✅ {shm_path} を削除しました")
                except Exception as e:
                    print(f"❌ ファイル削除エラー: {e}")
                    return False

        # 最終確認
        conn = sqlite3.connect(db_path, timeout=1.0)
        conn.execute("BEGIN IMMEDIATE")
        conn.rollback()
        conn.close()
        print("✅ データベースのロックが解除されました")
        return True

    except sqlite3.OperationalError as e:
        print(f"❌ データベースエラー: {e}")

        # 強制的なWALファイル削除
        if "database is locked" in str(e) or "locking protocol" in str(e):
            print("\n⚠️  データベースがロックされています")
            print("以下の手順を試してください：")
            print("1. すべてのPythonプロセスを終了")
            print("2. WAL/SHMファイルを手動で削除")
            print(f"   rm {wal_path}")
            print(f"   rm {shm_path}")
        return False

    except Exception as e:
        print(f"❌ 予期しないエラー: {e}")
        return False


if __name__ == "__main__":
    success = unlock_database()
    sys.exit(0 if success else 1)

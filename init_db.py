#!/usr/bin/env python3
"""
データベース初期化スクリプト for Render deployment
"""
import sys
from pathlib import Path

# プロジェクトルートをPYTHONPATHに追加
sys.path.append(str(Path(__file__).resolve().parent))

from db.db_schema import init_schema
from src.config import DB_PATH


def main():
    """データベースを初期化"""
    db_path = Path(DB_PATH)

    # ディレクトリが存在しない場合は作成
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Initializing database at: {db_path}")
    init_schema(db_path)
    print("Database initialized successfully!")


if __name__ == "__main__":
    main()

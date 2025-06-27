#!/usr/bin/env python3
"""
SQLite データベース閲覧ユーティリティ

SQLite BrowserがGUIで開けない場合の代替手段として、
コマンドラインでデータベースの内容を確認できます。
"""

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.utils.db_utils import get_db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class DBViewer:
    """データベース閲覧クラス"""

    def __init__(self, db_path: Optional[Path] = None):
        """
        Args:
            db_path: データベースファイルパス
        """
        self.db_manager = get_db_manager() if db_path is None else get_db_manager()
        if db_path:
            self.db_manager.db_path = Path(db_path)

    def show_tables(self) -> None:
        """テーブル一覧を表示"""
        print("📊 データベーステーブル一覧:")
        print("-" * 50)

        sql = """
        SELECT name, sql FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """

        try:
            tables = self.db_manager.execute_query(sql)

            for table in tables:
                print(f"テーブル名: {table['name']}")
                print(f"作成SQL: {table['sql'][:100]}...")
                print()

        except Exception as e:
            logger.error(f"テーブル一覧取得エラー: {e}")

    def show_table_info(self, table_name: str) -> None:
        """テーブルの詳細情報を表示"""
        print(f"📋 テーブル '{table_name}' の詳細:")
        print("-" * 50)

        try:
            # カラム情報
            pragma_sql = f"PRAGMA table_info({table_name})"
            columns = self.db_manager.execute_query(pragma_sql)

            print("カラム情報:")
            for col in columns:
                print(f"  {col['name']} ({col['type']}) {'NOT NULL' if col['notnull'] else 'NULL可'}")
            print()

            # 行数
            count_sql = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = self.db_manager.execute_query(count_sql)
            row_count = count_result[0]["count"] if count_result else 0
            print(f"総行数: {row_count:,} 行")
            print()

        except Exception as e:
            logger.error(f"テーブル情報取得エラー: {e}")

    def show_sample_data(self, table_name: str, limit: int = 10) -> None:
        """サンプルデータを表示"""
        print(f"📄 テーブル '{table_name}' のサンプルデータ（最初の{limit}行）:")
        print("-" * 80)

        try:
            sql = f"SELECT * FROM {table_name} LIMIT {limit}"
            rows = self.db_manager.execute_query(sql)

            if not rows:
                print("データがありません")
                return

            # DataFrameに変換して表示
            df = pd.DataFrame([dict(row) for row in rows])
            print(df.to_string(index=False))

        except Exception as e:
            logger.error(f"サンプルデータ取得エラー: {e}")

    def execute_query(self, query: str) -> None:
        """任意のクエリを実行"""
        print(f"🔍 クエリ実行: {query}")
        print("-" * 80)

        try:
            rows = self.db_manager.execute_query(query)

            if not rows:
                print("結果がありません")
                return

            # DataFrameに変換して表示
            df = pd.DataFrame([dict(row) for row in rows])
            print(df.to_string(index=False))

        except Exception as e:
            logger.error(f"クエリ実行エラー: {e}")

    def show_recent_data(self, table_name: str, date_column: str = "date", limit: int = 10) -> None:
        """最新のデータを表示"""
        print(f"📅 テーブル '{table_name}' の最新データ（{limit}行）:")
        print("-" * 80)

        try:
            sql = f"""
            SELECT * FROM {table_name}
            ORDER BY {date_column} DESC
            LIMIT {limit}
            """
            rows = self.db_manager.execute_query(sql)

            if not rows:
                print("データがありません")
                return

            # DataFrameに変換して表示
            df = pd.DataFrame([dict(row) for row in rows])
            print(df.to_string(index=False))

        except Exception as e:
            logger.error(f"最新データ取得エラー: {e}")


def main():
    """メイン関数"""
    import argparse

    parser = argparse.ArgumentParser(description="SQLite データベース閲覧ツール")
    parser.add_argument("--db", help="データベースファイルパス")
    parser.add_argument("--tables", action="store_true", help="テーブル一覧を表示")
    parser.add_argument("--info", help="指定テーブルの詳細情報を表示")
    parser.add_argument("--sample", help="指定テーブルのサンプルデータを表示")
    parser.add_argument("--recent", help="指定テーブルの最新データを表示")
    parser.add_argument("--limit", type=int, default=10, help="表示行数制限")
    parser.add_argument("--query", help="任意のSQLクエリを実行")

    args = parser.parse_args()

    # DBViewerインスタンス作成
    viewer = DBViewer(args.db)

    if args.tables:
        viewer.show_tables()
    elif args.info:
        viewer.show_table_info(args.info)
    elif args.sample:
        viewer.show_sample_data(args.sample, args.limit)
    elif args.recent:
        viewer.show_recent_data(args.recent, limit=args.limit)
    elif args.query:
        viewer.execute_query(args.query)
    else:
        # デフォルト: テーブル一覧を表示
        viewer.show_tables()
        print("\n💡 使用例:")
        print("  python db_viewer.py --tables              # テーブル一覧")
        print("  python db_viewer.py --info prices         # pricesテーブル詳細")
        print("  python db_viewer.py --sample prices       # pricesテーブルサンプル")
        print("  python db_viewer.py --recent prices       # prices最新データ")
        print("  python db_viewer.py --query 'SELECT * FROM prices WHERE code=\"1301\" LIMIT 5'")


if __name__ == "__main__":
    main()

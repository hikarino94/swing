#!/usr/bin/env python
"""
テクニカルスクリーニング高速化のためのデータベースインデックス作成スクリプト
"""
import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import DB_PATH


def create_indexes(db_path=DB_PATH):
    """データベースインデックスを作成"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 作成するインデックスのリスト
    indexes = [
        # prices テーブルのインデックス
        ("idx_prices_code_date", "prices", "(code, date)"),
        ("idx_prices_date", "prices", "(date)"),
        # technical_indicators テーブルのインデックス
        (
            "idx_tech_indicators_code_date",
            "technical_indicators",
            "(code, signal_date)",
        ),
        ("idx_tech_indicators_date", "technical_indicators", "(signal_date)"),
        (
            "idx_tech_indicators_signals",
            "technical_indicators",
            "(signal_date, signals_count, signals_short_count)",
        ),
        # listed_info テーブルのインデックス
        ("idx_listed_info_market", "listed_info", "(market_code)"),
    ]

    print("データベースインデックスの作成を開始します...")

    for index_name, table_name, columns in indexes:
        try:
            # インデックスが既に存在するか確認
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                (index_name,),
            )
            if cursor.fetchone():
                print(f"  ✓ {index_name} は既に存在します")
            else:
                # インデックスを作成
                query = f"CREATE INDEX {index_name} ON {table_name} {columns}"
                cursor.execute(query)
                print(f"  ✓ {index_name} を作成しました")
        except Exception as e:
            print(f"  ✗ {index_name} の作成に失敗しました: {e}")

    # データベース統計情報を更新
    print("\nデータベース統計情報を更新中...")
    cursor.execute("ANALYZE")

    conn.commit()
    conn.close()

    print("\nインデックス作成が完了しました")


if __name__ == "__main__":
    create_indexes()

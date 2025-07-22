"""取引日数の確認スクリプト"""

import sqlite3

from src.config import get_db_path


def check_trading_days():
    """各月の取引日数を確認"""
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        print("=== 2025年 月別の取引日数 ===")
        print(f"{'月':>3} {'先物日数':>8} {'株式日数':>8} {'実際の取引日数':>12}")
        print("-" * 40)

        for month in range(1, 13):
            start_date = f"2025-{month:02d}-01"
            if month == 12:
                end_date = "2026-01-01"
            else:
                end_date = f"2025-{month+1:02d}-01"

            # 先物の取引日数
            cursor.execute(
                """
                SELECT COUNT(DISTINCT trade_date)
                FROM daytrade_futures
                WHERE user_id = 1 AND trade_date >= ? AND trade_date < ?
            """,
                (start_date, end_date),
            )
            futures_days = cursor.fetchone()[0]

            # 株式の取引日数
            cursor.execute(
                """
                SELECT COUNT(DISTINCT trade_date)
                FROM daytrade_stocks
                WHERE user_id = 1 AND trade_date >= ? AND trade_date < ?
                  AND (trade_type LIKE '%返済%' OR trade_type = '現物売却' OR trade_type = '配当金')
            """,
                (start_date, end_date),
            )
            stocks_days = cursor.fetchone()[0]

            # 実際の取引日数（先物と株式の全取引日をユニークにカウント）
            cursor.execute(
                """
                SELECT COUNT(DISTINCT date) FROM (
                    SELECT trade_date as date FROM daytrade_futures
                    WHERE user_id = 1 AND trade_date >= ? AND trade_date < ?
                    UNION
                    SELECT trade_date as date FROM daytrade_stocks
                    WHERE user_id = 1 AND trade_date >= ? AND trade_date < ?
                      AND (trade_type LIKE '%返済%' OR trade_type = '現物売却' OR trade_type = '配当金')
                )
            """,
                (start_date, end_date, start_date, end_date),
            )
            actual_days = cursor.fetchone()[0]

            if futures_days > 0 or stocks_days > 0:
                print(
                    f"{month:3d} {futures_days:8d} {stocks_days:8d} {actual_days:12d}"
                )


if __name__ == "__main__":
    check_trading_days()

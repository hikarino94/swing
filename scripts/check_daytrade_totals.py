"""デイトレード損益の集計確認スクリプト"""

import sqlite3

from src.config import get_db_path


def check_stock_totals(year: int):
    """株式損益の集計を確認"""
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # 1. 月別の詳細集計
        print(f"\n=== {year}年 株式損益の月別詳細 ===")
        for month in range(1, 13):
            start_date = f"{year:04d}-{month:02d}-01"
            if month == 12:
                end_date = f"{year+1:04d}-01-01"
            else:
                end_date = f"{year:04d}-{month+1:02d}-01"

            # 月別集計と同じクエリ
            cursor.execute(
                """
                SELECT
                    COALESCE(SUM(CASE
                        WHEN trade_type != '配当金' THEN
                            COALESCE(capital_gains_tax, 0) + COALESCE(settlement_amount, 0)
                        ELSE 0
                    END), 0) as normal_profit,
                    COALESCE(SUM(COALESCE(day_trade_amount, 0)), 0) as day_trade_profit,
                    COALESCE(SUM(CASE
                        WHEN trade_type = '配当金' THEN
                            COALESCE(settlement_amount, 0)
                        ELSE 0
                    END), 0) as dividend_amount,
                    COUNT(*) as trade_count
                FROM daytrade_stocks
                WHERE user_id = 1 AND trade_date >= ? AND trade_date < ?
                  AND (trade_type LIKE '%返済%' OR trade_type = '現物売却' OR trade_type = '配当金')
            """,
                (start_date, end_date),
            )

            result = cursor.fetchone()
            if result and result[3] > 0:  # 取引がある月のみ表示
                normal = result[0]
                day_trade = result[1]
                dividend = result[2]
                total = normal + day_trade + dividend
                print(
                    f"{month}月: 通常={normal:,} + デイトレ={day_trade:,} + 配当={dividend:,} = 合計={total:,}"
                )

        # 2. 年間合計（月別と同じ方法）
        print("\n=== 月別集計の年間合計 ===")
        cursor.execute(
            """
            SELECT
                COALESCE(SUM(CASE
                    WHEN trade_type != '配当金' THEN
                        COALESCE(capital_gains_tax, 0) + COALESCE(settlement_amount, 0)
                    ELSE 0
                END), 0) as normal_profit,
                COALESCE(SUM(COALESCE(day_trade_amount, 0)), 0) as day_trade_profit,
                COALESCE(SUM(CASE
                    WHEN trade_type = '配当金' THEN
                        COALESCE(settlement_amount, 0)
                    ELSE 0
                END), 0) as dividend_amount
            FROM daytrade_stocks
            WHERE user_id = 1 AND trade_date >= ? AND trade_date < ?
              AND (trade_type LIKE '%返済%' OR trade_type = '現物売却' OR trade_type = '配当金')
        """,
            (f"{year}-01-01", f"{year+1}-01-01"),
        )

        result = cursor.fetchone()
        normal = result[0]
        day_trade = result[1]
        dividend = result[2]
        total = normal + day_trade + dividend
        print(
            f"通常={normal:,} + デイトレ={day_trade:,} + 配当={dividend:,} = 合計={total:,}"
        )

        # 3. 累積損益と同じ方法での集計
        print("\n=== 累積損益と同じ集計方法 ===")
        cursor.execute(
            """
            SELECT
                SUM(CASE
                    WHEN trade_type != '配当金' THEN
                        COALESCE(capital_gains_tax, 0) + COALESCE(settlement_amount, 0)
                    ELSE 0
                END) as daily_profit,
                SUM(COALESCE(day_trade_amount, 0)) as day_trade_profit,
                SUM(CASE
                    WHEN trade_type = '配当金' THEN
                        COALESCE(settlement_amount, 0)
                    ELSE 0
                END) as dividend_amount
            FROM daytrade_stocks
            WHERE user_id = 1 AND trade_date BETWEEN ? AND ?
              AND (trade_type LIKE '%返済%' OR trade_type = '現物売却' OR trade_type = '配当金')
        """,
            (f"{year}-01-01", f"{year}-12-31"),
        )

        result = cursor.fetchone()
        normal = result[0] or 0
        day_trade = result[1] or 0
        dividend = result[2] or 0
        total = normal + day_trade + dividend
        print(
            f"通常={normal:,} + デイトレ={day_trade:,} + 配当={dividend:,} = 合計={total:,}"
        )

        # 4. 全取引の確認（WHERE条件なし）
        print("\n=== 全取引での集計（フィルタなし） ===")
        cursor.execute(
            """
            SELECT
                SUM(COALESCE(capital_gains_tax, 0)) as total_tax,
                SUM(COALESCE(settlement_amount, 0)) as total_settlement,
                SUM(COALESCE(day_trade_amount, 0)) as total_day_trade,
                COUNT(*) as total_count,
                COUNT(DISTINCT trade_type) as trade_types
            FROM daytrade_stocks
            WHERE user_id = 1 AND trade_date BETWEEN ? AND ?
        """,
            (f"{year}-01-01", f"{year}-12-31"),
        )

        result = cursor.fetchone()
        print(f"税金合計={result[0]:,}")
        print(f"決済額合計={result[1]:,}")
        print(f"デイトレ合計={result[2]:,}")
        print(f"取引数={result[3]}")
        print(f"取引種別数={result[4]}")

        # 取引種別の内訳
        print("\n=== 取引種別の内訳 ===")
        cursor.execute(
            """
            SELECT trade_type, COUNT(*) as count,
                   SUM(COALESCE(capital_gains_tax, 0) + COALESCE(settlement_amount, 0) + COALESCE(day_trade_amount, 0)) as total
            FROM daytrade_stocks
            WHERE user_id = 1 AND trade_date BETWEEN ? AND ?
            GROUP BY trade_type
            ORDER BY count DESC
        """,
            (f"{year}-01-01", f"{year}-12-31"),
        )

        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]}件, 合計={row[2]:,}")


if __name__ == "__main__":
    check_stock_totals(2025)

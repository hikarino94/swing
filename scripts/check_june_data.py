"""6月の取引データを詳細確認"""

import sqlite3

from src.config import get_db_path


def check_june_data():
    """6月の取引データを詳細確認"""
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        print("=== 2025年6月の株式取引詳細 ===")

        # 1. 取引種別ごとの内訳
        cursor.execute(
            """
            SELECT
                trade_type,
                COUNT(*) as count,
                SUM(COALESCE(capital_gains_tax, 0)) as tax_total,
                SUM(COALESCE(settlement_amount, 0)) as settlement_total,
                SUM(COALESCE(day_trade_amount, 0)) as day_trade_total,
                SUM(COALESCE(capital_gains_tax, 0) + COALESCE(settlement_amount, 0) + COALESCE(day_trade_amount, 0)) as total
            FROM daytrade_stocks
            WHERE user_id = 1
              AND trade_date >= '2025-06-01'
              AND trade_date < '2025-07-01'
            GROUP BY trade_type
            ORDER BY total DESC
        """
        )

        print("\n取引種別ごとの内訳:")
        print(
            f"{'取引種別':<15} {'件数':>5} {'税金':>12} {'決済額':>12} {'デイトレ':>12} {'合計':>15}"
        )
        print("-" * 80)

        for row in cursor.fetchall():
            trade_type, count, tax, settlement, day_trade, total = row
            print(
                f"{trade_type:<15} {count:>5} {tax:>12,.0f} {settlement:>12,.0f} {day_trade:>12,.0f} {total:>15,.0f}"
            )

        # 2. 月別集計と同じ方法で集計
        print("\n\n=== 月別集計と同じ方法での6月集計 ===")
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
                COUNT(DISTINCT trade_date) as trading_days,
                COUNT(*) as total_trades
            FROM daytrade_stocks
            WHERE user_id = 1 AND trade_date >= '2025-06-01' AND trade_date < '2025-07-01'
              AND (trade_type LIKE '%返済%' OR trade_type = '現物売却' OR trade_type = '配当金')
        """
        )

        result = cursor.fetchone()
        normal = result[0]
        day_trade = result[1]
        dividend = result[2]
        total = normal + day_trade + dividend

        print(f"通常取引（配当除く）: {normal:,}")
        print(f"デイトレード: {day_trade:,}")
        print(f"配当金: {dividend:,}")
        print(f"合計: {total:,}")
        print(f"取引日数: {result[3]}")
        print(f"取引件数: {result[4]}")

        # 3. 日別の詳細
        print("\n\n=== 6月の日別損益 ===")
        cursor.execute(
            """
            SELECT
                trade_date,
                COUNT(*) as count,
                SUM(CASE
                    WHEN trade_type != '配当金' THEN
                        COALESCE(capital_gains_tax, 0) + COALESCE(settlement_amount, 0)
                    ELSE 0
                END) as normal_profit,
                SUM(COALESCE(day_trade_amount, 0)) as day_trade,
                SUM(CASE
                    WHEN trade_type = '配当金' THEN
                        COALESCE(settlement_amount, 0)
                    ELSE 0
                END) as dividend
            FROM daytrade_stocks
            WHERE user_id = 1
              AND trade_date >= '2025-06-01'
              AND trade_date < '2025-07-01'
              AND (trade_type LIKE '%返済%' OR trade_type = '現物売却' OR trade_type = '配当金')
            GROUP BY trade_date
            ORDER BY trade_date
        """
        )

        print(
            f"{'日付':<12} {'件数':>5} {'通常取引':>12} {'デイトレ':>12} {'配当金':>12} {'日計':>15}"
        )
        print("-" * 80)

        for row in cursor.fetchall():
            date, count, normal, day_trade, dividend = row
            total = (normal or 0) + (day_trade or 0) + (dividend or 0)
            print(
                f"{date:<12} {count:>5} {normal:>12,.0f} {day_trade:>12,.0f} {dividend:>12,.0f} {total:>15,.0f}"
            )

        # 4. 先物の6月データも確認
        print("\n\n=== 2025年6月の先物取引 ===")
        cursor.execute(
            """
            SELECT
                COALESCE(SUM(profit_loss), 0) as total_profit,
                COUNT(DISTINCT trade_date) as trading_days,
                COUNT(*) as total_trades
            FROM daytrade_futures
            WHERE user_id = 1 AND trade_date >= '2025-06-01' AND trade_date < '2025-07-01'
        """
        )

        result = cursor.fetchone()
        print(f"先物損益: {result[0]:,}")
        print(f"取引日数: {result[1]}")
        print(f"取引件数: {result[2]}")


if __name__ == "__main__":
    check_june_data()

"""
デイトレード集計結果の確認スクリプト
"""

import sqlite3
import sys
from pathlib import Path

# プロジェクトルートを追加
sys.path.append(str(Path(__file__).parent.parent))

from src.config import get_db_path


def check_specific_dates():
    """特定日付の集計を確認"""
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # 指定された日付の集計
        test_dates = ["2024-05-01", "2024-07-22", "2024-07-14"]

        print("=== 指定日付の集計結果 ===")
        for date in test_dates:
            cursor.execute(
                """
                SELECT COUNT(*) as trade_count, SUM(profit_loss) as total_profit
                FROM daytrade_futures
                WHERE trade_date = ?
            """,
                (date,),
            )

            result = cursor.fetchone()
            count = result[0] if result else 0
            profit = result[1] if result[1] is not None else 0

            print(f"{date}: {count}件, {profit:+,.0f}円")

        # 詳細確認（5月の例）
        print("\n=== 2024年5月の詳細 ===")
        cursor.execute(
            """
            SELECT trade_date, trade_datetime, profit_loss
            FROM daytrade_futures
            WHERE trade_date BETWEEN '2024-04-30' AND '2024-05-02'
            ORDER BY trade_datetime
        """
        )

        for row in cursor.fetchall():
            print(f"{row[0]} {row[1]}: {row[2]:+,.0f}円")

        # 7月の詳細確認
        print("\n=== 2024年7月の詳細 ===")
        cursor.execute(
            """
            SELECT trade_date, COUNT(*) as count, SUM(profit_loss) as total
            FROM daytrade_futures
            WHERE trade_date BETWEEN '2024-07-01' AND '2024-07-31'
            GROUP BY trade_date
            ORDER BY trade_date
        """
        )

        for row in cursor.fetchall():
            if row[2] is not None and abs(row[2]) > 50000:  # 大きな損益のみ表示
                print(f"{row[0]}: {row[1]}件, {row[2]:+,.0f}円")


if __name__ == "__main__":
    check_specific_dates()

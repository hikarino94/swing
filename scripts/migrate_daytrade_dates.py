"""
デイトレード取引日の営業日調整移行スクリプト
"""

import sqlite3
import sys
from pathlib import Path

# プロジェクトルートを追加
sys.path.append(str(Path(__file__).parent.parent))

from src.config import get_db_path
from src.utils.business_day import parse_trade_datetime
from src.utils.logging_config import get_logger

logger = get_logger("migrate_daytrade_dates")


def migrate_futures_dates():
    """先物取引の取引日を営業日に調整"""
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # 既存の取引データを取得
        cursor.execute(
            """
            SELECT id, trade_datetime, trade_date
            FROM daytrade_futures
            ORDER BY id
        """
        )

        trades = cursor.fetchall()
        logger.info(f"先物取引: {len(trades)}件の取引日を調整します")

        updated_count = 0
        for trade_id, trade_datetime_str, current_trade_date in trades:
            try:
                # 営業日調整
                _, adjusted_date = parse_trade_datetime(trade_datetime_str)
                new_trade_date = adjusted_date.strftime("%Y-%m-%d")

                # 変更がある場合のみ更新
                if new_trade_date != current_trade_date:
                    cursor.execute(
                        """
                        UPDATE daytrade_futures
                        SET trade_date = ?
                        WHERE id = ?
                    """,
                        (new_trade_date, trade_id),
                    )

                    updated_count += 1
                    logger.info(
                        f"ID {trade_id}: {trade_datetime_str} -> "
                        f"{current_trade_date} => {new_trade_date}"
                    )

            except Exception as e:
                logger.error(f"ID {trade_id}の処理でエラー: {e}")

        conn.commit()
        logger.info(f"先物取引: {updated_count}件の取引日を更新しました")

        return updated_count


def verify_migration():
    """移行結果の検証"""
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # 具体例として挙げられた日付の集計を確認
        test_dates = ["2024-05-01", "2024-07-22", "2024-07-14"]

        for date in test_dates:
            cursor.execute(
                """
                SELECT SUM(profit_loss) as total_profit
                FROM daytrade_futures
                WHERE trade_date = ?
            """,
                (date,),
            )

            result = cursor.fetchone()
            profit = result[0] if result[0] is not None else 0

            logger.info(f"{date}: {profit:+,.0f}円")


def main():
    """メイン処理"""
    logger.info("取引日の営業日調整移行を開始します")

    # 先物取引の移行
    futures_count = migrate_futures_dates()

    # 移行結果の検証
    logger.info("\n=== 移行結果の検証 ===")
    verify_migration()

    logger.info(f"\n移行完了: 合計{futures_count}件の取引日を更新しました")


if __name__ == "__main__":
    main()

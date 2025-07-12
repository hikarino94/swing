"""取引履歴管理ロジック"""

import sqlite3

from src.config import get_db_path
from src.utils.logging_config import get_logger

from .models import Holding, Transaction

logger = get_logger("portfolio.transaction_manager")


class TransactionManager:
    """取引履歴管理クラス"""

    @staticmethod
    def import_transactions_from_csv(
        user_id: int, transactions_data: list[dict]
    ) -> int:
        """
        CSVデータから取引履歴をインポート（保有銘柄への反映なし）

        Args:
            user_id: ユーザーID
            transactions_data: 取引データのリスト

        Returns:
            インポート件数
        """
        # ユーザーIDを追加
        for trans in transactions_data:
            trans["user_id"] = user_id

        # 一括挿入
        imported_count = Transaction.bulk_insert(transactions_data)

        # 保有銘柄の再計算は行わない（ユーザーの要望により）
        # if imported_count > 0:
        #     TransactionManager.recalculate_holdings(user_id)

        return imported_count

    @staticmethod
    def recalculate_holdings(user_id: int) -> None:
        """
        取引履歴から保有銘柄を再計算（平均法）

        Args:
            user_id: ユーザーID
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 各銘柄の取引履歴を時系列で取得
            cursor.execute(
                """
                SELECT code
                FROM transactions
                WHERE user_id = ?
                GROUP BY code
            """,
                (user_id,),
            )

            codes = [row[0] for row in cursor.fetchall()]

            for code in codes:
                # 時系列で取引を取得（同日の場合はIDで順序を保証）
                cursor.execute(
                    """
                    SELECT id, transaction_date, transaction_type, quantity, price, commission
                    FROM transactions
                    WHERE user_id = ? AND code = ?
                    ORDER BY transaction_date ASC, id ASC
                """,
                    (user_id, code),
                )

                transactions = cursor.fetchall()

                # 平均法で計算
                total_quantity = 0
                total_cost = 0.0

                for (
                    trans_id,
                    _date,
                    trans_type,
                    quantity,
                    price,
                    commission,
                ) in transactions:
                    if trans_type == "buy":
                        # 買付時：総数量と総コストを増やす（手数料込み）
                        total_quantity += quantity
                        total_cost += quantity * price + (commission or 0)
                    else:  # sell
                        # 売却時：平均単価で総コストを減らす
                        if total_quantity > 0:
                            # 現在の平均取得価格を計算
                            avg_price = (
                                total_cost / total_quantity if total_quantity > 0 else 0
                            )

                            # 売却数量が保有数量を超える場合の処理（デイトレードなど）
                            sell_quantity = min(quantity, total_quantity)

                            total_quantity -= sell_quantity
                            total_cost -= sell_quantity * avg_price

                            # 売却数量が保有数量を超えていた場合の警告
                            if quantity > sell_quantity:
                                logger.warning(
                                    f"売却数量が保有数量を超過: {code} - 売却{quantity}株, 保有{sell_quantity}株（取引ID: {trans_id}）"
                                )
                        else:
                            logger.warning(
                                f"保有していない銘柄の売却: {code} - {quantity}株（取引ID: {trans_id}）"
                            )

                # 保有数量が残っている場合のみ保存
                if total_quantity > 0:
                    average_price = (
                        total_cost / total_quantity if total_quantity > 0 else 0
                    )

                    # 保有銘柄を更新
                    holding = Holding.find_by_user_and_code(user_id, code)
                    if holding is None:
                        holding = Holding(user_id=user_id, code=code)

                    holding.quantity = int(total_quantity)
                    holding.average_price = average_price

                    # 現在の株価を取得して時価評価を更新
                    # pricesテーブルは5桁（末尾0埋め）なので変換
                    code_5digit = code.ljust(5, "0")
                    cursor.execute(
                        """
                        SELECT close FROM prices
                        WHERE code = ?
                        ORDER BY date DESC
                        LIMIT 1
                    """,
                        (code_5digit,),
                    )
                    price_row = cursor.fetchone()

                    if price_row:
                        current_price = price_row[0]
                        holding.update_market_value(current_price)

                    holding.save()
                    logger.info(
                        f"保有銘柄更新: {code} - {total_quantity}株 @ {average_price:.2f}円"
                    )

            # 保有数量が0になった銘柄を削除
            cursor.execute(
                """
                DELETE FROM holdings
                WHERE user_id = ? AND code NOT IN (
                    SELECT code FROM transactions
                    WHERE user_id = ?
                    GROUP BY code
                    HAVING SUM(CASE WHEN transaction_type = 'buy' THEN quantity
                                    WHEN transaction_type = 'sell' THEN -quantity
                                    ELSE 0 END) > 0
                )
                AND deleted_at IS NULL
            """,
                (user_id, user_id),
            )

            conn.commit()
            logger.info(f"保有銘柄再計算完了: ユーザーID {user_id}")

        except sqlite3.Error as e:
            logger.error(f"保有銘柄再計算エラー: {e}")
            conn.rollback()
        finally:
            conn.close()

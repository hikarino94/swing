"""取引履歴モデル"""

import sqlite3
from typing import Any

from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("portfolio.models.transaction")


class Transaction:
    """取引履歴モデル"""

    def __init__(
        self,
        user_id: int,
        code: str,
        transaction_date: str,
        transaction_type: str,
        quantity: int,
        price: float,
    ):
        self.id = None
        self.user_id = user_id
        self.code = code
        self.transaction_date = transaction_date
        self.transaction_type = transaction_type  # 'buy' or 'sell'
        self.detailed_type = ""  # '新規買い', '新規売り', '決済買い', '決済売り'
        self.quantity = quantity
        self.price = price
        self.commission = 0.0
        self.tax = 0.0
        self.total_amount = quantity * price
        self.realized_profit = None  # 決済損益
        self.remarks = ""
        self.created_at = None
        # 追加情報（DBには保存しない）
        self.company_name: str | None = None

    @classmethod
    def find_all_by_user(
        cls,
        user_id: int,
        code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list["Transaction"]:
        """ユーザーの取引履歴を取得"""
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()
        try:
            query = """
                SELECT t.id, t.user_id, t.code, t.transaction_date,
                       t.transaction_type, t.quantity, t.price, t.commission,
                       t.tax, t.total_amount, t.remarks, t.created_at,
                       li.company_name, t.detailed_type, t.realized_profit
                FROM transactions t
                LEFT JOIN listed_info li ON (t.code || '0') = li.code
                WHERE t.user_id = ?
            """
            params: list[Any] = [user_id]

            if code:
                query += " AND t.code = ?"
                params.append(code)

            if start_date:
                query += " AND t.transaction_date >= ?"
                params.append(start_date)

            if end_date:
                query += " AND t.transaction_date <= ?"
                params.append(end_date)

            query += " ORDER BY t.transaction_date DESC, t.id DESC"

            cursor.execute(query, params)

            transactions = []
            for row in cursor.fetchall():
                transaction = cls(
                    user_id=row[1],
                    code=row[2],
                    transaction_date=row[3],
                    transaction_type=row[4],
                    quantity=row[5],
                    price=row[6],
                )
                transaction.id = row[0]
                transaction.commission = row[7]
                transaction.tax = row[8]
                transaction.total_amount = row[9]
                transaction.remarks = row[10] or ""
                transaction.created_at = row[11]
                transaction.company_name = row[12]  # 追加情報
                transaction.detailed_type = row[13] or ""
                transaction.realized_profit = row[14]
                transactions.append(transaction)

            return transactions
        finally:
            conn.close()

    def save(self) -> bool:
        """取引情報を保存"""
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO transactions
                (user_id, code, transaction_date, transaction_type, quantity,
                 price, commission, tax, total_amount, remarks, detailed_type, realized_profit)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    self.user_id,
                    self.code,
                    self.transaction_date,
                    self.transaction_type,
                    self.quantity,
                    self.price,
                    self.commission,
                    self.tax,
                    self.total_amount,
                    self.remarks,
                    self.detailed_type,
                    self.realized_profit,
                ),
            )
            self.id = cursor.lastrowid  # type: ignore[assignment]
            conn.commit()
            logger.info(
                f"取引保存成功: {self.transaction_date} {self.code} "
                f"{self.transaction_type} {self.quantity}株"
            )
            return True
        except sqlite3.Error as e:
            logger.error(f"取引保存エラー: {e}")
            return False
        finally:
            conn.close()

    @staticmethod
    def bulk_insert(transactions: list[dict]) -> int:
        """複数の取引を一括挿入"""
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()
        inserted_count = 0

        try:
            for trans in transactions:
                try:
                    cursor.execute(
                        """
                        INSERT INTO transactions
                        (user_id, code, transaction_date, transaction_type, quantity,
                         price, commission, tax, total_amount, remarks, detailed_type, realized_profit)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            trans["user_id"],
                            trans["code"],
                            trans["transaction_date"],
                            trans["transaction_type"],
                            trans["quantity"],
                            trans["price"],
                            trans.get("commission", 0),
                            trans.get("tax", 0),
                            trans["total_amount"],
                            trans.get("remarks", ""),
                            trans.get("detailed_type", ""),
                            trans.get("realized_profit"),
                        ),
                    )
                    inserted_count += 1
                except sqlite3.Error as e:
                    # エラーが発生した場合はログに記録
                    logger.error(
                        f"取引挿入エラー: {trans['transaction_date']} {trans['code']} - {str(e)}"
                    )
                    continue

            conn.commit()
            logger.info(f"取引一括挿入完了: {inserted_count}/{len(transactions)}件")
            return inserted_count
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"取引一括挿入エラー: {e}")
            return 0
        finally:
            conn.close()

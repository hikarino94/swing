"""ポートフォリオ関連のデータモデル"""

import sqlite3
from typing import Any, Optional

from src.config import DB_PATH
from src.utils.logging_config import get_logger

logger = get_logger("portfolio.models")


class Holding:
    """保有銘柄モデル"""

    def __init__(self, user_id: int, code: str, account_name: str = "default"):
        self.id: int | None = None
        self.user_id: int = user_id
        self.code: str = code
        self.account_name: str = account_name
        self.quantity: int = 0
        self.average_price: float = 0.0
        self.market_value: float | None = None
        self.profit_loss: float | None = None
        self.profit_loss_ratio: float | None = None
        self.updated_at: str | None = None

    @classmethod
    def find_by_user_and_code(cls, user_id: int, code: str) -> Optional["Holding"]:
        """ユーザーIDと銘柄コードで保有銘柄を検索（後方互換性のため残す）"""
        return cls.find_by_user_code_and_account(user_id, code, "default")

    @classmethod
    def find_by_user_code_and_account(
        cls, user_id: int, code: str, account_name: str
    ) -> Optional["Holding"]:
        """ユーザーID、銘柄コード、口座名で保有銘柄を検索"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, user_id, code, account_name, quantity, average_price,
                       market_value, profit_loss, profit_loss_ratio, updated_at
                FROM holdings
                WHERE user_id = ? AND code = ? AND account_name = ?
            """,
                (user_id, code, account_name),
            )
            row = cursor.fetchone()
            if row:
                holding = cls(user_id=row[1], code=row[2], account_name=row[3])
                holding.id = row[0]
                holding.quantity = row[4]
                holding.average_price = row[5]
                holding.market_value = row[6]
                holding.profit_loss = row[7]
                holding.profit_loss_ratio = row[8]
                holding.updated_at = row[9]
                return holding
            return None
        finally:
            conn.close()

    @classmethod
    def find_all_by_user(cls, user_id: int) -> list["Holding"]:
        """ユーザーの全保有銘柄を取得"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT h.id, h.user_id, h.code, h.account_name, h.quantity, h.average_price,
                       h.market_value, h.profit_loss, h.profit_loss_ratio, h.updated_at,
                       li.company_name
                FROM holdings h
                LEFT JOIN listed_info li ON h.code = li.code
                WHERE h.user_id = ? AND h.quantity > 0
                ORDER BY h.code, h.account_name
            """,
                (user_id,),
            )

            holdings = []
            for row in cursor.fetchall():
                holding = cls(user_id=row[1], code=row[2], account_name=row[3])
                holding.id = row[0]
                holding.quantity = row[4]
                holding.average_price = row[5]
                holding.market_value = row[6]
                holding.profit_loss = row[7]
                holding.profit_loss_ratio = row[8]
                holding.updated_at = row[9]
                # holding.company_name = row[10]  # 追加情報（現在は未使用）
                holdings.append(holding)

            return holdings
        finally:
            conn.close()

    def save(self) -> bool:
        """保有銘柄情報を保存"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            if self.id is None:
                # 新規作成
                cursor.execute(
                    """
                    INSERT INTO holdings
                    (user_id, code, account_name, quantity, average_price, market_value,
                     profit_loss, profit_loss_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        self.user_id,
                        self.code,
                        self.account_name,
                        self.quantity,
                        self.average_price,
                        self.market_value,
                        self.profit_loss,
                        self.profit_loss_ratio,
                    ),
                )
                self.id = cursor.lastrowid  # type: ignore[assignment]
            else:
                # 更新
                cursor.execute(
                    """
                    UPDATE holdings
                    SET quantity = ?, average_price = ?, market_value = ?,
                        profit_loss = ?, profit_loss_ratio = ?,
                        updated_at = datetime('now')
                    WHERE id = ?
                """,
                    (
                        self.quantity,
                        self.average_price,
                        self.market_value,
                        self.profit_loss,
                        self.profit_loss_ratio,
                        self.id,
                    ),
                )

            conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"保有銘柄保存エラー: {e}")
            return False
        finally:
            conn.close()

    def update_market_value(self, current_price: float) -> None:
        """時価評価額と損益を更新"""
        if self.quantity > 0 and current_price > 0:
            self.market_value = float(self.quantity * current_price)
            total_cost = self.quantity * self.average_price
            if self.market_value is not None:
                self.profit_loss = float(self.market_value - total_cost)
                if total_cost > 0:
                    self.profit_loss_ratio = float(
                        (self.profit_loss / total_cost) * 100
                    )
                else:
                    self.profit_loss_ratio = 0.0
            else:
                self.profit_loss = None
                self.profit_loss_ratio = None


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
        self.quantity = quantity
        self.price = price
        self.commission = 0.0
        self.tax = 0.0
        self.total_amount = quantity * price
        self.remarks = ""
        self.created_at = None

    @classmethod
    def find_all_by_user(
        cls,
        user_id: int,
        code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list["Transaction"]:
        """ユーザーの取引履歴を取得"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            query = """
                SELECT t.id, t.user_id, t.code, t.transaction_date,
                       t.transaction_type, t.quantity, t.price, t.commission,
                       t.tax, t.total_amount, t.remarks, t.created_at,
                       li.company_name
                FROM transactions t
                LEFT JOIN listed_info li ON t.code = li.code
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
                # transaction.company_name = row[12]  # 追加情報（現在は未使用）
                transactions.append(transaction)

            return transactions
        finally:
            conn.close()

    def save(self) -> bool:
        """取引情報を保存"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO transactions
                (user_id, code, transaction_date, transaction_type, quantity,
                 price, commission, tax, total_amount, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        inserted_count = 0

        try:
            for trans in transactions:
                # 既存の取引をチェック
                cursor.execute(
                    """
                    SELECT id FROM transactions
                    WHERE user_id = ? AND code = ? AND transaction_date = ?
                    AND transaction_type = ? AND quantity = ? AND price = ?
                """,
                    (
                        trans["user_id"],
                        trans["code"],
                        trans["transaction_date"],
                        trans["transaction_type"],
                        trans["quantity"],
                        trans["price"],
                    ),
                )

                if cursor.fetchone():
                    # 重複データはスキップ
                    logger.debug(
                        f"重複取引スキップ: {trans['transaction_date']} {trans['code']}"
                    )
                    continue

                try:
                    cursor.execute(
                        """
                        INSERT INTO transactions
                        (user_id, code, transaction_date, transaction_type, quantity,
                         price, commission, tax, total_amount, remarks)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        ),
                    )
                    inserted_count += 1
                except sqlite3.IntegrityError:
                    # 重複データはスキップ
                    logger.debug(
                        f"重複取引スキップ: {trans['transaction_date']} {trans['code']}"
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

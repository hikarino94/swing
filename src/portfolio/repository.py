"""ポートフォリオデータベースアクセス層"""

from typing import Any

from src.utils.db_utils import get_db_connection
from src.utils.logging_config import get_logger

from .models import Holding, Transaction

logger = get_logger("portfolio.repository")


class PortfolioRepository:
    """ポートフォリオのデータベースアクセスを管理"""

    @staticmethod
    def get_holding(
        user_id: int, code: str, account_name: str, account_type: str
    ) -> Holding | None:
        """保有銘柄を取得"""
        with get_db_connection() as conn:
            cursor = conn.execute(
                """
                SELECT * FROM holdings
                WHERE user_id = ? AND code = ? AND account_name = ?
                AND account_type = ? AND deleted_at IS NULL
                """,
                (user_id, code, account_name, account_type),
            )
            row = cursor.fetchone()
            if row:
                return Holding.from_db_row(row, list(cursor.description))
        return None

    @staticmethod
    def upsert_holding(holding: Holding) -> tuple[bool, bool]:
        """保有銘柄を追加または更新

        Returns:
            (is_updated, is_new) のタプル
        """
        with get_db_connection() as conn:
            # 既存レコードの確認
            existing = PortfolioRepository.get_holding(
                holding.user_id,
                holding.code,
                holding.account_name,
                holding.account_type,
            )

            if existing:
                # 更新
                conn.execute(
                    """
                    UPDATE holdings
                    SET quantity = ?, average_price = ?, market_value = ?,
                        profit_loss = ?, profit_loss_ratio = ?,
                        expected_per = ?, actual_pbr = ?, dividend_yield = ?,
                        expected_eps = ?, actual_bps = ?, expected_dividend = ?,
                        lending_type = ?, updated_at = datetime('now')
                    WHERE user_id = ? AND code = ? AND account_name = ?
                    AND account_type = ? AND deleted_at IS NULL
                    """,
                    (
                        holding.quantity,
                        holding.average_price,
                        holding.market_value,
                        holding.profit_loss,
                        holding.profit_loss_ratio,
                        holding.expected_per,
                        holding.actual_pbr,
                        holding.dividend_yield,
                        holding.expected_eps,
                        holding.actual_bps,
                        holding.expected_dividend,
                        holding.lending_type,
                        holding.user_id,
                        holding.code,
                        holding.account_name,
                        holding.account_type,
                    ),
                )
                conn.commit()
                return True, False
            else:
                # 新規作成
                conn.execute(
                    """
                    INSERT INTO holdings (
                        user_id, code, account_name, account_type,
                        quantity, average_price, market_value,
                        profit_loss, profit_loss_ratio,
                        expected_per, actual_pbr, dividend_yield,
                        expected_eps, actual_bps, expected_dividend,
                        lending_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        holding.user_id,
                        holding.code,
                        holding.account_name,
                        holding.account_type,
                        holding.quantity,
                        holding.average_price,
                        holding.market_value,
                        holding.profit_loss,
                        holding.profit_loss_ratio,
                        holding.expected_per,
                        holding.actual_pbr,
                        holding.dividend_yield,
                        holding.expected_eps,
                        holding.actual_bps,
                        holding.expected_dividend,
                        holding.lending_type,
                    ),
                )
                conn.commit()
                return False, True

    @staticmethod
    def get_holdings(
        user_id: int,
        account_name: str | None = None,
        code: str | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        """保有銘柄リストを取得"""
        query = """
            SELECT h.*, li.company_name, li.market_code
            FROM holdings h
            LEFT JOIN listed_info li ON h.code = li.code
            WHERE h.user_id = ?
        """
        params: list[Any] = [user_id]

        if not include_deleted:
            query += " AND h.deleted_at IS NULL"

        if account_name:
            query += " AND h.account_name = ?"
            params.append(account_name)

        if code:
            query += " AND h.code = ?"
            params.append(code)

        query += " ORDER BY h.code"

        results = []
        with get_db_connection() as conn:
            cursor = conn.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor:
                results.append(dict(zip(columns, row, strict=False)))

        return results

    @staticmethod
    def soft_delete_holding(
        user_id: int, code: str, account_name: str, account_type: str
    ) -> bool:
        """保有銘柄を論理削除"""
        with get_db_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE holdings
                SET deleted_at = datetime('now'), updated_at = datetime('now')
                WHERE user_id = ? AND code = ? AND account_name = ?
                AND account_type = ? AND deleted_at IS NULL
                """,
                (user_id, code, account_name, account_type),
            )
            conn.commit()
            return cursor.rowcount > 0

    @staticmethod
    def delete_all_holdings(user_id: int) -> int:
        """全保有銘柄を論理削除"""
        with get_db_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE holdings
                SET deleted_at = datetime('now'), updated_at = datetime('now')
                WHERE user_id = ? AND deleted_at IS NULL
                """,
                (user_id,),
            )
            conn.commit()
            return cursor.rowcount

    @staticmethod
    def delete_holdings_by_account(user_id: int, account_name: str) -> int:
        """口座別に保有銘柄を論理削除"""
        with get_db_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE holdings
                SET deleted_at = datetime('now'), updated_at = datetime('now')
                WHERE user_id = ? AND account_name = ? AND deleted_at IS NULL
                """,
                (user_id, account_name),
            )
            conn.commit()
            return cursor.rowcount

    @staticmethod
    def insert_transaction(transaction: Transaction) -> int:
        """取引履歴を追加"""
        with get_db_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO transactions (
                    user_id, code, transaction_date, transaction_type,
                    quantity, price, commission, tax, total_amount,
                    remarks, detailed_type, realized_profit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction.user_id,
                    transaction.code,
                    transaction.transaction_date,
                    transaction.transaction_type,
                    transaction.quantity,
                    transaction.price,
                    transaction.commission,
                    transaction.tax,
                    transaction.total_amount,
                    transaction.remarks,
                    transaction.detailed_type,
                    transaction.realized_profit,
                ),
            )
            conn.commit()
            return cursor.lastrowid or 0

    @staticmethod
    def get_transactions(
        user_id: int,
        code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """取引履歴を取得"""
        query = """
            SELECT t.*, li.company_name
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

        results = []
        with get_db_connection() as conn:
            cursor = conn.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor:
                results.append(dict(zip(columns, row, strict=False)))

        return results

    @staticmethod
    def get_latest_prices(codes: list[str]) -> dict[str, float]:
        """銘柄コードリストの最新株価を取得"""
        if not codes:
            return {}

        placeholders = ",".join("?" * len(codes))
        query = f"""
            SELECT p.code, p.close
            FROM prices p
            INNER JOIN (
                SELECT code, MAX(date) as max_date
                FROM prices
                WHERE code IN ({placeholders})
                GROUP BY code
            ) latest ON p.code = latest.code AND p.date = latest.max_date
        """

        prices = {}
        with get_db_connection() as conn:
            cursor = conn.execute(query, codes)
            for code, close in cursor:
                prices[code] = close

        return prices

    @staticmethod
    def get_stock_info(codes: list[str]) -> dict[str, dict[str, Any]]:
        """銘柄情報を取得"""
        if not codes:
            return {}

        placeholders = ",".join("?" * len(codes))
        query = f"""
            SELECT code, company_name, market_code, sector33_name
            FROM listed_info
            WHERE code IN ({placeholders})
        """

        info = {}
        with get_db_connection() as conn:
            cursor = conn.execute(query, codes)
            for row in cursor:
                info[row[0]] = {
                    "company_name": row[1],
                    "market_code": row[2],
                    "sector": row[3],
                }

        return info

    @staticmethod
    def get_portfolio_summary(user_id: int) -> dict[str, Any]:
        """ポートフォリオサマリーを取得"""
        with get_db_connection() as conn:
            # 口座別の保有状況
            cursor = conn.execute(
                """
                SELECT
                    account_name,
                    account_type,
                    COUNT(DISTINCT code) as stock_count,
                    SUM(market_value) as total_value,
                    SUM(profit_loss) as total_profit_loss
                FROM holdings
                WHERE user_id = ? AND deleted_at IS NULL
                GROUP BY account_name, account_type
                """,
                (user_id,),
            )

            accounts = []
            total_value = 0
            total_profit_loss = 0

            for row in cursor:
                account_info = {
                    "account_name": row[0],
                    "account_type": row[1],
                    "stock_count": row[2],
                    "total_value": row[3] or 0,
                    "total_profit_loss": row[4] or 0,
                }
                accounts.append(account_info)
                total_value += account_info["total_value"]
                total_profit_loss += account_info["total_profit_loss"]

            return {
                "accounts": accounts,
                "total_value": total_value,
                "total_profit_loss": total_profit_loss,
                "profit_loss_ratio": (
                    (total_profit_loss / (total_value - total_profit_loss) * 100)
                    if total_value > total_profit_loss
                    else 0
                ),
            }

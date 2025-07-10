"""取引履歴リポジトリ

データベース操作を担当するリポジトリ層
"""

import sqlite3
from typing import Any

from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("web.portfolio.transactions.repositories")


class TransactionRepository:
    """取引履歴リポジトリ"""

    def get_transactions_with_pagination(
        self, params: dict[str, Any]
    ) -> tuple[list[dict], int]:
        """
        ページネーション付きで取引履歴を取得

        Args:
            params: 検索パラメータ

        Returns:
            (取引データのリスト, 総件数)
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 基本的なクエリ条件
            query_conditions = ["t.user_id = ?"]
            query_params: list[Any] = [params["user_id"]]

            if params.get("code"):
                query_conditions.append("t.code = ?")
                query_params.append(params["code"])
            if params.get("start_date"):
                query_conditions.append("t.transaction_date >= ?")
                query_params.append(params["start_date"])
            if params.get("end_date"):
                query_conditions.append("t.transaction_date <= ?")
                query_params.append(params["end_date"])

            # 取引タイプでのフィルタリング
            if params.get("transaction_type"):
                transaction_type = params["transaction_type"]
                if transaction_type == "new_buy":
                    query_conditions.append(
                        "t.transaction_type = 'buy' AND t.detailed_type = '新規買い'"
                    )
                elif transaction_type == "new_sell":
                    query_conditions.append(
                        "t.transaction_type = 'sell' AND t.detailed_type = '新規売り'"
                    )
                elif transaction_type == "close_buy":
                    query_conditions.append(
                        "t.transaction_type = 'buy' AND t.detailed_type = '決済買い'"
                    )
                elif transaction_type == "close_sell":
                    query_conditions.append(
                        "t.transaction_type = 'sell' AND t.detailed_type = '決済売り'"
                    )

            where_clause = " AND ".join(query_conditions)

            # 売却取引の実現損益を事前計算するためのCTE
            query = f"""
            WITH sell_transactions AS (
                SELECT
                    t.*,
                    li.company_name,
                    CASE
                        WHEN t.transaction_type = 'buy' THEN t.quantity * t.price + COALESCE(t.commission, 0)
                        ELSE 0
                    END as buy_amount,
                    CASE
                        WHEN t.transaction_type = 'sell' THEN t.quantity * t.price - COALESCE(t.commission, 0) - COALESCE(t.tax, 0)
                        ELSE 0
                    END as sell_amount
                FROM transactions t
                LEFT JOIN listed_info li ON t.code = li.code
                WHERE {where_clause}
            ),
            avg_costs AS (
                SELECT
                    s1.id,
                    s1.code,
                    s1.transaction_date,
                    s1.quantity as sell_quantity,
                    s1.sell_amount,
                    COALESCE(
                        SUM(CASE WHEN s2.transaction_type = 'buy' THEN s2.quantity ELSE -s2.quantity END),
                        0
                    ) as net_quantity_before,
                    COALESCE(
                        SUM(CASE WHEN s2.transaction_type = 'buy' THEN s2.quantity * s2.price + COALESCE(s2.commission, 0) ELSE 0 END),
                        0
                    ) as total_cost_before
                FROM sell_transactions s1
                LEFT JOIN transactions s2 ON s2.user_id = s1.user_id
                    AND s2.code = s1.code
                    AND (s2.transaction_date < s1.transaction_date
                        OR (s2.transaction_date = s1.transaction_date AND s2.id < s1.id))
                WHERE s1.transaction_type = 'sell'
                GROUP BY s1.id, s1.code, s1.transaction_date, s1.quantity, s1.sell_amount
            )
            SELECT
                st.*,
                CASE
                    WHEN st.transaction_type = 'sell' AND ac.net_quantity_before > 0 AND ac.total_cost_before > 0 THEN
                        st.sell_amount - (st.quantity * (ac.total_cost_before / ac.net_quantity_before))
                    WHEN st.transaction_type = 'sell' THEN 0
                    ELSE 0
                END as calculated_profit
            FROM sell_transactions st
            LEFT JOIN avg_costs ac ON st.id = ac.id
            ORDER BY st.transaction_date DESC, st.id DESC
            LIMIT ? OFFSET ?
            """

            # 全件数を取得（ページネーション用）
            count_query = f"""
            SELECT COUNT(*) FROM transactions t
            WHERE {where_clause}
            """
            cursor.execute(count_query, query_params)
            total_count = cursor.fetchone()[0]

            # ページネーション付きでデータ取得
            offset = (params["page"] - 1) * params["per_page"]
            query_params.extend([params["per_page"], offset])
            cursor.execute(query, query_params)
            columns = [desc[0] for desc in cursor.description]

            trans_data = []
            for row in cursor.fetchall():
                trans = dict(zip(columns, row, strict=False))

                # 既存のrealized_profitがあればそれを使用、なければ計算値を使用
                realized_profit = trans.get("realized_profit") or trans.get(
                    "calculated_profit", 0
                )

                trans_data.append(
                    {
                        "id": trans["id"],
                        "code": trans["code"],
                        "company_name": trans.get("company_name", "") or "",
                        "transaction_date": trans["transaction_date"],
                        "transaction_type": trans["transaction_type"],
                        "detailed_type": trans.get("detailed_type", "") or "",
                        "quantity": trans["quantity"],
                        "price": trans["price"],
                        "commission": trans.get("commission"),
                        "tax": trans.get("tax"),
                        "total_amount": trans.get("total_amount"),
                        "buy_amount": trans.get("buy_amount", 0),
                        "sell_amount": trans.get("sell_amount", 0),
                        "realized_profit": realized_profit,
                        "remarks": trans.get("remarks"),
                    }
                )

            return trans_data, total_count

        finally:
            conn.close()

    def get_transactions_for_performance(
        self, user_id: int, start_date: str | None = None
    ) -> list[dict]:
        """
        パフォーマンス計算用の取引データを取得

        Args:
            user_id: ユーザーID
            start_date: 開始日付

        Returns:
            取引データのリスト
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            if start_date:
                cursor.execute(
                    """
                    SELECT t.*, li.company_name
                    FROM transactions t
                    LEFT JOIN listed_info li ON t.code = li.code
                    WHERE t.user_id = ? AND t.transaction_date >= ?
                    ORDER BY t.transaction_date, t.id
                    """,
                    (user_id, start_date),
                )
            else:
                cursor.execute(
                    """
                    SELECT t.*, li.company_name
                    FROM transactions t
                    LEFT JOIN listed_info li ON t.code = li.code
                    WHERE t.user_id = ?
                    ORDER BY t.transaction_date, t.id
                    """,
                    (user_id,),
                )

            columns = [desc[0] for desc in cursor.description]
            transactions = []
            for row in cursor.fetchall():
                trans = dict(zip(columns, row, strict=False))
                transactions.append(trans)

            return transactions

        finally:
            conn.close()

    def get_holdings_for_performance(self, user_id: int) -> list[dict]:
        """
        パフォーマンス計算用の保有銘柄データを取得

        Args:
            user_id: ユーザーID

        Returns:
            保有銘柄データのリスト
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT h.code, li.company_name, h.quantity, h.average_price,
                       h.market_value, h.profit_loss, h.account_type
                FROM holdings h
                LEFT JOIN listed_info li ON h.code = li.code
                WHERE h.user_id = ? AND h.deleted_at IS NULL
                """,
                (user_id,),
            )

            holdings = []
            for row in cursor.fetchall():
                holdings.append(
                    {
                        "code": row[0],
                        "company_name": row[1] or "",
                        "quantity": row[2],
                        "average_price": row[3],
                        "market_value": row[4],
                        "profit_loss": row[5],
                        "account_type": row[6],
                    }
                )

            return holdings

        finally:
            conn.close()

    def find_transaction(
        self, user_id: int, transaction_id: int
    ) -> dict[str, Any] | None:
        """
        特定の取引を取得

        Args:
            user_id: ユーザーID
            transaction_id: 取引ID

        Returns:
            取引データまたはNone
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            cursor.execute(
                "SELECT * FROM transactions WHERE id = ? AND user_id = ?",
                (transaction_id, user_id),
            )
            row = cursor.fetchone()

            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row, strict=False))
            return None

        finally:
            conn.close()

    def delete_transaction(self, user_id: int, transaction_id: int) -> bool:
        """
        取引を削除

        Args:
            user_id: ユーザーID
            transaction_id: 取引ID

        Returns:
            削除成功の可否
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            cursor.execute(
                "DELETE FROM transactions WHERE id = ? AND user_id = ?",
                (transaction_id, user_id),
            )
            conn.commit()
            return cursor.rowcount > 0

        finally:
            conn.close()

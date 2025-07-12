"""ポートフォリオ集約・サマリー処理"""

import sqlite3

from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("portfolio.portfolio_aggregator")


class PortfolioAggregator:
    """ポートフォリオ集約クラス"""

    @staticmethod
    def aggregate_holdings_by_code(user_id: int) -> list[dict]:
        """
        ユーザーの保有銘柄を銘柄コードで集約（複数口座の合算）

        Args:
            user_id: ユーザーID

        Returns:
            集約された保有銘柄のリスト
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT
                    h.code,
                    li.company_name,
                    SUM(h.quantity) as total_quantity,
                    SUM(h.quantity * h.average_price) / NULLIF(SUM(h.quantity), 0) as weighted_avg_price,
                    SUM(h.market_value) as total_market_value,
                    SUM(h.profit_loss) as total_profit_loss,
                    COUNT(DISTINCT h.account_name) as account_count,
                    GROUP_CONCAT(DISTINCT h.account_name) as account_names,
                    GROUP_CONCAT(DISTINCT h.account_type) as account_types,
                    -- 株価指標は最初の値を使用（通常、同じ銘柄なら同じ値のはず）
                    MAX(h.expected_per) as expected_per,
                    MAX(h.actual_pbr) as actual_pbr,
                    MAX(h.dividend_yield) as dividend_yield,
                    MAX(h.expected_eps) as expected_eps,
                    MAX(h.actual_bps) as actual_bps,
                    MAX(h.expected_dividend) as expected_dividend,
                    MAX(h.lending_type) as lending_type
                FROM holdings h
                LEFT JOIN listed_info li ON (h.code || '0') = li.code
                WHERE h.user_id = ? AND h.quantity > 0 AND h.deleted_at IS NULL
                GROUP BY h.code, li.company_name
                ORDER BY h.code
            """,
                (user_id,),
            )

            aggregated_holdings = []
            for row in cursor.fetchall():
                holding = {
                    "type": "stock",  # 株式であることを示す
                    "code": row[0],
                    "company_name": row[1],
                    "total_quantity": row[2],
                    "weighted_avg_price": row[3] or 0,
                    "total_market_value": row[4],
                    "total_profit_loss": row[5],
                    "account_count": row[6],
                    "account_names": row[7],
                    "account_types": row[8],
                    "profit_loss_ratio": 0,
                    # 株価指標データ
                    "expected_per": row[9],
                    "actual_pbr": row[10],
                    "dividend_yield": row[11],
                    "expected_eps": row[12],
                    "actual_bps": row[13],
                    "expected_dividend": row[14],
                    "lending_type": row[15],
                }

                # 損益率の計算
                total_cost = holding["total_quantity"] * holding["weighted_avg_price"]
                if total_cost > 0 and holding["total_profit_loss"] is not None:
                    holding["profit_loss_ratio"] = (
                        holding["total_profit_loss"] / total_cost
                    ) * 100

                aggregated_holdings.append(holding)

            logger.info(f"保有銘柄集約完了: {len(aggregated_holdings)}銘柄")
            return aggregated_holdings

        finally:
            conn.close()

    @staticmethod
    def get_portfolio_summary(user_id: int) -> dict:
        """
        ポートフォリオのサマリー情報を取得

        Args:
            user_id: ユーザーID

        Returns:
            サマリー情報の辞書
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 株式の集計
            cursor.execute(
                """
                SELECT
                    COUNT(*) as stock_count,
                    SUM(quantity * average_price) as total_cost,
                    SUM(market_value) as total_market_value,
                    SUM(profit_loss) as total_profit_loss
                FROM holdings
                WHERE user_id = ? AND quantity > 0 AND deleted_at IS NULL
            """,
                (user_id,),
            )

            stock_row = cursor.fetchone()

            # 投資信託の集計
            cursor.execute(
                """
                SELECT
                    COUNT(*) as fund_count,
                    SUM(quantity * average_price / 10000) as total_cost,
                    SUM(market_value) as total_market_value,
                    SUM(profit_loss) as total_profit_loss
                FROM fund_holdings
                WHERE user_id = ? AND quantity > 0 AND deleted_at IS NULL
            """,
                (user_id,),
            )

            fund_row = cursor.fetchone()

            # 合計値を計算
            summary = {
                "stock_count": (stock_row[0] or 0) + (fund_row[0] or 0),
                "total_cost": (stock_row[1] or 0) + (fund_row[1] or 0),
                "total_market_value": (stock_row[2] or 0) + (fund_row[2] or 0),
                "total_profit_loss": (stock_row[3] or 0) + (fund_row[3] or 0),
                "total_profit_loss_ratio": 0,
            }

            # 損益率の計算
            if summary["total_cost"] > 0:
                summary["total_profit_loss_ratio"] = (
                    summary["total_profit_loss"] / summary["total_cost"] * 100
                )

            # 取引履歴の集計
            cursor.execute(
                """
                SELECT
                    COUNT(*) as transaction_count,
                    MIN(transaction_date) as first_transaction_date,
                    MAX(transaction_date) as last_transaction_date
                FROM transactions
                WHERE user_id = ?
            """,
                (user_id,),
            )

            trans_row = cursor.fetchone()
            summary["transaction_count"] = trans_row[0] or 0
            summary["first_transaction_date"] = trans_row[1]
            summary["last_transaction_date"] = trans_row[2]

            return summary

        finally:
            conn.close()

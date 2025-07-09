#!/usr/bin/env python
"""
包括的なデータベースインデックスの作成とクエリ最適化
"""
import sqlite3
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import DB_PATH
from src.utils.logging_config import get_logger

logger = get_logger("db.indexes")


def create_indexes(conn: sqlite3.Connection) -> None:
    """包括的なインデックスを作成"""

    indexes = [
        # prices テーブル
        ("idx_prices_code_date", "prices(code, date)"),
        ("idx_prices_date", "prices(date)"),
        ("idx_prices_code", "prices(code)"),
        # statements テーブル
        ("idx_statements_code", "statements(code)"),
        ("idx_statements_disclosed", "statements(DisclosedAt)"),
        ("idx_statements_code_disclosed", "statements(code, DisclosedAt)"),
        ("idx_statements_type_period", "statements(TypeOfCurrentPeriod)"),
        # listed_info テーブル
        ("idx_listed_info_code", "listed_info(code)"),
        ("idx_listed_info_delete_flag", "listed_info(delete_flag)"),
        ("idx_listed_info_sector17", "listed_info(sector17_code)"),
        ("idx_listed_info_sector33", "listed_info(sector33_code)"),
        # fundamental_signals テーブル
        ("idx_fund_signals_code", "fundamental_signals(code)"),
        ("idx_fund_signals_disclosed", "fundamental_signals(DisclosedAt)"),
        ("idx_fund_signals_created", "fundamental_signals(created_at)"),
        ("idx_fund_signals_code_disclosed", "fundamental_signals(code, DisclosedAt)"),
        # technical_indicators テーブル
        ("idx_tech_indicators_code", "technical_indicators(code)"),
        ("idx_tech_indicators_date", "technical_indicators(date)"),
        ("idx_tech_indicators_code_date", "technical_indicators(code, date)"),
        # holdings テーブル (ポートフォリオ機能用)
        ("idx_holdings_user_id", "holdings(user_id)"),
        ("idx_holdings_code", "holdings(code)"),
        ("idx_holdings_user_code", "holdings(user_id, code)"),
        ("idx_holdings_deleted", "holdings(deleted_at)"),
        # transactions テーブル (ポートフォリオ機能用)
        ("idx_transactions_user_id", "transactions(user_id)"),
        ("idx_transactions_code", "transactions(code)"),
        ("idx_transactions_date", "transactions(transaction_date)"),
        ("idx_transactions_user_date", "transactions(user_id, transaction_date)"),
        ("idx_transactions_type", "transactions(transaction_type)"),
    ]

    for idx_name, idx_def in indexes:
        try:
            start_time = time.time()
            conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_def}")
            elapsed = time.time() - start_time
            logger.info(f"インデックス作成: {idx_name} ({elapsed:.2f}秒)")
        except sqlite3.Error as e:
            logger.warning(f"インデックス作成エラー: {idx_name} - {e}")


def analyze_slow_queries(conn: sqlite3.Connection) -> None:
    """遅いクエリを分析してログに出力"""

    # よく使われる重いクエリパターンを分析
    queries_to_analyze = [
        # ファンダメンタルスクリーニング
        """
        SELECT * FROM statements
        WHERE DisclosedAt >= date('now', '-30 days')
        AND code IN (SELECT code FROM listed_info WHERE delete_flag = 0)
        """,
        # テクニカル分析
        """
        SELECT p.*, t.*
        FROM prices p
        LEFT JOIN technical_indicators t ON p.code = t.code AND p.date = t.date
        WHERE p.date >= date('now', '-180 days')
        """,
        # ポートフォリオ分析
        """
        SELECT h.*, li.company_name, li.sector17_name
        FROM holdings h
        LEFT JOIN listed_info li ON (h.code || '0') = li.code
        WHERE h.user_id = 1 AND h.deleted_at IS NULL
        """,
    ]

    for query in queries_to_analyze:
        try:
            # EXPLAIN QUERY PLANを実行
            explain_result = conn.execute(f"EXPLAIN QUERY PLAN {query}").fetchall()
            logger.info(f"クエリ分析:\n{query}")
            for row in explain_result:
                logger.info(f"  {row}")
        except sqlite3.Error as e:
            logger.warning(f"クエリ分析エラー: {e}")


def optimize_database(conn: sqlite3.Connection) -> None:
    """データベース全体の最適化"""

    logger.info("データベース最適化を開始")

    # 1. 統計情報の更新
    logger.info("統計情報を更新中...")
    conn.execute("ANALYZE")

    # 2. インデックスの再構築
    logger.info("インデックスを再構築中...")
    indexes = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()

    for (idx_name,) in indexes:
        try:
            conn.execute(f"REINDEX {idx_name}")
            logger.info(f"再構築完了: {idx_name}")
        except sqlite3.Error as e:
            logger.warning(f"再構築エラー: {idx_name} - {e}")

    logger.info("データベース最適化が完了")


def main():
    """メイン処理"""
    conn = sqlite3.connect(DB_PATH)

    try:
        # WALモードを有効化（並行アクセス性能向上）
        conn.execute("PRAGMA journal_mode=WAL")

        # インデックスの作成
        create_indexes(conn)

        # クエリ分析
        analyze_slow_queries(conn)

        # データベース最適化
        optimize_database(conn)

        conn.commit()

    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

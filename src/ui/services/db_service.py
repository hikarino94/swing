"""データベース関連サービス"""

from pathlib import Path
from typing import Any

from src.config import get_db_path
from src.utils.db_utils import get_db_connection
from src.utils.logging_config import get_logger

logger = get_logger("services.db")


class DatabaseService:
    """データベース操作を管理するサービス"""

    @staticmethod
    def get_db_summary() -> dict[str, Any]:
        """データベースのサマリー情報を取得

        Returns:
            テーブルごとの行数と日付範囲
        """
        tables = {
            "prices": "date",
            "listed_info": "date",
            "statements": "DisclosedDate",
            "fundamental_signals": "created_at",
            "technical_indicators": "signal_date",
        }

        summary = {}
        with get_db_connection() as conn:
            for table, date_col in tables.items():
                cur = conn.execute(
                    f"SELECT COUNT(*), MIN({date_col}), MAX({date_col}) FROM {table}"
                )
                count, min_date, max_date = cur.fetchone()
                summary[table] = {
                    "count": count or 0,
                    "min_date": min_date or "N/A",
                    "max_date": max_date or "N/A",
                }

        return summary

    @staticmethod
    def list_signals(
        signal_type: str, start_date: str | None = None, end_date: str | None = None
    ) -> list[dict[str, Any]]:
        """シグナルのリストを取得

        Args:
            signal_type: "fund" または "tech"
            start_date: 開始日
            end_date: 終了日

        Returns:
            シグナルのリスト
        """
        if signal_type == "fund":
            table = "fundamental_signals"
            date_col = "created_at"
            select_cols = """
                fs.code, li.company_name, fs.created_at,
                fs.eps_yoy_fy, fs.eps_yoy_q, fs.op_margin_delta,
                fs.feps_revision, fs.cf_quality, fs.eta_delta,
                fs.leverage, fs.turnaround, fs.treasury_delta
            """
            join_clause = "LEFT JOIN listed_info li ON fs.code = li.code"
        else:  # tech
            table = "technical_indicators"
            date_col = "signal_date"
            select_cols = """
                ti.code, li.company_name, ti.signal_date,
                ti.signals_count, ti.signals_short_count,
                ti.signal_ma, ti.signal_rsi, ti.signal_adx,
                ti.signal_bb, ti.signal_macd
            """
            join_clause = "LEFT JOIN listed_info li ON ti.code = li.code"

        # クエリ構築
        query = f"""
            SELECT {select_cols}
            FROM {table} {'fs' if signal_type == 'fund' else 'ti'}
            {join_clause}
            WHERE 1=1
        """
        params = []

        if start_date:
            query += f" AND {date_col} >= ?"
            params.append(start_date)
        if end_date:
            query += f" AND {date_col} <= ?"
            params.append(end_date)

        query += f" ORDER BY {date_col} DESC LIMIT 1000"

        results = []
        with get_db_connection() as conn:
            cursor = conn.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor:
                results.append(dict(zip(columns, row, strict=False)))

        return results

    @staticmethod
    def check_auth_tables() -> bool:
        """認証関連テーブルが存在するかチェック

        Returns:
            テーブルが存在する場合True
        """
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
                )
                return cursor.fetchone() is not None
        except Exception:
            return False

    @staticmethod
    def init_database() -> None:
        """データベースを初期化"""
        from db.db_schema import init_schema

        db_path = Path(get_db_path())
        if not db_path.exists():
            logger.info("データベースが存在しません。初期化を開始します...")
            db_path.parent.mkdir(parents=True, exist_ok=True)
            init_schema(db_path)
            logger.info("データベースの初期化が完了しました")
        else:
            # テーブルが存在するか確認
            if not DatabaseService.check_auth_tables():
                logger.info("認証テーブルが存在しません。スキーマを再作成します...")
                init_schema(db_path)
                logger.info("スキーマの再作成が完了しました")

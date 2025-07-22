"""
保有銘柄管理
"""

import logging
import sqlite3
from typing import Any

from src.config import get_db_path

logger = logging.getLogger(__name__)


def get_holdings(
    user_id: int, account_name: str | None = None, aggregate: bool = False
) -> list[dict[str, Any]]:
    """
    保有銘柄一覧を取得

    Args:
        user_id: ユーザーID
        account_name: 口座名（Noneの場合は全口座）
        aggregate: 複数口座・預かり区分を合算するか

    Returns:
        保有銘柄リスト
    """
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if aggregate:
            # 銘柄ごとに合算
            query = """
                WITH latest_prices AS (
                    SELECT
                        code,
                        close as current_price,
                        ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) as rn
                    FROM prices
                )
                SELECT
                    h.code,
                    li.company_name as name,
                    li.sector17_name as sector17,
                    li.sector33_name as sector33,
                    li.market_name as market,
                    SUM(CASE WHEN h.stock_type = '現物' THEN h.quantity ELSE 0 END) as spot_quantity,
                    SUM(CASE WHEN h.stock_type = '信用' AND h.trade_position = '買建' THEN h.quantity ELSE 0 END) as margin_buy_quantity,
                    SUM(CASE WHEN h.stock_type = '信用' AND h.trade_position = '売建' THEN h.quantity ELSE 0 END) as margin_sell_quantity,
                    SUM(h.quantity) as total_quantity,
                    CASE
                        WHEN SUM(CASE WHEN h.stock_type = '現物' OR h.trade_position = '買建' THEN h.quantity ELSE 0 END) > 0
                        THEN SUM(CASE WHEN h.stock_type = '現物' OR h.trade_position = '買建' THEN h.average_price * h.quantity ELSE 0 END) /
                             NULLIF(SUM(CASE WHEN h.stock_type = '現物' OR h.trade_position = '買建' THEN h.quantity ELSE 0 END), 0)
                        ELSE NULL
                    END as average_price,
                    lp.current_price,
                    -- 評価額の計算（最新株価 × 数量）
                    SUM(
                        CASE
                            WHEN h.stock_type = '信用' AND h.trade_position = '売建'
                            THEN -h.quantity * COALESCE(lp.current_price, 0)
                            ELSE h.quantity * COALESCE(lp.current_price, 0)
                        END
                    ) as market_value,
                    -- 評価損益の計算
                    SUM(
                        CASE
                            WHEN h.stock_type = '信用' AND h.trade_position = '売建'
                            THEN h.quantity * h.average_price - h.quantity * COALESCE(lp.current_price, 0)
                            ELSE h.quantity * COALESCE(lp.current_price, 0) - h.quantity * h.average_price
                        END
                    ) as profit_loss,
                    -- 評価損益率の計算
                    (SUM(
                        CASE
                            WHEN h.stock_type = '信用' AND h.trade_position = '売建'
                            THEN h.quantity * h.average_price - h.quantity * COALESCE(lp.current_price, 0)
                            ELSE h.quantity * COALESCE(lp.current_price, 0) - h.quantity * h.average_price
                        END
                    ) * 100.0) / NULLIF(SUM(h.quantity * h.average_price), 0) as profit_loss_ratio,
                    MAX(h.expected_per) as expected_per,
                    MAX(h.actual_pbr) as actual_pbr,
                    MAX(h.dividend_yield) as dividend_yield,
                    MAX(h.expected_eps) as expected_eps,
                    MAX(h.actual_bps) as actual_bps,
                    MAX(h.expected_dividend) as expected_dividend
                FROM holdings h
                LEFT JOIN listed_info li ON h.code || '0' = li.code
                LEFT JOIN latest_prices lp ON h.code || '0' = lp.code AND lp.rn = 1
                WHERE h.user_id = ? AND h.deleted_at IS NULL
            """
            params: list[Any] = [user_id]

            if account_name:
                query += " AND h.account_name = ?"
                params.append(account_name)

            query += " GROUP BY h.code, li.company_name, li.sector17_name, li.sector33_name, li.market_name, lp.current_price"

        else:
            # 通常の一覧取得
            query = """
                WITH latest_prices AS (
                    SELECT
                        code,
                        close as current_price,
                        ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) as rn
                    FROM prices
                )
                SELECT
                    h.*,
                    li.company_name as name,
                    li.sector17_name as sector17,
                    li.sector33_name as sector33,
                    li.market_name as market,
                    lp.current_price as latest_price,
                    -- 評価額の計算（最新株価 × 数量）
                    CASE
                        WHEN h.stock_type = '信用' AND h.trade_position = '売建'
                        THEN -h.quantity * COALESCE(lp.current_price, 0)
                        ELSE h.quantity * COALESCE(lp.current_price, 0)
                    END as market_value,
                    -- 評価損益の計算
                    CASE
                        WHEN h.stock_type = '信用' AND h.trade_position = '売建'
                        THEN h.quantity * h.average_price - h.quantity * COALESCE(lp.current_price, 0)
                        ELSE h.quantity * COALESCE(lp.current_price, 0) - h.quantity * h.average_price
                    END as profit_loss,
                    -- 評価損益率の計算
                    CASE
                        WHEN h.stock_type = '信用' AND h.trade_position = '売建'
                        THEN ((h.quantity * h.average_price - h.quantity * COALESCE(lp.current_price, 0)) * 100.0) / NULLIF(h.quantity * h.average_price, 0)
                        ELSE ((h.quantity * COALESCE(lp.current_price, 0) - h.quantity * h.average_price) * 100.0) / NULLIF(h.quantity * h.average_price, 0)
                    END as profit_loss_ratio
                FROM holdings h
                LEFT JOIN listed_info li ON h.code || '0' = li.code
                LEFT JOIN latest_prices lp ON h.code || '0' = lp.code AND lp.rn = 1
                WHERE h.user_id = ? AND h.deleted_at IS NULL
            """
            params = [user_id]

            if account_name:
                query += " AND h.account_name = ?"
                params.append(account_name)

            query += " ORDER BY h.code, h.account_name, h.account_type, h.stock_type"

        cursor.execute(query, params)

        holdings = []
        for row in cursor.fetchall():
            holding = dict(row)
            # current_priceフィールドを最新の株価で更新
            if "latest_price" in holding:
                holding["current_price"] = holding["latest_price"]
                del holding["latest_price"]
            holdings.append(holding)

        return holdings


def add_holding(
    user_id: int,
    code: str,
    account_name: str,
    account_type: str,
    stock_type: str,
    quantity: int,
    average_price: float,
    **kwargs,
) -> int:
    """
    保有銘柄を追加

    Args:
        user_id: ユーザーID
        code: 銘柄コード
        account_name: 口座名
        account_type: 預かり区分
        stock_type: 現物/信用
        quantity: 株数
        average_price: 取得単価
        **kwargs: その他のフィールド

    Returns:
        追加したレコードのID
    """
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # 既存のレコードを論理削除
        cursor.execute(
            """
            UPDATE holdings
            SET deleted_at = datetime('now')
            WHERE user_id = ? AND code = ? AND account_name = ?
              AND account_type = ? AND stock_type = ?
              AND (trade_position = ? OR (trade_position IS NULL AND ? IS NULL))
              AND deleted_at IS NULL
        """,
            (
                user_id,
                code,
                account_name,
                account_type,
                stock_type,
                kwargs.get("trade_position"),
                kwargs.get("trade_position"),
            ),
        )

        # 新しいレコードを挿入
        cursor.execute(
            """
            INSERT INTO holdings (
                user_id, code, account_name, account_type, stock_type,
                trade_position, margin_term, quantity, average_price, current_price,
                market_value, profit_loss, profit_loss_ratio, expected_per,
                actual_pbr, dividend_yield, expected_eps, actual_bps,
                expected_dividend, lending_type, acquisition_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                user_id,
                code,
                account_name,
                account_type,
                stock_type,
                kwargs.get("trade_position"),
                kwargs.get("margin_term"),
                quantity,
                average_price,
                kwargs.get("current_price"),
                kwargs.get("market_value"),
                kwargs.get("profit_loss"),
                kwargs.get("profit_loss_ratio"),
                kwargs.get("expected_per"),
                kwargs.get("actual_pbr"),
                kwargs.get("dividend_yield"),
                kwargs.get("expected_eps"),
                kwargs.get("actual_bps"),
                kwargs.get("expected_dividend"),
                kwargs.get("lending_type"),
                kwargs.get("acquisition_date"),
            ),
        )

        conn.commit()

        return cursor.lastrowid or 0


def update_holding(holding_id: int, **kwargs) -> bool:
    """
    保有銘柄を更新

    Args:
        holding_id: 保有銘柄ID
        **kwargs: 更新するフィールドと値

    Returns:
        更新成功フラグ
    """
    db_path = get_db_path()

    # 更新可能なフィールドを制限
    allowed_fields = [
        "quantity",
        "average_price",
        "current_price",
        "market_value",
        "profit_loss",
        "profit_loss_ratio",
        "expected_per",
        "actual_pbr",
        "dividend_yield",
        "expected_eps",
        "actual_bps",
        "expected_dividend",
        "lending_type",
        "acquisition_date",
    ]

    # 許可されたフィールドのみ抽出
    updates = {k: v for k, v in kwargs.items() if k in allowed_fields}

    if not updates:
        return False

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # SET句を構築
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.append(holding_id)

        cursor.execute(
            f"""
            UPDATE holdings
            SET {set_clause}, updated_at = datetime('now')
            WHERE id = ? AND deleted_at IS NULL
        """,
            values,
        )

        conn.commit()

        return cursor.rowcount > 0


def delete_holding(holding_id: int) -> bool:
    """
    保有銘柄を削除（論理削除）

    Args:
        holding_id: 保有銘柄ID

    Returns:
        削除成功フラグ
    """
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE holdings
            SET deleted_at = datetime('now')
            WHERE id = ? AND deleted_at IS NULL
        """,
            (holding_id,),
        )

        conn.commit()

        return cursor.rowcount > 0


def bulk_delete_holdings(user_id: int, account_name: str | None = None) -> int:
    """
    保有銘柄を一括削除（論理削除）

    Args:
        user_id: ユーザーID
        account_name: 口座名（Noneの場合は全口座）

    Returns:
        削除した件数
    """
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        query = """
            UPDATE holdings
            SET deleted_at = datetime('now')
            WHERE user_id = ? AND deleted_at IS NULL
        """
        params: list[Any] = [user_id]

        if account_name:
            query += " AND account_name = ?"
            params.append(account_name)

        cursor.execute(query, params)
        conn.commit()

        return cursor.rowcount


def get_all_accounts(user_id: int) -> list[str]:
    """
    ユーザーのすべての口座名を取得（削除済みも含む）

    Args:
        user_id: ユーザーID

    Returns:
        口座名のリスト
    """
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # 削除済みも含めてすべての口座名を取得
        cursor.execute(
            """
            SELECT DISTINCT account_name
            FROM holdings
            WHERE user_id = ?
            ORDER BY account_name
            """,
            (user_id,),
        )

        return [row[0] for row in cursor.fetchall()]


def search_listed_info(keyword: str) -> list[dict[str, Any]]:
    """
    銘柄情報を検索

    Args:
        keyword: 検索キーワード（銘柄コードまたは銘柄名の一部）

    Returns:
        銘柄情報リスト
    """
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 銘柄コードは4桁にパディング、5桁版も作成
        padded_code = keyword.zfill(4) if keyword.isdigit() else keyword
        padded_code_5digit = padded_code + "0" if keyword.isdigit() else keyword

        # より柔軟な検索クエリ（5桁のコードで検索）
        cursor.execute(
            """
            SELECT
                SUBSTR(code, 1, 4) as code,
                company_name,
                market_name,
                sector33_name
            FROM listed_info
            WHERE (code = ? OR code LIKE ? OR company_name LIKE ?)
              AND (delete_flag = 0 OR delete_flag IS NULL)
            ORDER BY
                CASE WHEN code = ? THEN 0 ELSE 1 END,
                code
            LIMIT 50
        """,
            (
                padded_code_5digit,
                f"{padded_code_5digit}%",
                f"%{keyword}%",
                padded_code_5digit,
            ),
        )

        return [dict(row) for row in cursor.fetchall()]

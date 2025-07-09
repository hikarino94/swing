"""
ポートフォリオ基本機能のルート定義
"""

import sqlite3
from typing import cast

from flask import Blueprint, jsonify
from flask import request as flask_request

from src.auth import login_required
from src.config import get_db_path
from src.portfolio import PortfolioManager
from src.types.flask_types import RequestWithUser, get_args_value
from src.utils.logging_config import get_logger

# ロガーの設定
logger = get_logger("web.portfolio.base")

# Blueprint作成
portfolio_base_bp = Blueprint("portfolio_base", __name__)

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)


@portfolio_base_bp.route("/funds", methods=["GET"])
@login_required
def get_funds():
    """投資信託一覧を取得"""
    try:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # 投資信託の保有情報を取得
        cursor.execute(
            """
            SELECT
                fh.fund_id,
                fm.fund_name,
                fh.account_name,
                fh.account_type,
                fh.quantity,
                fh.average_price,
                fh.market_value,
                fh.profit_loss,
                fh.profit_loss_ratio,
                fh.dividend_method,
                fh.updated_at,
                fp.nav as current_nav,
                fp.date as nav_date
            FROM fund_holdings fh
            JOIN fund_master fm ON fh.fund_id = fm.fund_id
            LEFT JOIN (
                SELECT fund_id, nav, date,
                       ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY date DESC) as rn
                FROM fund_prices
            ) fp ON fh.fund_id = fp.fund_id AND fp.rn = 1
            WHERE fh.user_id = ? AND fh.deleted_at IS NULL
            ORDER BY fh.account_type, fm.fund_name
            """,
            (request.current_user.id,),
        )

        funds_data = []
        total_value = 0
        total_profit_loss = 0

        for row in cursor.fetchall():
            fund_data = {
                "fund_id": row[0],
                "fund_name": row[1],
                "account_name": row[2],
                "account_type": row[3],
                "quantity": row[4],
                "average_price": row[5],
                "market_value": row[6],
                "profit_loss": row[7],
                "profit_loss_ratio": row[8],
                "dividend_method": row[9],
                "updated_at": row[10],
                "current_nav": row[11],
                "nav_date": row[12],
            }

            # 現在価値を再計算（基準価額がある場合）
            if row[11] is not None:  # current_nav
                fund_data["market_value"] = (
                    row[4] * row[11] / 10000
                )  # 口数 × 基準価額 / 10000
                fund_data["profit_loss"] = fund_data["market_value"] - (
                    row[4] * row[5] / 10000
                )
                fund_data["profit_loss_ratio"] = (
                    (fund_data["profit_loss"] / (row[4] * row[5] / 10000) * 100)
                    if row[5] > 0
                    else 0
                )

            funds_data.append(fund_data)

            if fund_data["market_value"]:
                total_value += fund_data["market_value"]
            if fund_data["profit_loss"]:
                total_profit_loss += fund_data["profit_loss"]

        conn.close()

        # 集計情報
        aggregate = {
            "total_funds": len(funds_data),
            "total_value": total_value,
            "total_profit_loss": total_profit_loss,
            "total_profit_loss_ratio": (
                (total_profit_loss / (total_value - total_profit_loss) * 100)
                if total_value > total_profit_loss
                else 0
            ),
        }

        return jsonify({"success": True, "funds": funds_data, "aggregated": aggregate})

    except Exception as e:
        logger.error(f"投資信託取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@portfolio_base_bp.route("/summary", methods=["GET"])
@login_required
def get_portfolio_summary():
    """ポートフォリオサマリーを取得"""
    try:
        summary = PortfolioManager.get_portfolio_summary(request.current_user.id)
        return jsonify({"success": True, "summary": summary})
    except Exception as e:
        logger.error(f"ポートフォリオサマリー取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@portfolio_base_bp.route("/accounts", methods=["GET"])
@login_required
def get_accounts():
    """口座一覧を取得"""
    try:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # 口座ごとの集計を取得
        cursor.execute(
            """
            SELECT DISTINCT account_name, account_type, COUNT(*) as holdings_count
            FROM holdings
            WHERE user_id = ?
            GROUP BY account_name, account_type
            ORDER BY account_type, account_name
            """,
            (request.current_user.id,),
        )

        accounts = []
        for row in cursor.fetchall():
            accounts.append(
                {
                    "account_name": row[0],
                    "account_type": row[1],
                    "holdings_count": row[2],
                }
            )

        conn.close()

        return jsonify({"success": True, "accounts": accounts})

    except Exception as e:
        logger.error(f"口座一覧取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@portfolio_base_bp.route("/stocks/search", methods=["GET"])
@login_required
def search_stocks():
    """銘柄検索"""
    try:
        query = get_args_value(request, "q", "").strip()
        if not query:
            return jsonify({"success": True, "stocks": []})

        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # コードまたは会社名で部分一致検索
        cursor.execute(
            """
            SELECT DISTINCT code, company_name, market_product
            FROM listed_info
            WHERE (code LIKE ? OR company_name LIKE ?)
            AND delete_flag = 0
            ORDER BY code
            LIMIT 50
            """,
            (f"%{query}%", f"%{query}%"),
        )

        stocks = []
        for row in cursor.fetchall():
            stocks.append(
                {
                    "code": row[0],
                    "company_name": row[1],
                    "market": row[2] if row[2] else "",
                }
            )

        conn.close()

        # 現在の価格情報も取得
        if stocks:
            conn = sqlite3.connect(get_db_path())
            cursor = conn.cursor()

            codes = [s["code"] for s in stocks]
            placeholders = ",".join("?" * len(codes))

            cursor.execute(
                f"""
                SELECT code, close, date
                FROM prices
                WHERE code IN ({placeholders})
                AND date = (SELECT MAX(date) FROM prices WHERE code = prices.code)
                """,
                codes,
            )

            price_data = {
                row[0]: {"close": row[1], "date": row[2]} for row in cursor.fetchall()
            }
            conn.close()

            # 価格情報を追加
            for stock in stocks:
                if stock["code"] in price_data:
                    stock["current_price"] = price_data[stock["code"]]["close"]
                    stock["price_date"] = price_data[stock["code"]]["date"]
                else:
                    stock["current_price"] = None
                    stock["price_date"] = None

        return jsonify({"success": True, "stocks": stocks})

    except Exception as e:
        logger.error(f"銘柄検索エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

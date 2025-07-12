"""
ポートフォリオ基本機能のルート定義
"""

import sqlite3
import time
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

# 検索結果のキャッシュ（10秒間有効）
_search_cache: dict[str, tuple[float, list]] = {}
_cache_expiry = 10  # seconds


def get_cached_search(query):
    """キャッシュから検索結果を取得"""
    if query in _search_cache:
        cache_time, result = _search_cache[query]
        if time.time() - cache_time < _cache_expiry:
            return result
    return None


def set_cached_search(query, result):
    """検索結果をキャッシュに保存"""
    _search_cache[query] = (time.time(), result)
    # 古いキャッシュを削除（100件を超えたら）
    if len(_search_cache) > 100:
        # 最も古いキャッシュを削除
        oldest_key = min(_search_cache.keys(), key=lambda k: _search_cache[k][0])
        del _search_cache[oldest_key]


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
        account_list = []  # プルダウン用のシンプルなリスト
        for row in cursor.fetchall():
            accounts.append(
                {
                    "account_name": row[0],
                    "account_type": row[1],
                    "holdings_count": row[2],
                    "display_name": f"{row[0]} ({row[1]})",  # 表示用の名前
                }
            )
            # プルダウン用のリストに追加
            account_list.append(f"{row[0]} ({row[1]})")

        conn.close()

        return jsonify(
            {"success": True, "accounts": accounts, "account_list": account_list}
        )

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

        # キャッシュをチェック
        cached_result = get_cached_search(query)
        if cached_result:
            return jsonify({"success": True, "stocks": cached_result})

        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # コードの前方一致または会社名の部分一致で検索
        cursor.execute(
            """
            SELECT DISTINCT code, company_name, market_code
            FROM listed_info
            WHERE (code LIKE ? OR company_name LIKE ?)
            AND delete_flag = 0
            ORDER BY
                CASE
                    WHEN code LIKE ? THEN 0  -- コードの前方一致を最優先
                    ELSE 1                   -- 会社名の一致
                END,
                code
            LIMIT 20
            """,
            (f"{query}%", f"%{query}%", f"{query}%"),
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

        # 価格情報の取得をスキップ（検索高速化のため）
        # 必要な場合のみ別途APIで取得

        # 結果をキャッシュに保存
        set_cached_search(query, stocks)

        return jsonify({"success": True, "stocks": stocks})

    except Exception as e:
        logger.error(f"銘柄検索エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

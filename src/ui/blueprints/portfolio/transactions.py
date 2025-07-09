"""ポートフォリオ - 取引履歴関連のルート"""

import sqlite3
from datetime import datetime
from typing import Any

import pandas as pd
from flask import jsonify

from src.auth import login_required
from src.config import get_db_path
from src.portfolio import PortfolioManager, SBICSVParser
from src.types.flask_types import (
    get_args_value,
    get_file,
    get_json_value,
    has_json_key,
)
from src.utils.cache import cache_result, clear_cache_by_prefix
from src.utils.logging_config import get_logger

from .base import portfolio_base_bp as portfolio_bp
from .base import request

# ロガーの設定
logger = get_logger("web.portfolio.transactions")


@portfolio_bp.route("/transactions", methods=["GET"])
@login_required
def get_transactions():
    """取引履歴一覧を取得"""
    try:

        # パラメータ取得
        code = get_args_value(request, "code")
        start_date = get_args_value(request, "start_date")
        end_date = get_args_value(request, "end_date")
        page = int(get_args_value(request, "page", "1"))
        per_page = int(get_args_value(request, "per_page", "50"))  # デフォルト50件

        # オフセットを計算
        offset = (page - 1) * per_page

        # 一括クエリで取引履歴を取得（実現損益の計算も含む）
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 基本的なクエリ条件
            query_conditions = ["t.user_id = ?"]
            query_params: list[Any] = [request.current_user.id]

            if code:
                query_conditions.append("t.code = ?")
                query_params.append(code)
            if start_date:
                query_conditions.append("t.transaction_date >= ?")
                query_params.append(start_date)
            if end_date:
                query_conditions.append("t.transaction_date <= ?")
                query_params.append(end_date)

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
            query_params.extend([per_page, offset])
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

        finally:
            conn.close()

        # ページネーション情報を含めて返す
        return jsonify(
            {
                "success": True,
                "transactions": trans_data,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total_count,
                    "pages": (total_count + per_page - 1) // per_page,
                },
            }
        )
    except Exception as e:
        logger.error(f"取引履歴取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@portfolio_bp.route("/transactions/upload", methods=["POST"])
@login_required
def upload_transactions():
    """取引履歴CSVアップロード"""
    try:
        file = get_file(request, "file")
        if not file or file.filename == "":
            return jsonify({"success": False, "error": "ファイルが選択されていません"})

        # CSVを読み込み（バイト列として渡してエンコーディングを自動検出）
        csv_content = file.read()
        logger.info(f"取引履歴CSVアップロード開始: {file.filename}")

        # 解析（エンコーディング検出はパーサー側で実施）
        try:
            transactions_data = SBICSVParser.parse_transactions_csv(csv_content)
            logger.info(f"CSV解析完了: {len(transactions_data)}件の取引を検出")
        except Exception as e:
            logger.error(f"CSV解析エラー: {str(e)}")
            return jsonify(
                {
                    "success": False,
                    "error": f"CSVファイルの解析に失敗しました: {str(e)}",
                }
            )

        if not transactions_data:
            return jsonify(
                {"success": False, "error": "取引データが見つかりませんでした"}
            )

        # 取引履歴をインポート
        try:
            imported = PortfolioManager.import_transactions_from_csv(
                request.current_user.id, transactions_data
            )
            logger.info(f"取引履歴インポート完了: {imported}件")

            # 部分的な成功も成功として扱う
            if imported > 0:
                total_count = len(transactions_data)
                if imported < total_count:
                    message = f"取引履歴を部分的にインポートしました（{imported}/{total_count}件）"
                    logger.warning(
                        f"一部の取引がインポートされませんでした: {total_count - imported}件"
                    )
                else:
                    message = f"取引履歴をインポートしました（{imported}件）"

                # キャッシュをクリア
                clear_cache_by_prefix(
                    f"transaction_performance_{request.current_user.id}"
                )

                return jsonify(
                    {
                        "success": True,
                        "message": message,
                        "imported": imported,
                        "total": total_count,
                        "partial": imported < total_count,
                    }
                )
            else:
                return jsonify(
                    {
                        "success": False,
                        "error": "取引をインポートできませんでした。データの形式を確認してください。",
                        "imported": 0,
                        "total": len(transactions_data),
                    }
                )
        except Exception as e:
            logger.error(f"インポート処理エラー: {str(e)}", exc_info=True)
            return jsonify(
                {
                    "success": False,
                    "error": f"インポート処理でエラーが発生しました: {str(e)}",
                }
            )

    except ValueError as e:
        logger.error(f"取引履歴アップロードエラー（値エラー）: {str(e)}")
        return jsonify({"success": False, "error": str(e)})
    except Exception as e:
        logger.error(f"取引履歴アップロードエラー: {str(e)}", exc_info=True)
        return jsonify(
            {"success": False, "error": f"アップロードに失敗しました: {str(e)}"}
        )


@portfolio_bp.route("/transactions/add", methods=["POST"])
@login_required
def add_transaction():
    """取引履歴を手動で追加"""
    try:
        code = get_json_value(request, "code", "").strip()
        transaction_date = get_json_value(request, "transaction_date", "").strip()
        transaction_type = get_json_value(request, "transaction_type", "").strip()
        quantity = get_json_value(request, "quantity")
        price = get_json_value(request, "price")
        commission = get_json_value(request, "commission", 0)
        tax = get_json_value(request, "tax", 0)
        remarks = get_json_value(request, "remarks", "").strip()

        # バリデーション
        if not code:
            return jsonify({"success": False, "error": "銘柄コードは必須です"})
        if not transaction_date:
            return jsonify({"success": False, "error": "取引日は必須です"})
        if transaction_type not in ["buy", "sell"]:
            return jsonify(
                {
                    "success": False,
                    "error": "取引種別は buy または sell を指定してください",
                }
            )
        if not quantity or quantity <= 0:
            return jsonify(
                {"success": False, "error": "数量は正の数を入力してください"}
            )
        if not price or price <= 0:
            return jsonify(
                {"success": False, "error": "価格は正の数を入力してください"}
            )

        # 取引を作成
        from src.portfolio.models.transaction import Transaction

        transaction = Transaction(
            user_id=request.current_user.id,
            code=code,
            transaction_date=transaction_date,
            transaction_type=transaction_type,
            quantity=quantity,
            price=price,
        )
        transaction.commission = commission
        transaction.tax = tax
        transaction.total_amount = quantity * price
        transaction.remarks = remarks

        # 詳細タイプを設定
        if transaction_type == "buy":
            transaction.detailed_type = "新規買い"
        else:
            transaction.detailed_type = "新規売り"

        # 保存
        if transaction.save():
            # キャッシュをクリア
            clear_cache_by_prefix(f"transaction_performance_{request.current_user.id}")

            logger.info(
                f"取引追加成功: {transaction_date} {code} {transaction_type} {quantity}株 @{price}円"
            )
            return jsonify({"success": True, "message": "取引を追加しました"})
        else:
            return jsonify({"success": False, "error": "取引の保存に失敗しました"})

    except Exception as e:
        logger.error(f"取引追加エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@portfolio_bp.route("/transactions/update/<int:transaction_id>", methods=["POST"])
@login_required
def update_transaction(transaction_id):
    """取引履歴を編集"""
    try:
        # バリデーション
        transaction_type = get_json_value(request, "transaction_type")
        if transaction_type and transaction_type not in ["buy", "sell"]:
            return jsonify(
                {
                    "success": False,
                    "error": "取引種別は buy または sell を指定してください",
                }
            )

        quantity = get_json_value(request, "quantity")
        if quantity is not None and quantity <= 0:
            return jsonify(
                {"success": False, "error": "数量は正の数を入力してください"}
            )

        price = get_json_value(request, "price")
        if price is not None and price <= 0:
            return jsonify(
                {"success": False, "error": "価格は正の数を入力してください"}
            )

        # 取引を取得して所有者を確認
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT user_id FROM transactions
            WHERE id = ?
            """,
            (transaction_id,),
        )

        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify(
                {"success": False, "error": "指定された取引が見つかりません"}
            )

        if row[0] != request.current_user.id:
            conn.close()
            return jsonify(
                {"success": False, "error": "この取引を編集する権限がありません"}
            )

        # 更新クエリを構築
        update_fields = []
        update_values = []

        transaction_date = get_json_value(request, "transaction_date")
        if transaction_date:
            update_fields.append("transaction_date = ?")
            update_values.append(transaction_date)

        if transaction_type:
            update_fields.append("transaction_type = ?")
            update_values.append(transaction_type)
            # 詳細タイプも更新
            update_fields.append("detailed_type = ?")
            update_values.append(
                "新規買い" if transaction_type == "buy" else "新規売り"
            )

        if quantity is not None:
            update_fields.append("quantity = ?")
            update_values.append(quantity)

        if price is not None:
            update_fields.append("price = ?")
            update_values.append(price)

        if has_json_key(request, "commission"):
            update_fields.append("commission = ?")
            update_values.append(get_json_value(request, "commission"))

        if has_json_key(request, "tax"):
            update_fields.append("tax = ?")
            update_values.append(get_json_value(request, "tax"))

        if has_json_key(request, "remarks"):
            update_fields.append("remarks = ?")
            update_values.append(get_json_value(request, "remarks"))

        # total_amountを再計算
        if quantity is not None or price is not None:
            # 現在の値を取得
            cursor.execute(
                "SELECT quantity, price FROM transactions WHERE id = ?",
                (transaction_id,),
            )
            current = cursor.fetchone()
            calc_quantity = quantity if quantity is not None else current[0]
            calc_price = price if price is not None else current[1]
            update_fields.append("total_amount = ?")
            update_values.append(calc_quantity * calc_price)

        if not update_fields:
            conn.close()
            return jsonify({"success": False, "error": "更新する項目がありません"})

        # 更新実行
        update_values.append(transaction_id)
        cursor.execute(
            f"""
            UPDATE transactions
            SET {', '.join(update_fields)}
            WHERE id = ?
            """,
            update_values,
        )

        conn.commit()
        conn.close()

        # キャッシュをクリア
        clear_cache_by_prefix(f"transaction_performance_{request.current_user.id}")

        logger.info(f"取引更新成功: ID={transaction_id}")
        return jsonify({"success": True, "message": "取引を更新しました"})

    except Exception as e:
        logger.error(f"取引更新エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@portfolio_bp.route("/transactions/delete/<int:transaction_id>", methods=["DELETE"])
@login_required
def delete_transaction(transaction_id):
    """取引履歴を削除"""
    try:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # 所有者確認と削除を同時に実行
        cursor.execute(
            """
            DELETE FROM transactions
            WHERE id = ? AND user_id = ?
            """,
            (transaction_id, request.current_user.id),
        )

        deleted = cursor.rowcount
        conn.commit()
        conn.close()

        if deleted > 0:
            # キャッシュをクリア
            clear_cache_by_prefix(f"transaction_performance_{request.current_user.id}")

            logger.info(f"取引削除成功: ID={transaction_id}")
            return jsonify({"success": True, "message": "取引を削除しました"})
        else:
            return jsonify(
                {"success": False, "error": "指定された取引が見つかりません"}
            )

    except Exception as e:
        logger.error(f"取引削除エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@portfolio_bp.route("/transactions/performance", methods=["GET"])
@login_required
def get_transaction_performance():
    """取引履歴のパフォーマンスを計算（キャッシュ付き）"""
    try:
        # キャッシュキー用のパラメータ
        user_id = request.current_user.id
        period = request.args.get("period", "all")  # all, 1y, 6m, 3m, 1m
        include_holdings = (
            request.args.get("include_holdings", "false").lower() == "true"
        )

        # キャッシュを使用した計算
        @cache_result(f"transaction_performance_{user_id}", ttl=300)  # 5分間キャッシュ
        def calculate_performance(period_param, include_holdings_param):
            return _calculate_transaction_performance(
                user_id, period_param, include_holdings_param
            )

        result = calculate_performance(period, include_holdings)
        return jsonify({"success": True, "data": result})

    except Exception as e:
        logger.error(f"取引パフォーマンス取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


def _calculate_transaction_performance(user_id, period, include_holdings):
    """実際のパフォーマンス計算処理"""
    # 期間に応じて開始日を設定
    end_date = datetime.now().strftime("%Y-%m-%d")
    if period == "1y":
        start_date = (datetime.now() - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
    elif period == "6m":
        start_date = (datetime.now() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
    elif period == "3m":
        start_date = (datetime.now() - pd.DateOffset(months=3)).strftime("%Y-%m-%d")
    elif period == "1m":
        start_date = (datetime.now() - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
    else:
        start_date = None

    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    # 期間内の取引を取得
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

    # 銘柄ごとのパフォーマンスを計算（シンプルな総額ベース）
    stock_performance = {}

    # 取引データを銘柄ごとに集計
    for trans in transactions:
        code = trans["code"]

        # 現物売り（決済売りでremarksが信用でない）はパフォーマンス集計から除外
        # 新規売りは全て信用取引（空売り）なので含める
        if (
            trans["transaction_type"] == "sell"
            and trans.get("detailed_type") == "決済売り"
            and trans.get("remarks", "") != "信用"
        ):
            continue

        if code not in stock_performance:
            stock_performance[code] = {
                "code": code,
                "company_name": trans.get("company_name", ""),
                "total_buy_amount": 0,
                "total_sell_amount": 0,
                "total_buy_quantity": 0,
                "total_sell_quantity": 0,
                "realized_profit": 0,
                "net_quantity": 0,
                "average_buy_price": 0,
                "transactions": [],
            }

        sp = stock_performance[code]
        sp["transactions"].append(trans)

        if trans["transaction_type"] == "buy":
            # 買付金額（手数料込み）
            buy_amount = trans["quantity"] * trans["price"] + (trans["commission"] or 0)
            sp["total_buy_amount"] += buy_amount
            sp["total_buy_quantity"] += trans["quantity"]
            sp["net_quantity"] += trans["quantity"]

        else:  # sell
            # 売却金額（手数料・税金控除後）
            sell_amount = (
                trans["quantity"] * trans["price"]
                - (trans["commission"] or 0)
                - (trans["tax"] or 0)
            )
            sp["total_sell_amount"] += sell_amount
            sp["total_sell_quantity"] += trans["quantity"]
            sp["net_quantity"] -= trans["quantity"]

        # 実現損益はtransactionsテーブルの値を使用（NULLは0として扱う）
        # 買い・売り両方で実現損益が記録されている場合があるため、全ての取引で加算
        realized_profit = trans.get("realized_profit") or 0
        sp["realized_profit"] += realized_profit

    # 銘柄ごとに平均買付価格を計算
    for _code, sp in stock_performance.items():
        if sp["total_buy_quantity"] > 0:
            sp["average_buy_price"] = sp["total_buy_amount"] / sp["total_buy_quantity"]

    # 含み損益の計算は行わない
    for _code, sp in stock_performance.items():
        sp["unrealized_profit"] = 0
        sp["current_price"] = None
        sp["market_value"] = None

    # 全体のパフォーマンスサマリー
    total_realized_profit = sum(
        sp["realized_profit"] for sp in stock_performance.values()
    )
    total_buy_amount = sum(sp["total_buy_amount"] for sp in stock_performance.values())
    total_sell_amount = sum(
        sp["total_sell_amount"] for sp in stock_performance.values()
    )

    summary = {
        "period": period,
        "start_date": start_date,
        "end_date": end_date,
        "total_realized_profit": total_realized_profit,
        "total_profit": total_realized_profit,  # 実現損益のみ
        "total_buy_amount": total_buy_amount,
        "total_sell_amount": total_sell_amount,
        "profit_rate": (
            (total_realized_profit / total_buy_amount * 100)
            if total_buy_amount > 0
            else 0
        ),
        "transaction_count": len(transactions),
        "stock_count": len(stock_performance),
    }

    # 月別損益の計算
    monthly_pnl = {}
    for trans in transactions:
        # 現物売りは除外
        if (
            trans["transaction_type"] == "sell"
            and trans.get("detailed_type") == "決済売り"
            and trans.get("remarks", "") != "信用"
        ):
            continue

        if (
            trans["transaction_type"] == "sell"
            and trans.get("realized_profit") is not None
        ):
            month = trans["transaction_date"][:7]  # YYYY-MM
            if month not in monthly_pnl:
                monthly_pnl[month] = 0
            monthly_pnl[month] += trans["realized_profit"]

    # 累積損益の計算
    cumulative_pnl = []
    cumulative_profit = 0
    for month in sorted(monthly_pnl.keys()):
        cumulative_profit += monthly_pnl[month]
        cumulative_pnl.append(
            {
                "month": month,
                "monthly_profit": monthly_pnl[month],
                "cumulative_profit": cumulative_profit,
            }
        )

    # 取引時間帯分布（仮データ - 実際の時間データがある場合は使用）
    trading_hours = {
        "9-10": len([t for t in transactions if t["transaction_type"] == "buy"]) // 4,
        "10-11": len([t for t in transactions if t["transaction_type"] == "buy"]) // 4,
        "11-12": len([t for t in transactions if t["transaction_type"] == "buy"]) // 4,
        "13-14": len([t for t in transactions if t["transaction_type"] == "buy"]) // 4,
        "14-15": len([t for t in transactions if t["transaction_type"] == "sell"]) // 2,
    }

    # 保有期間分布の計算
    holding_periods = {
        "1日以内": 0,
        "1週間以内": 0,
        "1ヶ月以内": 0,
        "3ヶ月以内": 0,
        "3ヶ月超": 0,
    }

    # 保有銘柄を含める場合の処理
    if include_holdings:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # 現在の保有銘柄を取得
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

        for row in cursor.fetchall():
            code = row[0]

            if code in stock_performance:
                # 取引履歴にある銘柄の場合、sourceを'both'に更新
                stock_performance[code]["source"] = "both"
            else:
                # 取引履歴にない銘柄の場合、新規追加
                stock_performance[code] = {
                    "code": code,
                    "company_name": row[1] or "",
                    "total_buy_amount": row[2] * row[3],  # 数量 × 平均取得価格
                    "total_sell_amount": 0,
                    "total_buy_quantity": row[2],
                    "total_sell_quantity": 0,
                    "realized_profit": 0,
                    "net_quantity": row[2],
                    "average_buy_price": row[3],
                    "unrealized_profit": row[5] or 0,  # profit_loss
                    "current_price": (
                        row[4] / row[2] if row[2] > 0 else None
                    ),  # market_value / quantity
                    "market_value": row[4],
                    "transactions": [],
                    "source": "holdings",  # 保有のみ
                }

        # ソース情報を既存の取引履歴銘柄に追加
        for _code, sp in stock_performance.items():
            if "source" not in sp:
                sp["source"] = "transaction"  # 取引履歴のみ

        # サマリーを再計算（保有銘柄を含める場合）
        total_realized_profit = sum(
            sp["realized_profit"] for sp in stock_performance.values()
        )
        total_unrealized_profit = sum(
            sp["unrealized_profit"] for sp in stock_performance.values()
        )
        total_buy_amount = sum(
            sp["total_buy_amount"] for sp in stock_performance.values()
        )
        total_sell_amount = sum(
            sp["total_sell_amount"] for sp in stock_performance.values()
        )
        total_profit = total_realized_profit + total_unrealized_profit

        summary.update(
            {
                "total_realized_profit": total_realized_profit,
                "total_unrealized_profit": total_unrealized_profit,
                "total_profit": total_profit,
                "total_buy_amount": total_buy_amount,
                "total_sell_amount": total_sell_amount,
                "profit_rate": (
                    (total_profit / total_buy_amount * 100)
                    if total_buy_amount > 0
                    else 0
                ),
                "stock_count": len(stock_performance),
            }
        )

        conn.close()

    # 結果を返す
    result = {
        "summary": summary,
        "stock_performance": list(stock_performance.values()),
        "monthly_pnl": monthly_pnl,
        "cumulative_pnl": cumulative_pnl,
        "trading_hours": trading_hours,
        "holding_periods": holding_periods,
    }

    return result

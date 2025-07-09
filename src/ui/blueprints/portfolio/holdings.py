"""
保有銘柄関連のルート定義
"""

import sqlite3
from typing import cast

from flask import Blueprint, jsonify
from flask import request as flask_request

from src.auth import login_required
from src.config import get_db_path
from src.portfolio import PortfolioManager, SBICSVParser
from src.types.flask_types import (
    RequestWithUser,
    get_args_value,
    get_file,
    get_form_value,
    get_json_value,
)
from src.utils.logging_config import get_logger

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)

# ロガーの設定
logger = get_logger("web.holdings")

# Blueprintの作成
holdings_bp = Blueprint("holdings", __name__)


@holdings_bp.route("/holdings", methods=["GET"])
@login_required
def get_holdings():
    """保有銘柄一覧を取得（株式と投資信託を統合）"""
    try:
        from src.portfolio.models.holding import Holding

        # 集約フラグを取得
        aggregate = get_args_value(request, "aggregate", "false").lower() == "true"

        holdings_data = []

        # 株式の保有銘柄を取得
        if aggregate:
            # 銘柄コードで集約
            holdings_data = PortfolioManager.aggregate_holdings_by_code(
                request.current_user.id
            )
        else:
            # 通常の一覧取得
            holdings = Holding.find_all_by_user(request.current_user.id)
            for h in holdings:
                holdings_data.append(
                    {
                        "type": "stock",  # 株式であることを示す
                        "code": h.code,
                        "company_name": h.company_name or "",
                        "account_name": h.account_name,
                        "account_type": getattr(
                            h, "account_type", "特定"
                        ),  # デフォルトは特定
                        "quantity": h.quantity,
                        "average_price": h.average_price,
                        "market_value": h.market_value,
                        "profit_loss": h.profit_loss,
                        "profit_loss_ratio": h.profit_loss_ratio,
                        "updated_at": h.updated_at,
                        # 株価指標データ
                        "expected_per": h.expected_per,
                        "actual_pbr": h.actual_pbr,
                        "dividend_yield": h.dividend_yield,
                        "expected_eps": h.expected_eps,
                        "actual_bps": h.actual_bps,
                        "expected_dividend": h.expected_dividend,
                        "lending_type": h.lending_type,
                    }
                )

        # 投資信託の保有情報を追加
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        if aggregate:
            # 集約表示の場合は投資信託も集約する
            cursor.execute(
                """
                SELECT
                    fm.fund_id,
                    fm.fund_name,
                    SUM(fh.quantity) as total_quantity,
                    SUM(fh.quantity * fh.average_price) / NULLIF(SUM(fh.quantity), 0) as weighted_avg_price,
                    SUM(fh.market_value) as total_market_value,
                    SUM(fh.profit_loss) as total_profit_loss,
                    COUNT(DISTINCT fh.account_name) as account_count,
                    GROUP_CONCAT(DISTINCT fh.account_name) as account_names,
                    GROUP_CONCAT(DISTINCT fh.account_type) as account_types,
                    MAX(fh.updated_at) as updated_at,
                    fp.nav as current_nav,
                    fp.date as nav_date
                FROM fund_holdings fh
                JOIN fund_master fm ON fh.fund_id = fm.fund_id
                LEFT JOIN (
                    SELECT fund_id, nav, date
                    FROM fund_prices fp1
                    WHERE date = (
                        SELECT MAX(date) FROM fund_prices fp2
                        WHERE fp2.fund_id = fp1.fund_id
                    )
                ) fp ON fh.fund_id = fp.fund_id
                WHERE fh.user_id = ? AND fh.quantity > 0 AND fh.deleted_at IS NULL
                GROUP BY fm.fund_id, fm.fund_name, fp.nav, fp.date
                ORDER BY fm.fund_name
            """,
                (request.current_user.id,),
            )
        else:
            # 通常表示の場合
            cursor.execute(
                """
                SELECT
                    fm.fund_id,
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
                    SELECT fund_id, nav, date
                    FROM fund_prices fp1
                    WHERE date = (
                        SELECT MAX(date) FROM fund_prices fp2
                        WHERE fp2.fund_id = fp1.fund_id
                    )
                ) fp ON fh.fund_id = fp.fund_id
                WHERE fh.user_id = ? AND fh.deleted_at IS NULL
                ORDER BY fm.fund_name
            """,
                (request.current_user.id,),
            )

        for row in cursor.fetchall():
            if aggregate:
                # 集約表示の場合
                fund_data = {
                    "type": "fund",  # 投資信託であることを示す
                    "fund_id": row[0],
                    "fund_name": row[1],
                    "total_quantity": row[2],  # 合計口数
                    "weighted_avg_price": row[3] or 0,  # 加重平均価格
                    "total_market_value": row[4],  # 合計評価額
                    "total_profit_loss": row[5],  # 合計損益
                    "account_count": row[6],
                    "account_names": row[7],
                    "account_types": row[8],
                    "profit_loss_ratio": 0,
                    "updated_at": row[9],
                    "current_nav": row[10],
                    "nav_date": row[11],
                    # 集約表示用の追加フィールド
                    "quantity": row[2],  # total_quantityと同じ値を設定
                    "average_price": row[3] or 0,
                    "market_value": row[4],
                    "profit_loss": row[5],
                    # 株式にはあるが投資信託にはない項目をNullで埋める
                    "code": None,
                    "company_name": row[1],  # fund_nameを使用
                    "expected_per": None,
                    "actual_pbr": None,
                    "dividend_yield": None,
                    "expected_eps": None,
                    "actual_bps": None,
                    "expected_dividend": None,
                    "lending_type": None,
                }

                # 損益率の計算
                total_cost = (
                    fund_data["total_quantity"]
                    * fund_data["weighted_avg_price"]
                    / 10000
                )
                if total_cost > 0:
                    fund_data["profit_loss_ratio"] = (
                        fund_data["total_profit_loss"] / total_cost
                    ) * 100

                # 現在価値を再計算（基準価額がある場合）
                if row[10] is not None:  # current_nav
                    fund_data["total_market_value"] = (
                        row[2] * row[10] / 10000
                    )  # 合計口数 × 基準価額 / 10000
                    fund_data["market_value"] = fund_data["total_market_value"]
                    fund_data["total_profit_loss"] = (
                        fund_data["total_market_value"] - total_cost
                    )
                    fund_data["profit_loss"] = fund_data["total_profit_loss"]
                    if total_cost > 0:
                        fund_data["profit_loss_ratio"] = (
                            fund_data["total_profit_loss"] / total_cost * 100
                        )
            else:
                # 通常表示の場合
                fund_data = {
                    "type": "fund",  # 投資信託であることを示す
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
                    # 株式にはあるが投資信託にはない項目をNullで埋める
                    "code": None,
                    "company_name": row[1],  # fund_nameを使用
                    "expected_per": None,
                    "actual_pbr": None,
                    "dividend_yield": None,
                    "expected_eps": None,
                    "actual_bps": None,
                    "expected_dividend": None,
                    "lending_type": None,
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

            holdings_data.append(fund_data)

        conn.close()

        return jsonify(
            {"success": True, "holdings": holdings_data, "aggregated": aggregate}
        )
    except Exception as e:
        logger.error(f"保有銘柄取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@holdings_bp.route("/holdings/upload", methods=["POST"])
@login_required
def upload_holdings():
    """保有銘柄CSVアップロード"""
    try:
        file = get_file(request, "file")
        if not file or file.filename == "":
            return jsonify({"success": False, "error": "ファイルが選択されていません"})

        # 口座名を取得（デフォルトは "default"）
        account_name = get_form_value(request, "account_name", "default").strip()
        if not account_name:
            account_name = "default"

        # CSVを読み込み（バイト列として渡してエンコーディングを自動検出）
        csv_content = file.read()
        logger.info(
            f"保有銘柄CSVアップロード開始: {file.filename} (口座: {account_name})"
        )

        # 解析（エンコーディング検出はパーサー側で実施）
        holdings_data = SBICSVParser.parse_holdings_csv(csv_content)

        # 保有銘柄を追加（更新ではなく追加）
        updated, new = PortfolioManager.update_holdings_from_csv(
            request.current_user.id, holdings_data, account_name
        )

        # 時価評価を更新
        PortfolioManager.update_market_values(request.current_user.id)

        logger.info(f"保有銘柄追加完了: {new}件（口座: {account_name}）")
        return jsonify(
            {
                "success": True,
                "message": f"保有銘柄を追加しました（{new}件、口座: {account_name}）",
                "updated": updated,
                "new": new,
                "account_name": account_name,
            }
        )
    except ValueError as e:
        logger.error(f"保有銘柄アップロードエラー（値エラー）: {str(e)}")
        return jsonify({"success": False, "error": str(e)})
    except Exception as e:
        logger.error(f"保有銘柄アップロードエラー: {str(e)}", exc_info=True)
        return jsonify(
            {"success": False, "error": f"アップロードに失敗しました: {str(e)}"}
        )


@holdings_bp.route("/holdings/delete", methods=["POST"])
@login_required
def delete_holdings():
    """保有銘柄を削除"""
    try:
        delete_type = get_json_value(request, "type", "all")  # all or account
        account_name = get_json_value(request, "account_name")

        if delete_type == "account" and not account_name:
            return jsonify({"success": False, "error": "口座名が指定されていません"})

        if delete_type == "account":
            # 特定口座の保有銘柄を削除
            deleted = PortfolioManager.delete_holdings_by_account(
                request.current_user.id, account_name
            )
            message = f"口座 '{account_name}' の保有銘柄を削除しました（{deleted}件）"
        else:
            # 全保有銘柄を削除
            deleted = PortfolioManager.delete_all_holdings(request.current_user.id)
            message = f"全ての保有銘柄を削除しました（{deleted}件）"

        logger.info(message)
        return jsonify({"success": True, "message": message, "deleted": deleted})
    except Exception as e:
        logger.error(f"保有銘柄削除エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@holdings_bp.route("/holdings/add", methods=["POST"])
@login_required
def add_holding():
    """保有銘柄を手動で追加"""
    try:
        code = get_json_value(request, "code", "").strip()
        # 証券コードが5桁の場合は末尾1桁を削除
        if len(code) == 5 and code.isdigit():
            code = code[:4]

        account_name = get_json_value(request, "account_name", "default").strip()
        quantity = get_json_value(request, "quantity")
        average_price = get_json_value(request, "average_price")
        company_name = get_json_value(request, "company_name", "").strip()

        # バリデーション
        if not code:
            return jsonify({"success": False, "error": "銘柄コードは必須です"})
        if quantity is None or quantity <= 0:
            return jsonify(
                {"success": False, "error": "数量は正の数を入力してください"}
            )
        if average_price is None or average_price <= 0:
            return jsonify(
                {"success": False, "error": "平均取得価格は正の数を入力してください"}
            )

        # 既存の保有銘柄をチェック
        from src.portfolio.models.holding import Holding

        existing = Holding.find_by_user_code_and_account(
            request.current_user.id, code, account_name
        )

        if existing:
            # 既存の保有銘柄がある場合は数量と平均価格を更新
            total_quantity = existing.quantity + quantity
            total_cost = (existing.quantity * existing.average_price) + (
                quantity * average_price
            )
            existing.quantity = total_quantity
            existing.average_price = total_cost / total_quantity
        else:
            # 新規追加
            existing = Holding(
                user_id=request.current_user.id, code=code, account_name=account_name
            )
            existing.quantity = quantity
            existing.average_price = average_price
            # 銘柄名を設定（DBには保存されないが、ログ用）
            if company_name:
                existing.company_name = company_name

        # 保存
        if existing.save():
            # 時価評価を更新
            PortfolioManager.update_market_values(request.current_user.id)
            logger.info(
                f"保有銘柄追加成功: {code} {quantity}株 @{average_price}円 (口座: {account_name})"
            )
            return jsonify({"success": True, "message": "保有銘柄を追加しました"})
        else:
            logger.error(f"保有銘柄の保存に失敗: {code} (口座: {account_name})")
            return jsonify({"success": False, "error": "保有銘柄の保存に失敗しました"})

    except Exception as e:
        logger.error(f"保有銘柄追加エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@holdings_bp.route("/holdings/update", methods=["POST"])
@login_required
def update_holding():
    """保有銘柄を編集"""
    try:
        code = get_json_value(request, "code", "").strip()
        account_name = get_json_value(request, "account_name", "").strip()
        quantity = get_json_value(request, "quantity")
        average_price = get_json_value(request, "average_price")

        # バリデーション
        if not code or not account_name:
            return jsonify({"success": False, "error": "銘柄コードと口座名は必須です"})
        if quantity is not None and quantity < 0:
            return jsonify({"success": False, "error": "数量は0以上を入力してください"})
        if average_price is not None and average_price <= 0:
            return jsonify(
                {"success": False, "error": "平均取得価格は正の数を入力してください"}
            )

        from src.portfolio.models.holding import Holding

        holding = Holding.find_by_user_code_and_account(
            request.current_user.id, code, account_name
        )

        if not holding:
            return jsonify(
                {"success": False, "error": "指定された保有銘柄が見つかりません"}
            )

        # 更新
        if quantity is not None:
            holding.quantity = quantity
        if average_price is not None:
            holding.average_price = average_price

        # 保存
        if holding.save():
            # 時価評価を更新
            PortfolioManager.update_market_values(request.current_user.id)
            logger.info(
                f"保有銘柄更新成功: {code} {holding.quantity}株 @{holding.average_price}円 (口座: {account_name})"
            )
            return jsonify({"success": True, "message": "保有銘柄を更新しました"})
        else:
            return jsonify({"success": False, "error": "保有銘柄の更新に失敗しました"})

    except Exception as e:
        logger.error(f"保有銘柄更新エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@holdings_bp.route("/holdings/delete/<code>/<account_name>", methods=["DELETE"])
@login_required
def delete_single_holding(code, account_name):
    """特定の保有銘柄を削除"""
    try:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        cursor.execute(
            """
            DELETE FROM holdings
            WHERE user_id = ? AND code = ? AND account_name = ?
            """,
            (request.current_user.id, code, account_name),
        )

        deleted = cursor.rowcount
        conn.commit()
        conn.close()

        if deleted > 0:
            logger.info(f"保有銘柄削除成功: {code} (口座: {account_name})")
            return jsonify({"success": True, "message": "保有銘柄を削除しました"})
        else:
            return jsonify(
                {"success": False, "error": "指定された保有銘柄が見つかりません"}
            )

    except Exception as e:
        logger.error(f"保有銘柄削除エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@holdings_bp.route("/indicators/update", methods=["POST"])
@login_required
def indicators_update():
    """保有銘柄の株価指標を一括更新"""
    try:
        # リクエストボディから銘柄コードリストを取得（オプション）
        data = request.get_json() or {}
        codes = data.get("codes", None)

        # 株価指標を更新
        updated_count = PortfolioManager.update_stock_indicators(
            request.current_user.id, codes
        )

        if updated_count > 0:
            return jsonify(
                {
                    "success": True,
                    "message": f"{updated_count}件の株価指標を更新しました",
                    "updated": updated_count,
                }
            )
        else:
            return jsonify(
                {
                    "success": True,
                    "message": "更新対象の銘柄がありませんでした",
                    "updated": 0,
                }
            )

    except Exception as e:
        logger.error(f"株価指標更新エラー: {str(e)}", exc_info=True)
        return jsonify(
            {"success": False, "error": f"株価指標の更新に失敗しました: {str(e)}"}
        )

"""
保有銘柄管理ルート
"""

import os
import tempfile
from typing import cast

from flask import jsonify
from flask import request as flask_request

from src.auth import login_required
from src.portfolio.csv_importer import import_holdings_csv
from src.portfolio.holdings import (
    add_holding,
    bulk_delete_holdings,
    delete_holding,
    get_all_accounts,
    get_holdings,
    search_listed_info,
    update_holding,
)
from src.types.flask_types import RequestWithUser
from src.ui.blueprints.holdings import holdings_bp
from src.ui.common import validate_csrf_token
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)


@holdings_bp.route("/list", methods=["GET"])
@login_required
def list_holdings():
    """保有銘柄一覧を取得"""
    try:
        user = request.current_user
        account_name = request.args.get("account_name")
        aggregate = request.args.get("aggregate", "false").lower() == "true"

        holdings = get_holdings(
            user_id=user.id, account_name=account_name, aggregate=aggregate
        )

        return jsonify({"success": True, "holdings": holdings})

    except Exception as e:
        logger.error(f"保有銘柄一覧取得エラー: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@holdings_bp.route("/import", methods=["POST"])
@login_required
def import_csv():
    """CSVファイルをインポート"""
    try:
        if not validate_csrf_token(request):
            return jsonify({"error": "Invalid CSRF token"}), 403

        user = request.current_user

        # フォームデータを取得
        account_name = request.form.get("account_name", "").strip()
        csv_type = request.form.get("csv_type", "spot")

        if not account_name:
            return (
                jsonify({"success": False, "message": "口座名を入力してください"}),
                400,
            )

        # ファイルを取得
        if "file" not in request.files:
            return (
                jsonify({"success": False, "message": "ファイルが選択されていません"}),
                400,
            )

        file = request.files["file"]
        if file.filename == "":
            return (
                jsonify({"success": False, "message": "ファイルが選択されていません"}),
                400,
            )

        # ファイルを一時保存
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as tmp:
            file.save(tmp.name)
            tmp_path = tmp.name

        try:
            # CSVをインポート
            imported_count = import_holdings_csv(
                user_id=user.id,
                account_name=account_name,
                file_path=tmp_path,
                csv_type=csv_type,
            )

            return jsonify(
                {
                    "success": True,
                    "message": f"{imported_count}件の銘柄をインポートしました",
                }
            )

        finally:
            # 一時ファイルを削除
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    except Exception as e:
        logger.error(f"CSVインポートエラー: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@holdings_bp.route("/add", methods=["POST"])
@login_required
def add_new_holding():
    """保有銘柄を手動追加"""
    try:
        if not validate_csrf_token(request):
            return jsonify({"error": "Invalid CSRF token"}), 403

        user = request.current_user
        data = request.get_json()

        # 必須項目のチェック
        required_fields = [
            "code",
            "account_name",
            "account_type",
            "quantity",
            "average_price",
        ]
        for field in required_fields:
            if field not in data:
                return (
                    jsonify({"success": False, "message": f"{field}は必須項目です"}),
                    400,
                )

        # デフォルト値の設定
        data.setdefault("stock_type", "現物")

        # 保有銘柄を追加
        holding_id = add_holding(user_id=user.id, **data)

        return jsonify(
            {
                "success": True,
                "message": "保有銘柄を追加しました",
                "holding_id": holding_id,
            }
        )

    except Exception as e:
        logger.error(f"保有銘柄追加エラー: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@holdings_bp.route("/update/<int:holding_id>", methods=["PUT"])
@login_required
def update_holding_info(holding_id):
    """保有銘柄を更新"""
    try:
        # user = request.user  # 現在未使用
        data = request.get_json()

        # 更新実行
        success = update_holding(holding_id, **data)

        if success:
            return jsonify({"success": True, "message": "保有銘柄を更新しました"})
        else:
            return (
                jsonify({"success": False, "message": "更新対象が見つかりません"}),
                404,
            )

    except Exception as e:
        logger.error(f"保有銘柄更新エラー: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@holdings_bp.route("/delete/<int:holding_id>", methods=["DELETE"])
@login_required
def delete_holding_info(holding_id):
    """保有銘柄を削除"""
    try:
        # user = request.user  # 現在未使用

        # 削除実行
        success = delete_holding(holding_id)

        if success:
            return jsonify({"success": True, "message": "保有銘柄を削除しました"})
        else:
            return (
                jsonify({"success": False, "message": "削除対象が見つかりません"}),
                404,
            )

    except Exception as e:
        logger.error(f"保有銘柄削除エラー: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@holdings_bp.route("/bulk_delete", methods=["POST"])
@login_required
def bulk_delete():
    """保有銘柄を一括削除"""
    try:
        if not validate_csrf_token(request):
            return jsonify({"error": "Invalid CSRF token"}), 403

        user = request.current_user
        data = request.get_json()

        account_name = data.get("account_name")

        # 一括削除実行
        deleted_count = bulk_delete_holdings(user_id=user.id, account_name=account_name)

        return jsonify(
            {"success": True, "message": f"{deleted_count}件の銘柄を削除しました"}
        )

    except Exception as e:
        logger.error(f"一括削除エラー: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@holdings_bp.route("/accounts", methods=["GET"])
@login_required
def list_accounts():
    """ユーザーの口座一覧を取得"""
    try:
        user = request.current_user
        accounts = get_all_accounts(user_id=user.id)

        return jsonify({"success": True, "accounts": accounts})

    except Exception as e:
        logger.error(f"口座一覧取得エラー: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@holdings_bp.route("/search", methods=["GET"])
@login_required
def search_stocks():
    """銘柄を検索"""
    try:
        keyword = request.args.get("keyword", "").strip()

        if not keyword:
            return jsonify({"success": True, "stocks": []})

        stocks = search_listed_info(keyword)

        return jsonify({"success": True, "stocks": stocks})

    except Exception as e:
        logger.error(f"銘柄検索エラー: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

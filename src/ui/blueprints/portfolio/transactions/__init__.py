"""ポートフォリオ - 取引履歴関連のルート

このモジュールは後方互換性のために既存のエンドポイントを維持しています。
実際の実装は各サブモジュールに分割されています。
"""

from flask import jsonify

from src.auth import login_required
from src.types.flask_types import get_args_value, get_file, get_json_value
from src.utils.cache import cache_result, clear_cache_by_prefix
from src.utils.logging_config import get_logger

from ..base import portfolio_base_bp as portfolio_bp
from ..base import request
from .repositories import TransactionRepository
from .services import TransactionService

# ロガーの設定
logger = get_logger("web.portfolio.transactions")

# サービスとリポジトリのインスタンス
transaction_repository = TransactionRepository()
transaction_service = TransactionService(transaction_repository)


@portfolio_bp.route("/transactions", methods=["GET"])
@login_required
def get_transactions():
    """取引履歴一覧を取得"""
    try:
        # パラメータ取得
        params = {
            "user_id": request.current_user.id,
            "code": get_args_value(request, "code"),
            "start_date": get_args_value(request, "start_date"),
            "end_date": get_args_value(request, "end_date"),
            "transaction_type": get_args_value(request, "transaction_type"),
            "page": int(get_args_value(request, "page", "1")),
            "per_page": int(get_args_value(request, "per_page", "50")),
        }

        result = transaction_service.get_transactions(params)
        return jsonify(result)

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

        csv_content = file.read()
        logger.info(f"取引履歴CSVアップロード開始: {file.filename}")

        result = transaction_service.import_transactions_csv(
            request.current_user.id, csv_content
        )

        # キャッシュをクリア
        if result["success"]:
            clear_cache_by_prefix(f"transaction_performance_{request.current_user.id}")

        return jsonify(result)

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
        transaction_data = {
            "user_id": request.current_user.id,
            "code": get_json_value(request, "code", "").strip(),
            "transaction_date": get_json_value(request, "transaction_date", "").strip(),
            "transaction_type": get_json_value(request, "transaction_type", "").strip(),
            "detailed_type": get_json_value(request, "detailed_type", "").strip(),
            "is_margin": get_json_value(request, "is_margin", False),
            "quantity": get_json_value(request, "quantity"),
            "price": get_json_value(request, "price"),
            "commission": get_json_value(request, "commission", 0),
            "tax": get_json_value(request, "tax", 0),
            "realized_profit": get_json_value(request, "realized_profit"),
            "remarks": get_json_value(request, "remarks", "").strip(),
        }

        result = transaction_service.add_transaction(transaction_data)

        # キャッシュをクリア
        if result["success"]:
            clear_cache_by_prefix(f"transaction_performance_{request.current_user.id}")

        return jsonify(result)

    except Exception as e:
        logger.error(f"取引追加エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@portfolio_bp.route("/transactions/update/<int:transaction_id>", methods=["POST"])
@login_required
def update_transaction(transaction_id):
    """取引履歴を編集"""
    try:
        transaction_data = {
            "user_id": request.current_user.id,
            "transaction_id": transaction_id,
            "transaction_date": get_json_value(request, "transaction_date"),
            "transaction_type": get_json_value(request, "transaction_type"),
            "detailed_type": get_json_value(request, "detailed_type"),
            "quantity": get_json_value(request, "quantity"),
            "price": get_json_value(request, "price"),
            "commission": get_json_value(request, "commission"),
            "tax": get_json_value(request, "tax"),
            "realized_profit": get_json_value(request, "realized_profit"),
            "remarks": get_json_value(request, "remarks"),
        }

        result = transaction_service.update_transaction(transaction_data)

        # キャッシュをクリア
        if result["success"]:
            clear_cache_by_prefix(f"transaction_performance_{request.current_user.id}")

        return jsonify(result)

    except Exception as e:
        logger.error(f"取引更新エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@portfolio_bp.route("/transactions/delete/<int:transaction_id>", methods=["DELETE"])
@login_required
def delete_transaction(transaction_id):
    """取引履歴を削除"""
    try:
        result = transaction_service.delete_transaction(
            request.current_user.id, transaction_id
        )

        # キャッシュをクリア
        if result["success"]:
            clear_cache_by_prefix(f"transaction_performance_{request.current_user.id}")

        return jsonify(result)

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
            return transaction_service.calculate_performance(
                user_id, period_param, include_holdings_param
            )

        result = calculate_performance(period, include_holdings)
        return jsonify({"success": True, "data": result})

    except Exception as e:
        logger.error(f"取引パフォーマンス取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

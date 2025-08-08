"""
デイトレード記録管理のルーティング
"""

from datetime import datetime
from typing import cast

from flask import Blueprint, jsonify, render_template
from flask import request as flask_request

from src.auth.decorators import trader_allowed
from src.types.flask_types import RequestWithUser
from src.ui.common import validate_csrf_token
from src.utils.logging_config import get_logger

from .services import DaytradeService

logger = get_logger("daytrade_routes")

daytrade_bp = Blueprint("daytrade", __name__, url_prefix="/api/daytrade")

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)


@daytrade_bp.route("/")
@trader_allowed
def index():
    """デイトレード管理画面のメインページ"""
    return render_template("daytrade/index.html")


@daytrade_bp.route("/calendar/<year>/<month>", methods=["GET"])
@trader_allowed
def calendar(year: str, month: str):
    """指定月のカレンダーデータを取得"""
    try:
        year_int = int(year)
        month_int = int(month)

        if not (1 <= month_int <= 12):
            return jsonify({"error": "Invalid month"}), 400

        service = DaytradeService(request.current_user.id)
        calendar_data = service.get_calendar_data(year_int, month_int)
        monthly_summary = service.get_monthly_summary(year_int, month_int)

        # デバッグログ
        logger.info(
            f"calendar_data keys: {calendar_data.keys() if calendar_data else 'None'}"
        )
        logger.info(
            f"monthly_summary keys: {monthly_summary.keys() if monthly_summary else 'None'}"
        )

        # フロントエンドが期待する形式に変換
        return jsonify(
            {
                "calendar_days": calendar_data.get("days", []) if calendar_data else [],
                "monthly_summary": monthly_summary or {},
            }
        )
    except ValueError:
        return jsonify({"error": "Invalid year or month"}), 400
    except Exception as e:
        import traceback

        logger.error(f"カレンダーデータ取得エラー: {e}")
        logger.error(f"エラータイプ: {type(e).__name__}")
        logger.error(f"スタックトレース:\n{traceback.format_exc()}")

        # より詳細なデバッグ情報を追加
        try:
            logger.error(f"year: {year_int}, month: {month_int}")
            logger.error(f"user_id: {request.current_user.id}")
        except Exception as debug_e:
            logger.error(f"デバッグ情報取得エラー: {debug_e}")

        return jsonify({"error": "カレンダーデータの取得に失敗しました"}), 500


@daytrade_bp.route("/import/futures", methods=["POST"])
@trader_allowed
def import_futures():
    """先物取引データのインポート"""
    if not validate_csrf_token(request):
        return jsonify({"error": "Invalid CSRF token"}), 403

    if "file" not in request.files:
        return jsonify({"error": "ファイルが選択されていません"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "ファイルが選択されていません"}), 400

    try:
        service = DaytradeService(request.current_user.id)
        result = service.import_futures_csv(file)

        message = f"{result['imported']}件のデータをインポートしました"
        if result["skipped"] > 0:
            message += f" ({result['skipped']}件はスキップ)"
        if result["errors"]:
            message += f" (エラー: {len(result['errors'])}件)"

        return jsonify({"success": True, "message": message, "details": result})
    except Exception as e:
        logger.error(f"先物データインポートエラー: {e}", exc_info=True)
        return (
            jsonify(
                {
                    "success": False,
                    "error": f"インポートエラー: {str(e)}",
                    "details": {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    },
                }
            ),
            500,
        )


@daytrade_bp.route("/import/stocks", methods=["POST"])
@trader_allowed
def import_stocks():
    """株式取引データのインポート（信用取引のみ）"""
    if not validate_csrf_token(request):
        return jsonify({"error": "Invalid CSRF token"}), 403

    if "file" not in request.files:
        return jsonify({"error": "ファイルが選択されていません"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "ファイルが選択されていません"}), 400

    try:
        service = DaytradeService(request.current_user.id)
        result = service.import_stocks_csv(file)

        return jsonify(
            {
                "success": True,
                "message": f"{result['imported']}件のデータをインポートしました",
                "details": result,
            }
        )
    except Exception as e:
        logger.error(f"株式データインポートエラー: {e}")
        return jsonify({"error": str(e)}), 500


@daytrade_bp.route("/import/spot-dividend", methods=["POST"])
@trader_allowed
def import_spot_dividend():
    """現物取引・配当金CSVのインポート"""
    if not validate_csrf_token(request):
        return jsonify({"error": "Invalid CSRF token"}), 403

    if "file" not in request.files:
        return jsonify({"error": "ファイルが選択されていません"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "ファイルが選択されていません"}), 400

    try:
        service = DaytradeService(request.current_user.id)
        result = service.import_spot_dividend_csv(file)

        return jsonify(
            {
                "success": True,
                "message": f"現物取引{result['spot_imported']}件をインポートしました（配当は取り込み対象外）",
                "details": result,
            }
        )
    except Exception as e:
        logger.error(f"現物・配当金データインポートエラー: {e}")
        return jsonify({"error": str(e)}), 500


@daytrade_bp.route("/import/dividends", methods=["POST"])
@trader_allowed
def import_dividends():
    """配当専用CSVのインポート"""
    if not validate_csrf_token(request):
        return jsonify({"error": "Invalid CSRF token"}), 403

    if "file" not in request.files:
        return jsonify({"error": "ファイルが選択されていません"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "ファイルが選択されていません"}), 400

    try:
        service = DaytradeService(request.current_user.id)
        result = service.import_dividends_csv(file)

        message = f"配当データ{result['imported']}件をインポートしました"
        if result.get("skipped", 0) > 0:
            message += f"（{result['skipped']}件スキップ）"
        if result.get("errors"):
            message += f"（エラー {len(result['errors'])}件）"

        return jsonify(
            {
                "success": True,
                "message": message,
                "details": result,
            }
        )
    except Exception as e:
        logger.error(f"配当データインポートエラー: {e}")
        return jsonify({"error": str(e)}), 500


@daytrade_bp.route("/summary/<year>/<month>", methods=["GET"])
@trader_allowed
def monthly_summary(year: str, month: str):
    """月別サマリーデータを取得"""
    try:
        year_int = int(year)
        month_int = int(month)

        if not (1 <= month_int <= 12):
            return jsonify({"error": "Invalid month"}), 400

        service = DaytradeService(request.current_user.id)
        summary = service.get_monthly_summary(year_int, month_int)

        return jsonify(summary)
    except ValueError:
        return jsonify({"error": "Invalid year or month"}), 400
    except Exception as e:
        logger.error(f"月別サマリー取得エラー: {e}")
        return jsonify({"error": "月別サマリーの取得に失敗しました"}), 500


@daytrade_bp.route("/details/<date>", methods=["GET"])
@trader_allowed
def daily_details(date: str):
    """指定日の取引詳細を取得"""
    try:
        # 日付形式の検証
        datetime.strptime(date, "%Y-%m-%d")

        service = DaytradeService(request.current_user.id)
        details = service.get_daily_details(date)

        return jsonify(details)
    except ValueError:
        return jsonify({"error": "Invalid date format"}), 400
    except Exception as e:
        logger.error(f"日別詳細取得エラー: {e}")
        return jsonify({"error": "日別詳細の取得に失敗しました"}), 500


@daytrade_bp.route("/cumulative/<start_date>/<end_date>", methods=["GET"])
@trader_allowed
def cumulative_profit(start_date: str, end_date: str):
    """指定期間の累積損益データを取得"""
    try:
        # 日付形式の検証
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        service = DaytradeService(request.current_user.id)
        cumulative_data = service.get_cumulative_profit_data(start_date, end_date)

        return jsonify(cumulative_data)
    except ValueError:
        return jsonify({"error": "Invalid date format"}), 400
    except Exception as e:
        logger.error(f"累積損益データ取得エラー: {e}")
        return jsonify({"error": "累積損益データの取得に失敗しました"}), 500


@daytrade_bp.route("/trades", methods=["GET"])
@trader_allowed
def trade_list():
    """取引履歴一覧を取得"""
    try:
        # クエリパラメータから期間を取得
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))

        service = DaytradeService(request.current_user.id)
        trades = service.get_trade_list(start_date, end_date, page, per_page)

        return jsonify(trades)
    except Exception as e:
        logger.error(f"取引履歴取得エラー: {e}")
        return jsonify({"error": "取引履歴の取得に失敗しました"}), 500


@daytrade_bp.route("/trade", methods=["POST"])
@trader_allowed
def create_trade():
    """取引を手動登録"""
    if not validate_csrf_token(request):
        return jsonify({"error": "Invalid CSRF token"}), 403

    try:
        service = DaytradeService(request.current_user.id)
        result = service.create_trade(request.json)

        return jsonify({"success": True, "trade_id": result["id"]})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"取引登録エラー: {e}")
        return jsonify({"error": "取引の登録に失敗しました"}), 500


@daytrade_bp.route("/trade/<int:trade_id>", methods=["PUT"])
@trader_allowed
def update_trade(trade_id: int):
    """取引を編集"""
    if not validate_csrf_token(request):
        return jsonify({"error": "Invalid CSRF token"}), 403

    try:
        service = DaytradeService(request.current_user.id)
        service.update_trade(trade_id, request.json)

        return jsonify({"success": True})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"取引更新エラー: {e}")
        return jsonify({"error": "取引の更新に失敗しました"}), 500


@daytrade_bp.route("/trade/<int:trade_id>", methods=["DELETE"])
@trader_allowed
def delete_trade(trade_id: int):
    """取引を削除"""
    if not validate_csrf_token(request):
        return jsonify({"error": "Invalid CSRF token"}), 403

    try:
        # リクエストボディからカテゴリを取得
        trade_category = request.json.get("trade_category") if request.json else None

        service = DaytradeService(request.current_user.id)
        service.delete_trade(trade_id, trade_category)

        return jsonify({"success": True})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"取引削除エラー: {e}")
        return jsonify({"error": "取引の削除に失敗しました"}), 500


@daytrade_bp.route("/monthly", methods=["GET"])
@trader_allowed
def monthly_profit():
    """月別損益データを取得"""
    try:
        year = int(request.args.get("year", datetime.now().year))

        service = DaytradeService(request.current_user.id)
        monthly_data = service.get_monthly_profit_data(year)

        return jsonify(monthly_data)
    except Exception as e:
        logger.error(f"月別損益データ取得エラー: {e}")
        return jsonify({"error": "月別損益データの取得に失敗しました"}), 500

"""
データフェッチ関連のルート定義
"""

import sys
from datetime import datetime
from typing import cast

from flask import Blueprint, jsonify
from flask import request as flask_request

from src.auth import admin_required, login_required
from src.types.flask_types import RequestWithUser, get_json_value
from src.ui.common import run_command
from src.utils.logging_config import get_logger

# ロガーの設定
logger = get_logger("web.fetch")

# Blueprint作成
fetch_bp = Blueprint("fetch", __name__, url_prefix="/api/fetch")

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)


@fetch_bp.route("/quotes", methods=["POST"])
@login_required
@admin_required
def fetch_quotes():
    """株価データ取得"""
    logger.info("株価データ取得APIが呼び出されました")
    print(
        f"\n[API] 株価データ取得リクエストを受信しました - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        cmd = [sys.executable, "fetch/daily_quotes.py"]

        start_date = get_json_value(request, "start_date")
        if start_date:
            cmd.extend(["--start", start_date])
            print(f"[API] 開始日: {start_date}")

        end_date = get_json_value(request, "end_date")
        if end_date:
            cmd.extend(["--end", end_date])
            print(f"[API] 終了日: {end_date}")

        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), "株価データ取得")

        if result["success"]:
            logger.info("株価データ取得が正常に完了しました")
            print("[API] 株価データ取得が正常に完了しました")
        else:
            logger.error(f"株価データ取得でエラーが発生しました: {result['error']}")
            print(f"[API] エラー: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"株価データ取得APIでエラーが発生しました: {str(e)}")
        print(f"[API] 例外エラー: {str(e)}")
        return jsonify(
            {"success": False, "error": str(e), "description": "株価データ取得"}
        )


@fetch_bp.route("/listed", methods=["POST"])
@login_required
@admin_required
def fetch_listed():
    """上場情報取得"""
    logger.info("上場情報取得APIが呼び出されました")
    print(
        f"\n[API] 上場情報取得リクエストを受信しました - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        cmd = [sys.executable, "fetch/listed_info.py"]
        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), "上場情報取得")

        if result["success"]:
            logger.info("上場情報取得が正常に完了しました")
            print("[API] 上場情報取得が正常に完了しました")
        else:
            logger.error(f"上場情報取得でエラーが発生しました: {result['error']}")
            print(f"[API] エラー: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"上場情報取得APIでエラーが発生しました: {str(e)}")
        print(f"[API] 例外エラー: {str(e)}")
        return jsonify(
            {"success": False, "error": str(e), "description": "上場情報取得"}
        )


@fetch_bp.route("/statements", methods=["POST"])
@login_required
@admin_required
def fetch_statements():
    """財務諸表取得"""
    logger.info("財務諸表取得APIが呼び出されました")
    print(
        f"\n[API] 財務諸表取得リクエストを受信しました - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        cmd = [sys.executable, "fetch/statements.py"]

        mode = get_json_value(request, "mode", "2")  # デフォルトは日次取得モード
        cmd.append(mode)
        print(f"[API] モード: {mode}")

        start_date = get_json_value(request, "start_date")
        if start_date:
            cmd.extend(["--start", start_date])
            print(f"[API] 開始日: {start_date}")

        end_date = get_json_value(request, "end_date")
        if end_date:
            cmd.extend(["--end", end_date])
            print(f"[API] 終了日: {end_date}")

        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), f"財務諸表{mode}")

        if result["success"]:
            logger.info(f"財務諸表取得（モード{mode}）が正常に完了しました")
            print(f"[API] 財務諸表取得（モード{mode}）が正常に完了しました")
        else:
            logger.error(f"財務諸表取得でエラーが発生しました: {result['error']}")
            print(f"[API] エラー: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"財務諸表取得APIでエラーが発生しました: {str(e)}")
        print(f"[API] 例外エラー: {str(e)}")
        return jsonify(
            {"success": False, "error": str(e), "description": "財務諸表取得"}
        )

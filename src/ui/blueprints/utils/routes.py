"""
ユーティリティ関連のルート定義
"""

import json
import sys
from pathlib import Path
from typing import cast

from flask import Blueprint, jsonify
from flask import request as flask_request

from src.auth import admin_required, login_required
from src.config import get_account_credentials
from src.types.flask_types import RequestWithUser, get_json_value
from src.ui.common import run_command
from src.utils.logging_config import get_logger

# ロガーの設定
logger = get_logger("web.utils")

# Blueprint作成
utils_bp = Blueprint("utils", __name__, url_prefix="/api/utils")

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)


@utils_bp.route("/update_token", methods=["POST"])
@login_required
@admin_required
def update_token():
    """IDトークン更新"""
    logger.info("IDトークン更新APIが呼び出されました")
    try:
        email = get_json_value(request, "email", "").strip()
        password = get_json_value(request, "password", "").strip()

        # メールアドレスまたはパスワードが空の場合、account.jsonから読み込む
        if not email or not password:
            try:
                account_data = get_account_credentials()
                if not email:
                    email = account_data.get("email", "")
                if not password:
                    password = account_data.get("password", "")
            except FileNotFoundError:
                return jsonify(
                    {
                        "success": False,
                        "error": "config/account.jsonが見つかりません。メールアドレスとパスワードを入力してください。",
                    }
                )
            except json.JSONDecodeError:
                return jsonify(
                    {"success": False, "error": "config/account.jsonの形式が不正です。"}
                )

        if not email or not password:
            return jsonify(
                {
                    "success": False,
                    "error": "メールアドレスとパスワードを入力するか、config/account.jsonに設定してください。",
                }
            )

        # update_idtoken.pyにメールアドレスとパスワードを渡す
        cmd = [
            sys.executable,
            "src/cli/update_idtoken.py",
            "--mail",
            email,
            "--password",
            password,
        ]
        logger.info(
            f"実行コマンド: {' '.join(cmd[:4])} *** ***"
        )  # パスワード部分はマスク
        result = run_command(" ".join(cmd), "IDトークン更新")

        if result["success"]:
            logger.info("IDトークン更新が正常に完了しました")
        else:
            logger.error(f"IDトークン更新でエラーが発生しました: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"IDトークン更新APIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@utils_bp.route("/db_summary", methods=["GET"])
@login_required
@admin_required
def db_summary():
    """DBサマリー取得"""
    logger.info("DBサマリー取得APIが呼び出されました")
    try:
        cmd = [sys.executable, "db/db_summary.py"]
        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), "DBサマリー")

        if result["success"]:
            logger.info("DBサマリー取得が正常に完了しました")
        else:
            logger.error(f"DBサマリー取得でエラーが発生しました: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"DBサマリー取得APIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@utils_bp.route("/list_signals", methods=["POST"])
@login_required
@admin_required
def list_signals():
    """シグナル一覧取得"""
    logger.info("シグナル一覧取得APIが呼び出されました")
    try:
        cmd = [sys.executable, "db/list_signals.py"]

        signal_type = get_json_value(request, "type", "fund")
        cmd.append(signal_type)

        start_date = get_json_value(request, "start_date")
        if start_date:
            cmd.extend(["--start", start_date])

        end_date = get_json_value(request, "end_date")
        if end_date:
            cmd.extend(["--end", end_date])

        limit = get_json_value(request, "limit")
        if limit:
            cmd.extend(["--limit", str(limit)])

        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), f"{signal_type}シグナル一覧")

        if result["success"]:
            logger.info(f"{signal_type}シグナル一覧取得が正常に完了しました")
        else:
            logger.error(
                f"{signal_type}シグナル一覧取得でエラーが発生しました: {result['error']}"
            )

        return jsonify(result)
    except Exception as e:
        logger.error(f"シグナル一覧取得APIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@utils_bp.route("/analyze_json", methods=["POST"])
@login_required
@admin_required
def analyze_json():
    """JSON分析"""
    logger.info("JSON分析APIが呼び出されました")
    try:
        files = get_json_value(request, "files", [])
        analysis_type = get_json_value(
            request, "analysis_type", "basic"
        )  # basic or advanced

        if not files:
            logger.warning("JSON分析でファイルが選択されていません")
            return jsonify({"success": False, "error": "ファイルが選択されていません"})

        if analysis_type == "advanced":
            # 高度な分析を使用
            cmd = [sys.executable, "backtest/analyze_json_advanced.py"] + files

            # 高度な分析のオプション
            if get_json_value(request, "export_excel"):
                cmd.append("--export-excel")
            if get_json_value(request, "export_pdf"):
                cmd.append("--export-pdf")
            if get_json_value(request, "compare"):
                cmd.append("--compare")

            output_dir = Path("data/output/analysis")
            output_dir.mkdir(parents=True, exist_ok=True)
            cmd.extend(["--output-dir", str(output_dir)])
        else:
            # 基本分析を使用
            cmd = [sys.executable, "backtest/analyze_backtest_json.py"] + files

        # 共通オプション
        if get_json_value(request, "show_trades"):
            cmd.append("--show-trades")

        side = get_json_value(request, "side")
        if side:
            cmd.extend(["--side", side])

        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), "JSON分析")

        # 高度な分析の場合、生成されたファイルのパスも返す
        if result["success"] and analysis_type == "advanced":
            analysis_files = list(Path("data/output/analysis").glob("*"))
            # 最新のファイルを取得
            latest_files = sorted(
                analysis_files, key=lambda p: p.stat().st_mtime, reverse=True
            )[:5]
            result["generated_files"] = [
                {"name": f.name, "path": str(f.relative_to(Path.cwd()))}
                for f in latest_files
            ]
            logger.info(f"高度な分析で生成されたファイル: {result['generated_files']}")

        if result["success"]:
            logger.info("JSON分析が正常に完了しました")
        else:
            logger.error(f"JSON分析でエラーが発生しました: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"JSON分析APIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@utils_bp.route("/thresholds", methods=["GET", "POST"])
@login_required
@admin_required
def thresholds():
    """闾値設定の取得/更新"""
    # configモジュールから設定ファイルパスを取得
    from src.config import config

    threshold_file = config.get_file_path("thresholds")

    if request.method == "GET":
        try:
            with open(threshold_file) as f:
                return jsonify({"success": True, "data": json.load(f)})
        except Exception as e:
            logger.error(f"閾値設定ファイル読み込みエラー: {threshold_file} - {e}")
            return jsonify({"success": False, "error": str(e)})

    else:  # POST
        try:
            data = (
                request.json
            )  # このケースでは全体のJSONを保存するのでget_json_valueは使わない
            if data:
                with open(threshold_file, "w") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            return jsonify({"success": True, "message": "闾値設定を保存しました"})
        except Exception as e:
            logger.error(f"閾値設定ファイル書き込みエラー: {threshold_file} - {e}")
            return jsonify({"success": False, "error": str(e)})

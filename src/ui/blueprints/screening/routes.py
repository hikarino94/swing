"""
スクリーニング関連のルート定義
"""

import sqlite3
import sys
from typing import cast

import pandas as pd
from flask import Blueprint, jsonify
from flask import request as flask_request

from src.auth import admin_required, login_required
from src.config import get_db_path
from src.types.flask_types import RequestWithUser, get_json_value
from src.ui.common import run_command, timestamped_path
from src.utils.logging_config import get_logger

# ロガーの設定
logger = get_logger("web.screening")

# Blueprint作成
screening_bp = Blueprint("screening", __name__, url_prefix="/api/screen")

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)


@screening_bp.route("/fundamental", methods=["POST"])
@login_required
@admin_required
def screen_fundamental():
    """ファンダメンタルスクリーニング"""
    cmd = [sys.executable, "screening/screen_statements.py"]

    lookback = get_json_value(request, "lookback")
    if lookback:
        cmd.extend(["--lookback", str(lookback)])

    recent = get_json_value(request, "recent")
    if recent:
        cmd.extend(["--recent", str(recent)])

    as_of = get_json_value(request, "as_of")
    if as_of:
        cmd.extend(["--as-of", as_of])

    result = run_command(" ".join(cmd), "ファンダメンタルスクリーニング")

    # スクリーニング成功時、DBから結果を取得してExcelファイルを生成
    if result["success"]:
        try:
            output_file = timestamped_path("screening", "fundamental", ".xlsx")
            conn = sqlite3.connect(get_db_path())

            # 最新のシグナルを取得
            query = """
                SELECT fs.*, li.company_name
                FROM fundamental_signals fs
                LEFT JOIN listed_info li ON fs.code = li.code
                WHERE DATE(fs.created_at) = DATE('now')
                ORDER BY fs.created_at DESC
            """
            df = pd.read_sql(query, conn)
            conn.close()

            if not df.empty:
                # Excelファイルに出力
                with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                    df.to_excel(writer, sheet_name="Signals", index=False)

                    # 列幅の自動調整
                    worksheet = writer.sheets["Signals"]
                    for i, col in enumerate(df.columns):
                        max_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
                        worksheet.set_column(i, i, min(max_len, 50))

                result["output_file"] = output_file
            else:
                result["output_file"] = None
                result["message"] = "スクリーニング結果がありません"
                logger.info("スクリーニング結果はありませんでした")
        except Exception as e:
            logger.error(f"Excel出力エラー: {str(e)}")
            result["error"] += f"\nExcel出力エラー: {str(e)}"
            result["output_file"] = None
    else:
        result["output_file"] = None

    if result["success"]:
        logger.info("ファンダメンタルスクリーニングが正常に完了しました")
    else:
        logger.error(
            f"ファンダメンタルスクリーニングでエラーが発生しました: {result['error']}"
        )

    return jsonify(result)


@screening_bp.route("/technical", methods=["POST"])
@login_required
@admin_required
def screen_technical():
    """テクニカルスクリーニング"""
    logger.info("テクニカルスクリーニングAPIが呼び出されました")
    try:
        # 高速版を使用
        cmd = [sys.executable, "screening/screen_technical.py"]

        action = get_json_value(request, "action", "screen")
        cmd.append(action)

        # indicatorsとscreenの両方でas_ofとlookbackパラメータを渡す
        as_of = get_json_value(request, "as_of")
        if as_of:
            cmd.extend(["--as-of", as_of])

        lookback = get_json_value(request, "lookback")
        if lookback:
            cmd.extend(["--lookback", str(lookback)])

        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), f"テクニカル{action}")

        # screen実行成功時、DBから結果を取得してExcelファイルを生成
        if result["success"] and action == "screen":
            try:
                output_file = timestamped_path("screening", "technical", ".xlsx")
                conn = sqlite3.connect(get_db_path())

                # 最新のシグナルを取得
                as_of_date = get_json_value(request, "as_of")
                if as_of_date:
                    query = """
                    SELECT ti.*, li.company_name
                    FROM technical_indicators ti
                    LEFT JOIN listed_info li ON ti.code = li.code
                    WHERE ti.signal_date = ?
                    AND (ti.signals_count >= 3 OR ti.signals_short_count >= 3)
                    ORDER BY ti.signals_count DESC, ti.signals_short_count DESC
                """
                    df = pd.read_sql(query, conn, params=[as_of_date])
                else:
                    query = """
                    SELECT ti.*, li.company_name
                    FROM technical_indicators ti
                    LEFT JOIN listed_info li ON ti.code = li.code
                    WHERE ti.signal_date = (SELECT MAX(signal_date) FROM technical_indicators)
                    AND (ti.signals_count >= 3 OR ti.signals_short_count >= 3)
                    ORDER BY ti.signals_count DESC, ti.signals_short_count DESC
                """
                    df = pd.read_sql(query, conn)

                conn.close()

                if not df.empty:
                    # Excelファイルに出力
                    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                        df.to_excel(writer, sheet_name="Signals", index=False)

                        # 列幅の自動調整
                        worksheet = writer.sheets["Signals"]
                        for i, col in enumerate(df.columns):
                            max_len = (
                                max(df[col].astype(str).str.len().max(), len(col)) + 2
                            )
                            worksheet.set_column(i, i, min(max_len, 50))

                    result["output_file"] = output_file
                else:
                    result["output_file"] = None
                    result["message"] = "スクリーニング結果がありません"
                    logger.info("スクリーニング結果はありませんでした")
            except Exception as e:
                logger.error(f"Excel出力エラー: {str(e)}")
                result["error"] += f"\nExcel出力エラー: {str(e)}"
                result["output_file"] = None
        else:
            result["output_file"] = None

        if result["success"] and action == "screen":
            logger.info("テクニカルスクリーニングが正常に完了しました")
        elif not result["success"]:
            logger.error(
                f"テクニカルスクリーニングでエラーが発生しました: {result['error']}"
            )

        return jsonify(result)
    except Exception as e:
        logger.error(f"テクニカルスクリーニングAPIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@screening_bp.route("/ml", methods=["POST"])
@login_required
@admin_required
def screen_ml():
    """MLスクリーニング"""
    cmd = [sys.executable, "screening/screen_ml.py"]

    action = get_json_value(request, "action", "screen")
    cmd.append(action)

    if action == "train":
        if get_json_value(request, "force"):
            cmd.append("--force")
    elif action == "screen":
        top = get_json_value(request, "top")
        if top:
            cmd.extend(["--top", str(top)])

        lookback = get_json_value(request, "lookback")
        if lookback:
            cmd.extend(["--lookback", str(lookback)])

        as_of = get_json_value(request, "as_of")
        if as_of:
            cmd.extend(["--as-of", as_of])
        # MLスクリーニングはExcel出力をサポートしていないため、結果をテキストで取得

    result = run_command(" ".join(cmd), f"ML{action}")
    # MLスクリーニングはテキスト出力のみなのでoutput_fileはNone
    result["output_file"] = None
    return jsonify(result)

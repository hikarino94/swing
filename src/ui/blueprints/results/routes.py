"""
結果管理関連のルート定義
"""

from datetime import datetime
from pathlib import Path
from typing import cast

from flask import Blueprint, jsonify
from flask import request as flask_request
from flask import send_file

from src.auth import login_required
from src.types.flask_types import RequestWithUser, get_args_value
from src.utils.logging_config import get_logger

# ロガーの設定
logger = get_logger("web.results")

# Blueprint作成
results_bp = Blueprint("results", __name__, url_prefix="/api/results")

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)


@results_bp.route("/list", methods=["GET"])
@login_required
def list_results():
    """結果ファイル一覧取得"""
    result_types = get_args_value(request, "types", "xlsx,json").split(",")
    category = get_args_value(request, "category", "")
    files = []

    # data/output/以下のファイルを検索
    project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
    output_dir = project_root / "data" / "output"

    # カテゴリが指定されている場合はそのディレクトリのみ検索
    if category:
        search_dirs = (
            [output_dir / category] if (output_dir / category).exists() else []
        )
    else:
        search_dirs = [
            output_dir / cat
            for cat in ["backtest", "screening", "reports"]
            if (output_dir / cat).exists()
        ]

    for search_dir in search_dirs:
        for ext in result_types:
            pattern = f"*.{ext}"
            for file_path in search_dir.glob(pattern):
                if not file_path.name.startswith("."):
                    relative_path = file_path.relative_to(output_dir)
                    files.append(
                        {
                            "name": file_path.name,
                            "path": str(relative_path),
                            "category": relative_path.parent.name,
                            "size": file_path.stat().st_size,
                            "modified": datetime.fromtimestamp(
                                file_path.stat().st_mtime
                            ).isoformat(),
                            "type": ext,
                        }
                    )

    files.sort(key=lambda x: x["modified"], reverse=True)
    return jsonify({"success": True, "files": files})


@results_bp.route("/download/<path:filepath>")
@login_required
def download_result(filepath):
    """結果ファイルダウンロード"""
    logger.info(f"結果ファイルダウンロードが要求されました: {filepath}")
    try:
        # パスのセキュリティチェック
        safe_path = Path(filepath)
        if ".." in safe_path.parts:
            logger.warning(f"不正なファイルパスが指定されました: {filepath}")
            raise ValueError("Invalid file path")

        project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
        full_path = project_root / "data" / "output" / safe_path

        if not full_path.exists() or not full_path.is_file():
            logger.warning(f"ファイルが存在しません: {filepath}")
            raise FileNotFoundError(f"File not found: {filepath}")

        logger.info(f"ファイルをダウンロードします: {filepath}")
        return send_file(full_path, as_attachment=True)
    except Exception as e:
        logger.error(f"ファイルダウンロードでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 404

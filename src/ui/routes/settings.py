"""設定画面のルート"""

import json

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for

from src.auth.decorators import login_required
from src.config import load_config
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

settings_bp = Blueprint("settings", __name__, url_prefix="/settings")


@settings_bp.route("/")
@login_required
def index():
    """設定画面の表示"""
    config = load_config()
    cleanup_config = config.get("data_cleanup", {})

    return render_template("settings/index.html", cleanup_config=cleanup_config)


@settings_bp.route("/cleanup", methods=["GET", "POST"])
@login_required
def data_cleanup():
    """データクリーンアップ設定"""
    config_path = "config/config.json"

    if request.method == "POST":
        try:
            # 現在の設定を読み込み
            with open(config_path, encoding="utf-8") as f:
                config = json.load(f)

            # フォームデータから設定を更新
            cleanup_config = config.setdefault("data_cleanup", {})

            # チェックボックスの値を取得（チェックされていない場合はFalse）
            cleanup_config["enabled"] = request.form.get("enabled") == "on"
            cleanup_config["backup_before_delete"] = (
                request.form.get("backup_before_delete") == "on"
            )
            cleanup_config["dry_run"] = request.form.get("dry_run") == "on"

            # 数値フィールドの更新
            try:
                cleanup_config["inactive_user_days"] = int(
                    request.form.get("inactive_user_days", 30)
                )
                cleanup_config["old_price_days"] = int(
                    request.form.get("old_price_days", 30)
                )
            except ValueError:
                flash("日数は数値で入力してください", "error")
                return redirect(url_for("settings.data_cleanup"))

            # 設定ファイルに保存
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)

            flash("データクリーンアップ設定を更新しました", "success")
            logger.info(f"データクリーンアップ設定が更新されました: {cleanup_config}")

            return redirect(url_for("settings.data_cleanup"))

        except Exception as e:
            logger.error(f"設定更新エラー: {e}")
            flash("設定の更新に失敗しました", "error")
            return redirect(url_for("settings.data_cleanup"))

    # GET: 現在の設定を表示
    config = load_config()
    cleanup_config = config.get(
        "data_cleanup",
        {
            "enabled": False,
            "inactive_user_days": 30,
            "old_price_days": 30,
            "backup_before_delete": True,
            "dry_run": True,
        },
    )

    return render_template("settings/data_cleanup.html", cleanup_config=cleanup_config)


@settings_bp.route("/cleanup/preview", methods=["POST"])
@login_required
def preview_cleanup():
    """クリーンアップ対象のプレビュー（AJAX）"""
    try:
        from src.cli.cleanup_database import get_inactive_users, get_old_price_data

        # リクエストパラメータ取得
        inactive_days = int(request.json.get("inactive_user_days", 30))
        price_days = int(request.json.get("old_price_days", 30))

        # プレビューデータ取得
        inactive_users = get_inactive_users(inactive_days, dry_run=True)
        price_stats = get_old_price_data(price_days, dry_run=True)

        return jsonify(
            {
                "success": True,
                "inactive_users": {
                    "count": len(inactive_users),
                    "users": [
                        {
                            "username": u["username"],
                            "email": u["email"],
                            "last_login": u["last_login"],
                        }
                        for u in inactive_users[:10]  # 最大10件まで表示
                    ],
                },
                "old_prices": price_stats,
            }
        )

    except Exception as e:
        logger.error(f"プレビューエラー: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@settings_bp.route("/cleanup/execute", methods=["POST"])
@login_required
def execute_cleanup():
    """クリーンアップの手動実行（AJAX）"""
    try:
        from src.cli.cleanup_database import cleanup_database

        config = load_config()
        cleanup_config = config.get("data_cleanup", {})

        # 手動実行時は dry_run = False
        results = cleanup_database(cleanup_config, dry_run=False, force=True)

        return jsonify(
            {
                "success": True,
                "results": results,
                "message": "データクリーンアップが完了しました",
            }
        )

    except Exception as e:
        logger.error(f"クリーンアップ実行エラー: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

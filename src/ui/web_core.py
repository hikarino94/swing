"""
Flask アプリケーションのコア設定とユーティリティ
"""

import os
import secrets
from pathlib import Path

from flask import Flask

from src.utils.logging_config import get_logger

logger = get_logger("web_core")


def create_app(template_dir: Path) -> Flask:
    """Flaskアプリケーションを作成し設定する

    Args:
        template_dir: テンプレートディレクトリのパス

    Returns:
        設定済みのFlaskアプリケーション
    """
    app = Flask(__name__, template_folder=str(template_dir))

    # セキュアなシークレットキーの設定
    secret_key = os.environ.get("SECRET_KEY")
    if not secret_key:
        # シークレットキーをファイルに保存して再利用
        secret_key_file = (
            Path(__file__).resolve().parent.parent.parent / "config" / ".secret_key"
        )
        if secret_key_file.exists():
            secret_key = secret_key_file.read_text().strip()
        else:
            secret_key = secrets.token_urlsafe(32)
            secret_key_file.parent.mkdir(exist_ok=True)
            secret_key_file.write_text(secret_key)
            secret_key_file.chmod(0o600)  # 所有者のみ読み書き可能

    app.config["SECRET_KEY"] = secret_key
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max file size
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SECURE"] = False  # 本番環境ではTrue
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["PERMANENT_SESSION_LIFETIME"] = 3600 * 24 * 30  # 30日間
    app.config["SESSION_COOKIE_NAME"] = "swing_session"

    return app

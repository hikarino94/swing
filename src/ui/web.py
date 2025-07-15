#!/usr/bin/env python3
"""
Swing Trading Tool - モダンなWeb UI版
タブ型インターフェースでGUIアプリの機能を統合
"""

import argparse
import sqlite3
from pathlib import Path
from typing import cast

from flask import Flask, redirect, render_template
from flask import request as flask_request
from flask import session, url_for

from src.auth import AuthManager
from src.config import get_db_path
from src.types.flask_types import RequestWithUser, get_args_value
from src.ui.blueprints import (
    auth_bp,
    backtest_bp,
    daytrade_bp,
    fetch_bp,
    results_bp,
    screening_bp,
    utils_bp,
)
from src.ui.common import generate_csrf_token, get_secret_key
from src.utils.logging_config import get_logger

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)

# ロガーの設定
logger = get_logger("web")

# プロジェクトルートとテンプレートディレクトリのパスを設定
project_root = Path(__file__).resolve().parent.parent.parent
template_dir = project_root / "templates"

app = Flask(__name__, template_folder=str(template_dir))

# アプリケーション設定
app.config["SECRET_KEY"] = get_secret_key()
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max file size
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SECURE"] = False  # 本番環境ではTrue
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["PERMANENT_SESSION_LIFETIME"] = 3600 * 24 * 30  # 30日間
app.config["SESSION_COOKIE_NAME"] = "swing_session"

# WSL2ネットワーク問題対策の設定
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0  # キャッシュ無効化
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False  # JSON圧縮
app.config["PROPAGATE_EXCEPTIONS"] = True

# テンプレートにCSRFトークンを渡す
app.jinja_env.globals["csrf_token"] = generate_csrf_token

# Flaskのログレベルを設定（開発環境）
if app.debug:
    app.logger.setLevel("DEBUG")
else:
    app.logger.setLevel("WARNING")


# データベース初期化
def init_database():
    """データベースが存在しない場合は初期化"""
    from db.db_schema import init_schema
    from src.auth.admin_setup import create_admin_from_env

    db_path = Path(get_db_path())
    if not db_path.exists():
        logger.info("データベースが存在しません。初期化を開始します...")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        init_schema(db_path)
        logger.info("データベースの初期化が完了しました")
    else:
        # テーブルが存在するか確認
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
                )
                if not cursor.fetchone():
                    logger.info(
                        "usersテーブルが存在しません。スキーマを再作成します..."
                    )
                    init_schema(db_path)
                    logger.info("スキーマの再作成が完了しました")
        except Exception as e:
            logger.error(f"データベースチェックでエラー: {e}")
            init_schema(db_path)

    # 環境変数から管理者ユーザーを作成
    create_admin_from_env()


# アプリケーション起動時にデータベースを初期化
init_database()


# チャンク転送エンコーディングを有効化
@app.after_request
def after_request(response):
    # 小さなチャンクサイズでレスポンスを送信
    response.direct_passthrough = False
    return response


# Blueprintの登録
app.register_blueprint(auth_bp)
app.register_blueprint(fetch_bp)
app.register_blueprint(screening_bp)
app.register_blueprint(backtest_bp)
app.register_blueprint(utils_bp)
app.register_blueprint(results_bp)
app.register_blueprint(daytrade_bp)


# メインページルート
@app.route("/")
def index():
    """メインページ"""
    logger.info("メインページへのアクセス")

    # URLパラメータからタブを取得
    selected_tab = get_args_value(request, "tab", "screening")

    # テスト環境では認証をスキップ
    if app.config.get("TESTING"):
        from src.auth.models import User

        test_user = User(
            id=1,
            username="testuser",
            email="test@example.com",
            password_hash="",  # nosec B106 - テスト用の空パスワード
            role="admin",
        )
        return render_template(
            "index.html",
            user=test_user,
            portfolio_only=False,
            selected_tab=selected_tab,
        )

    # 未ログインの場合はログインページへリダイレクト
    if "session_id" not in session:
        return redirect(url_for("auth.login"))

    user = AuthManager.get_user_by_session(session["session_id"])
    if not user:
        session.clear()
        return redirect(url_for("auth.login"))

    return render_template("index.html", user=user, selected_tab=selected_tab)


@app.route("/screening")
def screening():
    """スクリーニングページ（メインページへリダイレクト）"""

    # 認証チェック
    if "session_id" not in session:
        return redirect(url_for("auth.login"))

    user = AuthManager.get_user_by_session(session["session_id"])
    if not user:
        session.clear()
        return redirect(url_for("auth.login"))

    return redirect(url_for("index", tab="screening"))


@app.route("/backtest")
def backtest():
    """バックテストページ（メインページへリダイレクト）"""
    # 認証チェック
    if "session_id" not in session:
        return redirect(url_for("auth.login"))

    user = AuthManager.get_user_by_session(session["session_id"])
    if not user:
        session.clear()
        return redirect(url_for("auth.login"))

    return redirect(url_for("index", tab="backtest"))


@app.route("/import")
def import_page():
    """インポートページ（メインページへリダイレクト）"""
    # 認証チェック
    if "session_id" not in session:
        return redirect(url_for("auth.login"))

    user = AuthManager.get_user_by_session(session["session_id"])
    if not user:
        session.clear()
        return redirect(url_for("auth.login"))

    return redirect(url_for("index", tab="import"))


if __name__ == "__main__":
    # コマンドライン引数の解析
    parser = argparse.ArgumentParser(description="Swing Trading Tool Web UI")
    parser.add_argument(
        "--port", type=int, default=5005, help="ポート番号を指定（デフォルト: 5005）"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="ホストアドレスを指定（デフォルト: 0.0.0.0）",
    )
    parser.add_argument("--debug", action="store_true", help="デバッグモードで起動")
    args = parser.parse_args()

    # ホストを0.0.0.0に設定してWSL2からアクセス可能にする
    # threaded=Trueで並行処理を有効化
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)

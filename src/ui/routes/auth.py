"""認証関連のルート"""

from flask import Blueprint, redirect, render_template, request, session, url_for

from src.auth import AuthManager
from src.utils.logging_config import get_logger

logger = get_logger("routes.auth")

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/")
def index():
    """メインページ（要ログイン）"""
    # ログインチェック
    session_id = session.get("session_id")
    if not session_id:
        return redirect(url_for("auth.login"))

    # セッション有効性チェック
    auth = AuthManager()
    user_session = auth.get_session(session_id)
    if not user_session:
        session.clear()
        return redirect(url_for("auth.login"))

    # 通常セッションの場合、30分ごとに更新
    if not user_session.remember_me:
        auth.refresh_session(session_id)

    return render_template(
        "index.html",
        username=user_session.user.username,
        role=user_session.user.role,
        user_id=user_session.user.id,
    )


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    """ログインページ"""
    if request.method == "POST":
        auth = AuthManager()
        username = request.form.get("username")
        password = request.form.get("password")
        remember_me = request.form.get("remember_me") == "on"

        user = auth.authenticate(username, password)
        if user:
            user_session = auth.create_session(user.id, remember_me=remember_me)
            session["session_id"] = user_session.id
            session.permanent = remember_me
            logger.info(f"ユーザー {username} がログインしました")
            return redirect(url_for("auth.index"))
        else:
            return render_template("login.html", error="認証に失敗しました")

    return render_template("login.html")


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    """ユーザー登録ページ"""
    if request.method == "POST":
        auth = AuthManager()
        username = request.form.get("username")
        email = request.form.get("email")
        password = request.form.get("password")

        try:
            auth.create_user(username, email, password)
            logger.info(f"新規ユーザー {username} が登録されました")
            return redirect(url_for("auth.login"))
        except ValueError as e:
            return render_template("register.html", error=str(e))

    return render_template("register.html")


@auth_bp.route("/logout")
def logout():
    """ログアウト"""
    session_id = session.get("session_id")
    if session_id:
        auth = AuthManager()
        auth.destroy_session(session_id)
    session.clear()
    return redirect(url_for("auth.login"))

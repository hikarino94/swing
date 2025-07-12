"""
認証関連のルート定義
"""

from typing import cast

from flask import Blueprint, redirect, render_template
from flask import request as flask_request
from flask import session, url_for

from src.auth import AuthManager
from src.types.flask_types import RequestWithUser, get_form_value

# Blueprint作成
auth_bp = Blueprint("auth", __name__, url_prefix="/")

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    """ログインページ"""
    if request.method == "GET":
        # 既にログイン済みの場合はリダイレクト
        if "session_id" in session:
            user = AuthManager.get_user_by_session(session["session_id"])
            if user:
                return redirect(url_for("index"))
        return render_template("login.html", error=None)

    # POST: ログイン処理
    username_or_email = get_form_value(request, "username", "").strip()
    password = get_form_value(request, "password", "")
    remember_me = get_form_value(request, "remember_me") == "on"

    user, session_id, error = AuthManager.login(
        username_or_email, password, remember_me
    )

    if user and session_id:
        session["session_id"] = session_id
        # Remember Meが有効な場合はセッションを永続化
        if remember_me:
            session.permanent = True
        # リダイレクト先の処理
        next_url = session.pop("next_url", None)
        return redirect(next_url or url_for("index"))

    return render_template("login.html", error=error)


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    """新規登録ページ"""
    if request.method == "GET":
        return render_template("register.html", error=None)

    # POST: 登録処理
    username = get_form_value(request, "username", "").strip()
    email = get_form_value(request, "email", "").strip()
    password = get_form_value(request, "password", "")
    password_confirm = get_form_value(request, "password_confirm", "")

    # パスワード確認
    if password != password_confirm:
        return render_template("register.html", error="パスワードが一致しません")

    # 新規登録ユーザーは常にポートフォリオ専用ユーザーとして作成
    success, message = AuthManager.register_user(
        username, email, password, role="portfolio_only"
    )

    if success:
        # 登録成功したら自動的にログイン
        user, session_id, _ = AuthManager.login(username, password)
        if user and session_id:
            session["session_id"] = session_id
            return redirect(url_for("index"))

    return render_template("register.html", error=message)


@auth_bp.route("/logout")
def logout():
    """ログアウト処理"""
    session_id = session.get("session_id")
    if session_id:
        AuthManager.logout(session_id)
    session.clear()
    return redirect(url_for("auth.login"))

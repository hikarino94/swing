"""認証デコレータ"""

from functools import wraps

from flask import jsonify, redirect, request, session, url_for

from src.utils.logging_config import get_logger

from .auth import AuthManager

logger = get_logger("auth.decorators")


def login_required(f):
    """
    ログインが必要なビュー関数のデコレータ

    使用例:
        @app.route('/protected')
        @login_required
        def protected_view():
            return 'ログイン済みユーザーのみアクセス可能'
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        # セッションIDを取得
        session_id = session.get("session_id")

        # ユーザー情報を取得
        user = AuthManager.get_user_by_session(session_id)

        if not user:
            # APIエンドポイントの場合はJSONレスポンス
            if request.path.startswith("/api/"):
                logger.warning(f"未認証アクセス (API): {request.path}")
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": "ログインが必要です",
                            "code": "UNAUTHORIZED",
                        }
                    ),
                    401,
                )
            else:
                # 通常のビューの場合はログインページへリダイレクト
                logger.warning(f"未認証アクセス: {request.path}")
                session["next_url"] = request.url
                return redirect(url_for("login"))

        # リクエストコンテキストにユーザー情報を追加
        request.current_user = user

        return f(*args, **kwargs)

    return decorated_function


def admin_required(f):
    """
    管理者権限が必要なビュー関数のデコレータ

    使用例:
        @app.route('/admin')
        @admin_required
        def admin_view():
            return '管理者のみアクセス可能'
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        # まずログインチェック
        session_id = session.get("session_id")
        user = AuthManager.get_user_by_session(session_id)

        if not user:
            if request.path.startswith("/api/"):
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": "ログインが必要です",
                            "code": "UNAUTHORIZED",
                        }
                    ),
                    401,
                )
            else:
                session["next_url"] = request.url
                return redirect(url_for("login"))

        # 管理者権限チェック（将来的にusersテーブルにis_adminフラグを追加する場合）
        # if not user.is_admin:
        #     if request.path.startswith("/api/"):
        #         return jsonify({
        #             "success": False,
        #             "error": "管理者権限が必要です",
        #             "code": "FORBIDDEN"
        #         }), 403
        #     else:
        #         return "アクセス権限がありません", 403

        request.current_user = user
        return f(*args, **kwargs)

    return decorated_function

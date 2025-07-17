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
        # テスト環境では認証をスキップ
        from flask import current_app

        if current_app.config.get("TESTING"):
            from .models import User

            test_user = User(
                id=1,
                username="testuser",
                email="test@example.com",
                password_hash="",  # nosec B106 - テスト用の空パスワード
                role="admin",
            )
            request.current_user = test_user
            return f(*args, **kwargs)

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
        # テスト環境では認証をスキップ
        from flask import current_app

        if current_app.config.get("TESTING"):
            from .models import User

            test_user = User(
                id=1,
                username="testuser",
                email="test@example.com",
                password_hash="",  # nosec B106 - テスト用の空パスワード
                role="admin",
            )
            request.current_user = test_user
            return f(*args, **kwargs)

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

        # 管理者権限チェック
        if user.role != "admin":
            logger.warning(f"管理者権限なしアクセス: {request.path} by {user.username}")
            if request.path.startswith("/api/"):
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": "管理者権限が必要です",
                            "code": "FORBIDDEN",
                        }
                    ),
                    403,
                )
            else:
                return "アクセス権限がありません", 403

        request.current_user = user
        return f(*args, **kwargs)

    return decorated_function


def trader_allowed(f):
    """
    トレーダーロール以上のユーザーがアクセス可能なビュー関数のデコレータ
    使用例:
        @app.route('/api/daytrade/trades')
        @trader_allowed
        def daytrade_trades():
            return '取引管理機能'
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        # テスト環境では認証をスキップ
        from flask import current_app

        if current_app.config.get("TESTING"):
            from .models import User

            test_user = User(
                id=1,
                username="testuser",
                email="test@example.com",
                password_hash="",  # nosec B106 - テスト用の空パスワード
                role="trader",
            )
            request.current_user = test_user
            return f(*args, **kwargs)

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

        # trader, admin は許可
        # portfolio_only は拒否
        if user.role == "portfolio_only":
            logger.warning(
                f"取引管理権限なしアクセス: {request.path} by {user.username}"
            )
            if request.path.startswith("/api/"):
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": "取引管理権限が必要です",
                            "code": "FORBIDDEN",
                        }
                    ),
                    403,
                )
            else:
                return "アクセス権限がありません", 403

        request.current_user = user
        return f(*args, **kwargs)

    return decorated_function

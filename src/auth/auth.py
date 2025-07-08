"""認証処理の実装"""

import secrets
from datetime import datetime, timedelta

from werkzeug.security import check_password_hash, generate_password_hash

from src.utils.logging_config import get_logger

from .models import Session, User

logger = get_logger("auth")


class AuthManager:
    """認証管理クラス"""

    # セッションの有効期限
    SESSION_EXPIRE_HOURS = 24  # 通常のセッション（24時間）
    SESSION_EXPIRE_HOURS_REMEMBER = 24 * 30  # Remember Meセッション（30日間）

    @staticmethod
    def register_user(
        username: str, email: str, password: str, role: str = "admin"
    ) -> tuple[bool, str]:
        """
        新規ユーザー登録

        Args:
            username: ユーザー名
            email: メールアドレス
            password: パスワード（平文）
            role: ユーザーロール（admin or portfolio_only）

        Returns:
            (成功/失敗, メッセージ)のタプル
        """
        # 入力検証
        if not username or len(username) < 3:
            return False, "ユーザー名は3文字以上で入力してください"

        if not email or "@" not in email:
            return False, "有効なメールアドレスを入力してください"

        if not password or len(password) < 8:
            return False, "パスワードは8文字以上で入力してください"

        # 既存ユーザーチェック
        if User.find_by_username(username):
            return False, "このユーザー名は既に使用されています"

        if User.find_by_email(email):
            return False, "このメールアドレスは既に登録されています"

        # ユーザー作成
        user = User(
            username=username,
            email=email,
            password_hash=generate_password_hash(password),
            role=role,
        )

        if user.save():
            logger.info(f"新規ユーザー登録成功: {username}")
            return True, "ユーザー登録が完了しました"
        else:
            return False, "ユーザー登録に失敗しました"

    @staticmethod
    def login(
        username_or_email: str, password: str, remember_me: bool = False
    ) -> tuple[User | None, str | None, str]:
        """
        ログイン処理

        Args:
            username_or_email: ユーザー名またはメールアドレス
            password: パスワード
            remember_me: ログイン状態を保持するか

        Returns:
            (ユーザーオブジェクト, セッションID, エラーメッセージ)のタプル
        """
        # ユーザー検索
        user = User.find_by_username(username_or_email)
        if not user:
            user = User.find_by_email(username_or_email)

        if not user:
            logger.warning(
                f"ログイン失敗: ユーザーが見つかりません - {username_or_email}"
            )
            return None, None, "ユーザー名またはパスワードが正しくありません"

        # パスワード検証
        if not check_password_hash(user.password_hash, password):
            logger.warning(f"ログイン失敗: パスワード不一致 - {username_or_email}")
            return None, None, "ユーザー名またはパスワードが正しくありません"

        # セッション作成
        if user.id is None:
            logger.error("ユーザーIDが設定されていません")
            return None, None, "システムエラーが発生しました"

        session_id = secrets.token_urlsafe(32)
        # Remember Meが有効な場合は長期間有効なセッションを作成
        expire_hours = (
            AuthManager.SESSION_EXPIRE_HOURS_REMEMBER
            if remember_me
            else AuthManager.SESSION_EXPIRE_HOURS
        )
        expires_at = datetime.now() + timedelta(hours=expire_hours)

        session = Session(
            session_id=session_id, user_id=user.id, expires_at=expires_at.isoformat()
        )

        # Remember Meフラグも保存
        session.remember_me = remember_me

        if session.save():
            logger.info(f"ログイン成功: {user.username} (Remember Me: {remember_me})")
            return user, session_id, ""
        else:
            return None, None, "セッション作成に失敗しました"

    @staticmethod
    def logout(session_id: str) -> bool:
        """
        ログアウト処理

        Args:
            session_id: セッションID

        Returns:
            成功/失敗
        """
        session = Session.find_by_id(session_id)
        if session:
            success = session.delete()
            if success:
                logger.info(f"ログアウト成功: セッション {session_id}")
            return success
        return False

    @staticmethod
    def get_user_by_session(session_id: str) -> User | None:
        """
        セッションIDからユーザーを取得

        Args:
            session_id: セッションID

        Returns:
            ユーザーオブジェクトまたはNone
        """
        if not session_id:
            return None

        # 期限切れセッションのクリーンアップ
        Session.cleanup_expired()

        session = Session.find_by_id(session_id)
        if session:
            return User.find_by_id(session.user_id)
        return None

    @staticmethod
    def change_password(
        user_id: int, current_password: str, new_password: str
    ) -> tuple[bool, str]:
        """
        パスワード変更

        Args:
            user_id: ユーザーID
            current_password: 現在のパスワード
            new_password: 新しいパスワード

        Returns:
            (成功/失敗, メッセージ)のタプル
        """
        user = User.find_by_id(user_id)
        if not user:
            return False, "ユーザーが見つかりません"

        # 現在のパスワード確認
        if not check_password_hash(user.password_hash, current_password):
            return False, "現在のパスワードが正しくありません"

        # 新しいパスワードの検証
        if len(new_password) < 8:
            return False, "新しいパスワードは8文字以上で入力してください"

        # パスワード更新
        user.password_hash = generate_password_hash(new_password)
        if user.save():
            logger.info(f"パスワード変更成功: {user.username}")
            return True, "パスワードを変更しました"
        else:
            return False, "パスワード変更に失敗しました"

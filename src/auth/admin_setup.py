"""管理者ユーザーの自動セットアップ"""

import os
import sys
from pathlib import Path

# プロジェクトルートをPYTHONPATHに追加
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.auth.auth import AuthManager
from src.auth.models import User
from src.utils.logging_config import get_logger

logger = get_logger("admin_setup")


def create_admin_from_env():
    """
    環境変数から管理者ユーザーを作成

    環境変数:
        - ADMIN_USERNAME: 管理者ユーザー名
        - ADMIN_EMAIL: 管理者メールアドレス
        - ADMIN_PASSWORD: 管理者パスワード
    """
    username = os.environ.get("ADMIN_USERNAME")
    email = os.environ.get("ADMIN_EMAIL")
    password = os.environ.get("ADMIN_PASSWORD")

    # 環境変数が設定されていない場合はスキップ
    if not all([username, email, password]):
        logger.info(
            "管理者ユーザーの環境変数が設定されていないため、作成をスキップします"
        )
        return False

    # 既存ユーザーチェック
    existing_user = User.find_by_username(username)
    if existing_user:
        logger.info(f"管理者ユーザー '{username}' は既に存在します")
        return True

    # 管理者ユーザー作成
    success, message = AuthManager.register_user(
        username=username, email=email, password=password, role="admin"
    )

    if success:
        logger.info(f"管理者ユーザー '{username}' を作成しました")
        return True
    else:
        logger.error(f"管理者ユーザーの作成に失敗しました: {message}")
        return False


if __name__ == "__main__":
    create_admin_from_env()

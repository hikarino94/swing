#!/usr/bin/env python3
"""
デフォルトユーザーを登録する初期化スクリプト
"""

import json
import sys
from pathlib import Path

# プロジェクトルートをPYTHONPATHに追加
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.auth import AuthManager


def init_default_users():
    """config/users.jsonからデフォルトユーザーを登録"""
    config_path = Path(__file__).resolve().parent.parent / "config" / "users.json"

    if not config_path.exists():
        print(f"設定ファイルが見つかりません: {config_path}")
        return

    try:
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        print(f"設定ファイルの読み込みエラー: {e}")
        return

    users = config.get("users", [])
    if not users:
        print("登録するユーザーがありません")
        return

    for user_data in users:
        username = user_data.get("username")
        password = user_data.get("password")
        role = user_data.get("role", "admin")
        description = user_data.get("description", "")

        if not username or not password:
            print(f"不正なユーザーデータ: {user_data}")
            continue

        # デフォルトのメールアドレスを生成
        email = f"{username}@example.com"

        # ユーザー登録
        success, message = AuthManager.register_user(username, email, password, role)

        if success:
            print(f"✓ ユーザー登録成功: {username} (role: {role}) - {description}")
        else:
            print(f"✗ ユーザー登録失敗: {username} - {message}")


if __name__ == "__main__":
    print("デフォルトユーザーの登録を開始します...")
    init_default_users()
    print("完了しました。")

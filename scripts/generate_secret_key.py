#!/usr/bin/env python3
"""
シークレットキー生成スクリプト

本番環境用のセキュアなシークレットキーを生成します。

Usage:
    python scripts/generate_secret_key.py
"""

import secrets


def generate_secret_key():
    """セキュアなシークレットキーを生成"""
    # 32バイト（256ビット）のランダムなキーを生成
    secret_key = secrets.token_urlsafe(32)

    print("Generated SECRET_KEY:")
    print("=" * 60)
    print(secret_key)
    print("=" * 60)
    print("\nFly.ioで使用する場合:")
    print(f"fly secrets set SECRET_KEY={secret_key}")
    print("\n環境変数として設定する場合:")
    print(f"export SECRET_KEY={secret_key}")
    print("\n.envファイルに追加する場合:")
    print(f"SECRET_KEY={secret_key}")


if __name__ == "__main__":
    generate_secret_key()

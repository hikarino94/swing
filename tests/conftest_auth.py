"""認証テスト用の共通フィクスチャ"""

from unittest.mock import patch

import pytest


# テスト用の高速ハッシュ関数
def fast_generate_password_hash(password, method=None):
    """テスト用の高速パスワードハッシュ生成"""
    return f"fast_hash_{password}"


def fast_check_password_hash(hash_value, password):
    """テスト用の高速パスワード検証"""
    return hash_value == f"fast_hash_{password}"


@pytest.fixture(autouse=True)
def mock_password_hashing():
    """すべての認証テストで高速ハッシュを使用"""
    # werkzeug.securityのモジュールレベルでパッチを適用
    with patch(
        "werkzeug.security.generate_password_hash",
        side_effect=fast_generate_password_hash,
    ):
        with patch(
            "werkzeug.security.check_password_hash",
            side_effect=fast_check_password_hash,
        ):
            # src.auth.authモジュール内のインポートもパッチ
            with patch(
                "src.auth.auth.generate_password_hash",
                side_effect=fast_generate_password_hash,
            ):
                with patch(
                    "src.auth.auth.check_password_hash",
                    side_effect=fast_check_password_hash,
                ):
                    # test_auth.py内でも使用されているのでパッチ
                    with patch(
                        "tests.test_auth.generate_password_hash",
                        side_effect=fast_generate_password_hash,
                    ):
                        yield

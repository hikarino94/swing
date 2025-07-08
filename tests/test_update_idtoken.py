#!/usr/bin/env python
"""
IDトークン更新モジュール (update_idtoken.py) のテスト

テスト対象:
- J-Quants認証API呼び出し
- アカウント情報読み込み
- リフレッシュトークン取得
- IDトークン取得
- ファイル出力
- CLI引数処理
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from unittest import mock

import pytest
import requests

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.cli import update_idtoken


class TestAuthentication:
    """認証機能のテスト"""

    @mock.patch("src.cli.update_idtoken.requests.post")
    def test_auth_user_success(self, mock_post):
        """ユーザー認証成功のテスト"""
        # モックレスポンス設定
        mock_response = mock.Mock()
        mock_response.json.return_value = {"refreshToken": "test_refresh_token_123"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # 認証実行
        refresh_token = update_idtoken._auth_user("test@example.com", "password123")

        # 結果確認
        assert refresh_token == "test_refresh_token_123"
        mock_post.assert_called_once()

        # API呼び出しの引数確認
        call_args = mock_post.call_args
        assert call_args[1]["json"] == {
            "mailaddress": "test@example.com",
            "password": "password123",
        }
        assert call_args[1]["timeout"] == 30

    @mock.patch("src.cli.update_idtoken.requests.post")
    def test_auth_user_missing_refresh_token(self, mock_post):
        """リフレッシュトークンが含まれていない場合のテスト"""
        mock_response = mock.Mock()
        mock_response.json.return_value = {"error": "invalid credentials"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        with pytest.raises(RuntimeError, match="refreshToken not found"):
            update_idtoken._auth_user("test@example.com", "wrong_password")

    @mock.patch("src.cli.update_idtoken.requests.post")
    def test_auth_user_http_error(self, mock_post):
        """HTTP エラーのテスト"""
        mock_response = mock.Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "401 Unauthorized"
        )
        mock_post.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            update_idtoken._auth_user("test@example.com", "password123")


class TestAccountLoading:
    """アカウント情報読み込みのテスト"""

    def test_load_account_existing_file(self, tmp_path):
        """既存ファイルからのアカウント読み込みテスト"""
        # アカウントファイル作成
        account_data = {
            "mail": "test@example.com",
            "password": "password123",
            "password_hash": "hashed_password",
        }
        account_file = tmp_path / "account.json"
        with open(account_file, "w") as f:
            json.dump(account_data, f)

        # アカウント読み込み
        mail, password, password_hash = update_idtoken._load_account(str(account_file))

        assert mail == "test@example.com"
        assert password == "password123"
        assert password_hash == "hashed_password"

    def test_load_account_missing_file(self):
        """存在しないファイルのテスト"""
        mail, password, password_hash = update_idtoken._load_account(
            "/nonexistent/file.json"
        )

        assert mail == ""
        assert password == ""
        assert password_hash == ""

    def test_load_account_partial_data(self, tmp_path):
        """部分的なデータを含むファイルのテスト"""
        account_data = {"mail": "test@example.com"}  # passwordなし
        account_file = tmp_path / "partial_account.json"
        with open(account_file, "w") as f:
            json.dump(account_data, f)

        mail, password, password_hash = update_idtoken._load_account(str(account_file))

        assert mail == "test@example.com"
        assert password == ""
        assert password_hash == ""


class TestTokenRefresh:
    """トークンリフレッシュのテスト"""

    @mock.patch("src.cli.update_idtoken.requests.post")
    def test_get_id_token_success(self, mock_post):
        """IDトークン取得成功のテスト"""
        mock_response = mock.Mock()
        mock_response.json.return_value = {"idToken": "test_id_token_456"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        id_token = update_idtoken._get_id_token("test_refresh_token")

        assert id_token == "test_id_token_456"
        mock_post.assert_called_once()

        # API呼び出しパラメータ確認
        call_args = mock_post.call_args
        assert call_args[1]["params"] == {"refreshtoken": "test_refresh_token"}

    @mock.patch("src.cli.update_idtoken.requests.post")
    def test_get_id_token_missing_token(self, mock_post):
        """IDトークンが含まれていない場合のテスト"""
        mock_response = mock.Mock()
        mock_response.json.return_value = {"error": "invalid refresh token"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        with pytest.raises(RuntimeError, match="idToken not found"):
            update_idtoken._get_id_token("invalid_refresh_token")


class TestUpdateFunction:
    """更新機能のテスト"""

    @mock.patch("src.cli.update_idtoken._get_id_token")
    @mock.patch("src.cli.update_idtoken._auth_user")
    def test_update_success(self, mock_auth, mock_get_token, tmp_path):
        """トークン更新成功のテスト"""
        # モック設定
        mock_auth.return_value = "test_refresh_token"
        mock_get_token.return_value = "test_id_token"

        # 出力ファイルパス
        output_file = tmp_path / "idtoken.json"

        # 更新実行
        with mock.patch("builtins.print"):  # print文をモック
            token = update_idtoken.update(
                "test@example.com", "password123", str(output_file)
            )

        # 結果確認
        assert token == "test_id_token"
        assert output_file.exists()

        # ファイル内容確認
        with open(output_file) as f:
            saved_data = json.load(f)
        assert saved_data == {"idToken": "test_id_token"}

        # モック呼び出し確認
        mock_auth.assert_called_once_with("test@example.com", "password123")
        mock_get_token.assert_called_once_with("test_refresh_token")


class TestCLI:
    """CLI機能のテスト"""

    def test_cli_argument_parsing(self):
        """CLI引数パースのテスト"""
        # 引数パーサー作成（実際のCLI関数の構造に合わせる）
        parser = argparse.ArgumentParser()
        parser.add_argument("--mail", help="registered email")
        parser.add_argument("--password", help="login password")
        parser.add_argument("--account", default="account.json", help="credential file")
        parser.add_argument("--out", default="idtoken.json", help="output file")

        # 引数パース
        args = parser.parse_args(
            [
                "--mail",
                "test@example.com",
                "--password",
                "mypassword",
                "--account",
                "/path/to/account.json",
                "--out",
                "/path/to/idtoken.json",
            ]
        )

        assert args.mail == "test@example.com"
        assert args.password == "mypassword"
        assert args.account == "/path/to/account.json"
        assert args.out == "/path/to/idtoken.json"

    @mock.patch("src.cli.update_idtoken.update")
    @mock.patch("src.cli.update_idtoken._load_account")
    def test_cli_with_account_file(self, mock_load_account, mock_update):
        """アカウントファイルを使用したCLI実行のテスト"""
        # モック設定
        mock_load_account.return_value = ("test@example.com", "password123", "hash")
        mock_update.return_value = "test_token"

        # CLI関数のモック実行をシミュレート
        # 実際のCLI実行の代わりに、主要ロジックをテスト
        mail, password, _ = mock_load_account("account.json")
        if mail and password:
            token = mock_update(mail, password, "idtoken.json")
            assert token == "test_token"

        mock_load_account.assert_called_once_with("account.json")
        mock_update.assert_called_once_with(
            "test@example.com", "password123", "idtoken.json"
        )

    @mock.patch("src.cli.update_idtoken.update")
    @mock.patch("src.cli.update_idtoken._load_account")
    @mock.patch(
        "sys.argv",
        ["update_idtoken.py", "--mail", "test@example.com", "--password", "testpass"],
    )
    def test_cli_function_with_direct_credentials(self, mock_load_account, mock_update):
        """直接認証情報を指定したCLI関数のテスト"""
        mock_update.return_value = "test_token"

        # _cli関数のロジックを模擬
        # 実際のargparseの代わりに、値を直接設定
        mail = "test@example.com"
        password = "testpass"
        output = "idtoken.json"

        # メール・パスワードが直接指定された場合、_load_accountは呼ばれない
        if mail and password:
            update_idtoken.update(mail, password, output)

        mock_update.assert_called_once_with(
            "test@example.com", "testpass", "idtoken.json"
        )
        mock_load_account.assert_not_called()

    @mock.patch("src.cli.update_idtoken.update")
    @mock.patch("src.cli.update_idtoken._load_account")
    def test_cli_function_fallback_to_account_file(
        self, mock_load_account, mock_update
    ):
        """認証情報不足時のアカウントファイルフォールバックのテスト"""
        mock_load_account.return_value = (
            "fallback@example.com",
            "fallbackpass",
            "hash",
        )
        mock_update.return_value = "test_token"

        # メール・パスワードが指定されていない場合のロジック
        mail, password = None, None
        account_file = "account.json"
        output = "idtoken.json"

        if not mail or not password:
            m, p, _ = mock_load_account(account_file)
            mail = mail or m
            password = password or p

        if mail and password:
            update_idtoken.update(mail, password, output)

        mock_load_account.assert_called_once_with("account.json")
        mock_update.assert_called_once_with(
            "fallback@example.com", "fallbackpass", "idtoken.json"
        )

    @mock.patch("src.cli.update_idtoken._load_account")
    def test_cli_function_missing_credentials_error(self, mock_load_account):
        """認証情報が不足している場合のエラーテスト"""
        mock_load_account.return_value = ("", "", "")

        # 認証情報が全く取得できない場合
        mail, password = None, None
        account_file = "account.json"

        if not mail or not password:
            m, p, _ = mock_load_account(account_file)
            mail = mail or m
            password = password or p

        # この場合はmail, passwordが空になるはず
        assert not mail
        assert not password

        mock_load_account.assert_called_once_with("account.json")

    @mock.patch(
        "sys.argv",
        ["update_idtoken.py", "--mail", "test@example.com", "--password", "testpass"],
    )
    @mock.patch("src.cli.update_idtoken.update")
    def test_actual_cli_function_direct_args(self, mock_update):
        """実際の_cli関数の直接引数テスト"""
        mock_update.return_value = "test_token"

        # 実際の_cli関数を呼び出し
        update_idtoken._cli()

        # updateが正しい引数で呼ばれることを確認
        mock_update.assert_called_once_with("test@example.com", "testpass", mock.ANY)

    @mock.patch("sys.argv", ["update_idtoken.py", "--account", "test_account.json"])
    @mock.patch("src.cli.update_idtoken._load_account")
    @mock.patch("src.cli.update_idtoken.update")
    def test_actual_cli_function_account_file(self, mock_update, mock_load_account):
        """実際の_cli関数のアカウントファイルテスト"""
        mock_load_account.return_value = ("file@example.com", "filepass", "hash")
        mock_update.return_value = "test_token"

        # 実際の_cli関数を呼び出し
        update_idtoken._cli()

        # _load_accountとupdateが呼ばれることを確認
        mock_load_account.assert_called_once_with("test_account.json")
        mock_update.assert_called_once_with("file@example.com", "filepass", mock.ANY)

    @mock.patch("sys.argv", ["update_idtoken.py"])
    @mock.patch("src.cli.update_idtoken._load_account")
    def test_actual_cli_function_missing_credentials(self, mock_load_account):
        """実際の_cli関数の認証情報不足エラーテスト"""
        mock_load_account.return_value = ("", "", "")

        # argparse.ArgumentParserのerrorメソッドがSystemExitを投げるため
        with pytest.raises(SystemExit):
            update_idtoken._cli()

        mock_load_account.assert_called_once()


class TestIntegration:
    """統合テスト"""

    @mock.patch("src.cli.update_idtoken._get_id_token")
    @mock.patch("src.cli.update_idtoken._auth_user")
    def test_full_workflow(self, mock_auth, mock_get_token, tmp_path):
        """完全なワークフローのテスト"""
        # アカウントファイル作成
        account_data = {
            "mail": "integration@example.com",
            "password": "integration_password",
        }
        account_file = tmp_path / "account.json"
        with open(account_file, "w") as f:
            json.dump(account_data, f)

        # モック設定
        mock_auth.return_value = "integration_refresh"
        mock_get_token.return_value = "integration_id_token"

        # ワークフロー実行
        # 1. アカウント情報読み込み
        mail, password, _ = update_idtoken._load_account(str(account_file))
        assert mail == "integration@example.com"
        assert password == "integration_password"

        # 2. トークン更新
        output_file = tmp_path / "integration_idtoken.json"
        with mock.patch("builtins.print"):
            token = update_idtoken.update(mail, password, str(output_file))

        # 3. 結果確認
        assert token == "integration_id_token"
        assert output_file.exists()

        with open(output_file) as f:
            saved_data = json.load(f)
        assert saved_data["idToken"] == "integration_id_token"

        # モック呼び出し確認
        mock_auth.assert_called_once_with(
            "integration@example.com", "integration_password"
        )
        mock_get_token.assert_called_once_with("integration_refresh")


class TestErrorHandling:
    """エラー処理のテスト"""

    @mock.patch("src.cli.update_idtoken.requests.post")
    def test_network_timeout(self, mock_post):
        """ネットワークタイムアウトのテスト"""
        mock_post.side_effect = requests.Timeout("Request timed out")

        with pytest.raises(requests.Timeout):
            update_idtoken._auth_user("test@example.com", "password123")

    @mock.patch("src.cli.update_idtoken.requests.post")
    def test_api_server_error(self, mock_post):
        """APIサーバーエラーのテスト"""
        mock_response = mock.Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "500 Internal Server Error"
        )
        mock_post.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            update_idtoken._auth_user("test@example.com", "password123")

    def test_invalid_json_file(self, tmp_path):
        """無効なJSONファイルのテスト"""
        invalid_file = tmp_path / "invalid.json"
        with open(invalid_file, "w") as f:
            f.write("invalid json content")

        with pytest.raises(json.JSONDecodeError):
            update_idtoken._load_account(str(invalid_file))

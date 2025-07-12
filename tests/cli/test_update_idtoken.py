"""Tests for src/cli/update_idtoken.py"""

import json

# sys.pathの調整
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.cli.update_idtoken import (
    _auth_user,
    _get_id_token,
    _load_account,
    update,
)


class TestAuthUser:
    """_auth_user関数のテスト"""

    @patch("src.cli.update_idtoken.requests.post")
    def test_auth_user_success(self, mock_post):
        """認証成功のテスト"""
        mock_response = Mock()
        mock_response.json.return_value = {"refreshToken": "test_refresh_token"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        result = _auth_user("test@example.com", "password123")

        assert result == "test_refresh_token"
        mock_post.assert_called_once_with(
            mock_post.call_args[0][0],  # API_AUTH
            json={"mailaddress": "test@example.com", "password": "password123"},
            timeout=30,
        )

    @patch("src.cli.update_idtoken.requests.post")
    def test_auth_user_http_error(self, mock_post):
        """HTTPエラーのテスト"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "401 Unauthorized"
        )
        mock_post.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError):
            _auth_user("test@example.com", "wrong_password")

    @patch("src.cli.update_idtoken.requests.post")
    def test_auth_user_no_refresh_token(self, mock_post):
        """refreshTokenがレスポンスにない場合のテスト"""
        mock_response = Mock()
        mock_response.json.return_value = {"error": "Invalid credentials"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        with pytest.raises(RuntimeError, match="refreshToken not found"):
            _auth_user("test@example.com", "password123")


class TestLoadAccount:
    """_load_account関数のテスト"""

    def test_load_account_success(self):
        """正常にアカウント情報を読み込むテスト"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(
                {
                    "mail": "test@example.com",
                    "password": "password123",
                    "password_hash": "hash123",
                },
                f,
            )
            temp_path = f.name

        try:
            mail, password, password_hash = _load_account(temp_path)

            assert mail == "test@example.com"
            assert password == "password123"
            assert password_hash == "hash123"
        finally:
            Path(temp_path).unlink()

    def test_load_account_file_not_found(self):
        """ファイルが存在しない場合のテスト"""
        mail, password, password_hash = _load_account("/nonexistent/path.json")

        assert mail == ""
        assert password == ""
        assert password_hash == ""

    def test_load_account_partial_data(self):
        """一部のデータのみ存在する場合のテスト"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"mail": "test@example.com"}, f)
            temp_path = f.name

        try:
            mail, password, password_hash = _load_account(temp_path)

            assert mail == "test@example.com"
            assert password == ""
            assert password_hash == ""
        finally:
            Path(temp_path).unlink()


class TestGetIdToken:
    """_get_id_token関数のテスト"""

    @patch("src.cli.update_idtoken.requests.post")
    def test_get_id_token_success(self, mock_post):
        """IDトークン取得成功のテスト"""
        mock_response = Mock()
        mock_response.json.return_value = {"idToken": "test_id_token"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        result = _get_id_token("test_refresh_token")

        assert result == "test_id_token"
        mock_post.assert_called_once_with(
            mock_post.call_args[0][0],  # API_REFRESH
            params={"refreshtoken": "test_refresh_token"},
            timeout=30,
        )

    @patch("src.cli.update_idtoken.requests.post")
    def test_get_id_token_no_id_token(self, mock_post):
        """idTokenがレスポンスにない場合のテスト"""
        mock_response = Mock()
        mock_response.json.return_value = {"error": "Invalid refresh token"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        with pytest.raises(RuntimeError, match="idToken not found"):
            _get_id_token("invalid_refresh_token")


class TestUpdate:
    """update関数のテスト"""

    @patch("src.cli.update_idtoken._auth_user")
    @patch("src.cli.update_idtoken._get_id_token")
    @patch("builtins.print")
    def test_update_success(self, mock_print, mock_get_id, mock_auth):
        """正常なトークン更新のテスト"""
        mock_auth.return_value = "refresh_token"
        mock_get_id.return_value = "id_token"

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "idtoken.json"

            result = update("test@example.com", "password123", str(output_path))

            assert result == "id_token"
            assert output_path.exists()

            with output_path.open() as f:
                data = json.load(f)
            assert data["idToken"] == "id_token"

            mock_auth.assert_called_once_with("test@example.com", "password123")
            mock_get_id.assert_called_once_with("refresh_token")
            mock_print.assert_called_once_with("トークンを更新しました。")

    @patch("src.cli.update_idtoken._auth_user")
    def test_update_auth_failure(self, mock_auth):
        """認証失敗のテスト"""
        mock_auth.side_effect = requests.exceptions.HTTPError("401")

        with tempfile.NamedTemporaryFile(suffix=".json") as f:
            with pytest.raises(requests.exceptions.HTTPError):
                update("test@example.com", "wrong_password", f.name)


class TestCLI:
    """CLI関数のテスト"""

    @patch(
        "sys.argv",
        ["update_idtoken.py", "--mail", "test@example.com", "--password", "pass123"],
    )
    @patch("src.cli.update_idtoken.update")
    def test_cli_with_mail_and_password(self, mock_update):
        """メールとパスワードを直接指定した場合のテスト"""
        from src.cli.update_idtoken import _cli

        mock_update.return_value = "token123"

        _cli()

        # updateが正しい引数で呼ばれることを確認
        mock_update.assert_called_once()
        call_args = mock_update.call_args[0]
        assert call_args[0] == "test@example.com"
        assert call_args[1] == "pass123"

    @patch("sys.argv", ["update_idtoken.py", "--account", "account.json"])
    @patch("src.cli.update_idtoken._load_account")
    @patch("src.cli.update_idtoken.update")
    def test_cli_with_account_file(self, mock_update, mock_load_account):
        """アカウントファイルを指定した場合のテスト"""
        from src.cli.update_idtoken import _cli

        # アカウントファイルから読み込まれる値
        mock_load_account.return_value = ("file@example.com", "filepass", "hash")
        mock_update.return_value = "token123"

        _cli()

        # load_accountが呼ばれることを確認
        mock_load_account.assert_called_once_with("account.json")

        # updateが正しい引数で呼ばれることを確認
        mock_update.assert_called_once()
        call_args = mock_update.call_args[0]
        assert call_args[0] == "file@example.com"
        assert call_args[1] == "filepass"

    @patch("sys.argv", ["update_idtoken.py", "--mail", "test@example.com"])
    @patch("src.cli.update_idtoken._load_account")
    @patch("src.cli.update_idtoken.update")
    def test_cli_with_partial_args(self, mock_update, mock_load_account):
        """メールのみ指定してパスワードをファイルから読む場合のテスト"""
        from src.cli.update_idtoken import _cli

        # アカウントファイルから読み込まれる値
        mock_load_account.return_value = ("file@example.com", "filepass", "hash")
        mock_update.return_value = "token123"

        _cli()

        # updateが正しい引数で呼ばれることを確認（メールは引数、パスワードはファイルから）
        mock_update.assert_called_once()
        call_args = mock_update.call_args[0]
        assert call_args[0] == "test@example.com"
        assert call_args[1] == "filepass"

    @patch("sys.argv", ["update_idtoken.py"])
    @patch("src.cli.update_idtoken._load_account")
    def test_cli_no_credentials_error(self, mock_load_account):
        """認証情報が提供されない場合のエラーテスト"""
        from src.cli.update_idtoken import _cli

        # アカウントファイルも空
        mock_load_account.return_value = ("", "", "")

        # argparse.ArgumentParser.errorが呼ばれることを期待
        with pytest.raises(SystemExit):
            _cli()

    @patch(
        "sys.argv",
        [
            "update_idtoken.py",
            "--mail",
            "test@example.com",
            "--password",
            "pass123",
            "--out",
            "custom.json",
        ],
    )
    @patch("src.cli.update_idtoken.update")
    def test_cli_with_custom_output(self, mock_update):
        """カスタム出力ファイルを指定した場合のテスト"""
        from src.cli.update_idtoken import _cli

        mock_update.return_value = "token123"

        _cli()

        # updateが正しい引数で呼ばれることを確認
        mock_update.assert_called_once()
        call_args = mock_update.call_args[0]
        assert call_args[2] == "custom.json"

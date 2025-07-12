"""ui.commonのテスト"""

import gzip
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from flask import Flask, Response, session

from src.ui.common import (
    compress_response,
    generate_csrf_token,
    get_secret_key,
    run_command,
    timestamped_path,
)


class TestGetSecretKey:
    """get_secret_key関数のテスト"""

    def test_get_secret_key_from_env(self):
        """環境変数からシークレットキーを取得"""
        test_key = "test-secret-key-from-env"
        with patch.dict(os.environ, {"SECRET_KEY": test_key}):
            assert get_secret_key() == test_key

    @patch("src.ui.common.project_root", Path("/test/project"))
    def test_get_secret_key_from_file(self):
        """ファイルからシークレットキーを読み込み"""
        test_key = "test-secret-key-from-file"
        mock_file = MagicMock()
        mock_file.exists.return_value = True
        mock_file.read_text.return_value = f"{test_key}\n"

        with patch.dict(os.environ, {}, clear=True):  # 環境変数をクリア
            with patch("pathlib.Path.__truediv__", return_value=mock_file):
                assert get_secret_key() == test_key

    @patch("src.ui.common.project_root", Path("/test/project"))
    @patch("src.ui.common.secrets.token_urlsafe")
    def test_get_secret_key_generate_new(self, mock_token):
        """新しいシークレットキーを生成"""
        test_key = "new-generated-secret-key"
        mock_token.return_value = test_key

        mock_file = MagicMock()
        mock_file.exists.return_value = False
        mock_parent = MagicMock()
        mock_file.parent = mock_parent

        with patch.dict(os.environ, {}, clear=True):
            with patch("pathlib.Path.__truediv__", return_value=mock_file):
                result = get_secret_key()

                assert result == test_key
                mock_token.assert_called_once_with(32)
                mock_parent.mkdir.assert_called_once_with(exist_ok=True)
                mock_file.write_text.assert_called_once_with(test_key)
                mock_file.chmod.assert_called_once_with(0o600)


class TestGenerateCsrfToken:
    """generate_csrf_token関数のテスト"""

    @pytest.fixture
    def app(self):
        """テスト用Flaskアプリケーション"""
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "test-secret"
        return app

    def test_generate_csrf_token_new(self, app):
        """新しいCSRFトークンを生成"""
        with app.test_request_context():
            with patch("src.ui.common.secrets.token_hex") as mock_token:
                mock_token.return_value = "test-token-12345"

                token = generate_csrf_token()

                assert token == "test-token-12345"
                assert session["_csrf_token"] == "test-token-12345"
                mock_token.assert_called_once_with(16)

    def test_generate_csrf_token_existing(self, app):
        """既存のCSRFトークンを返す"""
        with app.test_request_context():
            # 既存のトークンを設定
            session["_csrf_token"] = "existing-token"

            token = generate_csrf_token()

            assert token == "existing-token"


class TestCompressResponse:
    """compress_responseデコレータのテスト"""

    def test_compress_text_response(self):
        """テキストレスポンスの圧縮"""

        @compress_response
        def view():
            response = Response("This is a test response", mimetype="text/plain")
            return response

        response = view()

        # 圧縮されていることを確認
        assert response.headers["Content-Encoding"] == "gzip"
        assert response.headers["Content-Length"] == str(len(response.data))

        # 解凍して元のテキストと比較
        decompressed = gzip.decompress(response.data).decode("utf-8")
        assert decompressed == "This is a test response"

    def test_compress_json_response(self):
        """JSONレスポンスの圧縮"""

        @compress_response
        def view():
            return Response(
                '{"test": "data"}',
                mimetype="application/json",
            )

        response = view()

        assert response.headers["Content-Encoding"] == "gzip"
        decompressed = gzip.decompress(response.data).decode("utf-8")
        assert decompressed == '{"test": "data"}'

    def test_compress_javascript_response(self):
        """JavaScriptレスポンスの圧縮"""

        @compress_response
        def view():
            return Response(
                'console.log("test");',
                mimetype="application/javascript",
            )

        response = view()

        assert response.headers["Content-Encoding"] == "gzip"
        decompressed = gzip.decompress(response.data).decode("utf-8")
        assert decompressed == 'console.log("test");'

    def test_no_compress_binary_response(self):
        """バイナリレスポンスは圧縮しない"""

        @compress_response
        def view():
            return Response(
                b"\x00\x01\x02\x03",
                mimetype="application/octet-stream",
            )

        response = view()

        # 圧縮されていないことを確認
        assert "Content-Encoding" not in response.headers
        assert response.data == b"\x00\x01\x02\x03"

    def test_no_compress_image_response(self):
        """画像レスポンスは圧縮しない"""

        @compress_response
        def view():
            return Response(
                b"fake-image-data",
                mimetype="image/png",
            )

        response = view()

        assert "Content-Encoding" not in response.headers
        assert response.data == b"fake-image-data"


class TestTimestampedPath:
    """timestamped_path関数のテスト"""

    @patch("src.ui.common.get_timestamped_output_path")
    def test_timestamped_path(self, mock_get_path):
        """タイムスタンプ付きパスの生成"""
        mock_get_path.return_value = Path("/output/screening/test_20240115_123456.xlsx")

        result = timestamped_path("screening", "test", ".xlsx")

        assert result == "/output/screening/test_20240115_123456.xlsx"
        mock_get_path.assert_called_once_with("screening", "test", ".xlsx")


class TestRunCommand:
    """run_command関数のテスト"""

    @patch("src.ui.common.subprocess.Popen")
    @patch("src.ui.common.logger")
    @patch("builtins.print")
    def test_run_command_success(self, mock_print, mock_logger, mock_popen):
        """コマンド実行成功"""
        # モックプロセス
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.stdout = ["Output line 1\n", "Output line 2\n"]
        mock_process.wait.return_value = None
        mock_popen.return_value = mock_process

        result = run_command("echo test", "テスト実行")

        assert result["success"] is True
        assert result["output"] == "Output line 1\nOutput line 2\n"
        assert result["description"] == "テスト実行"

        # ログ出力の確認
        assert mock_print.call_count > 0
        mock_logger.error.assert_not_called()

    @patch("src.ui.common.subprocess.Popen")
    @patch("src.ui.common.logger")
    def test_run_command_failure(self, mock_logger, mock_popen):
        """コマンド実行失敗"""
        # モックプロセス
        mock_process = Mock()
        mock_process.returncode = 1
        mock_process.stdout = ["Error occurred\n"]
        mock_process.wait.return_value = None
        mock_popen.return_value = mock_process

        result = run_command("false", "失敗テスト")

        assert result["success"] is False
        assert result["error"] == "Error occurred"
        assert result["description"] == "失敗テスト"

        mock_logger.error.assert_called()

    @patch("src.ui.common.subprocess.Popen")
    @patch("src.ui.common.logger")
    def test_run_command_empty_output_error(self, mock_logger, mock_popen):
        """出力なしでエラー"""
        # モックプロセス
        mock_process = Mock()
        mock_process.returncode = 1
        mock_process.stdout = []
        mock_process.wait.return_value = None
        mock_popen.return_value = mock_process

        result = run_command("false", "エラーテスト")

        assert result["success"] is False
        assert result["error"] == "エラーが発生しました"

    @patch("src.ui.common.subprocess.Popen")
    @patch("src.ui.common.logger")
    def test_run_command_exception(self, mock_logger, mock_popen):
        """例外発生"""
        mock_popen.side_effect = Exception("Popen failed")

        result = run_command("invalid-command", "例外テスト")

        assert result["success"] is False
        assert "Popen failed" in result["error"]
        assert result["description"] == "例外テスト"

        mock_logger.error.assert_called()

    @patch("src.ui.common.subprocess.Popen")
    def test_run_command_list_signals(self, mock_popen):
        """list_signals.pyの特殊処理"""
        # モックプロセス
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.stdout = ["Signal output\n"]
        mock_process.wait.return_value = None
        mock_popen.return_value = mock_process

        result = run_command("python list_signals.py", "シグナル一覧")

        assert result["success"] is True
        assert result["output"] == "Signal output\n"
        assert result["description"] == "シグナル一覧"

    @patch("src.ui.common.subprocess.Popen")
    def test_run_command_db_summary(self, mock_popen):
        """db_summary.pyの特殊処理"""
        # モックプロセス
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.stdout = ["DB Summary\n"]
        mock_process.wait.return_value = None
        mock_popen.return_value = mock_process

        result = run_command("python db_summary.py", "DB概要")

        assert result["success"] is True
        assert result["output"] == "DB Summary\n"
        assert result["description"] == "DB概要"

    @patch("src.ui.common.subprocess.Popen")
    @patch("builtins.print")
    def test_run_command_realtime_output(self, mock_print, mock_popen):
        """リアルタイム出力の確認"""
        # モックプロセス
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.stdout = ["Line 1\n", "Line 2\n", "Line 3\n"]
        mock_process.wait.return_value = None
        mock_popen.return_value = mock_process

        run_command("test command", "リアルタイム出力テスト")

        # 各行が出力されることを確認
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        assert "Line 1" in print_calls
        assert "Line 2" in print_calls
        assert "Line 3" in print_calls

    @patch("src.ui.common.subprocess.Popen")
    def test_run_command_popen_params(self, mock_popen):
        """Popenパラメータの確認"""
        # モックプロセス
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.stdout = []
        mock_process.wait.return_value = None
        mock_popen.return_value = mock_process

        run_command("test command", "パラメータテスト")

        # Popenの呼び出しパラメータを確認
        mock_popen.assert_called_once_with(
            "test command",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            shell=True,
            bufsize=1,
        )


class TestModuleLevelSettings:
    """モジュールレベルの設定のテスト"""

    def test_project_root(self):
        """プロジェクトルートの設定"""
        from src.ui.common import project_root

        # プロジェクトルートが正しく設定されているか確認
        assert isinstance(project_root, Path)
        assert project_root.name in ["swing", "src", "ui"]  # 環境による

    def test_wsgi_protocol_version(self):
        """WSGIプロトコルバージョンの設定"""
        from werkzeug.serving import WSGIRequestHandler

        assert WSGIRequestHandler.protocol_version == "HTTP/1.1"

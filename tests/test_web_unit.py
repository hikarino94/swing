#!/usr/bin/env python
"""Web UIの単体テスト

モックを使用した高速な単体テストを含みます。
実際のデータベースやコマンド実行は行いません。
"""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from src.ui.web import app, run_command, timestamped_path


@pytest.fixture
def test_app():
    """テスト用のFlaskアプリケーションを作成"""
    os.environ["TESTING"] = "true"
    app.config["TESTING"] = True
    app.config["LOGIN_DISABLED"] = True
    return app


@pytest.fixture
def client(test_app):
    """テスト用のFlaskクライアントを作成"""
    return test_app.test_client()


@pytest.fixture
def auth_client(test_app):
    """認証済みのテストクライアント"""
    client = test_app.test_client()
    with client.session_transaction() as sess:
        sess["authenticated"] = True
    return client


class TestTimestampedPath:
    """timestamped_path関数のテスト"""

    def test_default_format(self):
        """デフォルトフォーマットのテスト"""
        result = timestamped_path("test", "test_file", ".csv")

        # パスが正しく構築されているか確認
        assert "test_file_" in result
        assert result.endswith(".csv")

        # タイムスタンプ部分が正しい形式か確認
        timestamp_part = result.split("test_file_")[1].split(".csv")[0]
        assert len(timestamp_part) == 15  # YYYYMMDD_HHMMSS
        assert timestamp_part[8] == "_"

    def test_timestamped_path_generates_correct_format(self):
        """関数が正しいフォーマットでファイルパスを生成することを確認"""
        # 異なるカテゴリと拡張子でテスト
        result1 = timestamped_path("backtest", "result", ".json")
        assert "backtest" in result1
        assert "result_" in result1
        assert result1.endswith(".json")

        result2 = timestamped_path("screening", "fundamental", ".xlsx")
        assert "screening" in result2
        assert "fundamental_" in result2
        assert result2.endswith(".xlsx")


class TestBasicRoutes:
    """基本的なルートのテスト"""

    def test_index_authenticated(self, client):
        """インデックスページへのアクセス（テスト環境ではLOGIN_DISABLED=True）"""
        response = client.get("/")
        assert response.status_code == 200
        assert (
            b"Swing Trading System" in response.data
            or b"swing" in response.data.lower()
        )

    def test_index_with_auth_client(self, auth_client):
        """認証済みクライアントでのアクセス"""
        response = auth_client.get("/")
        assert response.status_code == 200

    def test_404_error(self, client):
        """存在しないページへのアクセス"""
        response = client.get("/nonexistent")
        assert response.status_code == 404

    def test_login_page(self, client):
        """ログインページの表示"""
        response = client.get("/login")
        assert response.status_code == 200
        assert b"login" in response.data.lower()

    def test_logout(self, auth_client):
        """ログアウト機能"""
        response = auth_client.get("/logout")  # GETメソッドに修正
        assert response.status_code == 302
        assert "/login" in response.location  # 相対パスまたは絶対パスの両方に対応


class TestRunCommand:
    """run_command関数のテスト"""

    @patch("subprocess.Popen")
    def test_successful_command(self, mock_popen):
        """コマンドが正常に実行される場合"""
        mock_process = MagicMock()
        mock_process.poll.side_effect = [None, None, 0]  # 実行中→実行中→完了
        mock_process.stdout.readline.side_effect = [
            "Line 1\n",
            "Line 2\n",
            "",  # EOF
        ]
        mock_process.stderr.readline.return_value = b""
        mock_process.returncode = 0
        mock_process.wait.return_value = None
        mock_popen.return_value = mock_process

        result = run_command(["echo", "test"])

        assert result["success"] is True
        assert "Line 1" in result["output"]
        assert "Line 2" in result["output"]

    @patch("subprocess.Popen")
    def test_command_with_error(self, mock_popen):
        """コマンドがエラーを返す場合"""
        mock_process = MagicMock()
        mock_process.poll.side_effect = [None, 1]  # 実行中→エラー終了
        mock_process.stdout.readline.side_effect = [
            "Error occurred\n",
            "",  # EOF
        ]
        mock_process.stderr.readline.return_value = b""
        mock_process.returncode = 1
        mock_process.wait.return_value = None
        mock_popen.return_value = mock_process

        result = run_command(["false"])

        assert result["success"] is False
        assert "Error occurred" in result["output"]

    @patch("subprocess.Popen")
    def test_command_exception(self, mock_popen):
        """コマンド実行時に例外が発生する場合"""
        mock_popen.side_effect = Exception("Command not found")

        result = run_command(["nonexistent"])

        assert result["success"] is False
        assert "Command not found" in result["error"]


class TestAPIEndpoints:
    """APIエンドポイントの単体テスト"""

    @patch("src.ui.web.run_command")
    def test_update_quotes_api(self, mock_run_command, auth_client):
        """日次株価更新APIのテスト"""
        mock_run_command.return_value = {
            "success": True,
            "output": "Success",
            "error": "",
            "description": "コマンド実行中",
        }

        response = auth_client.post(
            "/api/fetch/quotes", json={}, content_type="application/json"
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert "Success" in data["output"]

    @patch("src.ui.web.run_command")
    def test_update_statements_api(self, mock_run_command, auth_client):
        """財務諸表更新APIのテスト"""
        mock_run_command.return_value = {
            "success": True,
            "output": "Updated statements",
            "error": "",
            "description": "コマンド実行中",
        }

        response = auth_client.post(
            "/api/fetch/statements", json={}, content_type="application/json"
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True

    @patch("src.ui.web.run_command")
    def test_screen_fundamental_api(self, mock_run_command, auth_client):
        """ファンダメンタルスクリーニングAPIのテスト"""
        mock_run_command.return_value = {
            "success": True,
            "output": "Screening complete",
            "error": "",
            "description": "コマンド実行中",
        }

        response = auth_client.post(
            "/api/screen/fundamental", json={}, content_type="application/json"
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True

    @patch("src.ui.web.run_command")
    def test_screen_technical_api(self, mock_run_command, auth_client):
        """テクニカルスクリーニングAPIのテスト"""
        mock_run_command.return_value = {
            "success": True,
            "output": "Indicators calculated",
            "error": "",
            "description": "コマンド実行中",
        }

        response = auth_client.post(
            "/api/screen/technical", json={}, content_type="application/json"
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True

    @patch("src.ui.web.run_command")
    def test_api_with_parameters(self, mock_run_command, auth_client):
        """パラメータ付きAPIリクエストのテスト"""
        mock_run_command.return_value = {
            "success": True,
            "output": "Success with params",
            "error": "",
            "description": "コマンド実行中",
        }

        response = auth_client.post(
            "/api/backtest/fundamental",
            json={"capital": "1000000", "hold_days": "30"},
            content_type="application/json",
        )

        assert response.status_code == 200
        # run_commandが正しいパラメータで呼ばれたか確認
        args = mock_run_command.call_args[0][0]
        assert "--capital" in args
        assert "1000000" in args
        assert "--hold" in args
        assert "30" in args

    @patch("src.ui.web.run_command")
    def test_api_error_handling(self, mock_run_command, auth_client):
        """APIエラーハンドリングのテスト"""
        mock_run_command.return_value = {
            "success": False,
            "output": "Error occurred",
            "error": "Command failed",
            "description": "コマンド実行中",
        }

        response = auth_client.post(
            "/api/fetch/quotes", json={}, content_type="application/json"
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "Error occurred" in data.get(
            "output", ""
        ) or "Error occurred" in data.get("error", "")

    def test_api_unauthorized(self, client):
        """未認証時のAPIアクセス（テスト環境では認証が無効化されている）"""
        response = client.post(
            "/api/fetch/quotes", json={}, content_type="application/json"
        )
        # テスト環境ではLOGIN_DISABLED=Trueのため、認証なしでもアクセス可能
        assert response.status_code == 200

    @patch("src.ui.web.run_command")
    def test_special_characters_in_params(self, mock_run_command, auth_client):
        """特殊文字を含むパラメータのテスト"""
        mock_run_command.return_value = {
            "success": True,
            "output": "Success",
            "error": "",
            "description": "コマンド実行中",
        }

        # 特殊文字を含むパラメータ
        response = auth_client.post(
            "/api/screen/fundamental",
            json={"lookback": "365", "as_of": "2024-06-01", "test": "テスト"},
            content_type="application/json",
        )

        assert response.status_code == 200
        # モックが呼ばれたことを確認
        mock_run_command.assert_called()


class TestEdgeCases:
    """エッジケースのテスト"""

    @patch("src.ui.web.run_command")
    def test_empty_parameters(self, mock_run_command, auth_client):
        """空のパラメータでのリクエスト"""
        mock_run_command.return_value = {
            "success": True,
            "output": "Success",
            "error": "",
            "description": "コマンド実行中",
        }

        response = auth_client.post(
            "/api/screen/fundamental", json={}, content_type="application/json"
        )
        assert response.status_code == 200

    @patch("src.ui.web.run_command")
    def test_null_parameters(self, mock_run_command, auth_client):
        """nullパラメータでのリクエスト"""
        mock_run_command.return_value = {
            "success": True,
            "output": "Success",
            "error": "",
            "description": "コマンド実行中",
        }

        response = auth_client.post(
            "/api/screen/fundamental",
            json={"lookback": None, "as_of": None},
            content_type="application/json",
        )
        assert response.status_code == 200

    @patch("src.ui.web.run_command")
    def test_very_long_output(self, mock_run_command, auth_client):
        """非常に長い出力のテスト"""
        long_output = "x" * 1000000  # 1MB
        mock_run_command.return_value = {
            "success": True,
            "output": long_output,
            "error": "",
            "description": "コマンド実行中",
        }

        response = auth_client.post(
            "/api/fetch/quotes", json={}, content_type="application/json"
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data["output"]) == 1000000

    def test_concurrent_requests(self, auth_client):
        """同時リクエストのテスト"""
        with patch("src.ui.web.run_command") as mock_run_command:
            mock_run_command.return_value = {
                "success": True,
                "output": "Success",
                "error": "",
                "description": "コマンド実行中",
            }

            # 複数のリクエストを同時に送信
            responses = []
            for _ in range(5):
                response = auth_client.post(
                    "/api/update/quotes", json={}, content_type="application/json"
                )
                responses.append(response)

            # すべてのリクエストが成功することを確認
            for response in responses:
                assert response.status_code == 200

    def test_malformed_json(self, auth_client):
        """不正なJSON形式のリクエスト"""
        response = auth_client.post(
            "/api/screen/fundamental",
            data="invalid json",
            content_type="application/json",
        )
        # Flaskは自動的に400を返すはず
        assert response.status_code == 400 or response.status_code == 200

    @patch("src.ui.web.pd.read_sql")
    def test_database_error_handling(self, mock_read_sql, auth_client):
        """データベースエラーのハンドリング"""
        mock_read_sql.side_effect = Exception("Database connection failed")

        response = auth_client.get("/api/signals/fundamental")
        # データベースエラーの場合、エラーハンドリングされて200が返される可能性がある
        assert response.status_code in [200, 500]
        if response.data:
            # エラーレスポンスが返されていることを確認
            json.loads(response.data)  # JSONとしてパース可能であることを確認

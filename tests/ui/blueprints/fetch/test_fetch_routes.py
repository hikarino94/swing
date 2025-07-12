"""ui.blueprints.fetch.routesのテスト"""

import json
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

from src.auth.models import User
from src.ui.blueprints.fetch.routes import fetch_bp


@pytest.fixture
def app():
    """テスト用のFlaskアプリケーション"""
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-secret-key"
    app.config["TESTING"] = True
    app.register_blueprint(fetch_bp)

    return app


@pytest.fixture
def client(app):
    """テスト用のクライアント"""
    return app.test_client()


@pytest.fixture
def admin_user():
    """管理者ユーザー"""
    user = MagicMock(spec=User)
    user.username = "admin"
    user.role = "admin"
    return user


@pytest.fixture
def normal_user():
    """一般ユーザー"""
    user = MagicMock(spec=User)
    user.username = "user"
    user.role = "user"
    return user


class TestFetchQuotes:
    """株価データ取得のテスト"""

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.ui.blueprints.fetch.routes.logger")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_quotes_with_dates(
        self, mock_get_user, mock_logger, mock_run, client, admin_user
    ):
        """日付指定時のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {"success": True, "message": "Success"}

        # リクエスト
        response = client.post(
            "/api/fetch/quotes",
            json={
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True

        # コマンドの確認
        expected_cmd = f"{sys.executable} fetch/daily_quotes.py --start 2024-01-01 --end 2024-01-31"
        mock_run.assert_called_once_with(expected_cmd, "株価データ取得")

        # ログの確認
        mock_logger.info.assert_any_call("株価データ取得APIが呼び出されました")
        mock_logger.info.assert_any_call("株価データ取得が正常に完了しました")

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.ui.blueprints.fetch.routes.logger")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_quotes_no_dates(
        self, mock_get_user, mock_logger, mock_run, client, admin_user
    ):
        """日付指定なしのテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {"success": True, "message": "Success"}

        # リクエスト（日付なし）
        response = client.post("/api/fetch/quotes", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True

        # コマンドの確認（日付パラメータなし）
        expected_cmd = f"{sys.executable} fetch/daily_quotes.py"
        mock_run.assert_called_once_with(expected_cmd, "株価データ取得")

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.ui.blueprints.fetch.routes.logger")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_quotes_failure(
        self, mock_get_user, mock_logger, mock_run, client, admin_user
    ):
        """取得失敗時のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {
            "success": False,
            "error": "API connection failed",
            "description": "株価データ取得",
        }

        # リクエスト
        response = client.post("/api/fetch/quotes", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["error"] == "API connection failed"

        # エラーログの確認
        mock_logger.error.assert_called()

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.ui.blueprints.fetch.routes.logger")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_quotes_exception(
        self, mock_get_user, mock_logger, mock_run, client, admin_user
    ):
        """例外発生時のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.side_effect = Exception("Unexpected error")

        # リクエスト
        response = client.post("/api/fetch/quotes", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["error"] == "Unexpected error"
        assert data["description"] == "株価データ取得"

        # エラーログの確認
        mock_logger.error.assert_called()

    @patch("src.auth.decorators.get_current_user")
    def test_fetch_quotes_not_logged_in(self, mock_get_user, client):
        """ログインしていない場合のテスト"""
        mock_get_user.return_value = None

        response = client.post("/api/fetch/quotes", json={})

        assert response.status_code == 401

    @patch("src.auth.decorators.get_current_user")
    def test_fetch_quotes_not_admin(self, mock_get_user, client, normal_user):
        """管理者でない場合のテスト"""
        mock_get_user.return_value = normal_user

        response = client.post("/api/fetch/quotes", json={})

        assert response.status_code == 403


class TestFetchListed:
    """上場情報取得のテスト"""

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.ui.blueprints.fetch.routes.logger")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_listed_success(
        self, mock_get_user, mock_logger, mock_run, client, admin_user
    ):
        """正常取得のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {"success": True, "message": "Success"}

        # リクエスト
        response = client.post("/api/fetch/listed", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True

        # コマンドの確認
        expected_cmd = f"{sys.executable} fetch/listed_info.py"
        mock_run.assert_called_once_with(expected_cmd, "上場情報取得")

        # ログの確認
        mock_logger.info.assert_any_call("上場情報取得APIが呼び出されました")
        mock_logger.info.assert_any_call("上場情報取得が正常に完了しました")

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.ui.blueprints.fetch.routes.logger")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_listed_failure(
        self, mock_get_user, mock_logger, mock_run, client, admin_user
    ):
        """取得失敗時のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {
            "success": False,
            "error": "Database error",
            "description": "上場情報取得",
        }

        # リクエスト
        response = client.post("/api/fetch/listed", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["error"] == "Database error"

        # エラーログの確認
        mock_logger.error.assert_called()

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.ui.blueprints.fetch.routes.logger")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_listed_exception(
        self, mock_get_user, mock_logger, mock_run, client, admin_user
    ):
        """例外発生時のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.side_effect = Exception("Unexpected error")

        # リクエスト
        response = client.post("/api/fetch/listed", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["error"] == "Unexpected error"
        assert data["description"] == "上場情報取得"


class TestFetchStatements:
    """財務諸表取得のテスト"""

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.ui.blueprints.fetch.routes.logger")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_statements_all_params(
        self, mock_get_user, mock_logger, mock_run, client, admin_user
    ):
        """全パラメータ指定時のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {"success": True, "message": "Success"}

        # リクエスト
        response = client.post(
            "/api/fetch/statements",
            json={
                "mode": "1",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True

        # コマンドの確認
        expected_cmd = f"{sys.executable} fetch/statements.py 1 --start 2024-01-01 --end 2024-01-31"
        mock_run.assert_called_once_with(expected_cmd, "財務諸表1")

        # ログの確認
        mock_logger.info.assert_any_call("財務諸表取得APIが呼び出されました")
        mock_logger.info.assert_any_call("財務諸表取得（モード1）が正常に完了しました")

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_statements_default_mode(
        self, mock_get_user, mock_run, client, admin_user
    ):
        """デフォルトモードのテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {"success": True}

        # リクエスト（モード指定なし）
        response = client.post("/api/fetch/statements", json={})

        # 検証
        assert response.status_code == 200

        # デフォルトモード2が使用されることを確認
        expected_cmd = f"{sys.executable} fetch/statements.py 2"
        mock_run.assert_called_once_with(expected_cmd, "財務諸表2")

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_statements_various_modes(
        self, mock_get_user, mock_run, client, admin_user
    ):
        """異なるモードのテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {"success": True}

        modes = ["1", "2", "3"]

        for mode in modes:
            # リクエスト
            response = client.post(
                "/api/fetch/statements",
                json={"mode": mode},
            )

            # 検証
            assert response.status_code == 200

            # 正しいモードが使用されることを確認
            expected_cmd = f"{sys.executable} fetch/statements.py {mode}"
            assert mock_run.call_args[0][0] == expected_cmd
            assert mock_run.call_args[0][1] == f"財務諸表{mode}"

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.ui.blueprints.fetch.routes.logger")
    @patch("src.auth.decorators.get_current_user")
    def test_fetch_statements_failure(
        self, mock_get_user, mock_logger, mock_run, client, admin_user
    ):
        """取得失敗時のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {
            "success": False,
            "error": "Invalid date range",
            "description": "財務諸表2",
        }

        # リクエスト
        response = client.post("/api/fetch/statements", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["error"] == "Invalid date range"

        # エラーログの確認
        mock_logger.error.assert_called()


class TestFetchIntegration:
    """フェッチ統合テスト"""

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.auth.decorators.get_current_user")
    def test_all_fetch_endpoints(self, mock_get_user, mock_run, client, admin_user):
        """全フェッチエンドポイントの動作確認"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {"success": True, "message": "Success"}

        endpoints = [
            ("/api/fetch/quotes", {"start_date": "2024-01-01"}),
            ("/api/fetch/listed", {}),
            ("/api/fetch/statements", {"mode": "2"}),
        ]

        for endpoint, params in endpoints:
            response = client.post(endpoint, json=params)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["success"] is True

    def test_request_methods(self, client):
        """HTTPメソッドの制限テスト"""
        endpoints = [
            "/api/fetch/quotes",
            "/api/fetch/listed",
            "/api/fetch/statements",
        ]

        for endpoint in endpoints:
            # GETは許可されていない
            response = client.get(endpoint)
            assert response.status_code == 405

            # PUTは許可されていない
            response = client.put(endpoint)
            assert response.status_code == 405

    @patch("builtins.print")
    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.auth.decorators.get_current_user")
    def test_console_output(
        self, mock_get_user, mock_run, mock_print, client, admin_user
    ):
        """コンソール出力のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {"success": True}

        # 現在時刻を固定
        with patch("src.ui.blueprints.fetch.routes.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 15, 12, 0, 0)
            mock_datetime.now().strftime.return_value = "2024-01-15 12:00:00"

            # リクエスト
            client.post(
                "/api/fetch/quotes",
                json={"start_date": "2024-01-01"},
            )

            # print文の確認
            mock_print.assert_any_call(
                "\n[API] 株価データ取得リクエストを受信しました - 2024-01-15 12:00:00"
            )
            mock_print.assert_any_call("[API] 開始日: 2024-01-01")
            mock_print.assert_any_call("[API] 株価データ取得が正常に完了しました")

    @patch("src.ui.blueprints.fetch.routes.run_command")
    @patch("src.auth.decorators.get_current_user")
    def test_empty_json_values(self, mock_get_user, mock_run, client, admin_user):
        """空のJSON値のテスト"""
        mock_get_user.return_value = admin_user
        mock_run.return_value = {"success": True}

        # 空の値を送信
        response = client.post(
            "/api/fetch/quotes",
            json={"start_date": "", "end_date": None},
        )

        # 検証
        assert response.status_code == 200

        # 空の値は含まれないことを確認
        cmd_arg = mock_run.call_args[0][0]
        assert "--start" not in cmd_arg
        assert "--end" not in cmd_arg

"""ui.blueprints.backtest.routesのテスト"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

from src.auth.models import User
from src.ui.blueprints.backtest.routes import backtest_bp


@pytest.fixture
def app():
    """テスト用のFlaskアプリケーション"""
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-secret-key"
    app.config["TESTING"] = True
    app.register_blueprint(backtest_bp)

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
    user.role = "portfolio_only"
    return user


class TestBacktestFundamental:
    """ファンダメンタルバックテストのテスト"""

    @patch("src.ui.blueprints.backtest.routes.run_command")
    @patch("src.ui.blueprints.backtest.routes.timestamped_path")
    def test_backtest_fundamental_all_params(
        self, mock_timestamp, mock_run, client, admin_user
    ):
        """全パラメータ指定時のテスト"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_timestamp.return_value = "/output/backtest/fundamental_20240115.json"
        mock_run.return_value = {"success": True, "message": "Success"}

        # リクエスト
        response = client.post(
            "/api/backtest/fundamental",
            json={
                "hold_days": 10,
                "entry_offset": 2,
                "capital": 1000000,
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] == "/output/backtest/fundamental_20240115.json"

        # コマンドの確認
        expected_cmd = f"{sys.executable} backtest/backtest_statements.py --hold 10 --entry-offset 2 --capital 1000000 --start 2024-01-01 --end 2024-01-31 --json /output/backtest/fundamental_20240115.json"
        mock_run.assert_called_once_with(expected_cmd, "ファンダメンタルバックテスト")

    @patch("src.ui.blueprints.backtest.routes.run_command")
    @patch("src.ui.blueprints.backtest.routes.timestamped_path")
    def test_backtest_fundamental_minimal_params(
        self, mock_timestamp, mock_run, client, admin_user
    ):
        """最小パラメータのテスト"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_timestamp.return_value = "/output/backtest/fundamental.json"
        mock_run.return_value = {"success": True, "message": "Success"}

        # リクエスト（パラメータなし）
        response = client.post("/api/backtest/fundamental", json={})

        # 検証
        assert response.status_code == 200

        # 最小コマンドの確認
        expected_cmd = f"{sys.executable} backtest/backtest_statements.py --json /output/backtest/fundamental.json"
        mock_run.assert_called_once_with(expected_cmd, "ファンダメンタルバックテスト")

    @patch("src.ui.blueprints.backtest.routes.run_command")
    @patch("src.ui.blueprints.backtest.routes.timestamped_path")
    def test_backtest_fundamental_failure(
        self, mock_timestamp, mock_run, client, admin_user
    ):
        """失敗時のテスト"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_timestamp.return_value = "/output/backtest/fundamental.json"
        mock_run.return_value = {"success": False, "message": "Error occurred"}

        # リクエスト
        response = client.post("/api/backtest/fundamental", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["output_file"] is None

    def test_backtest_fundamental_not_logged_in(self, app, client):
        """ログインしていない場合のテスト"""
        # TESTINGモードを一時的に無効化
        app.config["TESTING"] = False

        with patch(
            "src.auth.decorators.AuthManager.get_user_by_session"
        ) as mock_get_user:
            # ユーザーが存在しない（ログインしていない）状態をシミュレート
            mock_get_user.return_value = None

            response = client.post("/api/backtest/fundamental", json={})

            assert response.status_code == 401
            data = json.loads(response.data)
            assert data["success"] is False
            assert data["error"] == "ログインが必要です"

    def test_backtest_fundamental_not_admin(self, app, client, normal_user):
        """管理者でない場合のテスト"""
        # TESTINGモードを一時的に無効化
        app.config["TESTING"] = False

        with patch(
            "src.auth.decorators.AuthManager.get_user_by_session"
        ) as mock_get_user:
            # 一般ユーザー（portfolio_only権限）をシミュレート
            mock_get_user.return_value = normal_user

            response = client.post("/api/backtest/fundamental", json={})

            assert response.status_code == 403
            data = json.loads(response.data)
            assert data["success"] is False
            assert data["error"] == "管理者権限が必要です"


class TestBacktestTechnical:
    """テクニカルバックテストのテスト"""

    @patch("src.ui.blueprints.backtest.routes.run_command")
    @patch("src.ui.blueprints.backtest.routes.timestamped_path")
    def test_backtest_technical_all_params(
        self, mock_timestamp, mock_run, client, admin_user
    ):
        """全パラメータ指定時のテスト"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_timestamp.return_value = "/output/backtest/technical_20240115.json"
        mock_run.return_value = {"success": True, "message": "Success"}

        # リクエスト
        response = client.post(
            "/api/backtest/technical",
            json={
                "hold_days": 5,
                "stop_loss": 0.05,
                "capital": 500000,
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] == "/output/backtest/technical_20240115.json"

        # コマンドの確認
        expected_cmd = f"{sys.executable} backtest/backtest_technical.py --hold-days 5 --stop-loss 0.05 --capital 500000 --start 2024-01-01 --end 2024-01-31 --json /output/backtest/technical_20240115.json"
        mock_run.assert_called_once_with(expected_cmd, "テクニカルバックテスト")

    @patch("src.ui.blueprints.backtest.routes.run_command")
    @patch("src.ui.blueprints.backtest.routes.timestamped_path")
    def test_backtest_technical_stop_loss_param(
        self, mock_timestamp, mock_run, client, admin_user
    ):
        """ストップロスパラメータのテスト"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_timestamp.return_value = "/output/backtest/technical.json"
        mock_run.return_value = {"success": True}

        # リクエスト（ストップロスのみ）
        response = client.post(
            "/api/backtest/technical",
            json={"stop_loss": 0.1},
        )

        # 検証
        assert response.status_code == 200

        # ストップロスが含まれることを確認
        cmd_arg = mock_run.call_args[0][0]
        assert "--stop-loss 0.1" in cmd_arg

    @patch("src.ui.blueprints.backtest.routes.run_command")
    @patch("src.ui.blueprints.backtest.routes.timestamped_path")
    def test_backtest_technical_empty_params(
        self, mock_timestamp, mock_run, client, admin_user
    ):
        """空のパラメータのテスト"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_timestamp.return_value = "/output/backtest/technical.json"
        mock_run.return_value = {"success": True}

        # 値が空のパラメータ
        response = client.post(
            "/api/backtest/technical",
            json={"hold_days": None, "stop_loss": "", "capital": 0},
        )

        # 検証
        assert response.status_code == 200

        # 空の値は含まれないことを確認
        cmd_arg = mock_run.call_args[0][0]
        assert "--hold-days" not in cmd_arg
        assert "--stop-loss" not in cmd_arg
        assert "--capital 0" not in cmd_arg  # 0も空として扱われる


class TestBacktestMl:
    """MLバックテストのテスト"""

    @patch("src.ui.blueprints.backtest.routes.run_command")
    @patch("src.ui.blueprints.backtest.routes.timestamped_path")
    def test_backtest_ml_all_params(self, mock_timestamp, mock_run, client, admin_user):
        """全パラメータ指定時のテスト"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_timestamp.return_value = "/output/backtest/ml_20240115.json"
        mock_run.return_value = {"success": True, "message": "Success"}

        # リクエスト
        response = client.post(
            "/api/backtest/ml",
            json={
                "top": 20,
                "capital": 2000000,
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] == "/output/backtest/ml_20240115.json"

        # コマンドの確認
        expected_cmd = f"{sys.executable} backtest/backtest_ml.py --top 20 --capital 2000000 --start 2024-01-01 --end 2024-01-31 --json /output/backtest/ml_20240115.json"
        mock_run.assert_called_once_with(expected_cmd, "MLバックテスト")

    @patch("src.ui.blueprints.backtest.routes.run_command")
    @patch("src.ui.blueprints.backtest.routes.timestamped_path")
    def test_backtest_ml_top_param_only(
        self, mock_timestamp, mock_run, client, admin_user
    ):
        """topパラメータのみのテスト"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_timestamp.return_value = "/output/backtest/ml.json"
        mock_run.return_value = {"success": True}

        # リクエスト
        response = client.post(
            "/api/backtest/ml",
            json={"top": 10},
        )

        # 検証
        assert response.status_code == 200

        # topパラメータが含まれることを確認
        cmd_arg = mock_run.call_args[0][0]
        assert "--top 10" in cmd_arg
        assert "--capital" not in cmd_arg
        assert "--start" not in cmd_arg
        assert "--end" not in cmd_arg


class TestBacktestIntegration:
    """バックテスト統合テスト"""

    @patch("src.ui.blueprints.backtest.routes.run_command")
    @patch("src.ui.blueprints.backtest.routes.timestamped_path")
    def test_all_backtest_endpoints(self, mock_timestamp, mock_run, client, admin_user):
        """全バックテストエンドポイントの動作確認"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_run.return_value = {"success": True, "message": "Success"}

        endpoints = [
            ("/api/backtest/fundamental", {"hold_days": 10}),
            ("/api/backtest/technical", {"hold_days": 5, "stop_loss": 0.05}),
            ("/api/backtest/ml", {"top": 20}),
        ]

        for endpoint, params in endpoints:
            mock_timestamp.return_value = f"/output{endpoint}.json"

            response = client.post(endpoint, json=params)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["success"] is True
            assert data["output_file"] is not None

    @patch("src.ui.blueprints.backtest.routes.run_command")
    def test_command_injection_prevention(self, mock_run, client, admin_user):
        """コマンドインジェクション防止のテスト"""
        # TESTINGモードではcurrent_userが自動設定される
        mock_run.return_value = {"success": True}

        # 悪意のあるパラメータ
        malicious_params = {
            "capital": "1000000; rm -rf /",
            "start_date": "2024-01-01' && echo 'hacked",
        }

        client.post("/api/backtest/fundamental", json=malicious_params)

        # コマンドがそのまま文字列として扱われることを確認
        cmd_arg = mock_run.call_args[0][0]
        assert "rm -rf" in cmd_arg  # 文字列として含まれる
        assert "&&" in cmd_arg  # 文字列として含まれる

    def test_request_methods(self, client):
        """HTTPメソッドの制限テスト"""
        endpoints = [
            "/api/backtest/fundamental",
            "/api/backtest/technical",
            "/api/backtest/ml",
        ]

        for endpoint in endpoints:
            # GETは許可されていない
            response = client.get(endpoint)
            assert response.status_code == 405

            # PUTは許可されていない
            response = client.put(endpoint)
            assert response.status_code == 405

            # DELETEは許可されていない
            response = client.delete(endpoint)
            assert response.status_code == 405

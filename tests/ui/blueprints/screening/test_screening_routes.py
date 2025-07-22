"""ui.blueprints.screening.routesのテスト"""

import json
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from flask import Flask

from src.auth.models import User
from src.ui.blueprints.screening.routes import screening_bp


@pytest.fixture
def app():
    """テスト用のFlaskアプリケーション"""
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-secret-key"
    app.config["TESTING"] = True
    app.register_blueprint(screening_bp)

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
    user.id = 2
    user.username = "user"
    user.role = "portfolio_only"  # admin_requiredデコレータが確認する値
    return user


class TestScreenFundamental:
    """ファンダメンタルスクリーニングのテスト"""

    @patch("src.ui.blueprints.screening.routes.pd.read_sql")
    @patch("src.ui.blueprints.screening.routes.sqlite3.connect")
    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.timestamped_path")
    @patch("src.ui.blueprints.screening.routes.logger")
    def test_screen_fundamental_all_params(
        self,
        mock_logger,
        mock_timestamp,
        mock_run,
        mock_connect,
        mock_read_sql,
        client,
        admin_user,
    ):
        """全パラメータ指定時のテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        # tempfileを使って実際のファイルパスを作成
        import os
        import tempfile

        temp_dir = tempfile.mkdtemp()
        temp_file = os.path.join(temp_dir, "fundamental_20240115.xlsx")
        mock_timestamp.return_value = temp_file
        mock_run.return_value = {"success": True, "message": "Success", "error": ""}

        # モックDBデータ
        from datetime import datetime

        mock_df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "company_name": ["会社A", "会社B"],
                "signal_reason": ["高ROE", "低PER"],
                "created_at": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 2,
            }
        )
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Excel書き込み関連のモック
        # xlsxwriterのモック
        mock_worksheet = MagicMock()
        mock_worksheet.set_column = MagicMock()

        mock_workbook = MagicMock()
        mock_workbook.add_worksheet = MagicMock(return_value=mock_worksheet)

        # ExcelWriterのモック
        mock_writer = MagicMock()
        mock_writer.sheets = {"Signals": mock_worksheet}
        mock_writer.book = mock_workbook
        mock_writer.save = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=None)

        # DataFrame.to_excelのモック
        def mock_to_excel(writer, sheet_name=None, index=True):
            # writer.sheetsにワークシートを追加
            if sheet_name:
                writer.sheets[sheet_name] = mock_worksheet

        with patch(
            "src.ui.blueprints.screening.routes.pd.ExcelWriter"
        ) as mock_excel_writer_class:
            mock_excel_writer_class.return_value = mock_writer

            with patch.object(pd.DataFrame, "to_excel", side_effect=mock_to_excel):
                # リクエスト
                response = client.post(
                    "/api/screening/fundamental",
                    json={
                        "lookback": 365,
                        "recent": 30,
                        "as_of": "2024-01-15",
                    },
                )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] == temp_file

        # テンポラリディレクトリを削除
        import shutil

        shutil.rmtree(temp_dir)

        # コマンドの確認
        expected_cmd = f"{sys.executable} screening/screen_statements.py --lookback 365 --recent 30 --as-of 2024-01-15"
        mock_run.assert_called_once_with(expected_cmd, "ファンダメンタルスクリーニング")

        # ログの確認
        mock_logger.info.assert_any_call(
            "ファンダメンタルスクリーニングが正常に完了しました"
        )

    @patch("src.ui.blueprints.screening.routes.pd.read_sql")
    @patch("src.ui.blueprints.screening.routes.sqlite3.connect")
    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.timestamped_path")
    def test_screen_fundamental_minimal_params(
        self,
        mock_timestamp,
        mock_run,
        mock_connect,
        mock_read_sql,
        client,
        admin_user,
    ):
        """最小パラメータのテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_timestamp.return_value = "/output/screening/fundamental.xlsx"
        mock_run.return_value = {"success": True, "error": ""}

        # モックDBデータ
        mock_df = pd.DataFrame({"code": ["1234"], "company_name": ["会社A"]})
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Excel書き込み関連のモック
        # xlsxwriterのモック
        mock_worksheet = MagicMock()
        mock_worksheet.set_column = MagicMock()

        mock_workbook = MagicMock()
        mock_workbook.add_worksheet = MagicMock(return_value=mock_worksheet)

        # ExcelWriterのモック
        mock_writer = MagicMock()
        mock_writer.sheets = {"Signals": mock_worksheet}
        mock_writer.book = mock_workbook
        mock_writer.save = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=None)

        # DataFrame.to_excelのモック
        def mock_to_excel(writer, sheet_name=None, index=True):
            # writer.sheetsにワークシートを追加
            if sheet_name:
                writer.sheets[sheet_name] = mock_worksheet

        with patch(
            "src.ui.blueprints.screening.routes.pd.ExcelWriter"
        ) as mock_excel_writer_class:
            mock_excel_writer_class.return_value = mock_writer

            with patch.object(pd.DataFrame, "to_excel", side_effect=mock_to_excel):
                # リクエスト（パラメータなし）
                response = client.post("/api/screening/fundamental", json={})

        # 検証
        assert response.status_code == 200

        # 最小コマンドの確認
        expected_cmd = f"{sys.executable} screening/screen_statements.py"
        mock_run.assert_called_once_with(expected_cmd, "ファンダメンタルスクリーニング")

    @patch("src.ui.blueprints.screening.routes.pd.read_sql")
    @patch("src.ui.blueprints.screening.routes.sqlite3.connect")
    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.timestamped_path")
    @patch("src.ui.blueprints.screening.routes.logger")
    def test_screen_fundamental_empty_results(
        self,
        mock_logger,
        mock_timestamp,
        mock_run,
        mock_connect,
        mock_read_sql,
        client,
        admin_user,
    ):
        """結果が空の場合のテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_timestamp.return_value = "/output/screening/fundamental.xlsx"
        mock_run.return_value = {"success": True, "error": ""}

        # 空のDataFrame
        mock_read_sql.return_value = pd.DataFrame()
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # リクエスト
        response = client.post("/api/screening/fundamental", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] is None
        assert data["message"] == "スクリーニング結果がありません"

        # ログの確認
        mock_logger.info.assert_any_call("スクリーニング結果はありませんでした")

    @patch("src.ui.blueprints.screening.routes.pd.read_sql")
    @patch("src.ui.blueprints.screening.routes.sqlite3.connect")
    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.timestamped_path")
    @patch("src.ui.blueprints.screening.routes.logger")
    def test_screen_fundamental_excel_error(
        self,
        mock_logger,
        mock_timestamp,
        mock_run,
        mock_connect,
        mock_read_sql,
        client,
        admin_user,
    ):
        """Excel出力エラーのテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_timestamp.return_value = "/output/screening/fundamental.xlsx"
        mock_run.return_value = {"success": True, "error": ""}

        # モックDBデータ
        mock_df = pd.DataFrame({"code": ["1234"], "company_name": ["会社A"]})
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Excel書き込みでエラー
        with patch(
            "src.ui.blueprints.screening.routes.pd.ExcelWriter"
        ) as mock_excel_writer:
            mock_excel_writer.side_effect = Exception("Write error")

            # リクエスト
            response = client.post("/api/screening/fundamental", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] is None
        assert "Excel出力エラー" in data["error"]

        # エラーログの確認
        mock_logger.error.assert_called()

    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.logger")
    def test_screen_fundamental_command_failure(
        self, mock_logger, mock_run, client, admin_user
    ):
        """コマンド実行失敗のテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_run.return_value = {
            "success": False,
            "error": "Command failed",
        }

        # リクエスト
        response = client.post("/api/screening/fundamental", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["output_file"] is None

        # エラーログの確認
        mock_logger.error.assert_called()

    def test_screen_fundamental_not_logged_in(self, app, client):
        """ログインしていない場合のテスト"""
        # TESTINGモードを一時的に無効化
        app.config["TESTING"] = False

        with patch(
            "src.auth.decorators.AuthManager.get_user_by_session"
        ) as mock_get_user:
            # ユーザーが存在しない（ログインしていない）状態をシミュレート
            mock_get_user.return_value = None

            response = client.post("/api/screening/fundamental", json={})

        assert response.status_code == 401

    def test_screen_fundamental_not_admin(self, app, client, normal_user):
        """管理者でない場合のテスト"""
        # TESTINGモードを一時的に無効化
        app.config["TESTING"] = False

        with patch(
            "src.auth.decorators.AuthManager.get_user_by_session"
        ) as mock_get_user:
            # portfolio_onlyユーザーを返す
            mock_get_user.return_value = normal_user

            response = client.post("/api/screening/fundamental", json={})

        assert response.status_code == 403


class TestScreenTechnical:
    """テクニカルスクリーニングのテスト"""

    @patch("src.ui.blueprints.screening.routes.pd.read_sql")
    @patch("src.ui.blueprints.screening.routes.sqlite3.connect")
    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.timestamped_path")
    @patch("src.ui.blueprints.screening.routes.logger")
    def test_screen_technical_screen_action(
        self,
        mock_logger,
        mock_timestamp,
        mock_run,
        mock_connect,
        mock_read_sql,
        client,
        admin_user,
    ):
        """screenアクションのテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_timestamp.return_value = "/output/screening/technical_20240115.xlsx"
        mock_run.return_value = {"success": True, "message": "Success", "error": ""}

        # モックDBデータ
        mock_df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "company_name": ["会社A", "会社B"],
                "signals_count": [5, 4],
                "signals_short_count": [2, 3],
            }
        )
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Excel書き込み関連のモック
        # xlsxwriterのモック
        mock_worksheet = MagicMock()
        mock_worksheet.set_column = MagicMock()

        mock_workbook = MagicMock()
        mock_workbook.add_worksheet = MagicMock(return_value=mock_worksheet)

        # ExcelWriterのモック
        mock_writer = MagicMock()
        mock_writer.sheets = {"Signals": mock_worksheet}
        mock_writer.book = mock_workbook
        mock_writer.save = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=None)

        # DataFrame.to_excelのモック
        def mock_to_excel(writer, sheet_name=None, index=True):
            # writer.sheetsにワークシートを追加
            if sheet_name:
                writer.sheets[sheet_name] = mock_worksheet

        with patch(
            "src.ui.blueprints.screening.routes.pd.ExcelWriter"
        ) as mock_excel_writer_class:
            mock_excel_writer_class.return_value = mock_writer

            with patch.object(pd.DataFrame, "to_excel", side_effect=mock_to_excel):
                # リクエスト
                response = client.post(
                    "/api/screening/technical",
                    json={
                        "action": "screen",
                        "as_of": "2024-01-15",
                        "lookback": 20,
                    },
                )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] == "/output/screening/technical_20240115.xlsx"

        # コマンドの確認
        expected_cmd = f"{sys.executable} screening/screen_technical.py screen --as-of 2024-01-15 --lookback 20"
        mock_run.assert_called_once_with(expected_cmd, "テクニカルscreen")

        # ログの確認
        mock_logger.info.assert_any_call(
            "テクニカルスクリーニングAPIが呼び出されました"
        )
        mock_logger.info.assert_any_call("テクニカルスクリーニングが正常に完了しました")

    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.logger")
    def test_screen_technical_indicators_action(
        self, mock_logger, mock_run, client, admin_user
    ):
        """indicatorsアクションのテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_run.return_value = {"success": True, "error": ""}

        # リクエスト
        response = client.post(
            "/api/screening/technical",
            json={
                "action": "indicators",
                "as_of": "2024-01-15",
                "lookback": 100,
            },
        )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] is None  # indicatorsアクションではExcel生成なし

        # コマンドの確認
        expected_cmd = f"{sys.executable} screening/screen_technical.py indicators --as-of 2024-01-15 --lookback 100"
        mock_run.assert_called_once_with(expected_cmd, "テクニカルindicators")

    @patch("src.ui.blueprints.screening.routes.pd.read_sql")
    @patch("src.ui.blueprints.screening.routes.sqlite3.connect")
    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.timestamped_path")
    def test_screen_technical_default_action(
        self,
        mock_timestamp,
        mock_run,
        mock_connect,
        mock_read_sql,
        client,
        admin_user,
    ):
        """デフォルトアクション（screen）のテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_timestamp.return_value = "/output/screening/technical.xlsx"
        mock_run.return_value = {"success": True, "error": ""}

        # モックDBデータ
        mock_df = pd.DataFrame({"code": ["1234"], "signals_count": [3]})
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Excel書き込み関連のモック
        # xlsxwriterのモック
        mock_worksheet = MagicMock()
        mock_worksheet.set_column = MagicMock()

        mock_workbook = MagicMock()
        mock_workbook.add_worksheet = MagicMock(return_value=mock_worksheet)

        # ExcelWriterのモック
        mock_writer = MagicMock()
        mock_writer.sheets = {"Signals": mock_worksheet}
        mock_writer.book = mock_workbook
        mock_writer.save = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=None)

        # DataFrame.to_excelのモック
        def mock_to_excel(writer, sheet_name=None, index=True):
            # writer.sheetsにワークシートを追加
            if sheet_name:
                writer.sheets[sheet_name] = mock_worksheet

        with patch(
            "src.ui.blueprints.screening.routes.pd.ExcelWriter"
        ) as mock_excel_writer_class:
            mock_excel_writer_class.return_value = mock_writer

            with patch.object(pd.DataFrame, "to_excel", side_effect=mock_to_excel):
                # リクエスト（actionなし）
                response = client.post("/api/screening/technical", json={})

        # 検証
        assert response.status_code == 200

        # デフォルトでscreenアクション
        expected_cmd = f"{sys.executable} screening/screen_technical.py screen"
        mock_run.assert_called_once_with(expected_cmd, "テクニカルscreen")

    @patch("src.ui.blueprints.screening.routes.pd.read_sql")
    @patch("src.ui.blueprints.screening.routes.sqlite3.connect")
    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.timestamped_path")
    @patch("src.ui.blueprints.screening.routes.logger")
    def test_screen_technical_empty_results(
        self,
        mock_logger,
        mock_timestamp,
        mock_run,
        mock_connect,
        mock_read_sql,
        client,
        admin_user,
    ):
        """結果が空の場合のテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_timestamp.return_value = "/output/screening/technical.xlsx"
        mock_run.return_value = {"success": True, "error": ""}

        # 空のDataFrame
        mock_read_sql.return_value = pd.DataFrame()
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # リクエスト
        response = client.post("/api/screening/technical", json={"action": "screen"})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] is None
        assert data["message"] == "スクリーニング結果がありません"

        # ログの確認
        mock_logger.info.assert_any_call("スクリーニング結果はありませんでした")

    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.logger")
    def test_screen_technical_exception(
        self, mock_logger, mock_run, client, admin_user
    ):
        """例外発生時のテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_run.side_effect = Exception("Unexpected error")

        # リクエスト
        response = client.post("/api/screening/technical", json={})

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["error"] == "Unexpected error"

        # エラーログの確認
        mock_logger.error.assert_called()

    @patch("src.ui.blueprints.screening.routes.pd.read_sql")
    @patch("src.ui.blueprints.screening.routes.sqlite3.connect")
    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.timestamped_path")
    @patch("src.ui.blueprints.screening.routes.logger")
    def test_screen_technical_with_as_of_date(
        self,
        mock_logger,
        mock_timestamp,
        mock_run,
        mock_connect,
        mock_read_sql,
        client,
        admin_user,
    ):
        """as_of日付指定時のSQLクエリテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_timestamp.return_value = "/output/screening/technical.xlsx"
        mock_run.return_value = {"success": True, "error": ""}

        # モックDBデータ
        mock_df = pd.DataFrame({"code": ["1234"], "signals_count": [3]})
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Excel書き込み関連のモック
        # xlsxwriterのモック
        mock_worksheet = MagicMock()
        mock_worksheet.set_column = MagicMock()

        mock_workbook = MagicMock()
        mock_workbook.add_worksheet = MagicMock(return_value=mock_worksheet)

        # ExcelWriterのモック
        mock_writer = MagicMock()
        mock_writer.sheets = {"Signals": mock_worksheet}
        mock_writer.book = mock_workbook
        mock_writer.save = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=None)

        # DataFrame.to_excelのモック
        def mock_to_excel(writer, sheet_name=None, index=True):
            # writer.sheetsにワークシートを追加
            if sheet_name:
                writer.sheets[sheet_name] = mock_worksheet

        with patch(
            "src.ui.blueprints.screening.routes.pd.ExcelWriter"
        ) as mock_excel_writer_class:
            mock_excel_writer_class.return_value = mock_writer

            with patch.object(pd.DataFrame, "to_excel", side_effect=mock_to_excel):
                # リクエスト
                response = client.post(
                    "/api/screening/technical",
                    json={"action": "screen", "as_of": "2024-01-15"},
                )

        # 検証
        assert response.status_code == 200

        # as_of指定時のSQL確認
        sql_call = mock_read_sql.call_args[0][0]
        assert "WHERE ti.signal_date = ?" in sql_call
        params = mock_read_sql.call_args[1]["params"]
        assert params == ["2024-01-15"]


class TestScreenMl:
    """MLスクリーニングのテスト"""

    @patch("src.ui.blueprints.screening.routes.run_command")
    def test_screen_ml_train_action(self, mock_run, client, admin_user):
        """trainアクションのテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_run.return_value = {"success": True, "message": "Training completed"}

        # リクエスト
        response = client.post(
            "/api/screening/ml",
            json={"action": "train", "force": True},
        )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] is None

        # コマンドの確認
        expected_cmd = f"{sys.executable} screening/screen_ml.py train --force"
        mock_run.assert_called_once_with(expected_cmd, "MLtrain")

    @patch("src.ui.blueprints.screening.routes.run_command")
    def test_screen_ml_screen_action(self, mock_run, client, admin_user):
        """screenアクションのテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_run.return_value = {
            "success": True,
            "message": "Screening completed",
        }

        # リクエスト
        response = client.post(
            "/api/screening/ml",
            json={
                "action": "screen",
                "top": 20,
                "lookback": 365,
                "as_of": "2024-01-15",
            },
        )

        # 検証
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["output_file"] is None

        # コマンドの確認
        expected_cmd = f"{sys.executable} screening/screen_ml.py screen --top 20 --lookback 365 --as-of 2024-01-15"
        mock_run.assert_called_once_with(expected_cmd, "MLscreen")

    @patch("src.ui.blueprints.screening.routes.run_command")
    def test_screen_ml_default_action(self, mock_run, client, admin_user):
        """デフォルトアクション（screen）のテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_run.return_value = {"success": True}

        # リクエスト（actionなし）
        response = client.post("/api/screening/ml", json={})

        # 検証
        assert response.status_code == 200

        # デフォルトでscreenアクション
        expected_cmd = f"{sys.executable} screening/screen_ml.py screen"
        mock_run.assert_called_once_with(expected_cmd, "MLscreen")

    @patch("src.ui.blueprints.screening.routes.run_command")
    def test_screen_ml_train_without_force(self, mock_run, client, admin_user):
        """forceフラグなしのtrainアクションテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_run.return_value = {"success": True}

        # リクエスト
        response = client.post(
            "/api/screening/ml",
            json={"action": "train"},
        )

        # 検証
        assert response.status_code == 200

        # forceフラグなし
        expected_cmd = f"{sys.executable} screening/screen_ml.py train"
        mock_run.assert_called_once_with(expected_cmd, "MLtrain")


class TestScreeningIntegration:
    """スクリーニング統合テスト"""

    @patch("src.ui.blueprints.screening.routes.run_command")
    def test_all_screening_endpoints(self, mock_run, client, admin_user):
        """全スクリーニングエンドポイントの動作確認"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_run.return_value = {"success": True, "message": "Success"}

        endpoints = [
            ("/api/screening/fundamental", {"lookback": 365}),
            ("/api/screening/technical", {"action": "screen", "lookback": 20}),
            ("/api/screening/ml", {"action": "screen", "top": 10}),
        ]

        for endpoint, params in endpoints:
            response = client.post(endpoint, json=params)

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["success"] is True

    def test_request_methods(self, client):
        """HTTPメソッドの制限テスト"""
        endpoints = [
            "/api/screening/fundamental",
            "/api/screening/technical",
            "/api/screening/ml",
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

    @patch("src.ui.blueprints.screening.routes.pd.read_sql")
    @patch("src.ui.blueprints.screening.routes.sqlite3.connect")
    @patch("src.ui.blueprints.screening.routes.run_command")
    @patch("src.ui.blueprints.screening.routes.timestamped_path")
    def test_excel_column_width_adjustment(
        self,
        mock_timestamp,
        mock_run,
        mock_connect,
        mock_read_sql,
        client,
        admin_user,
    ):
        """Excel列幅調整のテスト"""
        # TESTINGモードでは自動的にcurrent_userが設定される
        mock_timestamp.return_value = "/output/screening/fundamental.xlsx"
        mock_run.return_value = {"success": True, "error": ""}

        # 長い文字列を含むデータ
        mock_df = pd.DataFrame(
            {
                "code": ["1234"],
                "company_name": ["非常に長い会社名テスト株式会社" * 5],  # 長い名前
                "signal_reason": ["高ROE"],
            }
        )
        mock_read_sql.return_value = mock_df
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Excel書き込み関連のモック
        # xlsxwriterのモック
        mock_worksheet = MagicMock()
        mock_worksheet.set_column = MagicMock()

        mock_workbook = MagicMock()
        mock_workbook.add_worksheet = MagicMock(return_value=mock_worksheet)

        # ExcelWriterのモック
        mock_writer = MagicMock()
        mock_writer.sheets = {"Signals": mock_worksheet}
        mock_writer.book = mock_workbook
        mock_writer.save = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=None)

        # DataFrame.to_excelのモック
        def mock_to_excel(writer, sheet_name=None, index=True):
            # writer.sheetsにワークシートを追加
            if sheet_name:
                writer.sheets[sheet_name] = mock_worksheet

        with patch(
            "src.ui.blueprints.screening.routes.pd.ExcelWriter"
        ) as mock_excel_writer_class:
            mock_excel_writer_class.return_value = mock_writer

            with patch.object(pd.DataFrame, "to_excel", side_effect=mock_to_excel):
                # リクエスト
                response = client.post("/api/screening/fundamental", json={})
                assert response.status_code == 200

                # 列幅設定の確認
                # set_columnが呼ばれたことを確認
                mock_worksheet.set_column.assert_called()
                # 具体的な呼び出し内容は実装によって異なるため、詳細なアサーションは避ける

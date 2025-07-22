"""ui.blueprints.results.routesのテスト"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

from src.auth.models import User
from src.ui.blueprints.results.routes import results_bp


@pytest.fixture
def app():
    """テスト用のFlaskアプリケーション"""
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-secret-key"
    app.config["TESTING"] = True
    app.register_blueprint(results_bp)

    return app


@pytest.fixture
def client(app):
    """テスト用のクライアント"""
    return app.test_client()


@pytest.fixture
def user():
    """テスト用ユーザー"""
    user = MagicMock(spec=User)
    user.id = 1
    user.username = "testuser"
    return user


class TestListResults:
    """list_results関数のテスト"""

    @patch("src.ui.blueprints.results.routes.Path")
    def test_list_results_all_types(self, mock_path_class, client):
        """全ファイルタイプの一覧取得"""
        # モックファイルの設定
        mock_file1 = MagicMock()
        mock_file1.name = "backtest_result_20240115.xlsx"
        mock_file1.stat.return_value.st_size = 1024
        mock_file1.stat.return_value.st_mtime = 1705276800  # 2024-01-15
        mock_file1.relative_to.return_value = Path(
            "backtest/backtest_result_20240115.xlsx"
        )

        mock_file2 = MagicMock()
        mock_file2.name = "screening_result_20240115.json"
        mock_file2.stat.return_value.st_size = 2048
        mock_file2.stat.return_value.st_mtime = 1705276900
        mock_file2.relative_to.return_value = Path(
            "screening/screening_result_20240115.json"
        )

        # モックディレクトリの設定
        mock_backtest_dir = MagicMock()
        mock_backtest_dir.exists.return_value = True
        mock_backtest_dir.glob.side_effect = lambda pattern: (
            [mock_file1] if pattern == "*.xlsx" else []
        )

        mock_screening_dir = MagicMock()
        mock_screening_dir.exists.return_value = True
        mock_screening_dir.glob.side_effect = lambda pattern: (
            [mock_file2] if pattern == "*.json" else []
        )

        mock_reports_dir = MagicMock()
        mock_reports_dir.exists.return_value = False

        # output_dirのモック
        mock_output_dir = MagicMock()

        def output_dir_truediv(self, name):
            if name == "backtest":
                return mock_backtest_dir
            elif name == "screening":
                return mock_screening_dir
            elif name == "reports":
                return mock_reports_dir
            else:
                return MagicMock()

        mock_output_dir.__truediv__ = output_dir_truediv

        # project_rootのモック
        mock_project_root = MagicMock()

        def project_root_truediv(self, name):
            if name == "data":
                mock_data_dir = MagicMock()
                mock_data_dir.__truediv__ = lambda s, n: (
                    mock_output_dir if n == "output" else MagicMock()
                )
                return mock_data_dir
            else:
                return MagicMock()

        mock_project_root.__truediv__ = project_root_truediv

        # Path(__file__)のモック
        def path_constructor(arg):
            if isinstance(arg, str) and arg.endswith("routes.py"):
                mock_file_path = MagicMock()
                mock_file_path.resolve.return_value.parent.parent.parent.parent.parent = (
                    mock_project_root
                )
                return mock_file_path
            else:
                return Path(arg)

        mock_path_class.side_effect = path_constructor

        # リクエスト
        response = client.get("/api/results/list")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["files"]) == 2

        # ファイル情報の確認
        file1 = next(
            f for f in data["files"] if f["name"] == "backtest_result_20240115.xlsx"
        )
        assert file1["category"] == "backtest"
        assert file1["size"] == 1024
        assert file1["type"] == "xlsx"

    @patch("src.ui.blueprints.results.routes.Path")
    def test_list_results_specific_category(self, mock_path_class, client):
        """特定カテゴリのファイル一覧取得"""
        # モックファイルの設定
        mock_file = MagicMock()
        mock_file.name = "screening_result.xlsx"
        mock_file.stat.return_value.st_size = 1500
        mock_file.stat.return_value.st_mtime = 1705276800
        mock_file.relative_to.return_value = Path("screening/screening_result.xlsx")

        # スクリーニングディレクトリのみ設定
        mock_screening_dir = MagicMock()
        mock_screening_dir.exists.return_value = True
        mock_screening_dir.glob.side_effect = lambda pattern: (
            [mock_file] if pattern == "*.xlsx" else []
        )

        # output_dirのモック
        mock_output_dir = MagicMock()
        mock_output_dir.__truediv__ = lambda self, name: (
            mock_screening_dir if name == "screening" else MagicMock()
        )

        # project_rootのモック
        mock_project_root = MagicMock()

        def project_root_truediv(self, name):
            if name == "data":
                mock_data_dir = MagicMock()
                mock_data_dir.__truediv__ = lambda s, n: (
                    mock_output_dir if n == "output" else MagicMock()
                )
                return mock_data_dir
            else:
                return MagicMock()

        mock_project_root.__truediv__ = project_root_truediv

        # Path(__file__)のモック
        def path_constructor(arg):
            if isinstance(arg, str) and arg.endswith("routes.py"):
                mock_file_path = MagicMock()
                mock_file_path.resolve.return_value.parent.parent.parent.parent.parent = (
                    mock_project_root
                )
                return mock_file_path
            else:
                return Path(arg)

        mock_path_class.side_effect = path_constructor

        # リクエスト
        response = client.get("/api/results/list?category=screening")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["files"]) == 1
        assert data["files"][0]["category"] == "screening"

    @patch("src.ui.blueprints.results.routes.Path")
    def test_list_results_no_files(self, mock_path_class, client):
        """ファイルが存在しない場合"""
        # 全てのディレクトリが存在しない
        mock_dir = MagicMock()
        mock_dir.exists.return_value = False

        # output_dirのモック
        mock_output_dir = MagicMock()
        mock_output_dir.__truediv__ = lambda self, name: mock_dir

        # project_rootのモック
        mock_project_root = MagicMock()

        def project_root_truediv(self, name):
            if name == "data":
                mock_data_dir = MagicMock()
                mock_data_dir.__truediv__ = lambda s, n: (
                    mock_output_dir if n == "output" else MagicMock()
                )
                return mock_data_dir
            else:
                return MagicMock()

        mock_project_root.__truediv__ = project_root_truediv

        # Path(__file__)のモック
        def path_constructor(arg):
            if isinstance(arg, str) and arg.endswith("routes.py"):
                mock_file_path = MagicMock()
                mock_file_path.resolve.return_value.parent.parent.parent.parent.parent = (
                    mock_project_root
                )
                return mock_file_path
            else:
                return Path(arg)

        mock_path_class.side_effect = path_constructor

        # リクエスト
        response = client.get("/api/results/list")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["files"]) == 0


class TestDownloadResult:
    """download_result関数のテスト"""

    @patch("src.ui.blueprints.results.routes.send_file")
    @patch("src.ui.blueprints.results.routes.Path")
    @patch("src.ui.blueprints.results.routes.logger")
    def test_download_result_success(
        self, mock_logger, mock_path_class, mock_send_file, client
    ):
        """ファイルダウンロード成功"""
        # モックフルパス
        mock_full_path = MagicMock()
        mock_full_path.exists.return_value = True
        mock_full_path.is_file.return_value = True

        # output_dirのモック
        mock_output_dir = MagicMock()

        def output_dir_truediv(self, name):
            if name == Path("backtest/result.xlsx"):
                return mock_full_path
            else:
                return MagicMock()

        mock_output_dir.__truediv__ = output_dir_truediv

        # data_dirのモック
        mock_data_dir = MagicMock()
        mock_data_dir.__truediv__ = lambda s, n: (
            mock_output_dir if n == "output" else MagicMock()
        )

        # project_rootのモック
        mock_project_root = MagicMock()
        mock_project_root.__truediv__ = lambda s, n: (
            mock_data_dir if n == "data" else MagicMock()
        )

        # Path(__file__)とPath(filepath)のモック
        call_count = 0

        def path_constructor(arg):
            nonlocal call_count
            if isinstance(arg, str) and arg.endswith("routes.py"):
                mock_file_path = MagicMock()
                mock_file_path.resolve.return_value.parent.parent.parent.parent.parent = (
                    mock_project_root
                )
                return mock_file_path
            elif arg == "backtest/result.xlsx":
                mock_safe_path = MagicMock()
                mock_safe_path.parts = [
                    "backtest",
                    "result.xlsx",
                ]  # ".."を含まない安全なパス
                return mock_safe_path
            else:
                return Path(arg)

        mock_path_class.side_effect = path_constructor

        # send_fileのモック
        mock_send_file.return_value = "file_response"

        # リクエスト
        response = client.get("/api/results/download/backtest/result.xlsx")
        assert response.data == b"file_response"

        # ログの確認
        mock_logger.info.assert_any_call(
            "結果ファイルダウンロードが要求されました: backtest/result.xlsx"
        )
        mock_logger.info.assert_any_call(
            "ファイルをダウンロードします: backtest/result.xlsx"
        )

    @patch("src.ui.blueprints.results.routes.Path")
    @patch("src.ui.blueprints.results.routes.logger")
    def test_download_result_invalid_path(self, mock_logger, mock_path_class, client):
        """不正なパスの場合"""

        # Path(filepath)のモックで".."を含むパス
        def path_constructor(arg):
            if arg == "../etc/passwd":
                mock_safe_path = MagicMock()
                mock_safe_path.parts = ["..", "etc", "passwd"]  # 不正なパス
                return mock_safe_path
            else:
                return Path(arg)

        mock_path_class.side_effect = path_constructor

        # リクエスト
        response = client.get("/api/results/download/../etc/passwd")
        assert response.status_code == 404

        data = json.loads(response.data)
        assert data["success"] is False
        assert "Invalid file path" in data["error"]

        # 警告ログの確認
        mock_logger.warning.assert_called_with(
            "不正なファイルパスが指定されました: ../etc/passwd"
        )

    @patch("src.ui.blueprints.results.routes.Path")
    def test_list_results_hidden_files_excluded(self, mock_path_class, client):
        """隠しファイルが除外されることを確認"""
        # モックファイルの設定
        mock_file1 = MagicMock()
        mock_file1.name = "result.xlsx"
        mock_file1.stat.return_value.st_size = 1000
        mock_file1.stat.return_value.st_mtime = 1705276800
        mock_file1.relative_to.return_value = Path("backtest/result.xlsx")

        mock_hidden_file = MagicMock()
        mock_hidden_file.name = ".hidden_result.xlsx"

        # モックディレクトリの設定
        mock_backtest_dir = MagicMock()
        mock_backtest_dir.exists.return_value = True
        mock_backtest_dir.glob.side_effect = lambda pattern: (
            [mock_file1, mock_hidden_file] if pattern == "*.xlsx" else []
        )

        mock_screening_dir = MagicMock()
        mock_screening_dir.exists.return_value = False

        mock_reports_dir = MagicMock()
        mock_reports_dir.exists.return_value = False

        # output_dirのモック
        mock_output_dir = MagicMock()

        def output_dir_truediv(self, name):
            if name == "backtest":
                return mock_backtest_dir
            elif name == "screening":
                return mock_screening_dir
            elif name == "reports":
                return mock_reports_dir
            else:
                return MagicMock()

        mock_output_dir.__truediv__ = output_dir_truediv

        # project_rootのモック
        mock_project_root = MagicMock()

        def project_root_truediv(self, name):
            if name == "data":
                mock_data_dir = MagicMock()
                mock_data_dir.__truediv__ = lambda s, n: (
                    mock_output_dir if n == "output" else MagicMock()
                )
                return mock_data_dir
            else:
                return MagicMock()

        mock_project_root.__truediv__ = project_root_truediv

        # Path(__file__)のモック
        def path_constructor(arg):
            if isinstance(arg, str) and arg.endswith("routes.py"):
                mock_file_path = MagicMock()
                mock_file_path.resolve.return_value.parent.parent.parent.parent.parent = (
                    mock_project_root
                )
                return mock_file_path
            else:
                return Path(arg)

        mock_path_class.side_effect = path_constructor

        # リクエスト
        response = client.get("/api/results/list")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["files"]) == 1  # 隠しファイルは含まれない
        assert data["files"][0]["name"] == "result.xlsx"


class TestResultsIntegration:
    """results_bpの統合テスト"""

    def test_all_endpoints_require_login(self, app, client):
        """全エンドポイントがログイン必須であることを確認"""
        # TESTINGモードを一時的に無効化
        app.config["TESTING"] = False

        endpoints = [
            ("/api/results/list", "GET"),
            ("/api/results/download/test.xlsx", "GET"),
        ]

        with patch(
            "src.auth.decorators.AuthManager.get_user_by_session"
        ) as mock_get_user:
            # ユーザーが存在しない（ログインしていない）状態
            mock_get_user.return_value = None

            # APIエンドポイントなのでjsonifyが呼ばれる
            with patch("src.auth.decorators.jsonify") as mock_jsonify:
                from werkzeug.wrappers import Response

                # jsonifyの戻り値をモック（401レスポンスを返す）
                mock_response = Response(
                    "Unauthorized", status=401, mimetype="application/json"
                )
                mock_jsonify.return_value = mock_response

                for endpoint, method in endpoints:
                    if method == "GET":
                        response = client.get(endpoint)
                    assert response.status_code == 401

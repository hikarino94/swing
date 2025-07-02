"""web.pyのテスト"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# web.pyをインポートする前に環境を設定
os.environ["TESTING"] = "1"

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ui.web import app, timestamped_path


@pytest.fixture
def client():
    """Flask テストクライアントを作成"""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_account_json(tmp_path):
    """テスト用のaccount.jsonを作成"""
    account_file = tmp_path / "account.json"
    account_file.write_text(
        json.dumps({"mailaddress": "test@example.com", "password": "testpass123"})
    )
    return account_file


@pytest.fixture
def mock_thresholds_json(tmp_path):
    """テスト用のthresholds.jsonを作成"""
    thresholds_file = tmp_path / "screening" / "thresholds.json"
    thresholds_file.parent.mkdir(parents=True, exist_ok=True)
    thresholds_file.write_text(
        json.dumps(
            {
                "per_min": 5,
                "per_max": 20,
                "pbr_max": 2,
                "market_cap_min": 10000000000,
            }
        )
    )
    return thresholds_file


class TestTimestampedPath:
    """timestamped_path関数のテスト"""

    def test_timestamped_path_basic(self):
        """基本的なファイル名のタイムスタンプ付与"""
        result = timestamped_path("screening", "test", ".txt")
        assert "screening/test_" in result
        assert result.endswith(".txt")
        assert len(result) > len("screening/test_YYYYMMDD_HHMMSS.txt") - 1

    def test_timestamped_path_no_extension(self):
        """拡張子なしファイル名"""
        result = timestamped_path("backtest", "test", "")
        assert "backtest/test_" in result
        assert not result.endswith(".")

    def test_timestamped_path_multiple_dots(self):
        """複数のドットを含むファイル名"""
        result = timestamped_path("backtest", "test.backup", ".tar.gz")
        assert "backtest/test.backup_" in result
        assert result.endswith(".tar.gz")


class TestBasicRoutes:
    """基本的なルートのテスト"""

    def test_index_route(self, client):
        """インデックスページへのアクセス"""
        response = client.get("/")
        assert response.status_code == 200

    def test_404_error(self, client):
        """存在しないルートへのアクセス"""
        response = client.get("/nonexistent")
        assert response.status_code == 404


class TestFetchRoutes:
    """データ取得APIのテスト"""

    @patch("src.ui.web.run_command")
    def test_fetch_quotes_basic(self, mock_run, client):
        """株価データ取得の基本テスト"""
        mock_run.return_value = {
            "success": True,
            "output": "取得完了",
            "error": "",
            "description": "株価データ取得",
        }

        response = client.post("/api/fetch/quotes", json={})
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True
        assert "株価データ取得" in data["description"]

    @patch("src.ui.web.run_command")
    def test_fetch_quotes_with_dates(self, mock_run, client):
        """日付指定での株価データ取得"""
        mock_run.return_value = {
            "success": True,
            "output": "取得完了",
            "error": "",
            "description": "株価データ取得",
        }

        response = client.post(
            "/api/fetch/quotes",
            json={"start_date": "2024-01-01", "end_date": "2024-01-31"},
        )
        assert response.status_code == 200
        # コマンドに日付が含まれることを確認
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "--start 2024-01-01" in cmd
        assert "--end 2024-01-31" in cmd

    @patch("src.ui.web.run_command")
    def test_fetch_statements_mode_selection(self, mock_run, client):
        """財務諸表取得のモード選択テスト"""
        mock_run.return_value = {"success": True, "output": "", "error": ""}

        # モード1のテスト
        response = client.post("/api/fetch/statements", json={"mode": "1"})
        assert response.status_code == 200
        cmd = mock_run.call_args[0][0]
        assert " 1" in cmd

        # モード2のテスト（デフォルト）
        response = client.post("/api/fetch/statements", json={})
        assert response.status_code == 200
        cmd = mock_run.call_args[0][0]
        assert " 2" in cmd


class TestScreeningRoutes:
    """スクリーニングAPIのテスト"""

    @patch("src.ui.web.run_command")
    def test_screen_fundamental(self, mock_run, client):
        """ファンダメンタルスクリーニング"""
        mock_run.return_value = {
            "success": True,
            "output": "10銘柄を抽出",
            "error": "",
            "description": "ファンダメンタルスクリーニング",
        }

        response = client.post(
            "/api/screen/fundamental",
            json={"lookback": 60, "recent": 30, "as_of": "2024-01-01"},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True
        assert data["output_file"] is not None
        assert data["output_file"].startswith("fund_screen_")

    @patch("src.ui.web.run_command")
    def test_screen_technical_indicators(self, mock_run, client):
        """テクニカル指標計算"""
        mock_run.return_value = {"success": True, "output": "", "error": ""}

        response = client.post("/api/screen/technical", json={"action": "indicators"})
        assert response.status_code == 200
        data = response.get_json()
        assert data["output_file"] is None  # indicators実行時はファイル出力なし

    @patch("src.ui.web.run_command")
    def test_screen_ml_train(self, mock_run, client):
        """ML学習"""
        mock_run.return_value = {"success": True, "output": "学習完了", "error": ""}

        response = client.post(
            "/api/screen/ml", json={"action": "train", "force": True}
        )
        assert response.status_code == 200
        cmd = mock_run.call_args[0][0]
        assert "train" in cmd
        assert "--force" in cmd


class TestBacktestRoutes:
    """バックテストAPIのテスト"""

    @patch("src.ui.web.run_command")
    def test_backtest_fundamental(self, mock_run, client):
        """ファンダメンタルバックテスト"""
        mock_run.return_value = {
            "success": True,
            "output": "バックテスト完了",
            "error": "",
        }

        response = client.post(
            "/api/backtest/fundamental",
            json={
                "hold_days": 20,
                "entry_offset": 1,
                "capital": 1000000,
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
            },
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True
        assert data["output_file"].startswith("backtest_fund_")

    @patch("src.ui.web.run_command")
    def test_backtest_ml(self, mock_run, client):
        """MLバックテスト"""
        mock_run.return_value = {"success": True, "output": "", "error": ""}

        response = client.post(
            "/api/backtest/ml",
            json={"top": 10, "capital": 1000000},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["output_file"].startswith("backtest_ml_")


class TestUtilityRoutes:
    """ユーティリティAPIのテスト"""

    @patch("src.ui.web.run_command")
    def test_update_token_with_credentials(self, mock_run, client):
        """認証情報を指定したトークン更新"""
        mock_run.return_value = {
            "success": True,
            "output": "トークン更新完了",
            "error": "",
        }

        response = client.post(
            "/api/utils/update_token",
            json={"email": "user@example.com", "password": "password123"},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True

    @patch("src.ui.web.run_command")
    def test_update_token_from_account_json(
        self, mock_run, client, tmp_path, monkeypatch
    ):
        """account.jsonからの自動読み込みテスト"""
        # account.jsonを作成
        account_file = tmp_path / "account.json"
        account_file.write_text(
            json.dumps({"mailaddress": "test@example.com", "password": "testpass"})
        )
        monkeypatch.chdir(tmp_path)

        mock_run.return_value = {"success": True, "output": "", "error": ""}

        # 空のメールアドレスとパスワードで送信
        response = client.post(
            "/api/utils/update_token", json={"email": "", "password": ""}
        )
        assert response.status_code == 200

        # account.jsonの値が使用されることを確認
        cmd = mock_run.call_args[0][0]
        assert "--mail test@example.com" in cmd
        assert "--password testpass" in cmd

    def test_update_token_no_account_json(self, client, tmp_path, monkeypatch):
        """account.jsonが存在しない場合のエラー"""
        monkeypatch.chdir(tmp_path)

        response = client.post(
            "/api/utils/update_token", json={"email": "", "password": ""}
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is False
        assert "account.jsonが見つかりません" in data["error"]

    @patch("src.ui.web.run_command")
    def test_db_summary(self, mock_run, client):
        """DBサマリー取得"""
        mock_run.return_value = {
            "success": True,
            "output": "テーブル: prices - 1000件",
            "error": "",
        }

        response = client.get("/api/utils/db_summary")
        assert response.status_code == 200
        data = response.get_json()
        assert "prices" in data["output"]

    def test_thresholds_get(self, client, tmp_path, monkeypatch):
        """閾値設定の取得"""
        # screening/thresholds.jsonを作成
        screening_dir = tmp_path / "screening"
        screening_dir.mkdir()
        thresholds_file = screening_dir / "thresholds.json"
        thresholds_data = {"per_min": 5, "per_max": 20}
        thresholds_file.write_text(json.dumps(thresholds_data))
        monkeypatch.chdir(tmp_path)

        response = client.get("/api/utils/thresholds")
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True
        assert data["data"]["per_min"] == 5

    def test_thresholds_post(self, client, tmp_path, monkeypatch):
        """閾値設定の更新"""
        screening_dir = tmp_path / "screening"
        screening_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        new_thresholds = {"per_min": 10, "per_max": 30}
        response = client.post("/api/utils/thresholds", json=new_thresholds)
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True

        # ファイルが更新されたことを確認
        with open(screening_dir / "thresholds.json") as f:
            saved_data = json.load(f)
        assert saved_data["per_min"] == 10


class TestResultsRoutes:
    """結果ファイル関連APIのテスト"""

    def test_list_results(self, client, tmp_path, monkeypatch):
        """結果ファイル一覧取得"""
        monkeypatch.chdir(tmp_path)

        # テスト用ファイルを作成
        (tmp_path / "result1.xlsx").write_text("dummy")
        (tmp_path / "result2.json").write_text("{}")
        (tmp_path / ".hidden.xlsx").write_text("hidden")  # 隠しファイル

        response = client.get("/api/results/list")
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True
        assert len(data["files"]) == 2  # 隠しファイルは除外
        assert any(f["name"] == "result1.xlsx" for f in data["files"])
        assert any(f["name"] == "result2.json" for f in data["files"])

    def test_download_result(self, client, tmp_path, monkeypatch):
        """結果ファイルダウンロード"""
        monkeypatch.chdir(tmp_path)

        # テスト用ファイルを作成
        test_file = tmp_path / "test_result.xlsx"
        test_file.write_bytes(b"test data")

        response = client.get("/api/results/download/test_result.xlsx")
        assert response.status_code == 200
        assert response.data == b"test data"

    def test_download_result_not_found(self, client):
        """存在しないファイルのダウンロード"""
        response = client.get("/api/results/download/nonexistent.xlsx")
        assert response.status_code == 404


class TestAnalyzeJsonRoute:
    """JSON分析APIのテスト"""

    @patch("src.ui.web.run_command")
    def test_analyze_json_basic(self, mock_run, client):
        """基本的なJSON分析"""
        mock_run.return_value = {"success": True, "output": "分析結果", "error": ""}

        response = client.post(
            "/api/utils/analyze_json",
            json={"files": ["backtest1.json", "backtest2.json"]},
        )
        assert response.status_code == 200
        cmd = mock_run.call_args[0][0]
        assert "backtest1.json" in cmd
        assert "backtest2.json" in cmd

    def test_analyze_json_no_files(self, client):
        """ファイルが選択されていない場合"""
        response = client.post("/api/utils/analyze_json", json={"files": []})
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is False
        assert "ファイルが選択されていません" in data["error"]


class TestRunCommand:
    """run_command関数のテスト"""

    @patch("subprocess.run")
    def test_run_command_success(self, mock_subprocess):
        """コマンド実行成功"""
        from src.ui.web import run_command

        mock_subprocess.return_value = MagicMock(returncode=0, stdout="出力", stderr="")

        result = run_command("echo test", "テスト実行")
        assert result["success"] is True
        assert result["output"] == "出力"
        assert result["description"] == "テスト実行"

    @patch("subprocess.run")
    def test_run_command_failure(self, mock_subprocess):
        """コマンド実行失敗"""
        from src.ui.web import run_command

        mock_subprocess.return_value = MagicMock(
            returncode=1, stdout="", stderr="エラー"
        )

        result = run_command("false", "失敗テスト")
        assert result["success"] is False
        assert result["error"] == "エラー"

    @patch("subprocess.run")
    def test_run_command_exception(self, mock_subprocess):
        """コマンド実行時の例外"""
        from src.ui.web import run_command

        mock_subprocess.side_effect = Exception("実行エラー")

        result = run_command("invalid command", "例外テスト")
        assert result["success"] is False
        assert "実行エラー" in result["error"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

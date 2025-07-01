"""web.pyの追加カバレッジテスト - カバレッジ向上のための詳細なテスト"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

os.environ["TESTING"] = "1"

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from web import app


@pytest.fixture
def client():
    """Flask テストクライアントを作成"""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


class TestAdditionalScreeningCoverage:
    """スクリーニングAPIの追加カバレッジテスト"""

    @patch("web.run_command")
    def test_screen_technical_screen_action_with_all_params(self, mock_run, client):
        """テクニカルスクリーニング - 全パラメータ指定"""
        mock_run.return_value = {
            "success": True,
            "output": "スクリーニング完了",
            "error": "",
        }

        response = client.post(
            "/api/screen/technical",
            json={"action": "screen", "as_of": "2024-01-01", "lookback": 150},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True
        assert data["output_file"] is not None

        # コマンドに全パラメータが含まれることを確認
        cmd = mock_run.call_args[0][0]
        assert "screen" in cmd
        assert "--as-of 2024-01-01" in cmd
        assert "--lookback 150" in cmd

    @patch("web.run_command")
    def test_screen_ml_screen_action_with_all_params(self, mock_run, client):
        """MLスクリーニング - 全パラメータ指定"""
        mock_run.return_value = {
            "success": True,
            "output": "スクリーニング完了",
            "error": "",
        }

        response = client.post(
            "/api/screen/ml",
            json={"action": "screen", "top": 20, "lookback": 300},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["output_file"] is not None

        cmd = mock_run.call_args[0][0]
        assert "--top 20" in cmd
        assert "--lookback 300" in cmd


class TestAdditionalBacktestCoverage:
    """バックテストAPIの追加カバレッジテスト"""

    @patch("web.run_command")
    def test_backtest_technical_with_all_params(self, mock_run, client):
        """テクニカルバックテスト - 全パラメータ指定"""
        mock_run.return_value = {
            "success": True,
            "output": "バックテスト完了",
            "error": "",
        }

        response = client.post(
            "/api/backtest/technical",
            json={
                "hold_days": 30,
                "stop_loss": 7.5,
                "capital": 2000000,
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
            },
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True

        cmd = mock_run.call_args[0][0]
        assert "--hold-days 30" in cmd
        assert "--stop-loss 7.5" in cmd
        assert "--capital 2000000" in cmd
        assert "--start 2023-01-01" in cmd
        assert "--end 2023-12-31" in cmd

    @patch("web.run_command")
    def test_backtest_ml_with_minimal_params(self, mock_run, client):
        """MLバックテスト - 最小パラメータ"""
        mock_run.return_value = {
            "success": True,
            "output": "",
            "error": "",
        }

        response = client.post(
            "/api/backtest/ml",
            json={},  # パラメータなし
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["output_file"] is not None


class TestUtilityEdgeCases:
    """ユーティリティAPIのエッジケーステスト"""

    @patch("web.run_command")
    def test_update_token_partial_info_from_json(
        self, mock_run, client, tmp_path, monkeypatch
    ):
        """account.jsonから部分的な情報を読み込む"""
        # メールアドレスのみaccount.jsonに存在
        account_file = tmp_path / "account.json"
        account_file.write_text(
            json.dumps({"mailaddress": "stored@example.com"})  # パスワードなし
        )
        monkeypatch.chdir(tmp_path)

        mock_run.return_value = {"success": True, "output": "", "error": ""}

        # パスワードのみ提供
        response = client.post(
            "/api/utils/update_token", json={"email": "", "password": "newpass123"}
        )
        assert response.status_code == 200

        cmd = mock_run.call_args[0][0]
        assert "--mail stored@example.com" in cmd
        assert "--password newpass123" in cmd

    def test_update_token_invalid_json(self, client, tmp_path, monkeypatch):
        """不正なaccount.jsonの処理"""
        account_file = tmp_path / "account.json"
        account_file.write_text("invalid json content")
        monkeypatch.chdir(tmp_path)

        response = client.post(
            "/api/utils/update_token", json={"email": "", "password": ""}
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is False
        assert "形式が不正" in data["error"]

    @patch("web.run_command")
    def test_update_token_save_account_json_failure(
        self, mock_run, client, tmp_path, monkeypatch
    ):
        """account.json保存失敗時の処理"""
        monkeypatch.chdir(tmp_path)

        mock_run.return_value = {"success": True, "output": "成功", "error": ""}

        # account.jsonを読み取り専用にして保存を失敗させる
        account_file = tmp_path / "account.json"
        account_file.write_text("{}")
        account_file.chmod(0o444)

        response = client.post(
            "/api/utils/update_token",
            json={"email": "test@example.com", "password": "pass123"},
        )

        # 保存に失敗してもAPIは成功を返す
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is True

    @patch("web.run_command")
    def test_list_signals_with_all_params(self, mock_run, client):
        """シグナル一覧 - 全パラメータ指定"""
        mock_run.return_value = {
            "success": True,
            "output": "シグナル一覧",
            "error": "",
        }

        response = client.post(
            "/api/utils/list_signals",
            json={
                "type": "tech",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "limit": 50,
            },
        )
        assert response.status_code == 200

        cmd = mock_run.call_args[0][0]
        assert "tech" in cmd
        assert "--start 2024-01-01" in cmd
        assert "--end 2024-12-31" in cmd
        assert "--limit 50" in cmd

    @patch("web.run_command")
    def test_analyze_json_with_options(self, mock_run, client):
        """JSON分析 - オプション指定"""
        mock_run.return_value = {
            "success": True,
            "output": "分析結果",
            "error": "",
        }

        response = client.post(
            "/api/utils/analyze_json",
            json={"files": ["test1.json"], "show_trades": True, "side": "short"},
        )
        assert response.status_code == 200

        cmd = mock_run.call_args[0][0]
        assert "--show-trades" in cmd
        assert "--side short" in cmd

    def test_thresholds_get_file_not_found(self, client, tmp_path, monkeypatch):
        """閾値ファイルが存在しない場合"""
        monkeypatch.chdir(tmp_path)

        response = client.get("/api/utils/thresholds")
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is False
        assert "error" in data

    def test_thresholds_post_exception(self, client, tmp_path, monkeypatch):
        """閾値保存時の例外処理"""
        screening_dir = tmp_path / "screening"
        screening_dir.mkdir()
        # ディレクトリを読み取り専用にして書き込みを失敗させる
        screening_dir.chmod(0o555)
        monkeypatch.chdir(tmp_path)

        response = client.post("/api/utils/thresholds", json={"test": "data"})
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is False


class TestResultsEdgeCases:
    """結果ファイル関連のエッジケーステスト"""

    def test_list_results_with_custom_types(self, client, tmp_path, monkeypatch):
        """カスタムファイルタイプでの一覧取得"""
        monkeypatch.chdir(tmp_path)

        # 異なる拡張子のファイルを作成
        (tmp_path / "result.csv").write_text("csv data")
        (tmp_path / "result.txt").write_text("txt data")
        (tmp_path / "result.xlsx").write_text("xlsx data")

        response = client.get("/api/results/list?types=csv,txt")
        assert response.status_code == 200
        data = response.get_json()
        assert len(data["files"]) == 2
        assert any(f["type"] == "csv" for f in data["files"])
        assert any(f["type"] == "txt" for f in data["files"])
        assert not any(f["type"] == "xlsx" for f in data["files"])

    def test_download_result_exception_handling(self, client, tmp_path, monkeypatch):
        """ダウンロード時の例外処理"""
        monkeypatch.chdir(tmp_path)

        # send_fileが例外を投げるようにモック
        with patch("web.send_file") as mock_send:
            mock_send.side_effect = Exception("File error")

            response = client.get("/api/results/download/test.xlsx")
            assert response.status_code == 404
            data = response.get_json()
            assert "File error" in data["error"]


class TestErrorScenarios:
    """エラーシナリオのテスト"""

    @patch("web.run_command")
    def test_command_execution_with_stderr(self, mock_run, client):
        """標準エラー出力がある場合"""
        mock_run.return_value = {
            "success": False,
            "output": "",
            "error": "Permission denied",
            "description": "テスト実行",
        }

        response = client.post("/api/fetch/quotes", json={})
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is False
        assert "Permission denied" in data["error"]

    def test_statements_mode_default(self, client):
        """財務諸表取得のデフォルトモード"""
        with patch("web.run_command") as mock_run:
            mock_run.return_value = {"success": True, "output": "", "error": ""}

            # modeを指定しない
            response = client.post("/api/fetch/statements", json={})
            assert response.status_code == 200

            # デフォルトでモード2が使用される
            cmd = mock_run.call_args[0][0]
            assert " 2" in cmd


class TestSpecialCases:
    """特殊ケースのテスト"""

    def test_file_with_special_characters(self, client, tmp_path, monkeypatch):
        """特殊文字を含むファイル名"""
        monkeypatch.chdir(tmp_path)

        # 特殊文字を含むファイル名
        special_file = tmp_path / "result_テスト_2024.xlsx"
        special_file.write_text("test")

        response = client.get("/api/results/list")
        assert response.status_code == 200
        data = response.get_json()
        assert len(data["files"]) == 1

    @patch("subprocess.run")
    def test_run_command_with_unicode_output(self, mock_subprocess, client):
        """Unicode出力の処理"""
        from web import run_command

        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="日本語の出力です", stderr=""
        )

        result = run_command("echo test", "テスト")
        assert result["success"] is True
        assert result["output"] == "日本語の出力です"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

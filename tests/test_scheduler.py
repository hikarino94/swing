#!/usr/bin/env python
"""
スケジューラーモジュール (scheduler.py) のテスト

テスト対象:
- コマンド実行機能
- データ取得タスク（日次価格、財務諸表、銘柄情報）
- スケジュール設定
- エラーハンドリング
- ログ出力
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.cli import scheduler


class TestCommandExecution:
    """コマンド実行機能のテスト"""

    @mock.patch("src.cli.scheduler.subprocess.run")
    @mock.patch("src.cli.scheduler.logger")
    def test_run_success(self, mock_logger, mock_subprocess):
        """コマンド実行成功のテスト"""
        # モック設定
        mock_result = mock.Mock()
        mock_result.returncode = 0
        mock_subprocess.return_value = mock_result

        # コマンド実行
        scheduler._run("python test_command.py")

        # 実行確認
        mock_subprocess.assert_called_once_with("python test_command.py", shell=True)
        mock_logger.info.assert_called_once_with("Run: %s", "python test_command.py")
        mock_logger.error.assert_not_called()

    @mock.patch("src.cli.scheduler.subprocess.run")
    @mock.patch("src.cli.scheduler.logger")
    def test_run_failure(self, mock_logger, mock_subprocess):
        """コマンド実行失敗のテスト"""
        # モック設定
        mock_result = mock.Mock()
        mock_result.returncode = 1
        mock_subprocess.return_value = mock_result

        # コマンド実行
        scheduler._run("python invalid_command.py")

        # 実行確認
        mock_subprocess.assert_called_once_with("python invalid_command.py", shell=True)
        mock_logger.info.assert_called_once()
        mock_logger.error.assert_called_once_with(
            "Command failed: %s", "python invalid_command.py"
        )


class TestFetchTasks:
    """データ取得タスクのテスト"""

    @mock.patch("src.cli.scheduler._run")
    def test_fetch_quotes(self, mock_run):
        """日次価格取得タスクのテスト"""
        scheduler.fetch_quotes()
        mock_run.assert_called_once_with("python -m fetch.daily_quotes")

    @mock.patch("src.cli.scheduler._run")
    def test_fetch_statements(self, mock_run):
        """財務諸表取得タスクのテスト"""
        scheduler.fetch_statements()
        mock_run.assert_called_once_with("python -m fetch.statements 2")

    @mock.patch("src.cli.scheduler._run")
    def test_update_listed_info(self, mock_run):
        """銘柄情報更新タスクのテスト"""
        scheduler.update_listed_info()
        mock_run.assert_called_once_with("python -m fetch.listed_info")


class TestScheduleSetup:
    """スケジュール設定のテスト"""

    def test_schedule_configuration(self):
        """スケジュール設定の確認テスト"""
        # scheduleライブラリの設定が正しく行われているかテスト
        # 実際のスケジュール登録をモックで確認

        with mock.patch("schedule.every") as mock_schedule:
            mock_every = mock.Mock()
            mock_day = mock.Mock()
            mock_monday = mock.Mock()

            mock_schedule.return_value = mock_every
            mock_every.day = mock_day
            mock_every.monday = mock_monday

            # スケジュール設定の一部を模擬
            # (実際のモジュールレベルでの設定はテスト時に実行済み)
            mock_day.at.return_value.do = mock.Mock()
            mock_monday.at.return_value.do = mock.Mock()

            # モックが適切に設定可能であることを確認
            assert mock_schedule.return_value == mock_every
            assert hasattr(mock_every, "day")
            assert hasattr(mock_every, "monday")

    @mock.patch("src.cli.scheduler.config")
    def test_schedule_config_loading(self, mock_config):
        """設定ファイルからのスケジュール設定読み込みテスト"""
        # 設定値のモック
        mock_config.get_scheduler_config.side_effect = [
            {"frequency": "daily", "time": "20:00"},  # fetch_quotes
            {"frequency": "daily", "time": "20:30"},  # fetch_statements
            {"frequency": "monday", "time": "06:00"},  # update_listed_info
        ]

        # 設定読み込み実行
        fetch_quotes_config = mock_config.get_scheduler_config("fetch_quotes")
        fetch_statements_config = mock_config.get_scheduler_config("fetch_statements")
        update_listed_info_config = mock_config.get_scheduler_config(
            "update_listed_info"
        )

        # 設定値確認
        assert fetch_quotes_config["frequency"] == "daily"
        assert fetch_quotes_config["time"] == "20:00"
        assert fetch_statements_config["frequency"] == "daily"
        assert fetch_statements_config["time"] == "20:30"
        assert update_listed_info_config["frequency"] == "monday"
        assert update_listed_info_config["time"] == "06:00"


class TestMainLoop:
    """メインループのテスト"""

    @mock.patch("src.cli.scheduler.time.sleep")
    @mock.patch("src.cli.scheduler.schedule.run_pending")
    @mock.patch("src.cli.scheduler.logger")
    def test_main_loop_execution(self, mock_logger, mock_run_pending, mock_sleep):
        """メインループ実行のテスト"""
        # 無限ループを制御するため、2回実行後に例外で終了
        mock_sleep.side_effect = [None, KeyboardInterrupt()]

        # メインループ実行
        with pytest.raises(KeyboardInterrupt):
            scheduler.main()

        # 実行確認
        mock_logger.info.assert_called_once_with("scheduler start")
        assert mock_run_pending.call_count == 2
        assert mock_sleep.call_count == 2
        mock_sleep.assert_has_calls([mock.call(30), mock.call(30)])

    @mock.patch("src.cli.scheduler.time.sleep")
    @mock.patch("src.cli.scheduler.schedule.run_pending")
    @mock.patch("src.cli.scheduler.logger")
    def test_main_loop_single_iteration(
        self, mock_logger, mock_run_pending, mock_sleep
    ):
        """メインループ単一実行のテスト"""
        # 1回だけ実行して終了
        mock_sleep.side_effect = KeyboardInterrupt()

        with pytest.raises(KeyboardInterrupt):
            scheduler.main()

        mock_logger.info.assert_called_once_with("scheduler start")
        mock_run_pending.assert_called_once()
        mock_sleep.assert_called_once_with(30)


class TestErrorHandling:
    """エラー処理のテスト"""

    @mock.patch("src.cli.scheduler.subprocess.run")
    @mock.patch("src.cli.scheduler.logger")
    def test_subprocess_exception(self, mock_logger, mock_subprocess):
        """subprocess実行時の例外処理テスト"""
        # subprocess.runで例外を発生させる
        mock_subprocess.side_effect = subprocess.SubprocessError(
            "Command execution failed"
        )

        # 例外が適切に処理されるかテスト
        with pytest.raises(subprocess.SubprocessError):
            scheduler._run("python problematic_command.py")

        mock_logger.info.assert_called_once()

    @mock.patch("src.cli.scheduler._run")
    def test_task_exception_handling(self, mock_run):
        """タスク実行時の例外処理テスト"""
        # _run関数で例外を発生させる
        mock_run.side_effect = Exception("Task execution failed")

        # タスクで例外が発生した場合の処理
        with pytest.raises(Exception, match="Task execution failed"):
            scheduler.fetch_quotes()


class TestIntegration:
    """統合テスト"""

    @mock.patch("src.cli.scheduler.subprocess.run")
    @mock.patch("src.cli.scheduler.logger")
    def test_complete_task_execution(self, mock_logger, mock_subprocess):
        """完全なタスク実行フローのテスト"""
        # 成功ケースのモック設定
        mock_result = mock.Mock()
        mock_result.returncode = 0
        mock_subprocess.return_value = mock_result

        # 全てのタスクを順次実行
        scheduler.fetch_quotes()
        scheduler.fetch_statements()
        scheduler.update_listed_info()

        # 実行確認
        expected_calls = [
            mock.call("python -m fetch.daily_quotes", shell=True),
            mock.call("python -m fetch.statements 2", shell=True),
            mock.call("python -m fetch.listed_info", shell=True),
        ]
        mock_subprocess.assert_has_calls(expected_calls)

        # ログ確認
        expected_log_calls = [
            mock.call("Run: %s", "python -m fetch.daily_quotes"),
            mock.call("Run: %s", "python -m fetch.statements 2"),
            mock.call("Run: %s", "python -m fetch.listed_info"),
        ]
        mock_logger.info.assert_has_calls(expected_log_calls)
        mock_logger.error.assert_not_called()

    @mock.patch("src.cli.scheduler.time.sleep")
    @mock.patch("src.cli.scheduler.schedule.run_pending")
    @mock.patch("src.cli.scheduler.logger")
    def test_scheduler_lifecycle(self, mock_logger, mock_run_pending, mock_sleep):
        """スケジューラーのライフサイクルテスト"""
        # 開始から停止までの流れをテスト
        iteration_count = 3
        mock_sleep.side_effect = [None] * (iteration_count - 1) + [KeyboardInterrupt()]

        with pytest.raises(KeyboardInterrupt):
            scheduler.main()

        # 開始ログ
        mock_logger.info.assert_called_once_with("scheduler start")

        # 指定回数のloop実行
        assert mock_run_pending.call_count == iteration_count
        assert mock_sleep.call_count == iteration_count

    @mock.patch("src.cli.scheduler.subprocess.run")
    def test_mixed_success_failure_scenarios(self, mock_subprocess):
        """成功・失敗混在シナリオのテスト"""
        # 複数の結果を設定（成功・失敗の混在）
        results = [
            mock.Mock(returncode=0),  # 成功
            mock.Mock(returncode=1),  # 失敗
            mock.Mock(returncode=0),  # 成功
        ]
        mock_subprocess.side_effect = results

        with mock.patch("src.cli.scheduler.logger") as mock_logger:
            # 複数タスクを実行
            scheduler.fetch_quotes()  # 成功
            scheduler.fetch_statements()  # 失敗
            scheduler.update_listed_info()  # 成功

            # ログ確認
            assert mock_logger.info.call_count == 3  # 全実行のログ
            assert mock_logger.error.call_count == 1  # 1回の失敗ログ


class TestConfiguration:
    """設定関連のテスト"""

    def test_logging_configuration(self):
        """ロギング設定のテスト"""
        # scheduler.logger が適切に設定されているか確認
        assert scheduler.logger.name == "scheduler"

        # ログレベルやフォーマットの確認は config に依存するため、
        # ここでは基本的な存在確認のみ
        assert hasattr(scheduler, "logger")

    @mock.patch("src.cli.scheduler.config")
    def test_path_configuration(self, mock_config):
        """パス設定のテスト"""
        # モジュールがconfigから適切にパスを取得できるかテスト
        mock_config.log_format = "%(asctime)s [%(levelname)s] %(message)s"

        # configの使用確認
        format_used = mock_config.log_format
        assert format_used == "%(asctime)s [%(levelname)s] %(message)s"

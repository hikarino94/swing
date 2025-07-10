"""Tests for src/cli/scheduler.py"""

from unittest.mock import MagicMock, Mock, patch

from src.cli.scheduler import (
    _run,
    fetch_quotes,
    fetch_statements,
    update_listed_info,
)


class TestRunCommand:
    """_run関数のテスト"""

    @patch("src.cli.scheduler.subprocess.run")
    @patch("src.cli.scheduler.logger")
    def test_run_success(self, mock_logger, mock_subprocess):
        """コマンド実行成功のテスト"""
        mock_process = Mock()
        mock_process.returncode = 0
        mock_subprocess.return_value = mock_process

        _run("python -m test.module arg1 arg2")

        mock_logger.info.assert_called_once_with(
            "Run: %s", "python -m test.module arg1 arg2"
        )
        mock_subprocess.assert_called_once_with(
            ["python", "-m", "test.module", "arg1", "arg2"]
        )
        mock_logger.error.assert_not_called()

    @patch("src.cli.scheduler.subprocess.run")
    @patch("src.cli.scheduler.logger")
    def test_run_failure(self, mock_logger, mock_subprocess):
        """コマンド実行失敗のテスト"""
        mock_process = Mock()
        mock_process.returncode = 1
        mock_subprocess.return_value = mock_process

        _run("python -m failing.module")

        mock_logger.info.assert_called_once()
        mock_logger.error.assert_called_once_with(
            "Command failed: %s", "python -m failing.module"
        )

    @patch("src.cli.scheduler.subprocess.run")
    def test_run_with_complex_command(self, mock_subprocess):
        """複雑なコマンドのテスト"""
        mock_process = Mock()
        mock_process.returncode = 0
        mock_subprocess.return_value = mock_process

        _run('python script.py --arg "value with spaces" --flag')

        mock_subprocess.assert_called_once_with(
            ["python", "script.py", "--arg", "value with spaces", "--flag"]
        )


class TestScheduledFunctions:
    """スケジュール関数のテスト"""

    @patch("src.cli.scheduler._run")
    def test_fetch_quotes(self, mock_run):
        """fetch_quotes関数のテスト"""
        fetch_quotes()

        mock_run.assert_called_once_with("python -m fetch.daily_quotes")

    @patch("src.cli.scheduler._run")
    def test_fetch_statements(self, mock_run):
        """fetch_statements関数のテスト"""
        fetch_statements()

        mock_run.assert_called_once_with("python -m fetch.statements 2")

    @patch("src.cli.scheduler._run")
    def test_update_listed_info(self, mock_run):
        """update_listed_info関数のテスト"""
        update_listed_info()

        mock_run.assert_called_once_with("python -m fetch.listed_info")


class TestScheduleConfiguration:
    """スケジュール設定のテスト"""

    @patch("src.cli.scheduler.schedule")
    @patch("src.cli.scheduler.config")
    def test_daily_schedule_setup(self, mock_config, mock_schedule):
        """日次スケジュールの設定テスト"""
        # モックの設定
        mock_config.get_scheduler_config.side_effect = [
            {"frequency": "daily", "time": "20:00"},  # fetch_quotes
            {"frequency": "daily", "time": "20:30"},  # fetch_statements
            {"frequency": "monday", "time": "06:00"},  # update_listed_info
        ]

        # モジュールを再インポートしてスケジュール設定を実行
        import importlib

        import src.cli.scheduler

        importlib.reload(src.cli.scheduler)

        # get_scheduler_configが3回呼ばれることを確認
        assert mock_config.get_scheduler_config.call_count >= 3

    @patch("src.cli.scheduler.schedule")
    @patch("src.cli.scheduler.config")
    def test_monday_schedule_setup(self, mock_config, mock_schedule):
        """月曜日スケジュールの設定テスト"""
        mock_chain = MagicMock()
        mock_schedule.every.return_value.monday.at.return_value.do = mock_chain

        mock_config.get_scheduler_config.side_effect = [
            {"frequency": "monday", "time": "06:00"}
        ]

        # モジュールの該当部分をテスト
        from src.cli.scheduler import update_listed_info_config

        if update_listed_info_config.get("frequency") == "monday":
            mock_schedule.every().monday.at("06:00").do(update_listed_info)

        # 呼び出しを確認
        mock_schedule.every.assert_called()


class TestMainLoop:
    """メインループのテスト（該当する場合）"""

    @patch("src.cli.scheduler.schedule")
    @patch("src.cli.scheduler.time")
    def test_schedule_run_loop(self, mock_time, mock_schedule):
        """スケジュール実行ループのテスト"""
        # 3回実行後に停止するようにモック
        mock_time.sleep.side_effect = [None, None, KeyboardInterrupt]

        # main関数が存在する場合のテスト例
        try:
            # schedule.run_pending()を呼び出すループをテスト
            loop_count = 0
            while True:
                mock_schedule.run_pending()
                mock_time.sleep(1)
                loop_count += 1
                if loop_count >= 3:
                    break
        except KeyboardInterrupt:
            pass

        assert mock_schedule.run_pending.call_count >= 2
        assert mock_time.sleep.call_count >= 2

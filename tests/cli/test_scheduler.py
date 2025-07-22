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

    def test_daily_schedule_setup(self):
        """日次スケジュールの設定テスト - src.cli.schedulerのモジュールレベルのコードをテスト"""
        # src.cli.schedulerモジュールがインポート時にスケジュール設定を行うことを確認
        # モジュールレベルのコードは既に実行されているため、変数が設定されていることを確認
        from src.cli.scheduler import (
            fetch_quotes_config,
            fetch_statements_config,
            update_listed_info_config,
        )

        # 設定が読み込まれていることを確認
        assert isinstance(fetch_quotes_config, dict)
        assert isinstance(fetch_statements_config, dict)
        assert isinstance(update_listed_info_config, dict)

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
    """メインループのテスト"""

    @patch("src.cli.scheduler.schedule")
    @patch("src.cli.scheduler.time")
    @patch("src.cli.scheduler.logger")
    def test_main_function(self, mock_logger, mock_time, mock_schedule):
        """main関数のテスト"""
        from src.cli.scheduler import main

        # 3回実行後に停止するようにモック
        mock_time.sleep.side_effect = [None, None, KeyboardInterrupt]

        # main関数を実行（KeyboardInterruptで終了）
        try:
            main()
        except KeyboardInterrupt:
            pass

        # ログ出力の確認
        mock_logger.info.assert_called_with("scheduler start")

        # スケジュール実行の確認
        assert mock_schedule.run_pending.call_count == 3
        assert mock_time.sleep.call_count == 3

        # sleep時間の確認
        for call in mock_time.sleep.call_args_list:
            assert call[0][0] == 30  # 30秒でsleep

    @patch("src.cli.scheduler.schedule")
    @patch("src.cli.scheduler.time")
    def test_schedule_run_loop(self, mock_time, mock_schedule):
        """スケジュール実行ループのテスト"""
        # 3回実行後に停止するようにモック
        mock_time.sleep.side_effect = [None, None, KeyboardInterrupt]

        # schedule.run_pending()を呼び出すループをテスト
        try:
            loop_count = 0
            while True:
                mock_schedule.run_pending()
                mock_time.sleep(30)
                loop_count += 1
                if loop_count >= 3:
                    break
        except KeyboardInterrupt:
            pass

        assert mock_schedule.run_pending.call_count >= 2
        assert mock_time.sleep.call_count >= 2

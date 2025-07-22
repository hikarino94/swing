"""ui.services.command_serviceのテスト"""

import sys
from unittest.mock import MagicMock, patch

from src.ui.services.command_service import CommandService


class TestRunCommand:
    """run_commandメソッドのテスト"""

    @patch("src.ui.services.command_service.subprocess.run")
    @patch("src.ui.services.command_service.logger")
    def test_run_command_success(self, mock_logger, mock_run):
        """コマンド実行成功のテスト"""
        # モックの設定
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout = "Success output"
        mock_process.stderr = ""
        mock_run.return_value = mock_process

        # テスト実行
        success, stdout, stderr = CommandService.run_command(
            ["python", "--version"], "Pythonバージョン確認"
        )

        # 検証
        assert success is True
        assert stdout == "Success output"
        assert stderr == ""
        mock_logger.info.assert_any_call("Pythonバージョン確認: python --version")
        mock_logger.info.assert_any_call("Pythonバージョン確認 成功")

        # subprocess.runの呼び出し確認
        mock_run.assert_called_once()
        call_args = mock_run.call_args
        assert call_args[0][0] == ["python", "--version"]
        assert "PYTHONPATH" in call_args[1]["env"]

    @patch("src.ui.services.command_service.subprocess.run")
    @patch("src.ui.services.command_service.logger")
    def test_run_command_failure(self, mock_logger, mock_run):
        """コマンド実行失敗のテスト"""
        # モックの設定
        mock_process = MagicMock()
        mock_process.returncode = 1
        mock_process.stdout = "Some output"
        mock_process.stderr = "Error occurred"
        mock_run.return_value = mock_process

        # テスト実行
        success, stdout, stderr = CommandService.run_command(
            ["python", "nonexistent.py"], "存在しないスクリプト実行"
        )

        # 検証
        assert success is False
        assert stdout == "Some output"
        assert stderr == "Error occurred"
        mock_logger.error.assert_called_with(
            "存在しないスクリプト実行 失敗: Error occurred"
        )

    @patch("src.ui.services.command_service.subprocess.run")
    @patch("src.ui.services.command_service.logger")
    def test_run_command_exception(self, mock_logger, mock_run):
        """コマンド実行時の例外処理テスト"""
        # subprocess.runで例外を発生させる
        mock_run.side_effect = Exception("Subprocess failed")

        # テスト実行
        success, stdout, stderr = CommandService.run_command(
            ["invalid", "command"], "不正なコマンド"
        )

        # 検証
        assert success is False
        assert stdout == ""
        assert stderr == "不正なコマンド エラー: Subprocess failed"
        mock_logger.error.assert_called_with("不正なコマンド エラー: Subprocess failed")

    @patch("src.ui.services.command_service.subprocess.run")
    def test_environment_setup(self, mock_run):
        """環境変数の設定テスト"""
        # モックの設定
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout = ""
        mock_process.stderr = ""
        mock_run.return_value = mock_process

        # テスト実行
        CommandService.run_command(["echo", "test"])

        # subprocess.runの呼び出し確認
        call_args = mock_run.call_args
        env = call_args[1]["env"]

        # PYTHONPATHが設定されていることを確認
        assert "PYTHONPATH" in env
        # プロジェクトルートが含まれていることを確認
        assert "swing" in env["PYTHONPATH"]

        # cwdがプロジェクトルートに設定されていることを確認
        cwd = call_args[1]["cwd"]
        assert "swing" in cwd


class TestBuildFetchCommand:
    """build_fetch_commandメソッドのテスト"""

    def test_build_fetch_command_basic(self):
        """基本的なコマンド構築のテスト"""
        command = CommandService.build_fetch_command("daily_quotes.py")

        assert command[0] == sys.executable
        assert command[1] == "fetch/daily_quotes.py"
        assert len(command) == 2

    def test_build_fetch_command_with_dates(self):
        """日付指定ありのコマンド構築テスト"""
        command = CommandService.build_fetch_command(
            "daily_quotes.py", start_date="2024-01-01", end_date="2024-01-31"
        )

        assert command[0] == sys.executable
        assert command[1] == "fetch/daily_quotes.py"
        assert "--start" in command
        assert "2024-01-01" in command
        assert "--end" in command
        assert "2024-01-31" in command

    def test_build_fetch_command_start_date_only(self):
        """開始日のみ指定のテスト"""
        command = CommandService.build_fetch_command(
            "listed_info.py", start_date="2024-01-01"
        )

        assert "--start" in command
        assert "2024-01-01" in command
        assert "--end" not in command

    def test_build_fetch_command_end_date_only(self):
        """終了日のみ指定のテスト"""
        command = CommandService.build_fetch_command(
            "statements.py", end_date="2024-12-31"
        )

        assert "--start" not in command
        assert "--end" in command
        assert "2024-12-31" in command


class TestBuildScreeningCommand:
    """build_screening_commandメソッドのテスト"""

    def test_build_screening_command_basic(self):
        """基本的なスクリーニングコマンドのテスト"""
        command = CommandService.build_screening_command("screen_statements.py")

        assert command[0] == sys.executable
        assert command[1] == "screening/screen_statements.py"
        assert len(command) == 2

    def test_build_screening_command_with_options(self):
        """オプション付きスクリーニングコマンドのテスト"""
        command = CommandService.build_screening_command(
            "screen_technical.py", lookback=30, as_of="2024-01-15", show=True
        )

        assert command[0] == sys.executable
        assert command[1] == "screening/screen_technical.py"
        assert "--lookback" in command
        assert "30" in command
        assert "--as-of" in command
        assert "2024-01-15" in command
        assert "--show" in command

    def test_build_screening_command_underscore_conversion(self):
        """アンダースコアからハイフンへの変換テスト"""
        command = CommandService.build_screening_command(
            "screen_ml.py", top_n=10, min_volume=1000000
        )

        assert "--top-n" in command
        assert "10" in command
        assert "--min-volume" in command
        assert "1000000" in command

    def test_build_screening_command_boolean_false(self):
        """Falseのブール値はコマンドに含まれないテスト"""
        command = CommandService.build_screening_command(
            "screen_test.py", verbose=True, debug=False
        )

        assert "--verbose" in command
        assert "--debug" not in command

    def test_build_screening_command_none_values(self):
        """None値はコマンドに含まれないテスト"""
        command = CommandService.build_screening_command(
            "screen_test.py", param1="value1", param2=None, param3="value3"
        )

        assert "--param1" in command
        assert "value1" in command
        assert "--param2" not in command
        assert "--param3" in command
        assert "value3" in command


class TestBuildBacktestCommand:
    """build_backtest_commandメソッドのテスト"""

    def test_build_backtest_command_basic(self):
        """基本的なバックテストコマンドのテスト"""
        command = CommandService.build_backtest_command("backtest_statements.py")

        assert command[0] == sys.executable
        assert command[1] == "backtest/backtest_statements.py"
        assert len(command) == 2

    def test_build_backtest_command_with_options(self):
        """オプション付きバックテストコマンドのテスト"""
        command = CommandService.build_backtest_command(
            "backtest_technical.py",
            hold_days=10,
            capital=1000000,
            start="2024-01-01",
            end="2024-12-31",
        )

        assert "--hold-days" in command
        assert "10" in command
        assert "--capital" in command
        assert "1000000" in command
        assert "--start" in command
        assert "2024-01-01" in command
        assert "--end" in command
        assert "2024-12-31" in command

    def test_build_backtest_command_numeric_conversion(self):
        """数値型の文字列変換テスト"""
        command = CommandService.build_backtest_command(
            "backtest_ml.py", top=50, capital=5000000.5, stop_loss=0.05
        )

        # 数値が文字列に変換されていることを確認
        assert "--top" in command
        assert "50" in command
        assert "--capital" in command
        assert "5000000.5" in command
        assert "--stop-loss" in command
        assert "0.05" in command

    def test_build_backtest_command_none_values(self):
        """None値の除外テスト"""
        command = CommandService.build_backtest_command(
            "backtest_test.py",
            param1="value1",
            param2=None,
            param3=0,  # 0は有効な値
            param4="",  # 空文字列も有効
        )

        assert "--param1" in command
        assert "value1" in command
        assert "--param2" not in command
        assert "--param3" in command
        assert "0" in command
        assert "--param4" in command
        # 空文字列は引数として含まれる
        param4_index = command.index("--param4")
        assert command[param4_index + 1] == ""


class TestCommandServiceIntegration:
    """CommandServiceの統合テスト"""

    @patch("src.ui.services.command_service.subprocess.run")
    def test_fetch_command_execution(self, mock_run):
        """フェッチコマンドの構築と実行の統合テスト"""
        # モックの設定
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout = "Fetched 100 records"
        mock_process.stderr = ""
        mock_run.return_value = mock_process

        # コマンド構築
        command = CommandService.build_fetch_command(
            "daily_quotes.py", start_date="2024-01-01", end_date="2024-01-31"
        )

        # コマンド実行
        success, stdout, stderr = CommandService.run_command(command, "日次株価取得")

        # 検証
        assert success is True
        assert stdout == "Fetched 100 records"

        # 実行されたコマンドの確認
        executed_command = mock_run.call_args[0][0]
        assert executed_command[0] == sys.executable
        assert "daily_quotes.py" in executed_command[1]
        assert "--start" in executed_command
        assert "--end" in executed_command

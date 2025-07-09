"""コマンド実行サービス"""

import os
import subprocess
import sys
from pathlib import Path

from src.utils.logging_config import get_logger

logger = get_logger("services.command")


class CommandService:
    """外部コマンドの実行を管理するサービス"""

    @staticmethod
    def run_command(
        command: list[str], description: str = "コマンド実行中"
    ) -> tuple[bool, str, str]:
        """コマンドを実行して結果を返す

        Args:
            command: 実行するコマンド（リスト形式）
            description: ログ用の説明

        Returns:
            (成功フラグ, 標準出力, エラー出力)のタプル
        """
        logger.info(f"{description}: {' '.join(command)}")

        try:
            # プロジェクトルートを取得
            project_root = Path(__file__).resolve().parent.parent.parent.parent

            # 環境変数の設定
            env = dict(os.environ)
            env["PYTHONPATH"] = (
                str(project_root) + os.pathsep + env.get("PYTHONPATH", "")
            )

            # コマンド実行
            process = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                cwd=str(project_root),
                env=env,
            )

            if process.returncode == 0:
                logger.info(f"{description} 成功")
                return True, process.stdout, ""
            else:
                logger.error(f"{description} 失敗: {process.stderr}")
                return False, process.stdout, process.stderr

        except Exception as e:
            error_msg = f"{description} エラー: {str(e)}"
            logger.error(error_msg)
            return False, "", error_msg

    @staticmethod
    def build_fetch_command(
        script_name: str, start_date: str | None = None, end_date: str | None = None
    ) -> list[str]:
        """データ取得コマンドを構築する

        Args:
            script_name: スクリプト名（例: "daily_quotes.py"）
            start_date: 開始日
            end_date: 終了日

        Returns:
            コマンドのリスト
        """
        command = [sys.executable, f"fetch/{script_name}"]

        if start_date:
            command.extend(["--start", start_date])
        if end_date:
            command.extend(["--end", end_date])

        return command

    @staticmethod
    def build_screening_command(script_name: str, **kwargs) -> list[str]:
        """スクリーニングコマンドを構築する

        Args:
            script_name: スクリプト名（例: "screen_statements.py"）
            **kwargs: その他のオプション

        Returns:
            コマンドのリスト
        """
        command = [sys.executable, f"screening/{script_name}"]

        # オプションを追加
        for key, value in kwargs.items():
            if value is not None:
                if isinstance(value, bool):
                    if value:
                        command.append(f"--{key.replace('_', '-')}")
                else:
                    command.extend([f"--{key.replace('_', '-')}", str(value)])

        return command

    @staticmethod
    def build_backtest_command(script_name: str, **kwargs) -> list[str]:
        """バックテストコマンドを構築する

        Args:
            script_name: スクリプト名（例: "backtest_statements.py"）
            **kwargs: その他のオプション

        Returns:
            コマンドのリスト
        """
        command = [sys.executable, f"backtest/{script_name}"]

        # オプションを追加
        for key, value in kwargs.items():
            if value is not None:
                command.extend([f"--{key.replace('_', '-')}", str(value)])

        return command

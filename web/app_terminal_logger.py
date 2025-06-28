"""ターミナル専用のログハンドラー"""
import logging
import sys
from datetime import datetime


# 直接printを使用するシンプルな実装
def terminal_print(message):
    """ターミナルに直接出力"""
    print(message, flush=True)


class TerminalLogger:
    """ターミナルへの詳細ログ出力を管理するクラス"""

    @staticmethod
    def log_command_start(task_id: str, cmd: str):
        """コマンド実行開始時のログ"""
        messages = [
            f"\n{'='*60}",
            "🚀 コマンド実行開始",
            f"   タスクID: {task_id}",
            f"   コマンド: {cmd}",
            f"   開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"{'='*60}\n",
        ]
        for msg in messages:
            terminal_print(msg)

    @staticmethod
    def log_process_start(pid: int):
        """プロセス開始時のログ"""
        terminal_print(f"📊 プロセス開始 (PID: {pid})\n")

    @staticmethod
    def log_output_line(task_id: str, line: str):
        """出力行のログ"""
        terminal_print(f"[{task_id[:8]}] {line}")

    @staticmethod
    def log_command_end(success: bool, return_code: int, line_count: int):
        """コマンド実行終了時のログ"""
        messages = [
            f"\n{'='*60}",
            f"{'✅ コマンド実行完了' if success else '❌ コマンド実行失敗'}",
            f"   終了コード: {return_code}",
            f"   処理行数: {line_count}",
            f"   終了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"{'='*60}\n",
        ]
        for msg in messages:
            terminal_print(msg)

    @staticmethod
    def log_error(error: Exception):
        """エラー時のログ"""
        terminal_print(f"\n💥 エラー発生: {error}\n")


# 専用のターミナルロガーも作成
terminal_logger = logging.getLogger("terminal")
terminal_logger.setLevel(logging.DEBUG)

# コンソールハンドラーを追加（ターミナル出力用）
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s [TERMINAL] %(message)s")
console_handler.setFormatter(formatter)
terminal_logger.addHandler(console_handler)
terminal_logger.propagate = False  # 親ロガーへの伝播を防ぐ

import logging
import shlex
import subprocess
import sys
import time
from pathlib import Path

import schedule

# プロジェクトルートをPYTHONPATHに追加（スクリプト実行用）
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.config import config

logging.basicConfig(format=config.log_format, level=logging.INFO)
logger = logging.getLogger("scheduler")


def _run(cmd: str) -> None:
    """Run *cmd* and log if it fails."""
    logger.info("Run: %s", cmd)
    # shellを使わずに実行（セキュリティ向上）
    proc = subprocess.run(shlex.split(cmd))
    if proc.returncode:
        logger.error("Command failed: %s", cmd)


def fetch_quotes() -> None:
    _run("python -m fetch.daily_quotes")


def fetch_statements() -> None:
    _run("python -m fetch.statements 2")


def update_listed_info() -> None:
    _run("python -m fetch.listed_info")


def cleanup_database() -> None:
    """データベースクリーンアップを実行"""
    # 設定を確認
    cleanup_config = config.get("data_cleanup", {})
    if not cleanup_config.get("enabled", False):
        logger.info("データクリーンアップは無効に設定されています")
        return

    # dry_run 設定を確認
    dry_run_flag = "--execute" if not cleanup_config.get("dry_run", True) else ""
    _run(f"python -m src.cli.cleanup_database {dry_run_flag} --force")


# スケジュール設定を設定ファイルから読み込み
fetch_quotes_config = config.get_scheduler_config("fetch_quotes")
fetch_statements_config = config.get_scheduler_config("fetch_statements")
update_listed_info_config = config.get_scheduler_config("update_listed_info")
cleanup_config = config.get("data_cleanup", {})

if fetch_quotes_config.get("frequency") == "daily":
    schedule.every().day.at(fetch_quotes_config.get("time", "20:00")).do(fetch_quotes)

if fetch_statements_config.get("frequency") == "daily":
    schedule.every().day.at(fetch_statements_config.get("time", "20:30")).do(
        fetch_statements
    )

if update_listed_info_config.get("frequency") == "monday":
    schedule.every().monday.at(update_listed_info_config.get("time", "06:00")).do(
        update_listed_info
    )

# データクリーンアップスケジュール
if cleanup_config.get("enabled", False):
    cleanup_schedule = cleanup_config.get("schedule", {})
    if cleanup_schedule.get("frequency") == "daily":
        schedule.every().day.at(cleanup_schedule.get("time", "03:00")).do(
            cleanup_database
        )
        logger.info(
            "データクリーンアップを%sにスケジュールしました",
            cleanup_schedule.get("time", "03:00"),
        )


def main() -> None:
    logger.info("scheduler start")
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()

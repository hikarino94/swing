import logging
import subprocess
import time

import schedule

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO
)
logger = logging.getLogger("scheduler")


def _run(cmd: str) -> None:
    """Run *cmd* and log if it fails."""
    logger.info("Run: %s", cmd)
    proc = subprocess.run(cmd, shell=True)
    if proc.returncode:
        logger.error("Command failed: %s", cmd)


def fetch_quotes() -> None:
    _run("python fetch/daily_quotes.py")


def fetch_statements() -> None:
    _run("python fetch/statements.py 2")


def update_listed_info() -> None:
    _run("python fetch/listed_info.py")


schedule.every().day.at("20:00").do(fetch_quotes)
schedule.every().day.at("20:30").do(fetch_statements)
schedule.every().monday.at("06:00").do(update_listed_info)


def main() -> None:
    logger.info("scheduler start")
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()

"""
統一的なログ設定

プロジェクト全体で統一的なログ設定を提供します。
"""

import json
import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


def get_logger(name: str | None = None) -> logging.Logger:
    """
    統一的な設定が適用されたロガーを取得します。

    Args:
        name: ロガー名（Noneの場合はルートロガー）

    Returns:
        設定済みのロガー
    """
    logger = logging.getLogger(name)

    # 既にハンドラが設定されている場合はそのまま返す
    if logger.handlers:
        return logger

    # プロジェクトルートを取得
    project_root = Path(__file__).resolve().parent.parent.parent
    log_dir = project_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # ログレベルの設定
    logger.setLevel(logging.INFO)

    # フォーマットの設定（構造化ログ対応）
    formatter = StructuredFormatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # JSON形式のフォーマッター（ファイル出力用）
    json_formatter = JSONFormatter()

    # コンソールハンドラの設定
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # ファイルハンドラの設定（ローテーション付き）
    if name:
        log_file = log_dir / f"{name}.log"
        # 日次ローテーション、30日分保持
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=log_file,
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(json_formatter)
        logger.addHandler(file_handler)

        # サイズベースのローテーション（100MB）も追加
        size_handler = logging.handlers.RotatingFileHandler(
            filename=log_dir / f"{name}_latest.log",
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=5,
            encoding="utf-8",
        )
        size_handler.setLevel(logging.INFO)
        size_handler.setFormatter(json_formatter)
        logger.addHandler(size_handler)

    return logger


def setup_module_logger(module_name: str) -> logging.Logger:
    """
    モジュール用のロガーをセットアップします。

    Args:
        module_name: モジュール名（__name__を渡すことを想定）

    Returns:
        設定済みのロガー
    """
    # モジュール名から適切なログ名を生成
    # 例: "fetch.daily_quotes" -> "daily_quotes"
    log_name = module_name.split(".")[-1]
    return get_logger(log_name)


class StructuredFormatter(logging.Formatter):
    """
    構造化ログ用のフォーマッター
    追加のコンテキスト情報をログに含める
    """

    def format(self, record: logging.LogRecord) -> str:
        # 基本的なフォーマット
        message = super().format(record)

        # 追加のコンテキスト情報があれば付加
        extras = {}
        for key, value in record.__dict__.items():
            if key not in [
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "thread",
                "threadName",
                "getMessage",
                "asctime",
                "message",
            ]:
                extras[key] = value

        if extras:
            message += f" | context: {extras}"

        return message


class JSONFormatter(logging.Formatter):
    """
    JSON形式でログを出力するフォーマッター
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # エラーの場合はスタックトレースを含める
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # 追加のコンテキスト情報
        for key, value in record.__dict__.items():
            if key not in [
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "thread",
                "threadName",
                "getMessage",
                "exc_info",
                "exc_text",
            ]:
                log_data[key] = value

        return json.dumps(log_data, ensure_ascii=False, default=str)


def configure_logging(log_level: str = "INFO", json_output: bool = False) -> None:
    """
    アプリケーション全体のログ設定を行う

    Args:
        log_level: ログレベル（DEBUG, INFO, WARNING, ERROR, CRITICAL）
        json_output: JSON形式で出力するかどうか
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # 既存のハンドラをクリア
    root_logger.handlers.clear()

    # コンソールハンドラ
    console_handler = logging.StreamHandler(sys.stdout)
    if json_output:
        console_handler.setFormatter(JSONFormatter())
    else:
        console_handler.setFormatter(
            StructuredFormatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    root_logger.addHandler(console_handler)

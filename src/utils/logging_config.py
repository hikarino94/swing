"""ロギング設定モジュール"""
import logging
import logging.handlers
from pathlib import Path
from typing import Optional


def setup_logging(
    name: str, level: str = "INFO", log_file: Optional[Path] = None, format_string: Optional[str] = None
) -> logging.Logger:
    """統一されたロギング設定

    Args:
        name: ロガー名
        level: ログレベル (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: ログファイルパス（Noneの場合はコンソールのみ）
        format_string: ログフォーマット文字列

    Returns:
        設定済みのロガー
    """
    logger = logging.getLogger(name)

    # 既にハンドラーが設定されている場合はそのまま返す
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper()))

    # フォーマッターの設定
    if format_string is None:
        format_string = "%(asctime)s [%(name)s] [%(levelname)s] %(message)s"

    formatter = logging.Formatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")

    # コンソールハンドラー
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, level.upper()))
    logger.addHandler(console_handler)

    # ファイルハンドラー（オプション）
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"  # 10MB
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, level.upper()))
        logger.addHandler(file_handler)

    # 親ロガーへの伝播を防ぐ
    logger.propagate = False

    return logger


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """簡易ロガー取得関数

    Args:
        name: ロガー名
        level: ログレベル

    Returns:
        設定済みのロガー
    """
    return setup_logging(name, level)


def configure_root_logger(level: str = "INFO", log_file: Optional[Path] = None):
    """ルートロガーの設定

    Args:
        level: ログレベル
        log_file: ログファイルパス
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(name)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logging.getLogger().addHandler(file_handler)

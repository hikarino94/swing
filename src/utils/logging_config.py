"""
統一的なログ設定

プロジェクト全体で統一的なログ設定を提供します。
"""

import logging
import logging.handlers
import sys
from pathlib import Path


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

    # フォーマットの設定
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

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
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # サイズベースのローテーション（100MB）も追加
        size_handler = logging.handlers.RotatingFileHandler(
            filename=log_dir / f"{name}_latest.log",
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=5,
            encoding="utf-8",
        )
        size_handler.setLevel(logging.INFO)
        size_handler.setFormatter(formatter)
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

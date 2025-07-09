"""
共通例外クラスとエラーハンドリングユーティリティ
"""

import logging
import traceback
from functools import wraps
from typing import Any


class SwingException(Exception):
    """swingアプリケーションの基底例外クラス"""

    def __init__(
        self,
        message: str,
        code: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.details = details or {}


class DatabaseError(SwingException):
    """データベース関連のエラー"""

    pass


class APIError(SwingException):
    """API通信関連のエラー"""

    pass


class ValidationError(SwingException):
    """データ検証エラー"""

    pass


class ConfigurationError(SwingException):
    """設定関連のエラー"""

    pass


class DataProcessingError(SwingException):
    """データ処理中のエラー"""

    pass


class AuthenticationError(SwingException):
    """認証関連のエラー"""

    pass


class PortfolioError(SwingException):
    """ポートフォリオ管理関連のエラー"""

    pass


def handle_exceptions(
    logger: logging.Logger, default_return: Any = None, reraise: bool = False
):
    """
    例外をキャッチして標準化されたログを出力するデコレータ

    Args:
        logger: ログ出力に使用するロガー
        default_return: 例外発生時のデフォルト戻り値
        reraise: 例外を再スローするかどうか
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except SwingException as e:
                # アプリケーション固有の例外
                logger.error(
                    f"{func.__name__}でエラーが発生しました: {e}",
                    extra={
                        "function": func.__name__,
                        "error_code": e.code,
                        "details": e.details,
                        "traceback": traceback.format_exc(),
                    },
                )
                if reraise:
                    raise
                return default_return
            except Exception as e:
                # 予期しない例外
                logger.error(
                    f"{func.__name__}で予期しないエラーが発生しました: {e}",
                    extra={
                        "function": func.__name__,
                        "error_type": type(e).__name__,
                        "traceback": traceback.format_exc(),
                    },
                )
                if reraise:
                    raise
                return default_return

        return wrapper

    return decorator


def safe_execute(
    func, *args, logger: logging.Logger, default_return: Any = None, **kwargs
):
    """
    関数を安全に実行し、エラーをログに記録

    Args:
        func: 実行する関数
        *args: 関数の位置引数
        logger: ログ出力に使用するロガー
        default_return: エラー時のデフォルト戻り値
        **kwargs: 関数のキーワード引数

    Returns:
        関数の戻り値またはdefault_return
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(
            f"{func.__name__}の実行中にエラーが発生しました: {e}",
            extra={
                "function": func.__name__,
                "error_type": type(e).__name__,
                "traceback": traceback.format_exc(),
            },
        )
        return default_return


class ErrorContext:
    """
    withステートメントで使用するエラーコンテキストマネージャー

    Example:
        with ErrorContext(logger, "データ処理"):
            # エラーが発生する可能性のある処理
            process_data()
    """

    def __init__(self, logger: logging.Logger, operation: str, reraise: bool = True):
        self.logger = logger
        self.operation = operation
        self.reraise = reraise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.logger.error(
                f"{self.operation}中にエラーが発生しました: {exc_val}",
                extra={
                    "operation": self.operation,
                    "error_type": exc_type.__name__,
                    "traceback": traceback.format_exc(),
                },
            )
            return not self.reraise  # reraiseがFalseの場合は例外を抑制

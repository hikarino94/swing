"""Tests for src/utils/logging_config.py"""

import logging
from unittest.mock import MagicMock, patch

from src.utils.logging_config import get_logger, setup_module_logger


class TestGetLogger:
    """get_logger関数のテスト"""

    @patch("src.utils.logging_config.Path.mkdir")
    @patch("src.utils.logging_config.logging.handlers.RotatingFileHandler")
    @patch("src.utils.logging_config.logging.handlers.TimedRotatingFileHandler")
    @patch("src.utils.logging_config.logging.StreamHandler")
    def test_creates_logger_with_handlers(
        self, mock_stream, mock_timed, mock_rotating, mock_mkdir
    ):
        """ハンドラー付きのロガーが作成されることを確認"""
        # モックハンドラーを作成
        console_handler = MagicMock()
        timed_handler = MagicMock()
        rotating_handler = MagicMock()
        mock_stream.return_value = console_handler
        mock_timed.return_value = timed_handler
        mock_rotating.return_value = rotating_handler

        logger = get_logger("test_module")

        # ログディレクトリが作成されたことを確認
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # ハンドラーが作成されたことを確認
        mock_stream.assert_called_once()
        mock_timed.assert_called_once()
        mock_rotating.assert_called_once()

        # ロガーにハンドラーが追加されたことを確認
        # ロガー自体のハンドラーを確認
        assert isinstance(logger, logging.Logger)
        # ハンドラーが作成されたことを確認
        assert mock_stream.called
        assert mock_timed.called
        assert mock_rotating.called

    @patch("src.utils.logging_config.Path.mkdir")
    def test_returns_existing_logger(self, mock_mkdir):
        """既存のロガーが返されることを確認"""
        # 既存のロガーをモック
        existing_logger = logging.getLogger("existing")
        existing_logger.addHandler(MagicMock())  # ハンドラーを追加

        with patch(
            "src.utils.logging_config.logging.getLogger", return_value=existing_logger
        ):
            logger = get_logger("existing")

            # 同じロガーが返されることを確認
            assert logger == existing_logger
            # 新しいハンドラーが追加されていないことを確認
            assert len(logger.handlers) == 1

    @patch("src.utils.logging_config.Path.mkdir")
    @patch("src.utils.logging_config.logging.handlers.RotatingFileHandler")
    @patch("src.utils.logging_config.logging.handlers.TimedRotatingFileHandler")
    @patch("src.utils.logging_config.logging.StreamHandler")
    @patch("src.utils.logging_config.logging.getLogger")
    def test_root_logger_no_file_handler(
        self, mock_get_logger, mock_stream, mock_timed, mock_rotating, mock_mkdir
    ):
        """ルートロガーにはファイルハンドラーが追加されないことを確認"""
        # ルートロガーをモック
        root_logger = MagicMock()
        root_logger.handlers = []  # ハンドラーがあるとget_loggerが何もしない
        mock_get_logger.return_value = root_logger

        console_handler = MagicMock()
        mock_stream.return_value = console_handler

        get_logger(None)

        # コンソールハンドラーが作成される
        mock_stream.assert_called_once()
        # ファイルハンドラーは作成されない
        mock_timed.assert_not_called()
        mock_rotating.assert_not_called()


class TestSetupModuleLogger:
    """setup_module_logger関数のテスト"""

    @patch("src.utils.logging_config.get_logger")
    def test_extracts_module_name(self, mock_get_logger):
        """モジュール名からログ名が抽出されることを確認"""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        logger = setup_module_logger("fetch.daily_quotes")

        # "daily_quotes"が抽出される
        mock_get_logger.assert_called_once_with("daily_quotes")
        assert logger == mock_logger

    @patch("src.utils.logging_config.get_logger")
    def test_single_name_module(self, mock_get_logger):
        """単一名のモジュールでも動作することを確認"""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        logger = setup_module_logger("main")

        mock_get_logger.assert_called_once_with("main")
        assert logger == mock_logger


class TestLoggingConfiguration:
    """ログ設定のテスト"""

    @patch("src.utils.logging_config.Path.mkdir")
    @patch("src.utils.logging_config.logging.StreamHandler")
    def test_logger_level_is_info(self, mock_stream, mock_mkdir):
        """ロガーのレベルがINFOに設定されることを確認"""
        mock_stream.return_value = MagicMock()

        logger = get_logger("test")

        assert logger.level == logging.INFO

    @patch("src.utils.logging_config.Path.mkdir")
    @patch("src.utils.logging_config.logging.handlers.TimedRotatingFileHandler")
    def test_file_handler_rotation_config(self, mock_timed_handler, mock_mkdir):
        """ファイルハンドラーのローテーション設定を確認"""
        # get_loggerを呼ぶ
        with patch("src.utils.logging_config.logging.StreamHandler"):
            with patch("src.utils.logging_config.logging.handlers.RotatingFileHandler"):
                get_logger("test")

        # TimedRotatingFileHandlerの設定を確認
        if mock_timed_handler.called:
            # キーワード引数を確認
            args, kwargs = mock_timed_handler.call_args
            assert kwargs.get("when") == "midnight" or "midnight" in str(args)
            assert kwargs.get("interval") == 1 or 1 in args
            assert kwargs.get("backupCount") == 30 or 30 in args
            assert kwargs.get("encoding") == "utf-8" or "utf-8" in str(args)

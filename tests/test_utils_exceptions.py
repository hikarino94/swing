"""Test suite for src/utils/exceptions.py"""

import logging
import sys
from pathlib import Path

import pytest

# モジュールをインポート
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.exceptions import (
    APIError,
    AuthenticationError,
    ConfigurationError,
    DatabaseError,
    DataProcessingError,
    ErrorContext,
    PortfolioError,
    SwingException,
    ValidationError,
    handle_exceptions,
    safe_execute,
)


class TestSwingException:
    """Test SwingException base class"""

    def test_basic_exception(self):
        """基本的な例外の作成"""
        exc = SwingException("エラーメッセージ")
        assert str(exc) == "エラーメッセージ"
        assert exc.code is None
        assert exc.details == {}

    def test_exception_with_code(self):
        """エラーコード付き例外"""
        exc = SwingException("エラー", code="ERR001")
        assert exc.code == "ERR001"

    def test_exception_with_details(self):
        """詳細情報付き例外"""
        details = {"field": "username", "value": "invalid"}
        exc = SwingException("検証エラー", details=details)
        assert exc.details == details


class TestExceptionSubclasses:
    """Test all exception subclasses"""

    def test_database_error(self):
        """DatabaseError のテスト"""
        exc = DatabaseError("DB接続エラー", code="DB001")
        assert isinstance(exc, SwingException)
        assert str(exc) == "DB接続エラー"

    def test_api_error(self):
        """APIError のテスト"""
        exc = APIError("API通信エラー", details={"status_code": 500})
        assert isinstance(exc, SwingException)
        assert exc.details["status_code"] == 500

    def test_validation_error(self):
        """ValidationError のテスト"""
        exc = ValidationError("入力値が不正です")
        assert isinstance(exc, SwingException)

    def test_configuration_error(self):
        """ConfigurationError のテスト"""
        exc = ConfigurationError("設定ファイルが見つかりません")
        assert isinstance(exc, SwingException)

    def test_data_processing_error(self):
        """DataProcessingError のテスト"""
        exc = DataProcessingError("データ処理に失敗しました")
        assert isinstance(exc, SwingException)

    def test_authentication_error(self):
        """AuthenticationError のテスト"""
        exc = AuthenticationError("認証に失敗しました")
        assert isinstance(exc, SwingException)

    def test_portfolio_error(self):
        """PortfolioError のテスト"""
        exc = PortfolioError("ポートフォリオの計算エラー")
        assert isinstance(exc, SwingException)


class TestHandleExceptionsDecorator:
    """Test handle_exceptions decorator"""

    def test_successful_execution(self, caplog):
        """正常実行時"""
        logger = logging.getLogger("test")

        @handle_exceptions(logger)
        def success_func(x, y):
            return x + y

        result = success_func(1, 2)
        assert result == 3
        assert len(caplog.records) == 0

    def test_swing_exception_handling(self, caplog):
        """SwingException の処理"""
        logger = logging.getLogger("test")

        @handle_exceptions(logger, default_return=-1)
        def failing_func():
            raise ValidationError(
                "検証エラー", code="VAL001", details={"field": "test"}
            )

        result = failing_func()
        assert result == -1
        assert len(caplog.records) == 1
        assert "failing_funcでエラーが発生しました" in caplog.records[0].message
        assert caplog.records[0].error_code == "VAL001"
        assert caplog.records[0].details == {"field": "test"}

    def test_generic_exception_handling(self, caplog):
        """一般的な例外の処理"""
        logger = logging.getLogger("test")

        @handle_exceptions(logger, default_return=None)
        def failing_func():
            raise ValueError("予期しないエラー")

        result = failing_func()
        assert result is None
        assert len(caplog.records) == 1
        assert (
            "failing_funcで予期しないエラーが発生しました" in caplog.records[0].message
        )
        assert caplog.records[0].error_type == "ValueError"

    def test_reraise_option(self, caplog):
        """reraise オプションのテスト"""
        logger = logging.getLogger("test")

        @handle_exceptions(logger, reraise=True)
        def failing_func():
            raise DatabaseError("DB エラー")

        with pytest.raises(DatabaseError):
            failing_func()

        assert len(caplog.records) == 1

    def test_with_arguments(self, caplog):
        """引数付き関数のテスト"""
        logger = logging.getLogger("test")

        @handle_exceptions(logger, default_return=0)
        def divide(x, y):
            if y == 0:
                raise DataProcessingError("ゼロ除算")
            return x / y

        # 正常ケース
        assert divide(10, 2) == 5

        # エラーケース
        result = divide(10, 0)
        assert result == 0
        assert len(caplog.records) == 1


class TestSafeExecute:
    """Test safe_execute function"""

    def test_successful_execution(self, caplog):
        """正常実行時"""
        logger = logging.getLogger("test")

        def add(x, y):
            return x + y

        result = safe_execute(add, 1, 2, logger=logger)
        assert result == 3
        assert len(caplog.records) == 0

    def test_error_handling(self, caplog):
        """エラー処理"""
        logger = logging.getLogger("test")

        def failing_func():
            raise ValueError("エラー")

        result = safe_execute(failing_func, logger=logger, default_return=-1)
        assert result == -1
        assert len(caplog.records) == 1
        assert "failing_funcの実行中にエラーが発生しました" in caplog.records[0].message

    def test_with_kwargs(self, caplog):
        """キーワード引数のテスト"""
        logger = logging.getLogger("test")

        def multiply(x, y, factor=1):
            return x * y * factor

        result = safe_execute(multiply, 2, 3, logger=logger, factor=10)
        assert result == 60

    def test_none_default_return(self, caplog):
        """デフォルト戻り値がNoneの場合"""
        logger = logging.getLogger("test")

        def failing_func():
            raise Exception("エラー")

        result = safe_execute(failing_func, logger=logger)
        assert result is None


class TestErrorContext:
    """Test ErrorContext context manager"""

    def test_successful_context(self, caplog):
        """正常なコンテキスト実行"""
        logger = logging.getLogger("test")

        with ErrorContext(logger, "データ処理"):
            result = 1 + 1

        assert result == 2
        assert len(caplog.records) == 0

    def test_error_in_context(self, caplog):
        """コンテキスト内でのエラー"""
        logger = logging.getLogger("test")

        with pytest.raises(ValueError):
            with ErrorContext(logger, "データ処理"):
                raise ValueError("処理エラー")

        assert len(caplog.records) == 1
        assert "データ処理中にエラーが発生しました" in caplog.records[0].message
        assert caplog.records[0].operation == "データ処理"
        assert caplog.records[0].error_type == "ValueError"

    def test_no_reraise(self, caplog):
        """例外を再スローしない場合"""
        logger = logging.getLogger("test")

        with ErrorContext(logger, "バッチ処理", reraise=False):
            raise RuntimeError("バッチエラー")

        # 例外が抑制されていることを確認
        assert len(caplog.records) == 1
        assert "バッチ処理中にエラーが発生しました" in caplog.records[0].message

    def test_nested_contexts(self, caplog):
        """ネストしたコンテキスト"""
        logger = logging.getLogger("test")

        try:
            with ErrorContext(logger, "外側の処理"):
                with ErrorContext(logger, "内側の処理"):
                    raise APIError("API エラー")
        except APIError:
            pass

        # ネストしたコンテキストでは、外側のコンテキストでもログが記録される可能性がある
        assert len(caplog.records) >= 1
        # 最初のログが内側の処理に関するものであることを確認
        assert "内側の処理中にエラーが発生しました" in caplog.records[0].message

    def test_context_with_custom_exception(self, caplog):
        """カスタム例外でのコンテキスト"""
        logger = logging.getLogger("test")

        with pytest.raises(ConfigurationError):
            with ErrorContext(logger, "設定読み込み"):
                raise ConfigurationError("設定エラー", code="CFG001")

        assert len(caplog.records) == 1
        assert caplog.records[0].error_type == "ConfigurationError"


class TestIntegration:
    """Integration tests for exception handling"""

    def test_decorated_function_with_context(self, caplog):
        """デコレータとコンテキストマネージャーの組み合わせ"""
        logger = logging.getLogger("test")

        @handle_exceptions(logger, default_return=None)
        def process_data():
            with ErrorContext(logger, "データ変換", reraise=True):
                raise DataProcessingError("変換エラー")

        result = process_data()
        assert result is None
        # ErrorContextとhandle_exceptionsの両方でログが記録される
        assert len(caplog.records) == 2

    def test_safe_execute_with_swing_exception(self, caplog):
        """safe_execute でSwingExceptionを処理"""
        logger = logging.getLogger("test")

        def api_call():
            raise APIError("接続エラー", code="API001", details={"endpoint": "/test"})

        result = safe_execute(api_call, logger=logger, default_return={"error": True})
        assert result == {"error": True}
        assert "api_callの実行中にエラーが発生しました" in caplog.records[0].message

    def test_multiple_error_handlers(self, caplog):
        """複数のエラーハンドラーの組み合わせ"""
        logger = logging.getLogger("test")

        @handle_exceptions(logger, default_return="decorator_default")
        def outer_func():
            def inner_func():
                raise PortfolioError("計算エラー")

            return safe_execute(
                inner_func, logger=logger, default_return="safe_default"
            )

        result = outer_func()
        # inner_funcのsafe_executeが先に処理するので、そのデフォルト値が返る
        assert result == "safe_default"
        assert len(caplog.records) == 1

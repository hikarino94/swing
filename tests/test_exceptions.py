"""カスタム例外のテスト"""
import pytest

from src.utils.exceptions import APIError, ConfigError, DatabaseError, DataError


class TestCustomExceptions:
    """カスタム例外のテストクラス"""

    def test_api_error_creation(self):
        """APIErrorの作成テスト"""
        message = "API request failed"
        error = APIError(message)

        assert str(error) == message
        assert isinstance(error, Exception)
        assert error.__class__.__name__ == "APIError"

    def test_database_error_creation(self):
        """DatabaseErrorの作成テスト"""
        message = "Database connection failed"
        error = DatabaseError(message)

        assert str(error) == message
        assert isinstance(error, Exception)
        assert error.__class__.__name__ == "DatabaseError"

    def test_data_error_creation(self):
        """DataErrorの作成テスト"""
        message = "Invalid data format"
        error = DataError(message)

        assert str(error) == message
        assert isinstance(error, Exception)
        assert error.__class__.__name__ == "DataError"

    def test_config_error_creation(self):
        """ConfigErrorの作成テスト"""
        message = "Configuration file not found"
        error = ConfigError(message)

        assert str(error) == message
        assert isinstance(error, Exception)
        assert error.__class__.__name__ == "ConfigError"

    def test_exception_raising(self):
        """例外の発生テスト"""
        with pytest.raises(APIError) as exc_info:
            raise APIError("Test API error")

        assert str(exc_info.value) == "Test API error"

    def test_exception_chaining(self):
        """例外チェーンのテスト"""
        original_error = ValueError("Original error")

        try:
            raise original_error
        except ValueError as e:
            with pytest.raises(APIError) as exc_info:
                raise APIError("API error caused by original") from e

        assert "API error caused by original" in str(exc_info.value)
        assert exc_info.value.__cause__ is original_error

    @pytest.mark.parametrize(
        "exception_class,message",
        [
            (APIError, "API test message"),
            (DatabaseError, "Database test message"),
            (DataError, "Data test message"),
            (ConfigError, "Config test message"),
        ],
    )
    def test_all_exceptions_parametrized(self, exception_class, message):
        """全例外のパラメータ化テスト"""
        error = exception_class(message)

        assert str(error) == message
        assert isinstance(error, Exception)

        # 例外の発生確認
        with pytest.raises(exception_class) as exc_info:
            raise error

        assert str(exc_info.value) == message

    def test_exception_inheritance(self):
        """例外の継承関係テスト"""
        # 全てがExceptionのサブクラスであることを確認
        assert issubclass(APIError, Exception)
        assert issubclass(DatabaseError, Exception)
        assert issubclass(DataError, Exception)
        assert issubclass(ConfigError, Exception)

        # 互いに独立していることを確認
        assert not issubclass(APIError, DatabaseError)
        assert not issubclass(DatabaseError, DataError)
        assert not issubclass(DataError, ConfigError)

    def test_empty_message(self):
        """空メッセージでの例外作成テスト"""
        error = APIError("")
        assert str(error) == ""

        error = DatabaseError()
        assert str(error) == ""

    def test_exception_with_additional_data(self):
        """追加データ付き例外のテスト（将来の拡張用）"""
        # 基本的な例外として機能することを確認
        error = APIError("Error with status code")
        assert str(error) == "Error with status code"

        # 将来的にstatus_codeなどの属性を追加する場合のテンプレート
        # error.status_code = 500
        # assert error.status_code == 500

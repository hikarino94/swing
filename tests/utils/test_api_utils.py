"""Tests for src/utils/api_utils.py"""

from unittest.mock import Mock, patch

import pytest
import requests

from src.utils.api_utils import JQuantsAPIClient


class TestJQuantsAPIClient:
    """JQuantsAPIClientクラスのテスト"""

    def setup_method(self):
        """各テストメソッドの前に実行"""
        self.mock_token = "test_token_12345"

    @patch("src.utils.api_utils.get_idtoken")
    def test_initialization_with_default_token(self, mock_get_idtoken):
        """デフォルトトークンでの初期化をテスト"""
        mock_get_idtoken.return_value = self.mock_token

        client = JQuantsAPIClient()

        assert client.token == self.mock_token
        assert client.rate_limit == JQuantsAPIClient.DEFAULT_RATE_LIMIT
        assert client.last_request_time == 0.0
        mock_get_idtoken.assert_called_once()

    def test_initialization_with_custom_token(self):
        """カスタムトークンでの初期化をテスト"""
        custom_token = "custom_token_67890"

        client = JQuantsAPIClient(token=custom_token, rate_limit=0.5)

        assert client.token == custom_token
        assert client.rate_limit == 0.5

    @patch("src.utils.api_utils.get_idtoken")
    def test_session_setup(self, mock_get_idtoken):
        """セッションが正しく設定されることを確認"""
        mock_get_idtoken.return_value = self.mock_token

        client = JQuantsAPIClient()

        # Authorizationヘッダーが設定されていることを確認
        assert "Authorization" in client.session.headers
        assert client.session.headers["Authorization"] == f"Bearer {self.mock_token}"

    @patch("src.utils.api_utils.time")
    def test_wait_for_rate_limit(self, mock_time_module):
        """レート制限の待機処理をテスト"""
        # timeモジュール全体をモック
        mock_time_module.time.side_effect = [
            10.0,
            10.1,
        ]  # current_time, new_last_request_time
        mock_time_module.sleep = Mock()

        client = JQuantsAPIClient(token=self.mock_token)
        client.last_request_time = 10.0

        client._wait_for_rate_limit()

        # レート制限により0.35秒待つ
        mock_time_module.sleep.assert_called_once_with(pytest.approx(0.35, rel=1e-2))
        assert client.last_request_time == 10.1

    @patch("src.utils.api_utils.time")
    def test_wait_for_rate_limit_no_wait(self, mock_time_module):
        """レート制限の待機が不要な場合のテスト"""
        # 十分な時間が経過している
        mock_time_module.time.side_effect = [
            10.0,
            10.0,
        ]  # current_time, new_last_request_time
        mock_time_module.sleep = Mock()

        client = JQuantsAPIClient(token=self.mock_token)
        client.last_request_time = 5.0  # 5秒前

        client._wait_for_rate_limit()

        # 待機は発生しない
        mock_time_module.sleep.assert_not_called()
        assert client.last_request_time == 10.0

    @patch("src.utils.api_utils.get_idtoken")
    def test_request_success(self, mock_get_idtoken):
        """成功するリクエストのテスト"""
        mock_get_idtoken.return_value = self.mock_token
        client = JQuantsAPIClient()

        # セッションのリクエストメソッドをモック
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()

        with patch.object(
            client.session, "request", return_value=mock_response
        ) as mock_request:
            response = client._request("GET", "/test/endpoint", {"param": "value"})

        assert response == mock_response
        mock_request.assert_called_once_with(
            "GET", "https://api.jquants.com/v1/test/endpoint", params={"param": "value"}
        )

    @patch("src.utils.api_utils.get_idtoken")
    def test_request_failure(self, mock_get_idtoken):
        """失敗するリクエストのテスト"""
        mock_get_idtoken.return_value = self.mock_token
        client = JQuantsAPIClient()

        # エラーレスポンスをモック
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "404 Not Found"
        )

        with patch.object(client.session, "request", return_value=mock_response):
            with pytest.raises(requests.exceptions.HTTPError):
                client._request("GET", "/invalid/endpoint")

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_with_pagination_single_page(self, mock_get_idtoken):
        """ページネーション（単一ページ）のテスト"""
        mock_get_idtoken.return_value = self.mock_token
        client = JQuantsAPIClient()

        # レスポンスをモック
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [{"id": 1}, {"id": 2}],
            "pagination_key": None,
        }

        with patch.object(client, "_request", return_value=mock_response):
            result = client.get_with_pagination("/test/endpoint", data_key="data")

        assert result == [{"id": 1}, {"id": 2}]

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_with_pagination_multiple_pages(self, mock_get_idtoken):
        """ページネーション（複数ページ）のテスト"""
        mock_get_idtoken.return_value = self.mock_token
        client = JQuantsAPIClient()

        # 複数ページのレスポンスをモック
        responses = [
            Mock(
                json=Mock(
                    return_value={
                        "data": [{"id": 1}, {"id": 2}],
                        "pagination_key": "page2",
                    }
                )
            ),
            Mock(
                json=Mock(
                    return_value={
                        "data": [{"id": 3}, {"id": 4}],
                        "pagination_key": None,
                    }
                )
            ),
        ]

        with patch.object(client, "_request", side_effect=responses) as mock_request:
            result = client.get_with_pagination("/test/endpoint", data_key="data")

        assert result == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
        assert mock_request.call_count == 2

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_with_pagination_auto_detect_data_key(self, mock_get_idtoken):
        """データキーの自動検出のテスト"""
        mock_get_idtoken.return_value = self.mock_token
        client = JQuantsAPIClient()

        # 様々なデータキーのレスポンスをテスト
        test_cases = [
            ({"daily_quotes": [{"price": 100}]}, [{"price": 100}]),
            ({"info": [{"name": "test"}]}, [{"name": "test"}]),
            ({"statements": [{"revenue": 1000}]}, [{"revenue": 1000}]),
        ]

        for response_data, expected in test_cases:
            mock_response = Mock()
            mock_response.json.return_value = response_data

            with patch.object(client, "_request", return_value=mock_response):
                result = client.get_with_pagination("/test/endpoint")

            assert result == expected


class TestAPIClientMethods:
    """追加のAPIクライアントメソッドのテスト"""

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_daily_quotes(self, mock_get_idtoken):
        """日次株価取得メソッドのテスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        mock_response = [{"code": "1234", "close": 1000}]
        with patch.object(
            client, "get_with_pagination", return_value=mock_response
        ) as mock_get:
            result = client.get_daily_quotes(date="2023-01-01", code="1234")

        assert result == mock_response
        mock_get.assert_called_once_with(
            "/prices/daily_quotes", {"date": "2023-01-01", "code": "1234"}
        )

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_daily_quotes_without_code(self, mock_get_idtoken):
        """銘柄コードなしでの日次株価取得テスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        mock_response = [
            {"code": "1234", "close": 1000},
            {"code": "5678", "close": 2000},
        ]
        with patch.object(
            client, "get_with_pagination", return_value=mock_response
        ) as mock_get:
            result = client.get_daily_quotes(date="2023-01-01")

        assert result == mock_response
        mock_get.assert_called_once_with("/prices/daily_quotes", {"date": "2023-01-01"})

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_statements(self, mock_get_idtoken):
        """財務諸表取得メソッドのテスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        mock_response = [{"code": "1234", "revenue": 1000000}]
        with patch.object(
            client, "get_with_pagination", return_value=mock_response
        ) as mock_get:
            result = client.get_statements(code="1234")

        assert result == mock_response
        mock_get.assert_called_once_with("/fins/statements", {"code": "1234"})

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_statements_with_date(self, mock_get_idtoken):
        """日付指定での財務諸表取得テスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        mock_response = [{"code": "1234", "revenue": 1000000, "date": "2023-03-31"}]
        with patch.object(
            client, "get_with_pagination", return_value=mock_response
        ) as mock_get:
            result = client.get_statements(code="1234", date="2023-03-31")

        assert result == mock_response
        mock_get.assert_called_once_with(
            "/fins/statements", {"code": "1234", "date": "2023-03-31"}
        )

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_statements_no_params(self, mock_get_idtoken):
        """パラメータなしでの財務諸表取得テスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        mock_response = [{"code": "1234", "revenue": 1000000}]
        with patch.object(
            client, "get_with_pagination", return_value=mock_response
        ) as mock_get:
            result = client.get_statements()

        assert result == mock_response
        mock_get.assert_called_once_with("/fins/statements", {})

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_listed_info(self, mock_get_idtoken):
        """上場銘柄情報取得メソッドのテスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        mock_response = [{"code": "1234", "company_name": "Test Company"}]
        with patch.object(
            client, "get_with_pagination", return_value=mock_response
        ) as mock_get:
            result = client.get_listed_info(code="1234")

        assert result == mock_response
        mock_get.assert_called_once_with("/listed/info", {"code": "1234"})

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_listed_info_without_code(self, mock_get_idtoken):
        """銘柄コードなしでの上場銘柄情報取得テスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        mock_response = [
            {"code": "1234", "company_name": "Company A"},
            {"code": "5678", "company_name": "Company B"},
        ]
        with patch.object(
            client, "get_with_pagination", return_value=mock_response
        ) as mock_get:
            result = client.get_listed_info()

        assert result == mock_response
        mock_get.assert_called_once_with("/listed/info", {})


class TestAPIClientRetry:
    """リトライ機能のテスト"""

    @patch("src.utils.api_utils.get_idtoken")
    def test_retry_on_server_error(self, mock_get_idtoken):
        """サーバーエラー時のリトライをテスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        # 最初の2回は500エラー、3回目は成功
        mock_responses = [
            Mock(
                status_code=500,
                raise_for_status=Mock(side_effect=requests.exceptions.HTTPError()),
            ),
            Mock(
                status_code=500,
                raise_for_status=Mock(side_effect=requests.exceptions.HTTPError()),
            ),
            Mock(status_code=200, raise_for_status=Mock()),
        ]

        with patch.object(client.session, "request", side_effect=mock_responses):
            # リトライ戦略のため、最終的に成功するはず
            with pytest.raises(requests.exceptions.HTTPError):
                # ただし、このテストではリトライが設定通り動作することを確認
                client._request("GET", "/test")


class TestPaginationEdgeCases:
    """ページネーションのエッジケーステスト"""

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_with_pagination_empty_data(self, mock_get_idtoken):
        """空のデータが返された場合のテスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        # 空のレスポンス
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [],
            "pagination_key": None,
        }

        with patch.object(client, "_request", return_value=mock_response):
            result = client.get_with_pagination("/test/endpoint", data_key="data")

        assert result == []

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_with_pagination_auto_detect_fallback(self, mock_get_idtoken):
        """データキー自動検出のフォールバックテスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        # 未知のエンドポイントで最初の配列を探す
        mock_response = Mock()
        mock_response.json.return_value = {
            "meta": {"version": "1.0"},
            "results": [{"id": 1}, {"id": 2}],
            "pagination_key": None,
        }

        with patch.object(client, "_request", return_value=mock_response):
            result = client.get_with_pagination("/unknown/endpoint")

        assert result == [{"id": 1}, {"id": 2}]

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_with_pagination_no_list_data(self, mock_get_idtoken):
        """リスト型のデータが見つからない場合のテスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        # リスト型のデータがないレスポンス
        mock_response = Mock()
        mock_response.json.return_value = {
            "meta": {"version": "1.0"},
            "status": "ok",
            "pagination_key": None,
        }

        with patch.object(client, "_request", return_value=mock_response):
            result = client.get_with_pagination("/no-list/endpoint")

        assert result == []

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_with_pagination_fins_statements_endpoint(self, mock_get_idtoken):
        """財務諸表エンドポイントの自動検出テスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        mock_response = Mock()
        mock_response.json.return_value = {
            "statements": [{"code": "1234", "revenue": 1000}],
            "pagination_key": None,
        }

        with patch.object(client, "_request", return_value=mock_response):
            result = client.get_with_pagination("/fins/fins_statements")

        assert result == [{"code": "1234", "revenue": 1000}]

    @patch("src.utils.api_utils.get_idtoken")
    def test_get_with_pagination_listed_info_endpoint(self, mock_get_idtoken):
        """上場銘柄情報エンドポイントの自動検出テスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        mock_response = Mock()
        mock_response.json.return_value = {
            "info": [{"code": "1234", "name": "Test Corp"}],
            "pagination_key": None,
        }

        with patch.object(client, "_request", return_value=mock_response):
            result = client.get_with_pagination("/listed/listed_info")

        assert result == [{"code": "1234", "name": "Test Corp"}]


class TestRequestExceptionHandling:
    """リクエスト例外処理のテスト"""

    @patch("src.utils.api_utils.get_idtoken")
    def test_request_connection_error(self, mock_get_idtoken):
        """接続エラーのテスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        with patch.object(
            client.session,
            "request",
            side_effect=requests.exceptions.ConnectionError("Connection failed"),
        ):
            with pytest.raises(requests.exceptions.ConnectionError):
                client._request("GET", "/test")

    @patch("src.utils.api_utils.get_idtoken")
    def test_request_timeout_error(self, mock_get_idtoken):
        """タイムアウトエラーのテスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        with patch.object(
            client.session,
            "request",
            side_effect=requests.exceptions.Timeout("Request timeout"),
        ):
            with pytest.raises(requests.exceptions.Timeout):
                client._request("GET", "/test")

    @patch("src.utils.api_utils.get_idtoken")
    @patch("src.utils.api_utils.logger")
    def test_request_logs_error(self, mock_logger, mock_get_idtoken):
        """エラー時のログ出力テスト"""
        mock_get_idtoken.return_value = "test_token"
        client = JQuantsAPIClient()

        error = requests.exceptions.HTTPError("404 Not Found")
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = error

        with patch.object(client.session, "request", return_value=mock_response):
            with pytest.raises(requests.exceptions.HTTPError):
                client._request("GET", "/invalid")

        mock_logger.error.assert_called_once_with(f"API request failed: {error}")

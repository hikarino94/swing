"""J-Quants API通信の共通ユーティリティ"""

import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import get_idtoken
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class JQuantsAPIClient:
    """J-Quants API用の統一クライアント"""

    BASE_URL = "https://api.jquants.com/v1"
    DEFAULT_RATE_LIMIT = 0.35  # 秒（APIのレート制限対応）

    def __init__(
        self, token: str | None = None, rate_limit: float = DEFAULT_RATE_LIMIT
    ):
        """
        Args:
            token: APIトークン（省略時は設定から取得）
            rate_limit: リクエスト間の待機時間（秒）
        """
        self.token = token or get_idtoken()
        self.rate_limit = rate_limit
        self.last_request_time = 0.0

        # セッションの設定
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

        # リトライ設定
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

    def _wait_for_rate_limit(self) -> None:
        """レート制限のための待機"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            wait_time = self.rate_limit - elapsed
            logger.debug(f"Rate limit: waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
        self.last_request_time = time.time()

    def _request(
        self, method: str, endpoint: str, params: dict[str, Any] | None = None
    ) -> requests.Response:
        """HTTPリクエストの実行

        Args:
            method: HTTPメソッド
            endpoint: エンドポイント（例: "/prices/daily_quotes"）
            params: クエリパラメータ

        Returns:
            レスポンス

        Raises:
            requests.exceptions.RequestException: API呼び出しエラー
        """
        self._wait_for_rate_limit()

        url = f"{self.BASE_URL}{endpoint}"
        logger.debug(f"{method} {url} with params: {params}")

        try:
            response = self.session.request(method, url, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def get_with_pagination(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data_key: str | None = None,
    ) -> list[dict[str, Any]]:
        """ページネーション対応のGETリクエスト

        Args:
            endpoint: エンドポイント
            params: クエリパラメータ
            data_key: レスポンス内のデータキー（省略時は自動判定）

        Returns:
            全ページのデータをマージしたリスト
        """
        if params is None:
            params = {}

        all_data = []
        page_count = 0

        while True:
            response = self._request("GET", endpoint, params)
            data = response.json()
            page_count += 1

            # データキーの自動判定
            if data_key is None:
                # エンドポイントに基づいてデータキーを推測
                if "daily_quotes" in endpoint:
                    data_key = "daily_quotes"
                elif "fins_statements" in endpoint:
                    data_key = "statements"
                elif "listed_info" in endpoint:
                    data_key = "info"
                else:
                    # データキーが見つからない場合は、最初の配列を探す
                    for key, value in data.items():
                        if isinstance(value, list):
                            data_key = key
                            break

            # データ取得
            items = data.get(data_key, [])
            if not items:
                logger.debug(f"No more data at page {page_count}")
                break

            all_data.extend(items)
            logger.info(f"Fetched page {page_count}: {len(items)} items")

            # ページネーション処理
            pagination_key = data.get("pagination_key")
            if not pagination_key:
                break

            params["pagination_key"] = pagination_key

        logger.info(f"Total fetched: {len(all_data)} items in {page_count} pages")
        return all_data

    def get_daily_quotes(
        self, date: str, code: str | None = None
    ) -> list[dict[str, Any]]:
        """日次株価データの取得

        Args:
            date: 日付（YYYY-MM-DD形式）
            code: 銘柄コード（省略時は全銘柄）

        Returns:
            株価データのリスト
        """
        params = {"date": date}
        if code:
            params["code"] = code

        return self.get_with_pagination("/prices/daily_quotes", params)

    def get_statements(
        self, code: str | None = None, date: str | None = None
    ) -> list[dict[str, Any]]:
        """財務諸表データの取得

        Args:
            code: 銘柄コード
            date: 日付（YYYY-MM-DD形式）

        Returns:
            財務諸表データのリスト
        """
        params = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date

        return self.get_with_pagination("/fins/statements", params)

    def get_listed_info(self, code: str | None = None) -> list[dict[str, Any]]:
        """上場銘柄情報の取得

        Args:
            code: 銘柄コード（省略時は全銘柄）

        Returns:
            上場銘柄情報のリスト
        """
        params = {}
        if code:
            params["code"] = code

        return self.get_with_pagination("/listed/info", params)

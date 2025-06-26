"""J-Quants APIクライアント"""
import time
from typing import Dict, Any, Optional, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from datetime import date
from .exceptions import APIError
from .config import get_config_manager

logger = logging.getLogger(__name__)


class JQuantsClient:
    """J-Quants APIクライアント"""
    
    BASE_URL = "https://api.jquants.com/v1"
    RATE_LIMIT_SLEEP = 0.35  # レート制限対策
    
    def __init__(self, token: Optional[str] = None):
        """
        Args:
            token: APIトークン。Noneの場合は設定ファイルから読み込み
        """
        if token is None:
            config = get_config_manager()
            token = config.get_token()
        
        self.token = token
        self.session = self._create_session()
        logger.debug("JQuantsClient initialized")
    
    def _create_session(self) -> requests.Session:
        """リトライ設定済みのセッションを作成"""
        session = requests.Session()
        
        # リトライ戦略の設定
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # 認証ヘッダーの設定
        session.headers.update({
            "Authorization": f"Bearer {self.token}"
        })
        
        return session
    
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 60
    ) -> Dict[str, Any]:
        """API呼び出しの共通実装
        
        Args:
            endpoint: APIエンドポイント
            params: クエリパラメータ
            timeout: タイムアウト秒数
            
        Returns:
            APIレスポンス
            
        Raises:
            APIError: API呼び出しエラー
        """
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            logger.debug(f"GET request to {endpoint} with params: {params}")
            response = self.session.get(url, params=params, timeout=timeout)
            
            # HTTPエラーのチェック
            if response.status_code != 200:
                error_msg = f"API error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise APIError(error_msg)
            
            # レート制限対策
            time.sleep(self.RATE_LIMIT_SLEEP)
            
            data = response.json()
            logger.debug(f"API response received: {len(data.get('info', []))} items")
            return data
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error: {e}"
            logger.error(error_msg)
            raise APIError(error_msg)
        except ValueError as e:
            error_msg = f"JSON decode error: {e}"
            logger.error(error_msg)
            raise APIError(error_msg)
    
    def get_daily_quotes(
        self,
        code: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """日次株価情報を取得
        
        Args:
            code: 銘柄コード（指定しない場合は全銘柄）
            date_from: 開始日
            date_to: 終了日
            
        Returns:
            株価情報のリスト
        """
        params = {}
        if code:
            params["code"] = code
        if date_from:
            params["from"] = date_from.strftime("%Y%m%d")
        if date_to:
            params["to"] = date_to.strftime("%Y%m%d")
        
        response = self.get("daily_quotes", params)
        return response.get("daily_quotes", [])
    
    def get_statements(
        self,
        code: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """財務諸表データを取得
        
        Args:
            code: 銘柄コード
            date_from: 開始日
            date_to: 終了日
            
        Returns:
            財務諸表データのリスト
        """
        params = {}
        if code:
            params["code"] = code
        if date_from:
            params["from"] = date_from.strftime("%Y%m%d")
        if date_to:
            params["to"] = date_to.strftime("%Y%m%d")
        
        response = self.get("statements", params)
        return response.get("statements", [])
    
    def get_listed_info(
        self,
        code: Optional[str] = None,
        date_str: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """上場銘柄情報を取得
        
        Args:
            code: 銘柄コード
            date_str: 日付（YYYYMMDD形式）
            
        Returns:
            上場銘柄情報のリスト
        """
        params = {}
        if code:
            params["code"] = code
        if date_str:
            params["date"] = date_str
        
        response = self.get("listed/info", params)
        return response.get("info", [])
    
    def refresh_token(self, email: str, password: str) -> str:
        """トークンを更新
        
        Args:
            email: メールアドレス
            password: パスワード
            
        Returns:
            新しいトークン
            
        Raises:
            APIError: 認証エラー
        """
        url = "https://api.jquants.com/v1/token/auth_user"
        
        try:
            response = requests.post(
                url,
                json={"mailaddress": email, "password": password},
                timeout=30
            )
            
            if response.status_code != 200:
                raise APIError(f"認証エラー: {response.status_code}")
            
            data = response.json()
            new_token = data.get("idToken")
            
            if not new_token:
                raise APIError("トークンが取得できませんでした")
            
            # 新しいトークンで再初期化
            self.token = new_token
            self.session = self._create_session()
            
            logger.info("Token refreshed successfully")
            return new_token
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Token refresh error: {e}"
            logger.error(error_msg)
            raise APIError(error_msg)


# デフォルトのクライアントインスタンス
_default_client: Optional[JQuantsClient] = None


def get_jquants_client() -> JQuantsClient:
    """デフォルトのJQuantsClientインスタンスを取得"""
    global _default_client
    if _default_client is None:
        _default_client = JQuantsClient()
    return _default_client
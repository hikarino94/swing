"""設定ファイル管理モジュール"""
from pathlib import Path
import json
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """設定関連のエラー"""
    pass


class ConfigManager:
    """設定ファイルの統一管理クラス"""
    
    def __init__(self, base_path: Optional[Path] = None):
        """
        Args:
            base_path: 設定ファイルのベースディレクトリ。Noneの場合はプロジェクトルート
        """
        if base_path is None:
            # プロジェクトルートを自動検出
            self.base_path = Path(__file__).resolve().parents[2]
        else:
            self.base_path = Path(base_path)
        
        logger.debug(f"ConfigManager initialized with base_path: {self.base_path}")
    
    def load_json(self, filename: str) -> Dict[str, Any]:
        """JSONファイルを読み込む
        
        Args:
            filename: 読み込むJSONファイル名
            
        Returns:
            読み込んだJSONデータ
            
        Raises:
            ConfigError: ファイルが見つからないか、JSON形式が不正な場合
        """
        path = self.base_path / filename
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
                logger.debug(f"Successfully loaded {filename}")
                return data
        except FileNotFoundError:
            error_msg = f"{filename} が見つかりません: {path}"
            logger.error(error_msg)
            raise ConfigError(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"{filename} の形式が不正です: {e}"
            logger.error(error_msg)
            raise ConfigError(error_msg)
        except Exception as e:
            error_msg = f"{filename} の読み込み中にエラーが発生しました: {e}"
            logger.error(error_msg)
            raise ConfigError(error_msg)
    
    def load_idtoken(self) -> Dict[str, Any]:
        """idtoken.jsonを読み込む
        
        Returns:
            idTokenを含む辞書
        """
        return self.load_json("idtoken.json")
    
    def load_account(self) -> Dict[str, Any]:
        """account.jsonを読み込む
        
        Returns:
            アカウント情報を含む辞書
        """
        return self.load_json("account.json")
    
    def load_login(self) -> Dict[str, Any]:
        """login.jsonを読み込む
        
        Returns:
            ログイン情報を含む辞書
        """
        return self.load_json("login.json")
    
    def load_thresholds(self) -> Dict[str, Any]:
        """screening/thresholds.jsonを読み込む
        
        Returns:
            閾値設定を含む辞書
        """
        return self.load_json("screening/thresholds.json")
    
    def get_token(self) -> str:
        """idtoken.jsonからトークンを取得
        
        Returns:
            J-Quants APIトークン
            
        Raises:
            ConfigError: トークンが取得できない場合
        """
        data = self.load_json("idtoken.json")
        if "idToken" not in data:
            raise ConfigError("idToken キーが見つかりません")
        return data["idToken"]
    
    def get_account_info(self) -> Dict[str, str]:
        """account.jsonからアカウント情報を取得
        
        Returns:
            メールアドレスとパスワードを含む辞書
            
        Raises:
            ConfigError: アカウント情報が取得できない場合
        """
        data = self.load_json("account.json")
        required_keys = ["mailaddress", "password"]
        missing_keys = [key for key in required_keys if key not in data]
        if missing_keys:
            raise ConfigError(f"account.json に必要なキーがありません: {missing_keys}")
        return data
    
    def get_login_info(self) -> Dict[str, str]:
        """login.jsonからログイン情報を取得（Web UI用）
        
        Returns:
            ユーザー名とパスワードを含む辞書
            
        Raises:
            ConfigError: ログイン情報が取得できない場合
        """
        try:
            # まずlogin.jsonを試す
            data = self.load_json("login.json")
            if "username" in data and "password" in data:
                return data
        except ConfigError:
            # login.jsonが無い場合はaccount.jsonにフォールバック
            logger.info("login.json not found, falling back to account.json")
            account_data = self.get_account_info()
            return {
                "username": account_data["mailaddress"],
                "password": account_data["password"]
            }
        
        raise ConfigError("ログイン情報の取得に失敗しました")
    
    def get_thresholds(self) -> Dict[str, Any]:
        """screening/thresholds.jsonから閾値設定を取得
        
        Returns:
            スクリーニング閾値の辞書
            
        Raises:
            ConfigError: 閾値設定が取得できない場合
        """
        return self.load_json("screening/thresholds.json")
    
    def save_token(self, token: str) -> None:
        """トークンをidtoken.jsonに保存
        
        Args:
            token: 保存するトークン
            
        Raises:
            ConfigError: 保存に失敗した場合
        """
        path = self.base_path / "idtoken.json"
        try:
            with path.open("w", encoding="utf-8") as f:
                json.dump({"idToken": token}, f, ensure_ascii=False, indent=2)
                logger.info("Token saved successfully")
        except Exception as e:
            error_msg = f"トークンの保存に失敗しました: {e}"
            logger.error(error_msg)
            raise ConfigError(error_msg)


# シングルトンインスタンス
_config_manager: Optional[ConfigManager] = None


def get_config_manager() -> ConfigManager:
    """ConfigManagerのシングルトンインスタンスを取得"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager
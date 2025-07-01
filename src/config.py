"""
中央設定管理モジュール

プロジェクト全体で使用する設定値を一元管理します。
"""

import json
from pathlib import Path
from typing import Any


class Config:
    """設定管理クラス"""

    def __init__(self, config_path: Path | None = None):
        """
        設定を初期化します。

        Args:
            config_path: 設定ファイルのパス（指定しない場合はデフォルトを使用）
        """
        self.base_dir = (
            Path(__file__).resolve().parent.parent
        )  # プロジェクトルートを指す
        self.config_path = config_path or self.base_dir / "config" / "config.json"
        self._config: dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """設定ファイルを読み込みます"""
        if self.config_path.exists():
            with open(self.config_path, encoding="utf-8") as f:
                self._config = json.load(f)
        else:
            # デフォルト設定
            self._config = self._get_default_config()

    def _get_default_config(self) -> dict[str, Any]:
        """デフォルト設定を返します"""
        return {
            "database": {"path": str(self.base_dir / "db" / "stock.db")},
            "api": {
                "base_url": "https://api.jquants.com/v1",
                "endpoints": {
                    "auth": "/token/auth_user",
                    "refresh": "/token/auth_refresh",
                    "daily_quotes": "/prices/daily_quotes",
                    "listed_info": "/listed/info",
                    "statements": "/fins/statements",
                },
                "rate_limit": {"sleep_seconds": 0.35},
            },
            "scheduler": {
                "tasks": {
                    "fetch_quotes": {"time": "20:00", "frequency": "daily"},
                    "fetch_statements": {"time": "20:30", "frequency": "daily"},
                    "update_listed_info": {"time": "06:00", "frequency": "monday"},
                }
            },
            "files": {
                "account": "account.json",
                "idtoken": "idtoken.json",
                "thresholds": "screening/thresholds.json",
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        }

    @property
    def db_path(self) -> str:
        """データベースパスを返します

        TODO: 将来的にはPath型を返すように変更することを検討
              現在は後方互換性のためstr型を維持
        """
        return str(self._config["database"]["path"])

    @property
    def api_base_url(self) -> str:
        """APIベースURLを返します"""
        return str(self._config["api"]["base_url"])

    def get_api_endpoint(self, endpoint_name: str) -> str:
        """
        APIエンドポイントのフルURLを返します

        Args:
            endpoint_name: エンドポイント名（auth, refresh, daily_quotes等）

        Returns:
            フルURL
        """
        endpoint = self._config["api"]["endpoints"].get(endpoint_name)
        if not endpoint:
            raise ValueError(f"Unknown endpoint: {endpoint_name}")
        return str(self.api_base_url + endpoint)

    @property
    def api_rate_limit_sleep(self) -> float:
        """APIレート制限のスリープ時間を返します"""
        return float(self._config["api"]["rate_limit"]["sleep_seconds"])

    def get_scheduler_config(self, task_name: str) -> dict[str, str]:
        """
        スケジューラタスクの設定を返します

        Args:
            task_name: タスク名

        Returns:
            タスク設定（time, frequency）
        """
        result = self._config["scheduler"]["tasks"].get(task_name, {})
        return {str(k): str(v) for k, v in result.items()}

    def get_file_path(self, file_type: str) -> Path:
        """
        設定ファイルのパスを返します

        Args:
            file_type: ファイルタイプ（account, idtoken, thresholds）

        Returns:
            ファイルパス
        """
        filename = self._config["files"].get(file_type)
        if not filename:
            raise ValueError(f"Unknown file type: {file_type}")
        return Path(self.base_dir / filename)

    @property
    def log_level(self) -> str:
        """ログレベルを返します"""
        return str(self._config["logging"]["level"])

    @property
    def log_format(self) -> str:
        """ログフォーマットを返します"""
        return str(self._config["logging"]["format"])

    def get(self, key: str, default: Any = None) -> Any:
        """
        設定値を取得します（ネストしたキーは.で区切る）

        Args:
            key: 設定キー（例: "api.endpoints.auth"）
            default: デフォルト値

        Returns:
            設定値
        """
        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    @property
    def output_base_dir(self) -> Path:
        """出力ファイルのベースディレクトリを返します"""
        return self.base_dir / "data" / "output"

    @property
    def log_dir(self) -> Path:
        """ログファイルのディレクトリを返します"""
        return self.base_dir / "data" / "logs"

    @property
    def model_dir(self) -> Path:
        """機械学習モデルのディレクトリを返します"""
        return self.base_dir / "db" / "models"


# グローバル設定インスタンス
config = Config()

# よく使う値を直接エクスポート
DB_PATH = config.db_path
API_BASE_URL = config.api_base_url
API_RATE_LIMIT_SLEEP = config.api_rate_limit_sleep
OUTPUT_BASE_DIR = config.output_base_dir
LOG_DIR = config.log_dir
MODEL_DIR = config.model_dir

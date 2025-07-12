"""可視化基底クラス"""

import sqlite3
from abc import ABC, abstractmethod
from typing import Any

import japanize_matplotlib
import matplotlib

from src.config import DB_PATH
from src.utils.logging_config import get_logger

# バックエンドを設定（GUIを使わない）
matplotlib.use("Agg")

# 日本語フォントの設定
japanize_matplotlib.japanize()

logger = get_logger("portfolio.visualizers.base")


class BaseVisualizer(ABC):
    """可視化基底クラス"""

    def __init__(self, user_id: int):
        self.user_id = user_id

    @abstractmethod
    def create_chart(self) -> dict[str, Any]:
        """チャートを作成（サブクラスで実装）"""
        pass

    def get_db_connection(self) -> sqlite3.Connection:
        """データベース接続を取得"""
        return sqlite3.connect(DB_PATH)

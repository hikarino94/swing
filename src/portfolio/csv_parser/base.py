"""CSV解析の基底クラス"""

from abc import ABC, abstractmethod
from typing import Any

from .encodings import decode_content


class BaseCSVParser(ABC):
    """CSV解析の基底クラス"""

    @classmethod
    def parse(cls, csv_content: str | bytes) -> list[dict[str, Any]]:
        """
        CSVを解析

        Args:
            csv_content: CSVファイルの内容（文字列またはバイト列）

        Returns:
            解析結果のリスト
        """
        # バイト列の場合はデコード
        if isinstance(csv_content, bytes):
            csv_content = decode_content(csv_content)

        return cls._parse_content(csv_content)

    @classmethod
    @abstractmethod
    def _parse_content(cls, csv_content: str) -> list[dict[str, Any]]:
        """
        CSVコンテンツを解析（サブクラスで実装）

        Args:
            csv_content: デコード済みのCSV文字列

        Returns:
            解析結果のリスト
        """
        pass

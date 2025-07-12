"""CSVパーサーの基底クラス"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import chardet
import pandas as pd

from src.utils.logging_config import get_logger

logger = get_logger("portfolio.parsers.base")


class BaseCSVParser(ABC):
    """CSVパーサーの基底クラス"""

    def __init__(self):
        self.encoding = None
        self.df = None

    def detect_encoding(self, file_path: Path) -> str:
        """ファイルのエンコーディングを検出"""
        with open(file_path, "rb") as f:
            result = chardet.detect(f.read())
            detected = result["encoding"]

        # SBI証券のCSVは通常Shift-JISまたはCP932
        if detected and detected.lower() in ["shift-jis", "shift_jis", "sjis", "cp932"]:
            return "cp932"
        elif detected:
            return str(detected)
        else:
            # デフォルトでcp932を試す
            return "cp932"

    def read_csv(self, file_path: Path) -> pd.DataFrame:
        """CSVファイルを読み込む"""
        self.encoding = self.detect_encoding(file_path)
        logger.info(f"検出されたエンコーディング: {self.encoding}")

        try:
            df = pd.read_csv(file_path, encoding=self.encoding)
            self.df = df
            return df
        except Exception as e:
            logger.error(f"CSV読み込みエラー: {e}")
            # 別のエンコーディングで再試行
            for enc in ["utf-8", "cp932", "shift-jis"]:
                if enc != self.encoding:
                    try:
                        logger.info(f"エンコーディング {enc} で再試行")
                        df = pd.read_csv(file_path, encoding=enc)
                        self.encoding = enc
                        self.df = df
                        return df
                    except Exception:
                        continue
            raise

    @abstractmethod
    def detect_format(self, df: pd.DataFrame) -> str | None:
        """CSVフォーマットを検出"""
        pass

    @abstractmethod
    def parse(self, file_path: Path) -> list[dict[str, Any]]:
        """CSVファイルを解析してデータを返す"""
        pass

    def clean_numeric(self, value: Any) -> float:
        """数値データをクリーニング"""
        if pd.isna(value) or value in ["", "-", "－", "N/A", "n/a"]:
            return 0.0

        if isinstance(value, str):
            # カンマと円記号を削除
            value = value.replace(",", "").replace("円", "").replace("¥", "")
            # パーセント記号を削除
            value = value.replace("%", "").strip()

        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def clean_quantity(self, value: Any) -> int:
        """数量データをクリーニング"""
        if pd.isna(value) or value in ["", "-", "－"]:
            return 0

        if isinstance(value, str):
            # カンマと株を削除
            value = value.replace(",", "").replace("株", "").strip()

        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 0

    def format_code(self, code: Any) -> str:
        """銘柄コードを4桁にフォーマット"""
        if pd.isna(code):
            return ""

        code_str = str(code).strip()
        # 数字のみ抽出
        code_digits = "".join(c for c in code_str if c.isdigit())

        if len(code_digits) >= 4:
            return code_digits[:4]
        elif code_digits:
            return code_digits.zfill(4)
        else:
            return ""

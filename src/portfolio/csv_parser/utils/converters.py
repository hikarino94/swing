"""データ変換ユーティリティ"""

import re
from datetime import datetime
from typing import Any

from src.utils.logging_config import get_logger

logger = get_logger("portfolio.csv_parser.utils.converters")


def normalize_code(code: str) -> str:
    """
    銘柄コードを4桁に正規化

    Args:
        code: 正規化前の銘柄コード

    Returns:
        正規化された銘柄コード
    """
    if not code:
        return ""

    # 372Aのような英字付きコードに対応
    code = str(code).strip()

    # サフィックス（.T、T など）を削除
    code = re.sub(r"[.\s]?T$", "", code, flags=re.IGNORECASE)

    # 先頭の0を削除（5桁以上の場合）
    if code.isdigit() and len(code) > 4:
        code = code.lstrip("0")

    # 4桁の数字部分を抽出（英字は保持）
    if len(code) == 4:
        return code
    elif len(code) < 4 and code.isdigit():
        return code.zfill(4)
    else:
        # 数字のみを抽出
        digits = re.findall(r"\d+", code)
        if digits:
            digit_part = str(digits[0]).lstrip("0")
            if len(digit_part) < 4:
                return digit_part.zfill(4)
            return digit_part
    return code


def parse_number(value: Any, default: float | None = None) -> float | None:
    """
    数値を解析（カンマ、マイナス記号対応）

    Args:
        value: 解析対象の値
        default: 解析できない場合のデフォルト値

    Returns:
        解析された数値またはデフォルト値
    """
    if value is None or value == "" or value == "--":
        return default

    try:
        value_str = str(value).strip()

        # 矢印記号の場合はデフォルト値を返す
        if value_str in ["↑", "↓", "→", "←"]:
            return default

        # カンマを除去
        value_str = value_str.replace(",", "")

        # △や▲をマイナス記号に変換
        if value_str.startswith("△") or value_str.startswith("▲"):
            value_str = "-" + value_str[1:]

        # 括弧付きマイナス値の処理（例: "(1,000)")
        if value_str.startswith("(") and value_str.endswith(")"):
            value_str = "-" + value_str[1:-1]

        # マイナス記号の処理（例: "-1,000")
        if value_str.startswith('"') and value_str.endswith('"'):
            value_str = value_str[1:-1]

        # %記号を除去
        if value_str.endswith("%"):
            value_str = value_str[:-1]

        # 範囲表記（例: "194 ~ 200"）の場合は平均値を返す
        if "~" in value_str:
            parts = value_str.split("~")
            if len(parts) == 2:
                try:
                    min_val = float(parts[0].strip())
                    max_val = float(parts[1].strip())
                    return (min_val + max_val) / 2
                except ValueError:
                    pass

        return float(value_str)
    except (ValueError, AttributeError):
        # 矢印記号やダッシュ記号はエラーログを出さない
        value_str = str(value).strip()
        if value_str not in ["↑", "↓", "→", "←", "--%", "--", "-", "－", "―"]:
            logger.warning(f"数値解析エラー: {value}")
        return default


def parse_date(date_str: Any) -> str | None:
    """
    日付文字列を解析してYYYY-MM-DD形式に変換

    Args:
        date_str: 解析対象の日付文字列

    Returns:
        YYYY-MM-DD形式の日付文字列またはNone
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    # 一般的な日付フォーマットを試行
    date_formats = [
        "%Y/%m/%d %H:%M:%S",  # 時刻付き
        "%Y/%m/%d",
        "%Y-%m-%d",
        "%Y年%m月%d日",
        "%y/%m/%d",  # 2桁年
    ]

    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning(f"日付解析エラー: {date_str}")
    return None

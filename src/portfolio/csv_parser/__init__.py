"""SBI証券のCSVファイル解析モジュール

このモジュールは後方互換性のために既存のインターフェースを維持しています。
実際の実装は各サブモジュールに分割されています。
"""

from .encodings import detect_encoding
from .parsers.holdings import HoldingsParser
from .parsers.transactions import TransactionsParser


class SBICSVParser:
    """SBI証券のCSVファイルパーサー（互換性維持用のファサード）"""

    @staticmethod
    def detect_encoding(content: bytes) -> str:
        """バイト列のエンコーディングを検出"""
        return detect_encoding(content)

    @staticmethod
    def parse_transactions_csv(csv_content: str | bytes) -> list[dict]:
        """取引履歴CSVを解析"""
        return TransactionsParser.parse(csv_content)

    @staticmethod
    def parse_holdings_csv(csv_content: str | bytes) -> list[dict]:
        """保有銘柄CSVを解析"""
        return HoldingsParser.parse(csv_content)

    # 内部メソッドも互換性のために公開（非推奨）
    @staticmethod
    def _normalize_code(code: str) -> str:
        """銘柄コードを4桁に正規化（非推奨：utils.converters.normalize_codeを使用）"""
        from .utils.converters import normalize_code

        return normalize_code(code)

    @staticmethod
    def _parse_number(value, default=None) -> float | None:
        """数値を解析（非推奨：utils.converters.parse_numberを使用）"""
        from .utils.converters import parse_number

        return parse_number(value, default)

    @staticmethod
    def _parse_date(date_str) -> str | None:
        """日付文字列を解析（非推奨：utils.converters.parse_dateを使用）"""
        from .utils.converters import parse_date

        return parse_date(date_str)

    @staticmethod
    def _parse_trade_type(trade_type: str) -> tuple[str, str]:
        """取引区分を解析（非推奨：parsers.transactions.parse_trade_typeを使用）"""
        from .parsers.transactions import parse_trade_type

        return parse_trade_type(trade_type)

"""データ変換ユーティリティのテスト"""

from src.portfolio.csv_parser.utils.converters import (
    normalize_code,
    parse_date,
    parse_number,
)


class TestNormalizeCode:
    """normalize_code関数のテスト"""

    def test_normalize_code_4digit(self):
        """4桁コードの正規化テスト"""
        assert normalize_code("1234") == "1234"
        assert normalize_code("0123") == "0123"
        assert normalize_code("9999") == "9999"

    def test_normalize_code_less_than_4digit(self):
        """4桁未満コードの正規化テスト"""
        assert normalize_code("123") == "0123"
        assert normalize_code("12") == "0012"
        assert normalize_code("1") == "0001"
        assert normalize_code("0") == "0000"

    def test_normalize_code_more_than_4digit(self):
        """4桁超コードの正規化テスト"""
        assert normalize_code("12345") == "12345"  # 5桁はそのまま
        assert normalize_code("123456") == "123456"  # 6桁もそのまま

    def test_normalize_code_with_alpha(self):
        """英字付きコードの正規化テスト"""
        assert normalize_code("372A") == "372A"  # 4桁の英字付きはそのまま
        assert normalize_code("123A5") == "0123"  # 最初の数字部分を抽出

    def test_normalize_code_empty(self):
        """空コードの正規化テスト"""
        assert normalize_code("") == ""
        assert normalize_code(None) == ""

    def test_normalize_code_with_space(self):
        """スペース付きコードの正規化テスト"""
        assert normalize_code("  1234  ") == "1234"
        assert normalize_code(" 123 ") == "0123"

    def test_normalize_code_non_numeric(self):
        """非数値コードの正規化テスト"""
        assert normalize_code("ABCD") == "ABCD"  # 数字が含まれない場合はそのまま
        assert normalize_code("AB12CD") == "0012"  # 数字部分を抽出


class TestParseNumber:
    """parse_number関数のテスト"""

    def test_parse_number_basic(self):
        """基本的な数値解析テスト"""
        assert parse_number("123") == 123.0
        assert parse_number(123) == 123.0
        assert parse_number("123.45") == 123.45
        assert parse_number(123.45) == 123.45

    def test_parse_number_with_comma(self):
        """カンマ区切り数値の解析テスト"""
        assert parse_number("1,234") == 1234.0
        assert parse_number("1,234,567") == 1234567.0
        assert parse_number("1,234.56") == 1234.56

    def test_parse_number_negative(self):
        """負の数値の解析テスト"""
        assert parse_number("-123") == -123.0
        assert parse_number("-1,234") == -1234.0
        assert parse_number("△123") == -123.0
        assert parse_number("▲1,234") == -1234.0
        assert parse_number("(123)") == -123.0
        assert parse_number("(1,234)") == -1234.0

    def test_parse_number_percentage(self):
        """パーセント記号付き数値の解析テスト"""
        assert parse_number("12.3%") == 12.3
        assert parse_number("-5.5%") == -5.5
        assert parse_number("△10%") == -10.0

    def test_parse_number_with_quotes(self):
        """引用符付き数値の解析テスト"""
        assert parse_number('"123"') == 123.0
        assert parse_number('"-1,234"') == -1234.0

    def test_parse_number_range(self):
        """範囲表記の解析テスト"""
        assert parse_number("194 ~ 200") == 197.0  # 平均値
        assert parse_number("10~20") == 15.0
        assert parse_number("100 ~ 150") == 125.0

    def test_parse_number_special_values(self):
        """特殊値の解析テスト"""
        assert parse_number("") is None
        assert parse_number(None) is None
        assert parse_number("--") is None
        assert parse_number("↑") is None
        assert parse_number("↓") is None
        assert parse_number("→") is None
        assert parse_number("←") is None
        assert parse_number("-") is None
        assert parse_number("－") is None
        assert parse_number("―") is None

    def test_parse_number_with_default(self):
        """デフォルト値付き解析テスト"""
        assert parse_number("", default=0.0) == 0.0
        assert parse_number("--", default=100.0) == 100.0
        assert parse_number("invalid", default=999.0) == 999.0
        assert parse_number("↑", default=10.0) == 10.0

    def test_parse_number_invalid(self):
        """無効な値の解析テスト"""
        assert parse_number("abc") is None
        assert parse_number("12a34") is None
        assert parse_number("1.2.3") is None

    def test_parse_number_zero(self):
        """ゼロ値の解析テスト"""
        assert parse_number("0") == 0.0
        assert parse_number("0.0") == 0.0
        assert parse_number("0,000") == 0.0


class TestParseDate:
    """parse_date関数のテスト"""

    def test_parse_date_slash_format(self):
        """スラッシュ区切り日付の解析テスト"""
        assert parse_date("2024/01/15") == "2024-01-15"
        assert parse_date("2024/1/5") == "2024-01-05"
        assert parse_date("24/12/31") == "2024-12-31"

    def test_parse_date_hyphen_format(self):
        """ハイフン区切り日付の解析テスト"""
        assert parse_date("2024-01-15") == "2024-01-15"
        assert parse_date("2024-1-5") == "2024-01-05"

    def test_parse_date_japanese_format(self):
        """日本語形式日付の解析テスト"""
        assert parse_date("2024年1月15日") == "2024-01-15"
        assert parse_date("2024年12月31日") == "2024-12-31"

    def test_parse_date_with_time(self):
        """時刻付き日付の解析テスト"""
        assert parse_date("2024/01/15 10:30:45") == "2024-01-15"
        assert parse_date("2024/01/15 09:00:00") == "2024-01-15"

    def test_parse_date_empty(self):
        """空日付の解析テスト"""
        assert parse_date("") is None
        assert parse_date(None) is None

    def test_parse_date_with_space(self):
        """スペース付き日付の解析テスト"""
        assert parse_date("  2024/01/15  ") == "2024-01-15"
        assert parse_date(" 2024-01-15 ") == "2024-01-15"

    def test_parse_date_invalid(self):
        """無効な日付の解析テスト"""
        assert parse_date("invalid date") is None
        assert parse_date("2024/13/01") is None  # 13月は無効
        assert parse_date("2024/01/32") is None  # 32日は無効
        assert parse_date("12345") is None

    def test_parse_date_edge_cases(self):
        """エッジケースの解析テスト"""
        assert parse_date("2024/02/29") == "2024-02-29"  # うるう年
        assert parse_date("2023/02/29") is None  # 平年の2月29日は無効
        assert parse_date("2024/12/31") == "2024-12-31"  # 年末
        assert parse_date("2024/01/01") == "2024-01-01"  # 年始

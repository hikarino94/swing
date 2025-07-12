"""エンコーディング検出機能のテスト"""

from unittest.mock import patch

import pytest

from src.portfolio.csv_parser.encodings import decode_content, detect_encoding


class TestDetectEncoding:
    """detect_encoding関数のテスト"""

    def test_detect_encoding_utf8_bom(self):
        """UTF-8 BOM付きの検出テスト"""
        # UTF-8 BOM + UTF-8エンコードされた「こんにちは」
        content = (
            b"\xef\xbb\xbf\xe3\x81\x93\xe3\x82\x93\xe3\x81\xab\xe3\x81\xa1\xe3\x81\xaf"
        )
        assert detect_encoding(content) == "utf-8-sig"

    @patch("src.portfolio.csv_parser.encodings.chardet.detect")
    def test_detect_encoding_shift_jis(self, mock_detect):
        """Shift-JISの検出テスト"""
        mock_detect.return_value = {"encoding": "shift_jis", "confidence": 0.9}
        content = (
            b"\x82\xb1\x82\xf1\x82\xc9\x82\xbf\x82\xcd"  # Shift-JISの「こんにちは」
        )
        assert detect_encoding(content) == "shift_jis"

    @patch("src.portfolio.csv_parser.encodings.chardet.detect")
    def test_detect_encoding_cp932(self, mock_detect):
        """CP932の検出テスト"""
        mock_detect.return_value = {"encoding": "CP932", "confidence": 0.9}
        content = b"test"
        assert detect_encoding(content) == "CP932"

    @patch("src.portfolio.csv_parser.encodings.chardet.detect")
    def test_detect_encoding_euc_jp(self, mock_detect):
        """EUC-JPの検出テスト"""
        mock_detect.return_value = {"encoding": "euc-jp", "confidence": 0.9}
        content = b"test"
        assert detect_encoding(content) == "euc-jp"

    @patch("src.portfolio.csv_parser.encodings.chardet.detect")
    def test_detect_encoding_iso2022jp(self, mock_detect):
        """ISO-2022-JPの検出テスト"""
        mock_detect.return_value = {"encoding": "iso-2022-jp", "confidence": 0.9}
        content = b"test"
        assert detect_encoding(content) == "iso-2022-jp"

    @patch("src.portfolio.csv_parser.encodings.chardet.detect")
    def test_detect_encoding_utf8(self, mock_detect):
        """UTF-8の検出テスト"""
        mock_detect.return_value = {"encoding": "utf-8", "confidence": 0.9}
        content = b"test"
        assert detect_encoding(content) == "utf-8"

    @patch("src.portfolio.csv_parser.encodings.chardet.detect")
    def test_detect_encoding_utf16(self, mock_detect):
        """UTF-16の検出テスト"""
        mock_detect.return_value = {"encoding": "UTF-16", "confidence": 0.9}
        content = b"test"
        assert detect_encoding(content) == "UTF-16"

    @patch("src.portfolio.csv_parser.encodings.chardet.detect")
    def test_detect_encoding_unknown(self, mock_detect):
        """不明なエンコーディングの検出テスト（デフォルトでshift_jis）"""
        mock_detect.return_value = {"encoding": "unknown-8bit", "confidence": 0.5}
        content = b"test"
        assert detect_encoding(content) == "shift_jis"

    @patch("src.portfolio.csv_parser.encodings.chardet.detect")
    def test_detect_encoding_none(self, mock_detect):
        """エンコーディングがNoneの場合のテスト"""
        mock_detect.return_value = {"encoding": None, "confidence": 0}
        content = b"test"
        assert detect_encoding(content) == "shift_jis"


class TestDecodeContent:
    """decode_content関数のテスト"""

    def test_decode_content_string_input(self):
        """文字列入力の場合はそのまま返すテスト"""
        content = "こんにちは"
        assert decode_content(content) == "こんにちは"

    def test_decode_content_utf8_bom(self):
        """UTF-8 BOM付きのデコードテスト"""
        content = (
            b"\xef\xbb\xbf\xe3\x81\x93\xe3\x82\x93\xe3\x81\xab\xe3\x81\xa1\xe3\x81\xaf"
        )
        result = decode_content(content)
        assert result == "こんにちは"

    @patch("src.portfolio.csv_parser.encodings.detect_encoding")
    def test_decode_content_shift_jis(self, mock_detect):
        """Shift-JISのデコードテスト"""
        mock_detect.return_value = "shift_jis"
        content = (
            b"\x82\xb1\x82\xf1\x82\xc9\x82\xbf\x82\xcd"  # Shift-JISの「こんにちは」
        )
        result = decode_content(content)
        assert result == "こんにちは"

    @patch("src.portfolio.csv_parser.encodings.detect_encoding")
    def test_decode_content_utf8(self, mock_detect):
        """UTF-8のデコードテスト"""
        mock_detect.return_value = "utf-8"
        content = b"\xe3\x81\x93\xe3\x82\x93\xe3\x81\xab\xe3\x81\xa1\xe3\x81\xaf"  # UTF-8の「こんにちは」
        result = decode_content(content)
        assert result == "こんにちは"

    @patch("src.portfolio.csv_parser.encodings.detect_encoding")
    def test_decode_content_with_bom_removal(self, mock_detect):
        """BOM除去のテスト"""
        mock_detect.return_value = "utf-8"
        # BOMありのUTF-8文字列（デコード後）
        content = (
            b"\xef\xbb\xbf\xe3\x81\x93\xe3\x82\x93\xe3\x81\xab\xe3\x81\xa1\xe3\x81\xaf"
        )
        result = decode_content(content)
        # BOMが除去されていることを確認
        assert result == "こんにちは"
        assert not result.startswith("\ufeff")

    @patch("src.portfolio.csv_parser.encodings.detect_encoding")
    def test_decode_content_fallback_utf8_sig(self, mock_detect):
        """フォールバックでUTF-8-SIGでデコード成功するテスト"""
        mock_detect.return_value = "ascii"  # 間違ったエンコーディング
        content = (
            b"\xef\xbb\xbf\xe3\x81\x93\xe3\x82\x93\xe3\x81\xab\xe3\x81\xa1\xe3\x81\xaf"
        )
        result = decode_content(content)
        assert result == "こんにちは"

    @patch("src.portfolio.csv_parser.encodings.detect_encoding")
    def test_decode_content_fallback_shift_jis(self, mock_detect):
        """フォールバックでShift-JISでデコード成功するテスト"""
        mock_detect.return_value = "ascii"  # 間違ったエンコーディング
        content = b"\x82\xb1\x82\xf1\x82\xc9\x82\xbf\x82\xcd"
        result = decode_content(content)
        assert result == "こんにちは"

    @patch("src.portfolio.csv_parser.encodings.detect_encoding")
    def test_decode_content_all_fallback_fail(self, mock_detect):
        """すべてのフォールバックが失敗する場合のテスト"""
        mock_detect.return_value = "ascii"
        # 不正なUTF-8シーケンス（どのエンコーディングでもデコードできない）
        content = b"\x80\x81\x82\x83\x84\x85"

        with pytest.raises(ValueError) as exc_info:
            decode_content(content)
        assert "CSVファイルのエンコーディングを判定できません" in str(exc_info.value)

    def test_decode_content_empty(self):
        """空のバイト列のデコードテスト"""
        content = b""
        result = decode_content(content)
        assert result == ""

    @patch("src.portfolio.csv_parser.encodings.detect_encoding")
    def test_decode_content_ascii(self, mock_detect):
        """ASCIIのデコードテスト"""
        mock_detect.return_value = "ascii"
        content = b"Hello, World!"
        result = decode_content(content)
        assert result == "Hello, World!"

    @patch("src.portfolio.csv_parser.encodings.detect_encoding")
    def test_decode_content_mixed_encoding_fallback(self, mock_detect):
        """混在エンコーディングでフォールバックするテスト"""
        # 最初のエンコーディングが失敗し、CP932で成功する例
        mock_detect.return_value = "utf-8"
        # CP932の「表」という文字
        content = b"\x95\x5c"
        result = decode_content(content)
        assert result == "表"

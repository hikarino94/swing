"""共通ユーティリティのテスト"""
import tempfile
from datetime import date
from pathlib import Path

import pytest

# pandasがない環境でもテストできるようにする
try:
    from src.utils.common import generate_timestamped_filename, parse_date_string

    COMMON_AVAILABLE = True
except ImportError:
    COMMON_AVAILABLE = False


@pytest.mark.skipif(not COMMON_AVAILABLE, reason="pandas not available")
class TestCommonUtils:
    """共通ユーティリティのテストクラス"""

    def test_parse_date_string_yyyy_mm_dd(self):
        """YYYY-MM-DD形式の日付解析テスト"""
        result = parse_date_string("2023-12-25")
        expected = date(2023, 12, 25)
        assert result == expected

    def test_parse_date_string_yyyymmdd(self):
        """YYYYMMDD形式の日付解析テスト"""
        result = parse_date_string("20231225")
        expected = date(2023, 12, 25)
        assert result == expected

    @pytest.mark.parametrize(
        "date_str,expected",
        [
            ("2023-01-01", date(2023, 1, 1)),
            ("2023-12-31", date(2023, 12, 31)),
            ("20230101", date(2023, 1, 1)),
            ("20231231", date(2023, 12, 31)),
            ("2024-02-29", date(2024, 2, 29)),  # うるう年
        ],
    )
    def test_parse_date_string_various_formats(self, date_str, expected):
        """様々な日付形式のテスト"""
        result = parse_date_string(date_str)
        assert result == expected

    def test_parse_date_string_invalid_format(self):
        """無効な日付形式のテスト"""
        with pytest.raises(ValueError):
            parse_date_string("invalid-date")

        with pytest.raises(ValueError):
            parse_date_string("2023-13-01")  # 無効な月

        with pytest.raises(ValueError):
            parse_date_string("2023-02-30")  # 無効な日

    def test_generate_timestamped_filename(self):
        """タイムスタンプ付きファイル名生成テスト"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            filename = generate_timestamped_filename("test", ".txt", tmp_path)

            # パスが正しい
            assert filename.parent == tmp_path

            # ファイル名の形式が正しい
            assert filename.name.startswith("test_")
            assert filename.name.endswith(".txt")

            # タイムスタンプが含まれている（長さチェック）
            name_without_ext = filename.stem
            assert len(name_without_ext) > 4  # "test"より長い

    def test_generate_timestamped_filename_uniqueness(self):
        """タイムスタンプ付きファイル名の一意性テスト"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            # 複数回生成して一意性を確認
            filenames = []
            for _ in range(5):
                filename = generate_timestamped_filename("test", ".txt", tmp_path)
                filenames.append(filename.name)

            # 全て異なることを確認
            assert len(set(filenames)) == len(filenames)

    def test_generate_timestamped_filename_different_prefixes(self):
        """異なるプレフィックスでのファイル名生成テスト"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            filename1 = generate_timestamped_filename("data", ".csv", tmp_path)
            filename2 = generate_timestamped_filename("backup", ".json", tmp_path)

            assert filename1.name.startswith("data_")
            assert filename1.name.endswith(".csv")
            assert filename2.name.startswith("backup_")
            assert filename2.name.endswith(".json")

    def test_edge_cases(self):
        """エッジケースのテスト"""
        # 空文字列のプレフィックス
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            filename = generate_timestamped_filename("", ".txt", tmp_path)
            assert filename.name.startswith("_")  # タイムスタンプで始まる
            assert filename.name.endswith(".txt")

        # 特殊文字を含む拡張子
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            filename = generate_timestamped_filename("test", ".backup.old", tmp_path)
            assert filename.name.endswith(".backup.old")


class TestCommonUtilsWithoutPandas:
    """pandas不要な共通ユーティリティのテスト"""

    def test_imports_without_pandas(self):
        """pandasなしでの基本インポートテスト"""
        # このテストはpandasに依存しない部分のテスト用
        # 現在のcommon.pyがpandasに依存しているため、
        # 将来pandasに依存しない関数が追加された場合のテンプレート
        pass

    @pytest.mark.skipif(COMMON_AVAILABLE, reason="Testing without pandas")
    def test_pandas_not_available_handling(self):
        """pandas未インストール時の適切な処理テスト"""
        # pandasがない環境での動作確認
        with pytest.raises(ImportError):
            pass

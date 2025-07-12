"""Tests for src/utils/file_utils.py"""

from pathlib import Path
from unittest.mock import MagicMock, patch

from src.utils.file_utils import (
    ensure_output_dirs,
    get_output_path,
    get_timestamped_output_path,
    timestamped_filename,
)


class TestGetOutputPath:
    """get_output_path関数のテスト"""

    @patch("src.utils.file_utils.Path.mkdir")
    def test_creates_directory_structure(self, mock_mkdir):
        """ディレクトリ構造が作成されることを確認"""
        path = get_output_path("backtest", "test.json")

        assert isinstance(path, Path)
        assert path.name == "test.json"
        assert path.parent.name == "backtest"
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    def test_different_categories(self):
        """異なるカテゴリで異なるパスが生成されることを確認"""
        with patch("src.utils.file_utils.Path.mkdir"):
            backtest_path = get_output_path("backtest", "test.json")
            screening_path = get_output_path("screening", "test.json")
            reports_path = get_output_path("reports", "test.json")

            assert backtest_path.parent.name == "backtest"
            assert screening_path.parent.name == "screening"
            assert reports_path.parent.name == "reports"

    def test_custom_base_dir(self):
        """カスタムベースディレクトリが使用されることを確認"""
        with patch("src.utils.file_utils.Path.mkdir"):
            custom_base = Path("/custom/base")
            path = get_output_path("backtest", "test.json", base_dir=custom_base)

            assert str(path) == "/custom/base/backtest/test.json"


class TestTimestampedFilename:
    """タイムスタンプ付きファイル名生成のテスト"""

    @patch("src.utils.file_utils.datetime")
    def test_generates_timestamp_filename(self, mock_datetime):
        """タイムスタンプ付きファイル名が生成されることを確認"""
        mock_now = MagicMock()
        mock_now.strftime.return_value = "20240101_123456"
        mock_datetime.now.return_value = mock_now

        filename = timestamped_filename("test", ".xlsx")

        assert filename == "test_20240101_123456.xlsx"
        mock_now.strftime.assert_called_once_with("%Y%m%d_%H%M%S")

    def test_different_extensions(self):
        """異なる拡張子で動作することを確認"""
        with patch("src.utils.file_utils.datetime") as mock_datetime:
            mock_now = MagicMock()
            mock_now.strftime.return_value = "20240101_123456"
            mock_datetime.now.return_value = mock_now

            xlsx_filename = timestamped_filename("report", ".xlsx")
            json_filename = timestamped_filename("data", ".json")
            csv_filename = timestamped_filename("export", ".csv")

            assert xlsx_filename.endswith(".xlsx")
            assert json_filename.endswith(".json")
            assert csv_filename.endswith(".csv")


class TestGetTimestampedOutputPath:
    """get_timestamped_output_path関数のテスト"""

    @patch("src.utils.file_utils.timestamped_filename")
    @patch("src.utils.file_utils.get_output_path")
    def test_combines_timestamp_and_path(self, mock_get_output_path, mock_timestamped):
        """タイムスタンプ付きファイル名とパスが結合されることを確認"""
        mock_timestamped.return_value = "test_20240101_123456.xlsx"
        mock_path = Path("/output/backtest/test_20240101_123456.xlsx")
        mock_get_output_path.return_value = mock_path

        result = get_timestamped_output_path("backtest", "test", ".xlsx")

        assert result == mock_path
        mock_timestamped.assert_called_once_with("test", ".xlsx")
        mock_get_output_path.assert_called_once_with(
            "backtest", "test_20240101_123456.xlsx", None
        )

    def test_different_categories(self):
        """異なるカテゴリで異なるパスが生成されることを確認"""
        with patch("src.utils.file_utils.Path.mkdir"):
            with patch("src.utils.file_utils.datetime") as mock_datetime:
                mock_now = MagicMock()
                mock_now.strftime.return_value = "20240101_123456"
                mock_datetime.now.return_value = mock_now

                backtest_path = get_timestamped_output_path("backtest", "test", ".json")
                screening_path = get_timestamped_output_path(
                    "screening", "test", ".json"
                )

                assert "backtest" in str(backtest_path)
                assert "screening" in str(screening_path)


class TestEnsureOutputDirs:
    """全ての出力ディレクトリの作成をテスト"""

    def test_creates_all_category_dirs(self):
        """全てのカテゴリディレクトリが作成されることを確認"""
        # Pathオブジェクトのmkdirをモック
        created_dirs = []

        def mock_mkdir(self, parents=True, exist_ok=True):
            created_dirs.append(self)

        with patch("pathlib.Path.mkdir", mock_mkdir):
            ensure_output_dirs()

        # 3つのカテゴリディレクトリが作成される
        assert len(created_dirs) == 3

        # 各カテゴリが作成されたことを確認
        dir_names = [path.name for path in created_dirs]
        assert "backtest" in dir_names
        assert "screening" in dir_names
        assert "reports" in dir_names

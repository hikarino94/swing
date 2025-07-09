"""Test suite for src/utils/file_utils.py module."""

import sys
from pathlib import Path
from unittest import mock

import pytest

# プロジェクトルートをPYTHONPATHに追加
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.file_utils import (
    ensure_output_dirs,
    get_output_path,
    get_timestamped_output_path,
    timestamped_filename,
)


class TestGetOutputPath:
    """Test get_output_path function."""

    def test_default_base_dir(self, tmp_path, monkeypatch):
        """デフォルトのベースディレクトリでの動作確認"""
        # file_utils.pyの__file__を偽装してプロジェクトルートを制御
        fake_module_path = tmp_path / "src" / "utils" / "file_utils.py"
        fake_module_path.parent.mkdir(parents=True)
        fake_module_path.touch()

        # __file__をモック
        with mock.patch("src.utils.file_utils.__file__", str(fake_module_path)):
            path = get_output_path("backtest", "result.json")

            expected_path = tmp_path / "data" / "output" / "backtest" / "result.json"
            assert path == expected_path

            # ディレクトリが作成されたことを確認
            assert path.parent.exists()

    def test_custom_base_dir(self, tmp_path):
        """カスタムベースディレクトリの指定"""
        custom_base = tmp_path / "custom_output"

        path = get_output_path("screening", "stocks.xlsx", base_dir=custom_base)

        expected_path = custom_base / "screening" / "stocks.xlsx"
        assert path == expected_path

        # ディレクトリが作成されたことを確認
        assert path.parent.exists()

    def test_category_validation(self, tmp_path):
        """カテゴリの値をテスト（型ヒントの範囲内）"""
        categories = ["backtest", "screening", "reports"]

        for category in categories:
            path = get_output_path(category, "test.txt", base_dir=tmp_path)  # type: ignore
            assert path.parent.name == category
            assert path.name == "test.txt"
            assert path.parent.exists()

    def test_nested_directory_creation(self, tmp_path):
        """ネストしたディレクトリの作成"""
        base_dir = tmp_path / "deep" / "nested" / "path"

        path = get_output_path("reports", "report.pdf", base_dir=base_dir)

        assert path.parent.exists()
        assert path == base_dir / "reports" / "report.pdf"


class TestTimestampedFilename:
    """Test timestamped_filename function."""

    @mock.patch("src.utils.file_utils.datetime")
    def test_timestamped_filename(self, mock_datetime):
        """タイムスタンプ付きファイル名の生成"""
        # 固定のタイムスタンプを設定
        mock_now = mock.MagicMock()
        mock_now.strftime.return_value = "20240115_143052"
        mock_datetime.now.return_value = mock_now

        filename = timestamped_filename("fundamental", ".xlsx")

        assert filename == "fundamental_20240115_143052.xlsx"
        mock_now.strftime.assert_called_once_with("%Y%m%d_%H%M%S")

    def test_real_timestamp_format(self):
        """実際のタイムスタンプ形式を確認"""
        filename = timestamped_filename("test", ".csv")

        # ファイル名の形式を検証
        parts = filename.split("_")
        assert len(parts) == 3  # test_YYYYMMDD_HHMMSS.csv
        assert parts[0] == "test"

        # 日付部分の検証
        date_part = parts[1]
        assert len(date_part) == 8
        assert date_part.isdigit()

        # 時間部分の検証（拡張子付き）
        time_part = parts[2]
        assert time_part.endswith(".csv")
        time_without_ext = time_part[:-4]
        assert len(time_without_ext) == 6
        assert time_without_ext.isdigit()

    def test_different_extensions(self):
        """様々な拡張子でのテスト"""
        extensions = [".xlsx", ".json", ".csv", ".pdf", ".txt", ""]

        for ext in extensions:
            filename = timestamped_filename("base", ext)
            assert filename.startswith("base_")
            assert filename.endswith(ext)


class TestGetTimestampedOutputPath:
    """Test get_timestamped_output_path function."""

    @mock.patch("src.utils.file_utils.datetime")
    def test_timestamped_output_path(self, mock_datetime, tmp_path):
        """タイムスタンプ付き出力パスの生成"""
        # 固定のタイムスタンプを設定
        mock_now = mock.MagicMock()
        mock_now.strftime.return_value = "20240115_143052"
        mock_datetime.now.return_value = mock_now

        path = get_timestamped_output_path(
            "backtest", "result", ".json", base_dir=tmp_path
        )

        expected_path = tmp_path / "backtest" / "result_20240115_143052.json"
        assert path == expected_path
        assert path.parent.exists()

    def test_integration_with_get_output_path(self, tmp_path):
        """get_output_pathとの統合テスト"""
        # 両方の関数が同じベースディレクトリを使用することを確認
        static_path = get_output_path("screening", "static.csv", base_dir=tmp_path)
        timestamped_path = get_timestamped_output_path(
            "screening", "dynamic", ".csv", base_dir=tmp_path
        )

        # 親ディレクトリが同じ
        assert static_path.parent == timestamped_path.parent

        # ファイル名が異なる
        assert static_path.name == "static.csv"
        assert timestamped_path.name.startswith("dynamic_")
        assert timestamped_path.name.endswith(".csv")


class TestEnsureOutputDirs:
    """Test ensure_output_dirs function."""

    def test_ensure_all_directories(self, tmp_path):
        """すべての出力ディレクトリが作成されることを確認"""
        # file_utils.pyの__file__を偽装
        fake_module_path = tmp_path / "src" / "utils" / "file_utils.py"
        fake_module_path.parent.mkdir(parents=True)
        fake_module_path.touch()

        with mock.patch("src.utils.file_utils.__file__", new=str(fake_module_path)):
            ensure_output_dirs()

            # すべてのカテゴリディレクトリが作成されたことを確認
            base_output_dir = tmp_path / "data" / "output"
            for category in ["backtest", "screening", "reports"]:
                category_dir = base_output_dir / category
                assert category_dir.exists()
                assert category_dir.is_dir()

    def test_idempotent_directory_creation(self, tmp_path):
        """ディレクトリ作成の冪等性をテスト"""
        fake_module_path = tmp_path / "src" / "utils" / "file_utils.py"
        fake_module_path.parent.mkdir(parents=True)
        fake_module_path.touch()

        with mock.patch("src.utils.file_utils.__file__", new=str(fake_module_path)):
            # 2回実行してもエラーにならない
            ensure_output_dirs()
            ensure_output_dirs()

            # ディレクトリが存在することを確認
            base_output_dir = tmp_path / "data" / "output"
            assert (base_output_dir / "backtest").exists()


class TestIntegration:
    """Integration tests for file_utils module."""

    def test_full_workflow(self, tmp_path):
        """完全なワークフローのテスト"""
        # 1. ディレクトリの作成
        base_dir = tmp_path / "project"

        # 2. タイムスタンプ付きファイルパスの生成
        path1 = get_timestamped_output_path(
            "screening", "fundamental", ".xlsx", base_dir=base_dir
        )

        # 3. 静的ファイルパスの生成
        path2 = get_output_path("screening", "config.json", base_dir=base_dir)

        # 4. ファイルが同じディレクトリに配置される
        assert path1.parent == path2.parent

        # 5. 実際にファイルを作成できることを確認
        path1.write_text("test1")
        path2.write_text("test2")

        assert path1.read_text() == "test1"
        assert path2.read_text() == "test2"

    def test_multiple_categories(self, tmp_path):
        """複数カテゴリでの動作確認"""
        categories = ["backtest", "screening", "reports"]

        for category in categories:
            # タイムスタンプ付きパス
            ts_path = get_timestamped_output_path(
                category, f"{category}_result", ".json", base_dir=tmp_path  # type: ignore
            )

            # 静的パス
            static_path = get_output_path(
                category, "summary.txt", base_dir=tmp_path  # type: ignore
            )

            # 両方のパスが正しいカテゴリディレクトリを指している
            assert ts_path.parent.name == category
            assert static_path.parent.name == category

            # ディレクトリが作成されている
            assert ts_path.parent.exists()
            assert static_path.parent.exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

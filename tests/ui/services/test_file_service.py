"""ui.services.file_serviceのテスト"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from src.ui.services.file_service import FileService


class TestGetOutputDir:
    """get_output_dirメソッドのテスト"""

    def test_get_output_dir_returns_correct_path(self):
        """正しい出力ディレクトリパスを返すテスト"""
        output_dir = FileService.get_output_dir()

        assert isinstance(output_dir, Path)
        assert output_dir.name == "output"
        assert output_dir.parent.name == "data"
        assert "swing" in str(output_dir)


class TestCreateTimestampedPath:
    """create_timestamped_pathメソッドのテスト"""

    @patch("src.ui.services.file_service.get_timestamped_output_path")
    def test_create_timestamped_path(self, mock_get_path):
        """タイムスタンプ付きパス生成のテスト"""
        mock_path = Path("/data/output/screening/test_20240115_120000.xlsx")
        mock_get_path.return_value = mock_path

        result = FileService.create_timestamped_path("screening", "test", ".xlsx")

        assert result == mock_path
        mock_get_path.assert_called_once_with("screening", "test", ".xlsx")

    @patch("src.ui.services.file_service.get_timestamped_output_path")
    def test_create_timestamped_path_various_categories(self, mock_get_path):
        """異なるカテゴリでのパス生成テスト"""
        categories = ["backtest", "screening", "reports"]

        for category in categories:
            mock_path = Path(f"/data/output/{category}/test.json")
            mock_get_path.return_value = mock_path

            result = FileService.create_timestamped_path(category, "test", ".json")

            assert result == mock_path
            mock_get_path.assert_called_with(category, "test", ".json")


class TestListResultFiles:
    """list_result_filesメソッドのテスト"""

    @patch("src.ui.services.file_service.FileService.get_output_dir")
    def test_list_result_files_no_directory(self, mock_get_output_dir):
        """出力ディレクトリが存在しない場合のテスト"""
        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = False
        mock_get_output_dir.return_value = mock_path

        result = FileService.list_result_files()

        assert result == []

    @patch("src.ui.services.file_service.FileService.get_output_dir")
    @patch("src.ui.services.file_service.datetime")
    def test_list_result_files_with_files(self, mock_datetime, mock_get_output_dir):
        """ファイルが存在する場合のテスト"""
        # 現在時刻のモック
        mock_now = datetime(2024, 1, 15, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        mock_datetime.fromtimestamp.side_effect = datetime.fromtimestamp

        # ディレクトリ構造のモック
        mock_output_dir = MagicMock(spec=Path)
        mock_output_dir.exists.return_value = True
        mock_get_output_dir.return_value = mock_output_dir

        # カテゴリディレクトリのモック
        mock_screening_dir = MagicMock(spec=Path)
        mock_screening_dir.exists.return_value = True

        # ファイルのモック
        mock_file1 = MagicMock(spec=Path)
        mock_file1.is_file.return_value = True
        mock_file1.name = "result1.xlsx"
        mock_file1.suffix = ".xlsx"
        mock_file1.relative_to.return_value = Path("screening/result1.xlsx")

        mock_stat1 = MagicMock()
        mock_stat1.st_size = 1024
        mock_stat1.st_mtime = (mock_now - timedelta(days=1)).timestamp()
        mock_file1.stat.return_value = mock_stat1

        mock_file2 = MagicMock(spec=Path)
        mock_file2.is_file.return_value = True
        mock_file2.name = "result2.json"
        mock_file2.suffix = ".json"
        mock_file2.relative_to.return_value = Path("screening/result2.json")

        mock_stat2 = MagicMock()
        mock_stat2.st_size = 2048
        mock_stat2.st_mtime = (mock_now - timedelta(days=2)).timestamp()
        mock_file2.stat.return_value = mock_stat2

        # 古いファイル（7日以上前）
        mock_old_file = MagicMock(spec=Path)
        mock_old_file.is_file.return_value = True
        mock_stat_old = MagicMock()
        mock_stat_old.st_mtime = (mock_now - timedelta(days=8)).timestamp()
        mock_old_file.stat.return_value = mock_stat_old

        mock_screening_dir.glob.return_value = [mock_file1, mock_file2, mock_old_file]

        # ディレクトリ構造の設定
        def mock_truediv(self, path):
            if path == "screening":
                return mock_screening_dir
            else:
                mock_other = MagicMock()
                mock_other.exists.return_value = False
                return mock_other

        mock_output_dir.__truediv__ = mock_truediv

        # テスト実行
        result = FileService.list_result_files(category="screening", days=7)

        # 検証
        assert len(result) == 2
        assert result[0]["name"] == "result1.xlsx"
        assert result[0]["category"] == "screening"
        assert result[0]["size"] == 1024
        assert result[1]["name"] == "result2.json"

    @patch("src.ui.services.file_service.FileService.get_output_dir")
    def test_list_result_files_all_categories(self, mock_get_output_dir):
        """全カテゴリのファイル取得テスト"""
        mock_output_dir = MagicMock(spec=Path)
        mock_output_dir.exists.return_value = True
        mock_get_output_dir.return_value = mock_output_dir

        # 各カテゴリディレクトリのモック
        category_dirs = {}
        for cat in ["screening", "backtest", "analysis"]:
            mock_dir = MagicMock(spec=Path)
            mock_dir.exists.return_value = True
            mock_dir.glob.return_value = []
            category_dirs[cat] = mock_dir

        mock_output_dir.__truediv__.side_effect = lambda x: category_dirs.get(
            x, MagicMock(exists=lambda: False)
        )

        # テスト実行
        FileService.list_result_files()

        # 全カテゴリがチェックされたことを確認
        for cat in ["screening", "backtest", "analysis"]:
            category_dirs[cat].glob.assert_called_once_with("**/*")


class TestReadJsonFile:
    """read_json_fileメソッドのテスト"""

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"key": "value", "number": 123}',
    )
    def test_read_json_file_success(self, mock_file):
        """JSONファイル読み込み成功のテスト"""
        filepath = Path("/test/data.json")

        result = FileService.read_json_file(filepath)

        assert result == {"key": "value", "number": 123}
        mock_file.assert_called_once_with(filepath, encoding="utf-8")

    @patch("builtins.open", new_callable=mock_open, read_data="[]")
    def test_read_json_file_empty_array(self, mock_file):
        """空の配列JSONのテスト"""
        filepath = Path("/test/empty.json")

        result = FileService.read_json_file(filepath)

        assert result == []

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_read_json_file_not_found(self, mock_file):
        """ファイルが見つからない場合のテスト"""
        filepath = Path("/test/nonexistent.json")

        with pytest.raises(FileNotFoundError):
            FileService.read_json_file(filepath)

    @patch("builtins.open", new_callable=mock_open, read_data="invalid json")
    def test_read_json_file_invalid_json(self, mock_file):
        """不正なJSONの場合のテスト"""
        filepath = Path("/test/invalid.json")

        with pytest.raises(json.JSONDecodeError):
            FileService.read_json_file(filepath)


class TestGetSafeDownloadPath:
    """get_safe_download_pathメソッドのテスト"""

    @patch("src.ui.services.file_service.FileService.get_output_dir")
    def test_get_safe_download_path_valid(self, mock_get_output_dir):
        """有効なパスの場合のテスト"""
        mock_output_dir = Path("/data/output")
        mock_get_output_dir.return_value = mock_output_dir

        # 正常なファイルパス
        with patch.object(Path, "resolve") as mock_resolve:
            with patch.object(Path, "exists") as mock_exists:
                with patch.object(Path, "is_file") as mock_is_file:
                    mock_resolve.return_value = Path(
                        "/data/output/screening/result.xlsx"
                    )
                    mock_exists.return_value = True
                    mock_is_file.return_value = True

                    result = FileService.get_safe_download_path("screening/result.xlsx")

                    assert result == Path("/data/output/screening/result.xlsx")

    @patch("src.ui.services.file_service.FileService.get_output_dir")
    @patch("src.ui.services.file_service.logger")
    def test_get_safe_download_path_directory_traversal(
        self, mock_logger, mock_get_output_dir
    ):
        """ディレクトリトラバーサル攻撃の場合のテスト"""
        mock_output_dir = Path("/data/output")
        mock_get_output_dir.return_value = mock_output_dir

        # 悪意のあるパス
        with patch.object(Path, "resolve") as mock_resolve:
            mock_resolve.return_value = Path("/etc/passwd")

            result = FileService.get_safe_download_path("../../etc/passwd")

            assert result is None
            mock_logger.warning.assert_called_once()

    @patch("src.ui.services.file_service.FileService.get_output_dir")
    def test_get_safe_download_path_nonexistent(self, mock_get_output_dir):
        """存在しないファイルの場合のテスト"""
        mock_output_dir = Path("/data/output")
        mock_get_output_dir.return_value = mock_output_dir

        with patch.object(Path, "resolve") as mock_resolve:
            with patch.object(Path, "exists") as mock_exists:
                mock_resolve.return_value = Path("/data/output/nonexistent.txt")
                mock_exists.return_value = False

                result = FileService.get_safe_download_path("nonexistent.txt")

                assert result is None

    @patch("src.ui.services.file_service.FileService.get_output_dir")
    @patch("src.ui.services.file_service.logger")
    def test_get_safe_download_path_exception(self, mock_logger, mock_get_output_dir):
        """例外が発生した場合のテスト"""
        mock_get_output_dir.return_value = Path("/data/output")

        with patch.object(Path, "resolve", side_effect=Exception("Path error")):
            result = FileService.get_safe_download_path("bad/path")

            assert result is None
            mock_logger.error.assert_called_once()


class TestSaveThresholds:
    """save_thresholdsメソッドのテスト"""

    @patch("builtins.open", new_callable=mock_open)
    @patch("src.ui.services.file_service.Path")
    @patch("src.ui.services.file_service.logger")
    def test_save_thresholds_success(self, mock_logger, mock_path_class, mock_file):
        """閾値保存成功のテスト"""
        # Pathのモック設定
        mock_path = MagicMock()
        mock_parent = MagicMock()
        mock_path.parent = mock_parent
        mock_path_class.return_value = mock_path

        thresholds = {"eps_yoy_fy": 0.1, "op_margin_delta": 0.05, "leverage": 1.0}

        result = FileService.save_thresholds(thresholds)

        assert result is True
        mock_parent.mkdir.assert_called_once_with(exist_ok=True)
        mock_file.assert_called_once_with(mock_path, "w", encoding="utf-8")

        # JSON書き込みの確認
        handle = mock_file()
        written_data = "".join(call.args[0] for call in handle.write.call_args_list)
        assert "eps_yoy_fy" in written_data

        mock_logger.info.assert_called_once_with("閾値設定を保存しました")

    @patch("builtins.open", side_effect=Exception("Write error"))
    @patch("src.ui.services.file_service.logger")
    def test_save_thresholds_failure(self, mock_logger, mock_file):
        """閾値保存失敗のテスト"""
        thresholds = {"test": 123}

        result = FileService.save_thresholds(thresholds)

        assert result is False
        mock_logger.error.assert_called_once()


class TestLoadThresholds:
    """load_thresholdsメソッドのテスト"""

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"eps_yoy_fy": 0.1, "leverage": 1.0}',
    )
    @patch("src.ui.services.file_service.Path")
    def test_load_thresholds_success(self, mock_path_class, mock_file):
        """閾値読み込み成功のテスト"""
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path_class.return_value = mock_path

        result = FileService.load_thresholds()

        assert result == {"eps_yoy_fy": 0.1, "leverage": 1.0}
        mock_file.assert_called_once_with(mock_path, encoding="utf-8")

    @patch("src.ui.services.file_service.Path")
    def test_load_thresholds_file_not_exists(self, mock_path_class):
        """ファイルが存在しない場合のテスト"""
        mock_path = MagicMock()
        mock_path.exists.return_value = False
        mock_path_class.return_value = mock_path

        result = FileService.load_thresholds()

        assert result == {}

    @patch("builtins.open", side_effect=Exception("Read error"))
    @patch("src.ui.services.file_service.Path")
    @patch("src.ui.services.file_service.logger")
    def test_load_thresholds_exception(self, mock_logger, mock_path_class, mock_file):
        """読み込み時の例外処理テスト"""
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_path_class.return_value = mock_path

        result = FileService.load_thresholds()

        assert result == {}
        mock_logger.error.assert_called_once()


class TestFileServiceIntegration:
    """FileServiceの統合テスト"""

    @patch("src.ui.services.file_service.get_timestamped_output_path")
    @patch("builtins.open", new_callable=mock_open)
    def test_create_and_read_flow(self, mock_file, mock_get_path):
        """ファイル作成と読み込みの統合テスト"""
        # パス生成
        mock_path = Path("/data/output/screening/test_20240115.json")
        mock_get_path.return_value = mock_path

        path = FileService.create_timestamped_path("screening", "test", ".json")
        assert path == mock_path

        # ファイル読み込み
        mock_file.return_value.read.return_value = '{"result": "success"}'

        # open()を再度モックして読み込みをシミュレート
        with patch("builtins.open", mock_open(read_data='{"result": "success"}')):
            data = FileService.read_json_file(path)
            assert data == {"result": "success"}

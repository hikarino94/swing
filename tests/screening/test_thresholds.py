"""thresholds.pyのテスト"""

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from screening.thresholds import (
    _load_from_json,
    load_thresholds,
    log_thresholds,
)


class TestThresholds:
    """thresholds.pyのテスト"""

    def test_load_from_json_file_exists(self):
        """JSONファイルが存在する場合の読み込みテスト"""
        test_data = {"EPS_YOY_MIN": 0.5, "CF_QUALITY_MIN": 0.9, "NEW_THRESHOLD": 100.0}
        json_content = json.dumps(test_data)

        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = True
        mock_path.open.return_value.__enter__.return_value.read.return_value = (
            json_content
        )

        result = _load_from_json(mock_path)

        assert result == test_data
        mock_path.open.assert_called_once_with("r", encoding="utf-8")

    def test_load_from_json_file_not_exists(self):
        """JSONファイルが存在しない場合のテスト"""
        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = False

        result = _load_from_json(mock_path)

        assert result == {}
        mock_path.open.assert_not_called()

    def test_load_from_json_invalid_json(self, caplog):
        """無効なJSONファイルの場合のテスト"""
        invalid_json = "{ invalid json content"

        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = True
        mock_path.__str__.return_value = "test.json"

        with patch("builtins.open", mock_open(read_data=invalid_json)):
            with caplog.at_level(logging.WARNING):
                result = _load_from_json(mock_path)

        assert result == {}
        assert "Failed to load" in caplog.text

    def test_load_from_json_io_error(self, caplog):
        """ファイル読み込みエラーの場合のテスト"""
        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = True
        mock_path.__str__.return_value = "test.json"
        mock_path.open.side_effect = OSError("Permission denied")

        with caplog.at_level(logging.WARNING):
            result = _load_from_json(mock_path)

        assert result == {}
        assert "Failed to load" in caplog.text

    def test_load_thresholds_default_values(self):
        """デフォルト値のみの場合のテスト"""
        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = False

        with patch("screening.thresholds.config.get_file_path", return_value=mock_path):
            with patch("screening.thresholds._load_from_json", return_value={}):
                result = load_thresholds()

        # デフォルト値が返される
        assert result["EPS_YOY_MIN"] == 0.30
        assert result["CF_QUALITY_MIN"] == 0.8
        assert result["ETA_DELTA_MIN"] == 0.0
        assert result["TREASURY_DELTA_MAX"] == 0.0
        assert result["RSI_THRESHOLD"] == 50
        assert result["ADX_THRESHOLD"] == 20
        assert result["OVERHEAT_FACTOR"] == 1.1
        assert result["OVERSOLD_FACTOR"] == 0.95
        assert result["SIGNAL_COUNT_MIN"] == 3
        assert result["SHORT_SIGNAL_COUNT_MIN"] == 4
        assert result["FIRST_LOOKBACK_DAYS"] == 30
        assert len(result) == 11

    def test_load_thresholds_with_custom_values(self):
        """カスタム値で上書きされる場合のテスト"""
        custom_values = {
            "EPS_YOY_MIN": 0.5,
            "CF_QUALITY_MIN": 0.9,
            "NEW_CUSTOM_VALUE": 999.0,
        }

        mock_path = MagicMock(spec=Path)

        with patch("screening.thresholds.config.get_file_path", return_value=mock_path):
            with patch(
                "screening.thresholds._load_from_json", return_value=custom_values
            ):
                result = load_thresholds()

        # カスタム値で上書きされる
        assert result["EPS_YOY_MIN"] == 0.5
        assert result["CF_QUALITY_MIN"] == 0.9
        # デフォルト値は保持される
        assert result["ETA_DELTA_MIN"] == 0.0
        # 新しい値も追加される
        assert result["NEW_CUSTOM_VALUE"] == 999.0

    def test_load_thresholds_with_custom_path(self):
        """カスタムパスを指定した場合のテスト"""
        custom_path = Path("/custom/path/thresholds.json")
        custom_values = {"EPS_YOY_MIN": 0.7}

        with patch(
            "screening.thresholds._load_from_json", return_value=custom_values
        ) as mock_load:
            result = load_thresholds(custom_path)

        mock_load.assert_called_once_with(custom_path)
        assert result["EPS_YOY_MIN"] == 0.7

    def test_load_thresholds_logging(self, caplog):
        """ログ出力のテスト"""
        mock_path = MagicMock(spec=Path)
        mock_path.__str__.return_value = "/path/to/thresholds.json"

        with patch("screening.thresholds.config.get_file_path", return_value=mock_path):
            with patch("screening.thresholds._load_from_json", return_value={}):
                with caplog.at_level(logging.INFO):
                    load_thresholds()

        assert "Thresholds loaded from /path/to/thresholds.json" in caplog.text

    def test_log_thresholds_default_logger(self, caplog):
        """デフォルトロガーでのlog_thresholdsテスト"""
        # モジュールレベルの変数をモック
        with patch("screening.thresholds.EPS_YOY_MIN", 0.35):
            with patch("screening.thresholds.CF_QUALITY_MIN", 0.85):
                with patch("screening.thresholds.ETA_DELTA_MIN", 0.01):
                    with patch("screening.thresholds.TREASURY_DELTA_MAX", 0.02):
                        with patch("screening.thresholds.RSI_THRESHOLD", 55):
                            with patch("screening.thresholds.ADX_THRESHOLD", 25):
                                with patch(
                                    "screening.thresholds.OVERHEAT_FACTOR", 1.15
                                ):
                                    with patch(
                                        "screening.thresholds.OVERSOLD_FACTOR", 0.90
                                    ):
                                        with patch(
                                            "screening.thresholds.SIGNAL_COUNT_MIN", 4
                                        ):
                                            with patch(
                                                "screening.thresholds.SHORT_SIGNAL_COUNT_MIN",
                                                5,
                                            ):
                                                with patch(
                                                    "screening.thresholds.FIRST_LOOKBACK_DAYS",
                                                    35,
                                                ):
                                                    with caplog.at_level(logging.INFO):
                                                        log_thresholds()

        log_text = caplog.text
        assert "EPS_YOY_MIN=0.35" in log_text
        assert "CF_QUALITY_MIN=0.85" in log_text
        assert "RSI_THRESHOLD=55" in log_text
        assert "FIRST_LOOKBACK_DAYS=35" in log_text

    def test_log_thresholds_custom_logger(self):
        """カスタムロガーでのlog_thresholdsテスト"""
        custom_logger = MagicMock(spec=logging.Logger)

        # モジュールレベルの変数をモック
        with patch("screening.thresholds.EPS_YOY_MIN", 0.4):
            with patch("screening.thresholds.CF_QUALITY_MIN", 0.85):
                with patch("screening.thresholds.ETA_DELTA_MIN", 0.0):
                    with patch("screening.thresholds.TREASURY_DELTA_MAX", 0.0):
                        with patch("screening.thresholds.RSI_THRESHOLD", 50):
                            with patch("screening.thresholds.ADX_THRESHOLD", 20):
                                with patch("screening.thresholds.OVERHEAT_FACTOR", 1.1):
                                    with patch(
                                        "screening.thresholds.OVERSOLD_FACTOR", 0.95
                                    ):
                                        with patch(
                                            "screening.thresholds.SIGNAL_COUNT_MIN", 3
                                        ):
                                            with patch(
                                                "screening.thresholds.SHORT_SIGNAL_COUNT_MIN",
                                                4,
                                            ):
                                                with patch(
                                                    "screening.thresholds.FIRST_LOOKBACK_DAYS",
                                                    30,
                                                ):
                                                    log_thresholds(custom_logger)

        custom_logger.info.assert_called_once()
        call_args = custom_logger.info.call_args[0]
        assert "EPS_YOY_MIN=%s" in call_args[0]
        assert 0.4 in call_args  # EPS_YOY_MIN value

    def test_module_level_variables(self):
        """モジュールレベル変数の初期化テスト"""
        # 現在の値を確認（モジュールインポート時に既に設定されている）
        from screening import thresholds

        # デフォルト値が設定されていることを確認
        assert hasattr(thresholds, "EPS_YOY_MIN")
        assert hasattr(thresholds, "CF_QUALITY_MIN")
        assert hasattr(thresholds, "RSI_THRESHOLD")
        assert hasattr(thresholds, "FIRST_LOOKBACK_DAYS")

        # 型が正しいことを確認
        assert isinstance(thresholds.EPS_YOY_MIN, int | float)
        assert isinstance(thresholds.CF_QUALITY_MIN, int | float)
        assert isinstance(thresholds.RSI_THRESHOLD, int | float)
        assert isinstance(thresholds.FIRST_LOOKBACK_DAYS, int | float)

    def test_load_from_json_unicode_content(self):
        """Unicode文字を含むJSONファイルのテスト"""
        test_data = {"EPS_YOY_MIN": 0.5, "注釈": "これはテストです"}  # Unicode key
        json_content = json.dumps(test_data, ensure_ascii=False)

        mock_path = MagicMock(spec=Path)
        mock_path.exists.return_value = True
        mock_path.open.return_value.__enter__.return_value.read.return_value = (
            json_content
        )

        result = _load_from_json(mock_path)

        assert result == test_data
        assert result["注釈"] == "これはテストです"

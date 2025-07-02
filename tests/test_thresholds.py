#!/usr/bin/env python
"""
閾値管理モジュール (screening/thresholds.py) のテスト

テスト対象:
- JSONファイルからの閾値読み込み
- デフォルト値の適用
- ログ出力機能
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock

sys.path.append(str(Path(__file__).resolve().parents[1]))
from screening import thresholds


class TestThresholdsLoading:
    """閾値読み込み機能のテスト"""

    def test_load_from_json_success(self, tmp_path):
        """正常なJSONファイルからの読み込みテスト"""
        json_path = tmp_path / "thresholds.json"
        custom_values = {
            "EPS_YOY_MIN": 0.25,
            "CF_QUALITY_MIN": 0.9,
            "RSI_THRESHOLD": 55,
        }
        json_path.write_text(json.dumps(custom_values))

        result = thresholds._load_from_json(json_path)

        assert result == custom_values

    def test_load_from_json_not_exists(self, tmp_path):
        """存在しないファイルの場合のテスト"""
        json_path = tmp_path / "not_exists.json"

        result = thresholds._load_from_json(json_path)

        assert result == {}

    def test_load_from_json_invalid_format(self, tmp_path):
        """無効なJSONフォーマットの場合のテスト"""
        json_path = tmp_path / "invalid.json"
        json_path.write_text("{ invalid json")

        with mock.patch("screening.thresholds.logger") as mock_logger:
            result = thresholds._load_from_json(json_path)

            assert result == {}
            mock_logger.warning.assert_called_once()

    def test_load_thresholds_defaults(self):
        """デフォルト値の読み込みテスト"""
        with mock.patch("screening.thresholds._load_from_json") as mock_load:
            mock_load.return_value = {}

            result = thresholds.load_thresholds()

            # デフォルト値が含まれているか確認
            assert "EPS_YOY_MIN" in result
            assert "CF_QUALITY_MIN" in result
            assert "ETA_DELTA_MIN" in result
            assert "TREASURY_DELTA_MAX" in result
            assert "RSI_THRESHOLD" in result
            assert "ADX_THRESHOLD" in result
            assert "OVERHEAT_FACTOR" in result
            assert "OVERSOLD_FACTOR" in result
            assert "SIGNAL_COUNT_MIN" in result
            assert "SHORT_SIGNAL_COUNT_MIN" in result
            assert "FIRST_LOOKBACK_DAYS" in result

            # デフォルト値の確認
            assert result["EPS_YOY_MIN"] == 0.30
            assert result["CF_QUALITY_MIN"] == 0.8
            assert result["RSI_THRESHOLD"] == 50

    def test_load_thresholds_override(self, tmp_path):
        """カスタム値でデフォルト値を上書きするテスト"""
        json_path = tmp_path / "thresholds.json"
        custom_values = {
            "EPS_YOY_MIN": 0.40,  # デフォルト: 0.30
            "NEW_THRESHOLD": 123,  # 新しい閾値
        }
        json_path.write_text(json.dumps(custom_values))

        with mock.patch("src.config.config.get_file_path") as mock_get_path:
            mock_get_path.return_value = json_path

            result = thresholds.load_thresholds()

            # カスタム値で上書きされているか確認
            assert result["EPS_YOY_MIN"] == 0.40
            assert result["NEW_THRESHOLD"] == 123
            # その他のデフォルト値は保持されているか確認
            assert result["CF_QUALITY_MIN"] == 0.8

    def test_load_thresholds_with_path(self, tmp_path):
        """パス指定での読み込みテスト"""
        json_path = tmp_path / "custom_thresholds.json"
        custom_values = {"EPS_YOY_MIN": 0.35}
        json_path.write_text(json.dumps(custom_values))

        result = thresholds.load_thresholds(json_path)

        assert result["EPS_YOY_MIN"] == 0.35


class TestThresholdValues:
    """閾値の値のテスト"""

    def test_module_level_constants(self):
        """モジュールレベルの定数が正しく設定されているかテスト"""
        # 定数が存在するか確認
        assert hasattr(thresholds, "EPS_YOY_MIN")
        assert hasattr(thresholds, "CF_QUALITY_MIN")
        assert hasattr(thresholds, "ETA_DELTA_MIN")
        assert hasattr(thresholds, "TREASURY_DELTA_MAX")
        assert hasattr(thresholds, "RSI_THRESHOLD")
        assert hasattr(thresholds, "ADX_THRESHOLD")
        assert hasattr(thresholds, "OVERHEAT_FACTOR")
        assert hasattr(thresholds, "OVERSOLD_FACTOR")
        assert hasattr(thresholds, "SIGNAL_COUNT_MIN")
        assert hasattr(thresholds, "SHORT_SIGNAL_COUNT_MIN")
        assert hasattr(thresholds, "FIRST_LOOKBACK_DAYS")

        # 型の確認
        assert isinstance(thresholds.EPS_YOY_MIN, int | float)
        assert isinstance(thresholds.CF_QUALITY_MIN, int | float)
        assert isinstance(thresholds.SIGNAL_COUNT_MIN, int | float)


class TestLogging:
    """ログ出力機能のテスト"""

    def test_log_thresholds_default_logger(self):
        """デフォルトロガーでのログ出力テスト"""
        with mock.patch("screening.thresholds.logger") as mock_logger:
            thresholds.log_thresholds()

            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args[0]
            # ログメッセージに閾値名が含まれているか確認
            assert "EPS_YOY_MIN" in call_args[0]
            assert "CF_QUALITY_MIN" in call_args[0]

    def test_log_thresholds_custom_logger(self):
        """カスタムロガーでのログ出力テスト"""
        custom_logger = mock.Mock()

        thresholds.log_thresholds(custom_logger)

        custom_logger.info.assert_called_once()
        call_args = custom_logger.info.call_args[0]
        # すべての閾値が含まれているか確認
        assert len(call_args) == 12  # フォーマット文字列 + 11個の閾値

    def test_log_output_format(self):
        """ログ出力フォーマットのテスト"""
        with mock.patch("screening.thresholds.logger") as mock_logger:
            thresholds.log_thresholds()

            call_args = mock_logger.info.call_args
            format_string = call_args[0][0]
            values = call_args[0][1:]

            # フォーマット文字列に適切な数のプレースホルダーがあるか確認
            assert format_string.count("%s") == len(values)

            # 値が正しい順序で渡されているか確認
            assert values[0] == thresholds.EPS_YOY_MIN
            assert values[1] == thresholds.CF_QUALITY_MIN
            assert values[2] == thresholds.ETA_DELTA_MIN
            assert values[3] == thresholds.TREASURY_DELTA_MAX


class TestIntegration:
    """統合テスト"""

    def test_reload_thresholds(self, tmp_path):
        """閾値の再読み込みテスト"""
        # 初期状態の値を保存
        original_eps = thresholds.EPS_YOY_MIN

        # 新しい閾値ファイルを作成
        json_path = tmp_path / "new_thresholds.json"
        json_path.write_text(json.dumps({"EPS_YOY_MIN": 0.50}))

        # 再読み込み
        new_vals = thresholds.load_thresholds(json_path)

        # load_thresholds関数の戻り値は新しい値を含む
        assert new_vals["EPS_YOY_MIN"] == 0.50

        # ただし、モジュールレベルの定数は変更されない（初回読み込み時の値のまま）
        assert thresholds.EPS_YOY_MIN == original_eps

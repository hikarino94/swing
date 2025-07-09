"""Test suite for screening/thresholds.py module."""

import json
import sys
from pathlib import Path
from unittest import mock

import pytest

# プロジェクトルートをPYTHONPATHに追加
sys.path.insert(0, str(Path(__file__).parent.parent))
from screening.thresholds import (
    ADX_THRESHOLD,
    CF_QUALITY_MIN,
    EPS_YOY_MIN,
    ETA_DELTA_MIN,
    FIRST_LOOKBACK_DAYS,
    OVERHEAT_FACTOR,
    OVERSOLD_FACTOR,
    RSI_THRESHOLD,
    SHORT_SIGNAL_COUNT_MIN,
    SIGNAL_COUNT_MIN,
    TREASURY_DELTA_MAX,
    _load_from_json,
    load_thresholds,
    log_thresholds,
)


class TestLoadFromJson:
    """Test _load_from_json function."""

    def test_load_existing_file(self, tmp_path):
        """既存ファイルの読み込みテスト"""
        # テスト用のJSONファイルを作成
        test_file = tmp_path / "test_thresholds.json"
        test_data = {
            "EPS_YOY_MIN": 0.25,
            "CF_QUALITY_MIN": 0.75,
            "RSI_THRESHOLD": 45,
        }
        test_file.write_text(json.dumps(test_data))

        # ファイルを読み込む
        result = _load_from_json(test_file)

        assert result == test_data
        assert result["EPS_YOY_MIN"] == 0.25
        assert result["CF_QUALITY_MIN"] == 0.75
        assert result["RSI_THRESHOLD"] == 45

    def test_load_nonexistent_file(self, tmp_path):
        """存在しないファイルの場合は空の辞書を返す"""
        test_file = tmp_path / "nonexistent.json"

        result = _load_from_json(test_file)

        assert result == {}

    def test_load_invalid_json(self, tmp_path, caplog):
        """無効なJSONファイルの場合は空の辞書を返し、警告を記録"""
        test_file = tmp_path / "invalid.json"
        test_file.write_text("{ invalid json }")

        result = _load_from_json(test_file)

        assert result == {}
        assert "Failed to load" in caplog.text
        assert str(test_file) in caplog.text

    def test_load_non_dict_json(self, tmp_path):
        """辞書以外のJSONデータの場合"""
        test_file = tmp_path / "array.json"
        test_file.write_text("[1, 2, 3]")

        # JSONとしては有効だが、辞書ではないので正しくキャストされない
        result = _load_from_json(test_file)

        # リストがそのまま返される（現在の実装）
        assert result == [1, 2, 3]
        # 注: 本来は辞書型を期待しているので、これは潜在的なバグ


class TestLoadThresholds:
    """Test load_thresholds function."""

    def test_load_default_thresholds(self):
        """デフォルトの閾値が正しく読み込まれることを確認"""
        with mock.patch("screening.thresholds._load_from_json", return_value={}):
            thresholds = load_thresholds()

        # デフォルト値の確認
        assert thresholds["EPS_YOY_MIN"] == 0.30
        assert thresholds["CF_QUALITY_MIN"] == 0.8
        assert thresholds["ETA_DELTA_MIN"] == 0.0
        assert thresholds["TREASURY_DELTA_MAX"] == 0.0
        assert thresholds["RSI_THRESHOLD"] == 50
        assert thresholds["ADX_THRESHOLD"] == 20
        assert thresholds["OVERHEAT_FACTOR"] == 1.1
        assert thresholds["OVERSOLD_FACTOR"] == 0.95
        assert thresholds["SIGNAL_COUNT_MIN"] == 3
        assert thresholds["SHORT_SIGNAL_COUNT_MIN"] == 4
        assert thresholds["FIRST_LOOKBACK_DAYS"] == 30

    def test_load_custom_thresholds(self, tmp_path):
        """カスタム閾値が正しく上書きされることを確認"""
        # カスタム値を含むJSONファイルを作成
        custom_file = tmp_path / "custom_thresholds.json"
        custom_data = {
            "EPS_YOY_MIN": 0.50,  # デフォルト: 0.30
            "RSI_THRESHOLD": 60,  # デフォルト: 50
            "NEW_THRESHOLD": 100,  # 新しい閾値
        }
        custom_file.write_text(json.dumps(custom_data))

        # カスタムファイルを指定して読み込み
        thresholds = load_thresholds(custom_file)

        # カスタム値が反映されていることを確認
        assert thresholds["EPS_YOY_MIN"] == 0.50
        assert thresholds["RSI_THRESHOLD"] == 60
        assert thresholds["NEW_THRESHOLD"] == 100

        # 上書きされていない値はデフォルトのまま
        assert thresholds["CF_QUALITY_MIN"] == 0.8
        assert thresholds["ADX_THRESHOLD"] == 20

    @mock.patch("screening.thresholds.config.get_file_path")
    def test_load_from_config_path(self, mock_get_file_path):
        """config経由でのパス取得をテスト"""
        mock_path = mock.MagicMock()
        mock_get_file_path.return_value = mock_path

        with mock.patch("screening.thresholds._load_from_json", return_value={}):
            load_thresholds()

        # config.get_file_pathが"thresholds"で呼ばれたことを確認
        mock_get_file_path.assert_called_once_with("thresholds")

    def test_logging_output(self, caplog):
        """閾値読み込み時のログ出力を確認"""
        with mock.patch("screening.thresholds._load_from_json", return_value={}):
            with mock.patch("screening.thresholds.config.get_file_path") as mock_path:
                mock_path.return_value = Path("/test/path/thresholds.json")

                with caplog.at_level("INFO"):
                    load_thresholds()

        # ログメッセージの確認
        assert "Thresholds loaded from" in caplog.text
        assert "/test/path/thresholds.json" in caplog.text
        assert "EPS_YOY_MIN" in caplog.text


class TestModuleConstants:
    """Test module-level constants."""

    def test_constants_loaded(self):
        """モジュールレベルの定数が正しく読み込まれていることを確認"""
        # デフォルト値の確認（_load_from_jsonが空の辞書を返すと仮定）
        assert EPS_YOY_MIN == 0.30
        assert CF_QUALITY_MIN == 0.8
        assert ETA_DELTA_MIN == 0.0
        assert TREASURY_DELTA_MAX == 0.0
        assert RSI_THRESHOLD == 50
        assert ADX_THRESHOLD == 20
        assert OVERHEAT_FACTOR == 1.1
        assert OVERSOLD_FACTOR == 0.95
        assert SIGNAL_COUNT_MIN == 3
        assert SHORT_SIGNAL_COUNT_MIN == 4
        assert FIRST_LOOKBACK_DAYS == 30


class TestLogThresholds:
    """Test log_thresholds function."""

    def test_log_with_default_logger(self, caplog):
        """デフォルトロガーでのログ出力テスト"""
        with caplog.at_level("INFO"):
            log_thresholds()

        # すべての閾値がログに含まれていることを確認
        assert "Thresholds:" in caplog.text
        assert f"EPS_YOY_MIN={EPS_YOY_MIN}" in caplog.text
        assert f"CF_QUALITY_MIN={CF_QUALITY_MIN}" in caplog.text
        assert f"ETA_DELTA_MIN={ETA_DELTA_MIN}" in caplog.text
        assert f"TREASURY_DELTA_MAX={TREASURY_DELTA_MAX}" in caplog.text
        assert f"RSI_THRESHOLD={RSI_THRESHOLD}" in caplog.text
        assert f"ADX_THRESHOLD={ADX_THRESHOLD}" in caplog.text
        assert f"OVERHEAT_FACTOR={OVERHEAT_FACTOR}" in caplog.text
        assert f"OVERSOLD_FACTOR={OVERSOLD_FACTOR}" in caplog.text
        assert f"SIGNAL_COUNT_MIN={SIGNAL_COUNT_MIN}" in caplog.text
        assert f"SHORT_SIGNAL_COUNT_MIN={SHORT_SIGNAL_COUNT_MIN}" in caplog.text
        assert f"FIRST_LOOKBACK_DAYS={FIRST_LOOKBACK_DAYS}" in caplog.text

    def test_log_with_custom_logger(self):
        """カスタムロガーでのログ出力テスト"""
        import logging

        # カスタムロガーのモック
        mock_logger = mock.MagicMock(spec=logging.Logger)

        log_thresholds(mock_logger)

        # info()が1回呼ばれたことを確認
        mock_logger.info.assert_called_once()

        # ログメッセージの内容を確認
        call_args = mock_logger.info.call_args[0]
        assert "Thresholds:" in call_args[0]

        # すべての閾値が引数に含まれていることを確認
        assert len(call_args) == 12  # フォーマット文字列 + 11個の値


class TestIntegration:
    """Integration tests for the thresholds module."""

    def test_reload_thresholds(self, tmp_path):
        """閾値の再読み込みをテスト"""
        # 初期値を記録
        initial_thresholds = load_thresholds()

        # カスタムファイルを作成
        custom_file = tmp_path / "new_thresholds.json"
        custom_file.write_text(json.dumps({"EPS_YOY_MIN": 0.99}))

        # 新しい値で再読み込み
        new_thresholds = load_thresholds(custom_file)

        # 新しい値が反映されていることを確認
        assert new_thresholds["EPS_YOY_MIN"] == 0.99

        # モジュールレベルの定数は変更されないことを確認
        # （モジュールのインポート時に一度だけ設定されるため）
        assert EPS_YOY_MIN == initial_thresholds["EPS_YOY_MIN"]

    def test_partial_override(self, tmp_path):
        """一部の値のみ上書きする場合のテスト"""
        # 一部の値のみ含むJSONファイル
        partial_file = tmp_path / "partial.json"
        partial_file.write_text(
            json.dumps(
                {
                    "RSI_THRESHOLD": 70,
                    "ADX_THRESHOLD": 30,
                }
            )
        )

        thresholds = load_thresholds(partial_file)

        # 指定した値が上書きされている
        assert thresholds["RSI_THRESHOLD"] == 70
        assert thresholds["ADX_THRESHOLD"] == 30

        # 指定していない値はデフォルトのまま
        assert thresholds["EPS_YOY_MIN"] == 0.30
        assert thresholds["CF_QUALITY_MIN"] == 0.8

    def test_type_preservation(self, tmp_path):
        """数値型が保持されることを確認"""
        # 様々な数値型を含むJSON
        test_file = tmp_path / "types.json"
        test_file.write_text(
            json.dumps(
                {
                    "INTEGER_VALUE": 42,
                    "FLOAT_VALUE": 3.14,
                    "ZERO_VALUE": 0,
                    "NEGATIVE_VALUE": -1.5,
                }
            )
        )

        thresholds = load_thresholds(test_file)

        # 型と値が正しく保持されている
        assert thresholds["INTEGER_VALUE"] == 42
        assert isinstance(thresholds["INTEGER_VALUE"], int)

        assert thresholds["FLOAT_VALUE"] == 3.14
        assert isinstance(thresholds["FLOAT_VALUE"], float)

        assert thresholds["ZERO_VALUE"] == 0
        assert thresholds["NEGATIVE_VALUE"] == -1.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

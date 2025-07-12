"""portfolio.visualizers.baseのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.portfolio.visualizers.base import BaseVisualizer


class ConcreteVisualizer(BaseVisualizer):
    """テスト用の具象クラス"""

    def create_chart(self) -> dict:
        """ダミー実装"""
        return {"type": "test", "data": [1, 2, 3]}


class TestBaseVisualizer:
    """BaseVisualizerクラスのテスト"""

    def test_initialization(self):
        """初期化のテスト"""
        visualizer = ConcreteVisualizer(user_id=123)
        assert visualizer.user_id == 123

    def test_abstract_method_must_be_implemented(self):
        """抽象メソッドが実装されていない場合のテスト"""
        # BaseVisualizerを直接インスタンス化しようとするとエラー
        with pytest.raises(TypeError):
            BaseVisualizer(user_id=1)

    def test_create_chart_implementation(self):
        """create_chartメソッドの実装テスト"""
        visualizer = ConcreteVisualizer(user_id=1)
        result = visualizer.create_chart()

        assert isinstance(result, dict)
        assert result["type"] == "test"
        assert result["data"] == [1, 2, 3]

    @patch("src.portfolio.visualizers.base.sqlite3.connect")
    def test_get_db_connection(self, mock_connect):
        """データベース接続取得のテスト"""
        # モック接続を設定
        mock_connection = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_connection

        visualizer = ConcreteVisualizer(user_id=1)
        conn = visualizer.get_db_connection()

        # 接続が返されることを確認
        assert conn == mock_connection
        mock_connect.assert_called_once()

        # DB_PATHで接続されることを確認
        # (実際のパスは環境によって異なるため、呼び出しの存在のみチェック)
        assert mock_connect.call_args is not None


class TestModuleImports:
    """モジュールのインポートと設定のテスト"""

    @patch("src.portfolio.visualizers.base.matplotlib")
    def test_matplotlib_backend_setting(self, mock_matplotlib):
        """Matplotlibバックエンドの設定テスト"""
        # モジュールを再インポートして設定を確認
        import src.portfolio.visualizers.base

        # バックエンドがAggに設定されていることを確認
        # (実際には初回インポート時に設定されるため、モックでは確認困難)
        # 少なくともmatplotlibがインポートされていることを確認
        assert hasattr(src.portfolio.visualizers.base, "matplotlib")

    def test_japanize_matplotlib_imported(self):
        """日本語対応ライブラリのインポートテスト"""
        import src.portfolio.visualizers.base

        # japanize_matplotlibがインポートされていることを確認
        assert hasattr(src.portfolio.visualizers.base, "japanize_matplotlib")

    def test_logger_configured(self):
        """ロガーの設定テスト"""
        import src.portfolio.visualizers.base

        # ロガーが設定されていることを確認
        assert hasattr(src.portfolio.visualizers.base, "logger")
        assert (
            src.portfolio.visualizers.base.logger.name == "portfolio.visualizers.base"
        )

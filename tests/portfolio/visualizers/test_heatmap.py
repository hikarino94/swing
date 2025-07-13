"""portfolio.visualizers.heatmapのテスト"""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from plotly.graph_objects import Figure

from src.portfolio.visualizers.heatmap import HeatmapVisualizer


class TestHeatmapVisualizer:
    """HeatmapVisualizerの基本テスト"""

    def test_init(self):
        """初期化のテスト"""
        visualizer = HeatmapVisualizer(user_id=123)
        assert visualizer.user_id == 123

    def test_create_chart_calls_create_heatmap(self):
        """create_chartがcreate_heatmapを呼び出すことを確認"""
        visualizer = HeatmapVisualizer(user_id=123)
        visualizer.create_heatmap = MagicMock(return_value={"test": "data"})

        result = visualizer.create_chart()

        assert result == {"test": "data"}
        visualizer.create_heatmap.assert_called_once()


class TestCreateHeatmap:
    """create_heatmapメソッドのテスト"""

    @patch("src.portfolio.visualizers.heatmap.pd.read_sql")
    def test_create_heatmap_success(self, mock_read_sql):
        """正常なヒートマップ作成のテスト"""
        # モックデータ
        mock_df = pd.DataFrame(
            {
                "code": ["1234", "5678", "9012"],
                "market_value": [150000, 120000, 80000],
                "profit_loss": [15000, -5000, 8000],
                "profit_loss_ratio": [11.11, -4.0, 11.11],
                "company_name": ["会社A", "会社B", "会社C"],
                "sector17_name": ["情報通信", "電気機器", "情報通信"],
            }
        )
        mock_read_sql.return_value = mock_df

        visualizer = HeatmapVisualizer(user_id=123)

        # get_db_connectionをモック
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # 各種チャート作成メソッドをモック
        mock_fig = MagicMock(spec=Figure)
        mock_fig.to_json.return_value = '{"test": "chart"}'

        visualizer._create_stock_heatmap = MagicMock(return_value=mock_fig)
        visualizer._create_sector_heatmap = MagicMock(return_value=mock_fig)
        visualizer._create_distribution_chart = MagicMock(return_value=mock_fig)
        visualizer._calculate_performance_stats = MagicMock(
            return_value={"test": "stats"}
        )

        # テスト実行
        result = visualizer.create_heatmap()

        # 検証
        assert "stock_heatmap" in result
        assert "sector_heatmap" in result
        assert "distribution_chart" in result
        assert "stats" in result
        assert result["stats"] == {"test": "stats"}

        # SQLクエリの確認
        mock_read_sql.assert_called_once()
        sql = mock_read_sql.call_args[0][0]
        assert "holdings h" in sql
        assert "LEFT JOIN listed_info li" in sql
        assert "deleted_at IS NULL" in sql
        assert "ORDER BY h.market_value DESC" in sql

        # 各チャート作成メソッドが呼ばれたことを確認
        visualizer._create_stock_heatmap.assert_called_once_with(mock_df)
        visualizer._create_sector_heatmap.assert_called_once_with(mock_df)
        visualizer._create_distribution_chart.assert_called_once_with(mock_df)
        visualizer._calculate_performance_stats.assert_called_once_with(mock_df)

        # データベース接続が閉じられたことを確認
        mock_conn.close.assert_called_once()

    @patch("src.portfolio.visualizers.heatmap.pd.read_sql")
    def test_create_heatmap_empty_data(self, mock_read_sql):
        """データがない場合のテスト"""
        # 空のDataFrame
        mock_read_sql.return_value = pd.DataFrame()

        visualizer = HeatmapVisualizer(user_id=123)
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # テスト実行
        result = visualizer.create_heatmap()

        # 検証
        assert result == {"error": "保有銘柄データがありません"}
        mock_conn.close.assert_called_once()

    @patch("src.portfolio.visualizers.heatmap.pd.read_sql")
    def test_create_heatmap_exception_handling(self, mock_read_sql):
        """例外処理のテスト"""
        mock_read_sql.side_effect = Exception("Database error")

        visualizer = HeatmapVisualizer(user_id=123)
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # テスト実行
        with pytest.raises(Exception, match="Database error"):
            visualizer.create_heatmap()

        # データベース接続が閉じられることを確認
        mock_conn.close.assert_called_once()


class TestCreateStockHeatmap:
    """_create_stock_heatmapメソッドのテスト"""

    def test_create_stock_heatmap_top20(self):
        """上位20銘柄のツリーマップ作成テスト"""
        # 25銘柄のテストデータ
        data = []
        for i in range(25):
            data.append(
                {
                    "code": f"{1000 + i}",
                    "company_name": f"会社{i}" if i % 5 != 0 else None,  # 一部None
                    "market_value": (25 - i) * 10000,
                    "profit_loss": (25 - i) * 1000 * (1 if i % 3 != 0 else -1),
                    "profit_loss_ratio": 10.0 if i % 3 != 0 else -5.0,
                }
            )
        df = pd.DataFrame(data)

        visualizer = HeatmapVisualizer(user_id=123)

        # テスト実行
        with patch("src.portfolio.visualizers.heatmap.px.treemap") as mock_treemap:
            mock_fig = MagicMock(spec=Figure)
            mock_treemap.return_value = mock_fig

            result = visualizer._create_stock_heatmap(df)

            # 検証
            assert result == mock_fig

            # treemapの呼び出し確認
            mock_treemap.assert_called_once()
            call_args = mock_treemap.call_args
            treemap_df = call_args[0][0]

            # 上位20件のみ処理されていることを確認
            assert len(treemap_df) == 20

            # display_nameが正しく設定されていることを確認
            assert "display_name" in treemap_df.columns
            # NoneのcompanyNameは"不明"として表示される
            assert "1000 不明" in treemap_df["display_name"].values

            # color_valueが設定されていることを確認
            assert "color_value" in treemap_df.columns

    def test_create_stock_heatmap_update_traces(self):
        """update_tracesが呼ばれることを確認"""
        df = pd.DataFrame(
            {
                "code": ["1234"],
                "company_name": ["会社A"],
                "market_value": [100000],
                "profit_loss": [10000],
                "profit_loss_ratio": [11.11],
            }
        )

        visualizer = HeatmapVisualizer(user_id=123)

        with patch("src.portfolio.visualizers.heatmap.px.treemap") as mock_treemap:
            mock_fig = MagicMock(spec=Figure)
            mock_treemap.return_value = mock_fig

            visualizer._create_stock_heatmap(df)

            # update_tracesとupdate_layoutが呼ばれたことを確認
            mock_fig.update_traces.assert_called_once_with(
                textinfo="label+value",
                texttemplate="%{label}<br>%{value:,.0f}円<br>%{color:.1f}%",
            )
            mock_fig.update_layout.assert_called_once()


class TestCreateSectorHeatmap:
    """_create_sector_heatmapメソッドのテスト"""

    def test_create_sector_heatmap(self):
        """セクター別ツリーマップ作成のテスト"""
        df = pd.DataFrame(
            {
                "sector17_name": ["情報通信", "電気機器", "情報通信", "不明"],
                "market_value": [150000, 120000, 80000, 50000],
                "profit_loss": [15000, -5000, 8000, 2000],
            }
        )

        visualizer = HeatmapVisualizer(user_id=123)

        with patch("src.portfolio.visualizers.heatmap.px.treemap") as mock_treemap:
            mock_fig = MagicMock(spec=Figure)
            mock_treemap.return_value = mock_fig

            result = visualizer._create_sector_heatmap(df)

            # 検証
            assert result == mock_fig

            # treemapの呼び出し確認
            mock_treemap.assert_called_once()
            call_args = mock_treemap.call_args
            sector_df = call_args[0][0]

            # セクター別に集約されていることを確認
            assert len(sector_df) == 3  # 情報通信、電気機器、不明

            # 情報通信セクターの集約確認
            info_comm = sector_df[sector_df["sector17_name"] == "情報通信"]
            assert len(info_comm) == 1
            assert info_comm.iloc[0]["market_value"] == 230000  # 150000 + 80000
            assert info_comm.iloc[0]["profit_loss"] == 23000  # 15000 + 8000

            # 不明セクターの確認
            unknown = sector_df[sector_df["sector17_name"] == "不明"]
            assert len(unknown) == 1
            assert unknown.iloc[0]["market_value"] == 50000

    def test_create_sector_heatmap_profit_loss_ratio_calculation(self):
        """損益率計算の確認"""
        df = pd.DataFrame(
            {
                "sector17_name": ["テスト"],
                "market_value": [110000],
                "profit_loss": [10000],
            }
        )

        visualizer = HeatmapVisualizer(user_id=123)

        with patch("src.portfolio.visualizers.heatmap.px.treemap") as mock_treemap:
            mock_fig = MagicMock(spec=Figure)
            mock_treemap.return_value = mock_fig

            visualizer._create_sector_heatmap(df)

            call_args = mock_treemap.call_args
            sector_df = call_args[0][0]

            # 損益率が正しく計算されていることを確認
            # profit_loss_ratio = 10000 / (110000 - 10000) * 100 = 10.0
            assert abs(sector_df.iloc[0]["profit_loss_ratio"] - 10.0) < 0.01


class TestCreateDistributionChart:
    """_create_distribution_chartメソッドのテスト"""

    def test_create_distribution_chart_both_positive_and_negative(self):
        """プラスとマイナス両方のデータがある場合"""
        df = pd.DataFrame(
            {
                "profit_loss_ratio": [10.0, 5.0, -3.0, -8.0, 0.0, 15.0],
            }
        )

        visualizer = HeatmapVisualizer(user_id=123)

        # テスト実行
        fig = visualizer._create_distribution_chart(df)

        # 検証
        assert isinstance(fig, Figure)
        # 2つのトレース（プラスとマイナス）が追加されている
        assert len(fig.data) == 2
        # トレース名を確認
        trace_names = [trace.name for trace in fig.data]
        assert "プラス" in trace_names
        assert "マイナス" in trace_names

    def test_create_distribution_chart_only_positive(self):
        """プラスのデータのみの場合"""
        df = pd.DataFrame(
            {
                "profit_loss_ratio": [10.0, 5.0, 15.0],
            }
        )

        visualizer = HeatmapVisualizer(user_id=123)
        fig = visualizer._create_distribution_chart(df)

        # プラスのトレースのみ
        assert len(fig.data) == 1
        assert fig.data[0].name == "プラス"

    def test_create_distribution_chart_only_negative(self):
        """マイナスのデータのみの場合"""
        df = pd.DataFrame(
            {
                "profit_loss_ratio": [-10.0, -5.0, -15.0],
            }
        )

        visualizer = HeatmapVisualizer(user_id=123)
        fig = visualizer._create_distribution_chart(df)

        # マイナスのトレースのみ
        assert len(fig.data) == 1
        assert fig.data[0].name == "マイナス"


class TestCalculatePerformanceStats:
    """_calculate_performance_statsメソッドのテスト"""

    def test_calculate_performance_stats_normal(self):
        """正常な統計計算のテスト"""
        df = pd.DataFrame(
            {
                "code": ["1234", "5678", "9012", "3456"],
                "company_name": ["会社A", "会社B", None, "会社D"],
                "market_value": [150000, 120000, 80000, 50000],
                "profit_loss": [15000, 10000, -5000, -2000],
                "profit_loss_ratio": [11.11, 9.09, -5.88, -3.85],
            }
        )

        visualizer = HeatmapVisualizer(user_id=123)
        stats = visualizer._calculate_performance_stats(df)

        # 検証
        assert stats["total_value"] == 400000
        assert stats["total_profit_loss"] == 18000
        assert abs(stats["total_profit_loss_ratio"] - 4.71) < 0.01
        assert stats["positive_count"] == 2
        assert stats["negative_count"] == 2
        assert stats["win_rate"] == 50.0

        # 最高パフォーマー
        assert stats["best_performer"]["code"] == "1234"
        assert stats["best_performer"]["company_name"] == "会社A"
        assert stats["best_performer"]["profit_loss_ratio"] == 11.11

        # 最低パフォーマー
        assert stats["worst_performer"]["code"] == "9012"
        assert stats["worst_performer"]["company_name"] == "不明"  # Noneは"不明"に変換
        assert stats["worst_performer"]["profit_loss_ratio"] == -5.88

    def test_calculate_performance_stats_single_stock(self):
        """銘柄が1つの場合"""
        df = pd.DataFrame(
            {
                "code": ["1234"],
                "company_name": ["会社A"],
                "market_value": [100000],
                "profit_loss": [10000],
                "profit_loss_ratio": [11.11],
            }
        )

        visualizer = HeatmapVisualizer(user_id=123)
        stats = visualizer._calculate_performance_stats(df)

        # 検証
        assert stats["total_value"] == 100000
        assert stats["total_profit_loss"] == 10000
        assert stats["positive_count"] == 1
        assert stats["negative_count"] == 0
        assert stats["win_rate"] == 100.0

        # 最高・最低パフォーマーが同じ
        assert stats["best_performer"]["code"] == "1234"
        assert stats["worst_performer"]["code"] == "1234"

    def test_calculate_performance_stats_all_loss(self):
        """全て損失の場合"""
        df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "company_name": ["会社A", "会社B"],
                "market_value": [90000, 80000],
                "profit_loss": [-10000, -20000],
                "profit_loss_ratio": [-10.0, -20.0],
            }
        )

        visualizer = HeatmapVisualizer(user_id=123)
        stats = visualizer._calculate_performance_stats(df)

        # 検証
        assert stats["total_value"] == 170000
        assert stats["total_profit_loss"] == -30000
        assert stats["positive_count"] == 0
        assert stats["negative_count"] == 2
        assert stats["win_rate"] == 0.0


class TestHeatmapVisualizerIntegration:
    """HeatmapVisualizerの統合テスト"""

    @patch("src.portfolio.visualizers.heatmap.pd.read_sql")
    def test_full_visualization_flow(self, mock_read_sql):
        """完全な可視化フローのテスト"""
        # リアルなデータセット
        mock_df = pd.DataFrame(
            {
                "code": [
                    "7203",
                    "6758",
                    "9984",
                    "4661",
                    "6861",
                    "8306",
                    "9433",
                    "8035",
                    "2914",
                    "7974",
                ],
                "market_value": [
                    300000,
                    250000,
                    200000,
                    180000,
                    160000,
                    140000,
                    120000,
                    100000,
                    80000,
                    60000,
                ],
                "profit_loss": [
                    30000,
                    25000,
                    -10000,
                    20000,
                    -5000,
                    15000,
                    -8000,
                    10000,
                    5000,
                    -2000,
                ],
                "profit_loss_ratio": [
                    11.11,
                    11.11,
                    -4.76,
                    12.50,
                    -3.03,
                    12.00,
                    -6.25,
                    11.11,
                    6.67,
                    -3.23,
                ],
                "company_name": [
                    "トヨタ",
                    "ソニー",
                    "ソフトバンク",
                    "オリエンタル",
                    "キーエンス",
                    "三菱UFJ",
                    "KDDI",
                    "東京エレク",
                    "JT",
                    "任天堂",
                ],
                "sector17_name": [
                    "輸送用機器",
                    "電気機器",
                    "情報通信",
                    "サービス",
                    "電気機器",
                    "銀行",
                    "情報通信",
                    "電気機器",
                    "食料品",
                    "その他製品",
                ],
            }
        )
        mock_read_sql.return_value = mock_df

        visualizer = HeatmapVisualizer(user_id=123)
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # テスト実行
        result = visualizer.create_heatmap()

        # 検証
        assert "stock_heatmap" in result
        assert "sector_heatmap" in result
        assert "distribution_chart" in result
        assert "stats" in result

        # 統計情報の確認
        stats = result["stats"]
        assert stats["total_value"] == 1590000
        assert stats["total_profit_loss"] == 80000
        assert stats["positive_count"] == 6
        assert stats["negative_count"] == 4
        assert stats["win_rate"] == 60.0

        # 各チャートがJSON形式で返されることを確認
        for key in ["stock_heatmap", "sector_heatmap", "distribution_chart"]:
            json_data = json.loads(result[key])
            assert isinstance(json_data, dict)

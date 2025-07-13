"""portfolio.visualizers.compositionのテスト"""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from plotly.graph_objects import Figure

from src.portfolio.visualizers.composition import CompositionVisualizer


class TestCompositionVisualizer:
    """CompositionVisualizerの基本テスト"""

    def test_init(self):
        """初期化のテスト"""
        visualizer = CompositionVisualizer(user_id=123)
        assert visualizer.user_id == 123

    def test_create_chart_calls_composition_pie_charts(self):
        """create_chartがcreate_composition_pie_chartsを呼び出すことを確認"""
        visualizer = CompositionVisualizer(user_id=123)
        visualizer.create_composition_pie_charts = MagicMock(
            return_value={"test": "data"}
        )

        result = visualizer.create_chart()

        assert result == {"test": "data"}
        visualizer.create_composition_pie_charts.assert_called_once()


class TestCreateCompositionPieCharts:
    """create_composition_pie_chartsメソッドのテスト"""

    @patch("src.portfolio.visualizers.composition.pd.read_sql")
    def test_create_composition_pie_charts_success(self, mock_read_sql):
        """正常な円グラフ作成のテスト"""
        # モックデータ
        mock_df = pd.DataFrame(
            {
                "code": ["1234", "5678", "9012"],
                "market_value": [100000, 80000, 50000],
                "account_name": ["特定口座", "特定口座", "NISA"],
                "company_name": ["会社A", "会社B", "会社C"],
                "sector17_name": ["情報通信", "電気機器", "情報通信"],
                "sector33_name": ["ソフトウェア", "半導体", "ソフトウェア"],
                "market_name": ["東証プライム", "東証グロース", "東証プライム"],
            }
        )
        mock_read_sql.return_value = mock_df

        visualizer = CompositionVisualizer(user_id=123)

        # get_db_connectionをモック
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # 各種円グラフ作成メソッドをモック
        mock_fig = MagicMock(spec=Figure)
        mock_fig.to_json.return_value = '{"test": "chart"}'

        visualizer._create_stock_composition_chart = MagicMock(return_value=mock_fig)
        visualizer._create_sector_composition_chart = MagicMock(return_value=mock_fig)
        visualizer._create_market_composition_chart = MagicMock(return_value=mock_fig)
        visualizer._create_account_composition_chart = MagicMock(return_value=mock_fig)

        # テスト実行
        result = visualizer.create_composition_pie_charts()

        # 検証
        assert "stock_composition" in result
        assert "sector_composition" in result
        assert "market_composition" in result
        assert "account_composition" in result
        assert result["total_value"] == 230000

        # SQLクエリの確認
        mock_read_sql.assert_called_once()
        sql = mock_read_sql.call_args[0][0]
        assert "holdings h" in sql
        assert "LEFT JOIN listed_info li" in sql
        assert "deleted_at IS NULL" in sql

        # 各円グラフ作成メソッドが呼ばれたことを確認
        visualizer._create_stock_composition_chart.assert_called_once()
        visualizer._create_sector_composition_chart.assert_called_once()
        visualizer._create_market_composition_chart.assert_called_once()
        visualizer._create_account_composition_chart.assert_called_once()

        # データベース接続が閉じられたことを確認
        mock_conn.close.assert_called_once()

    @patch("src.portfolio.visualizers.composition.pd.read_sql")
    def test_create_composition_pie_charts_empty_data(self, mock_read_sql):
        """データがない場合のテスト"""
        # 空のDataFrame
        mock_read_sql.return_value = pd.DataFrame()

        visualizer = CompositionVisualizer(user_id=123)
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # テスト実行
        result = visualizer.create_composition_pie_charts()

        # 検証
        assert result == {"error": "保有銘柄データがありません"}
        mock_conn.close.assert_called_once()

    @patch("src.portfolio.visualizers.composition.pd.read_sql")
    def test_create_composition_pie_charts_exception_handling(self, mock_read_sql):
        """例外処理のテスト"""
        mock_read_sql.side_effect = Exception("Database error")

        visualizer = CompositionVisualizer(user_id=123)
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # テスト実行
        with pytest.raises(Exception, match="Database error"):
            visualizer.create_composition_pie_charts()

        # データベース接続が閉じられることを確認
        mock_conn.close.assert_called_once()


class TestCreateStockCompositionChart:
    """_create_stock_composition_chartメソッドのテスト"""

    def test_create_stock_composition_chart_top10(self):
        """銘柄数が10を超える場合のテスト"""
        # 15銘柄のテストデータ
        data = []
        for i in range(15):
            data.append(
                {
                    "code": f"{1000 + i}",
                    "company_name": f"会社{i}",
                    "market_value": (15 - i) * 10000,  # 降順になるように
                }
            )
        df = pd.DataFrame(data)
        total_value = df["market_value"].sum()

        visualizer = CompositionVisualizer(user_id=123)

        # テスト実行
        fig = visualizer._create_stock_composition_chart(df, total_value)

        # 検証
        assert isinstance(fig, Figure)
        # Pieチャートのラベル数が11（上位10 + その他）
        assert len(fig.data[0].labels) == 11
        assert "その他" in fig.data[0].labels[-1]

        # レイアウトの確認
        assert fig.layout.title.text == "銘柄別構成比"
        assert fig.layout.height == 500

    def test_create_stock_composition_chart_less_than_10(self):
        """銘柄数が10以下の場合のテスト"""
        df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "company_name": ["会社A", "会社B"],
                "market_value": [100000, 80000],
            }
        )
        total_value = 180000

        visualizer = CompositionVisualizer(user_id=123)

        # テスト実行
        fig = visualizer._create_stock_composition_chart(df, total_value)

        # 検証
        assert len(fig.data[0].labels) == 2
        assert "その他" not in str(fig.data[0].labels)

    def test_create_stock_composition_chart_percentage_calculation(self):
        """パーセンテージ計算の確認"""
        df = pd.DataFrame(
            {
                "code": ["1234"],
                "company_name": ["会社A"],
                "market_value": [50000],
            }
        )
        total_value = 100000

        visualizer = CompositionVisualizer(user_id=123)
        fig = visualizer._create_stock_composition_chart(df, total_value)

        # 50%と表示されることを確認
        assert "50.0%" in fig.data[0].text[0]


class TestCreateSectorCompositionChart:
    """_create_sector_composition_chartメソッドのテスト"""

    def test_create_sector_composition_chart(self):
        """セクター別円グラフ作成のテスト"""
        df = pd.DataFrame(
            {
                "sector17_name": ["情報通信", "電気機器", "情報通信", None],
                "market_value": [100000, 80000, 50000, 20000],
            }
        )
        total_value = 250000

        visualizer = CompositionVisualizer(user_id=123)

        # テスト実行
        fig = visualizer._create_sector_composition_chart(df, total_value)

        # 検証
        assert isinstance(fig, Figure)
        # セクター別に集約されている（Noneは除外される）
        assert len(fig.data[0].labels) == 2
        # 情報通信は集約されて1つになる
        assert "情報通信" in fig.data[0].labels
        assert "電気機器" in fig.data[0].labels
        assert fig.layout.title.text == "セクター別構成比"

    def test_create_sector_composition_chart_colors(self):
        """セクター別円グラフの色設定確認"""
        df = pd.DataFrame(
            {
                "sector17_name": ["セクター1"],
                "market_value": [100000],
            }
        )

        visualizer = CompositionVisualizer(user_id=123)
        fig = visualizer._create_sector_composition_chart(df, 100000)

        # Set3カラーパレットが使用されていることを確認
        assert fig.data[0].marker.colors is not None


class TestCreateMarketCompositionChart:
    """_create_market_composition_chartメソッドのテスト"""

    def test_create_market_composition_chart(self):
        """市場別円グラフ作成のテスト"""
        df = pd.DataFrame(
            {
                "market_name": ["東証プライム", "東証グロース", None],
                "market_value": [150000, 80000, 20000],
            }
        )
        total_value = 250000

        visualizer = CompositionVisualizer(user_id=123)

        # テスト実行
        fig = visualizer._create_market_composition_chart(df, total_value)

        # 検証
        assert isinstance(fig, Figure)
        # Noneは除外される
        assert len(fig.data[0].labels) == 2
        # 東証プライムと東証グロースのみ
        assert "東証プライム" in fig.data[0].labels
        assert "東証グロース" in fig.data[0].labels
        assert fig.layout.title.text == "市場別構成比"
        # Boldカラーパレットが使用されている
        assert fig.data[0].marker.colors is not None

    def test_create_market_composition_chart_sorting(self):
        """市場別データのソート確認"""
        df = pd.DataFrame(
            {
                "market_name": ["市場A", "市場B", "市場C"],
                "market_value": [50000, 100000, 80000],
            }
        )

        visualizer = CompositionVisualizer(user_id=123)
        fig = visualizer._create_market_composition_chart(df, 230000)

        # 市場価値の降順でソートされていることを確認
        assert fig.data[0].values[0] == 100000  # 市場B
        assert fig.data[0].values[1] == 80000  # 市場C
        assert fig.data[0].values[2] == 50000  # 市場A


class TestCreateAccountCompositionChart:
    """_create_account_composition_chartメソッドのテスト"""

    def test_create_account_composition_chart(self):
        """口座別円グラフ作成のテスト"""
        df = pd.DataFrame(
            {
                "account_name": ["特定口座", "NISA", "特定口座"],
                "market_value": [100000, 80000, 50000],
            }
        )
        total_value = 230000

        visualizer = CompositionVisualizer(user_id=123)

        # テスト実行
        fig = visualizer._create_account_composition_chart(df, total_value)

        # 検証
        assert isinstance(fig, Figure)
        # 口座別に集約されている
        assert len(fig.data[0].labels) == 2
        assert fig.layout.title.text == "口座別構成比"
        # Pastelカラーパレットが使用されている
        assert fig.data[0].marker.colors is not None

    def test_create_account_composition_chart_percentage_format(self):
        """パーセンテージ表示フォーマットの確認"""
        df = pd.DataFrame(
            {
                "account_name": ["口座A"],
                "market_value": [33333],
            }
        )
        total_value = 100000

        visualizer = CompositionVisualizer(user_id=123)
        fig = visualizer._create_account_composition_chart(df, total_value)

        # 小数点1桁で表示されることを確認
        assert "33.3%" in fig.data[0].text[0]


class TestCompositionVisualizerIntegration:
    """CompositionVisualizerの統合テスト"""

    @patch("src.portfolio.visualizers.composition.pd.read_sql")
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
                    "8058",
                    "3382",
                ],
                "market_value": [
                    500000,
                    450000,
                    400000,
                    350000,
                    300000,
                    250000,
                    200000,
                    150000,
                    100000,
                    80000,
                    70000,
                    50000,
                ],
                "account_name": ["特定口座"] * 8 + ["NISA"] * 4,
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
                    "三菱商事",
                    "セブン&アイ",
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
                    "卸売",
                    "小売",
                ],
                "market_name": ["東証プライム"] * 12,
            }
        )
        mock_read_sql.return_value = mock_df

        visualizer = CompositionVisualizer(user_id=123)
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # テスト実行
        result = visualizer.create_composition_pie_charts()

        # 検証
        assert result["total_value"] == 2900000

        # 各チャートがJSON形式で返されることを確認
        for key in [
            "stock_composition",
            "sector_composition",
            "market_composition",
            "account_composition",
        ]:
            assert key in result
            # JSON形式であることを確認
            json_data = json.loads(result[key])
            assert isinstance(json_data, dict)

    def test_visualizer_with_empty_company_names(self):
        """会社名が空の場合の処理テスト"""
        df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "company_name": ["会社A", None],
                "market_value": [100000, 80000],
            }
        )

        visualizer = CompositionVisualizer(user_id=123)
        fig = visualizer._create_stock_composition_chart(df, 180000)

        # 会社名がNoneでも空文字として扱われる
        assert len(fig.data[0].labels) == 2
        # ラベルにコードが含まれることを確認
        assert "1234" in fig.data[0].labels[0]
        assert "5678" in fig.data[0].labels[1]

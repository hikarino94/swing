"""可視化テスト用の共通フィクスチャ"""

import json
from unittest.mock import MagicMock, patch

import pytest


# モックグラフデータを返す関数
def mock_plotly_to_json():
    """plotlyのto_json()メソッドのモック"""
    return json.dumps(
        {
            "data": [
                {
                    "type": "pie",
                    "labels": ["label1", "label2"],
                    "values": [100, 200],
                    "textinfo": "label+percent",
                    "hovertemplate": "%{label}<br>%{value:,.0f}<br>%{percent}<extra></extra>",
                }
            ],
            "layout": {
                "title": {"text": "Test Chart"},
                "showlegend": True,
                "height": 400,
                "font": {"size": 14},
            },
        }
    )


def mock_plotly_line_to_json():
    """plotlyの折れ線グラフto_json()メソッドのモック"""
    return json.dumps(
        {
            "data": [
                {
                    "type": "scatter",
                    "x": ["2025-06-01", "2025-06-02"],
                    "y": [100, 110],
                    "mode": "lines+markers",
                    "name": "Test Line",
                }
            ],
            "layout": {
                "title": {"text": "Test Line Chart"},
                "xaxis": {"title": "Date"},
                "yaxis": {"title": "Value"},
            },
        }
    )


def mock_plotly_bar_to_json():
    """plotlyの棒グラフto_json()メソッドのモック"""
    return json.dumps(
        {
            "data": [
                {
                    "type": "bar",
                    "x": ["A", "B", "C"],
                    "y": [10, 20, 30],
                    "text": ["10", "20", "30"],
                    "textposition": "outside",
                }
            ],
            "layout": {
                "title": {"text": "Test Bar Chart"},
                "xaxis": {"title": "Category"},
                "yaxis": {"title": "Value"},
            },
        }
    )


def mock_plotly_treemap_to_json():
    """plotlyのtreemapグラフto_json()メソッドのモック"""
    return json.dumps(
        {
            "data": [
                {
                    "type": "treemap",
                    "labels": ["Total", "A", "B"],
                    "parents": ["", "Total", "Total"],
                    "values": [100, 60, 40],
                    "textinfo": "label+value+percent parent",
                    "marker": {"colorscale": "RdYlGn"},
                }
            ],
            "layout": {"title": {"text": "Test Treemap"}, "height": 600},
        }
    )


@pytest.fixture(autouse=True)
def mock_plotly_visualization():
    """すべての可視化テストでplotlyをモック化"""

    # Figureクラスのモック作成
    class MockFigure:
        def __init__(self, *args, **kwargs):
            self.data = kwargs.get("data", [])
            self.layout = {}
            self.traces = []

        def update_layout(self, **kwargs):
            self.layout.update(kwargs)

        def add_trace(self, trace, row=None, col=None):
            """トレースを追加"""
            self.traces.append(trace)

        def add_vline(self, x=None, **kwargs):
            """垂直線を追加"""
            pass

        def add_hline(self, y=None, **kwargs):
            """水平線を追加"""
            pass

        def to_json(self):
            # データタイプに応じて適切なモックJSONを返す
            if self.data and hasattr(self.data[0], "__class__"):
                data_type = self.data[0].__class__.__name__
                if "Pie" in data_type:
                    return mock_plotly_to_json()
                elif "Scatter" in data_type:
                    return mock_plotly_line_to_json()
                elif "Bar" in data_type:
                    return mock_plotly_bar_to_json()
                elif "Treemap" in data_type:
                    return mock_plotly_treemap_to_json()
            return mock_plotly_to_json()

    # グラフオブジェクトのモック
    mock_pie = MagicMock()
    mock_pie.__class__.__name__ = "Pie"

    mock_scatter = MagicMock()
    mock_scatter.__class__.__name__ = "Scatter"

    mock_bar = MagicMock()
    mock_bar.__class__.__name__ = "Bar"

    mock_treemap = MagicMock()
    mock_treemap.__class__.__name__ = "Treemap"

    # plotly.graph_objectsのパッチ
    with patch("plotly.graph_objects.Figure", MockFigure):
        with patch("plotly.graph_objects.Pie", return_value=mock_pie):
            with patch("plotly.graph_objects.Scatter", return_value=mock_scatter):
                with patch("plotly.graph_objects.Bar", return_value=mock_bar):
                    with patch(
                        "plotly.graph_objects.Treemap", return_value=mock_treemap
                    ):
                        # plotly.expressも同様にモック
                        with patch("plotly.express.line", return_value=MockFigure()):
                            with patch("plotly.express.bar", return_value=MockFigure()):
                                with patch(
                                    "plotly.express.pie", return_value=MockFigure()
                                ):
                                    yield


@pytest.fixture(autouse=True)
def mock_matplotlib():
    """matplotlibのインポートをモック化"""
    with patch("matplotlib.use"):
        with patch("japanize_matplotlib.japanize"):
            yield

"""ポートフォリオ可視化機能

このモジュールは後方互換性のために維持されています。
新しいコードではvisualizersパッケージを直接使用してください。
"""

from typing import Any

from .visualizers import (
    CompositionVisualizer,
    HeatmapVisualizer,
    PerformanceVisualizer,
)


class PortfolioVisualizer:
    """ポートフォリオ可視化クラス（互換性レイヤー）"""

    def __init__(self, user_id: int):
        self.user_id = user_id
        self.composition_viz = CompositionVisualizer(user_id)
        self.performance_viz = PerformanceVisualizer(user_id)
        self.heatmap_viz = HeatmapVisualizer(user_id)

    def create_composition_pie_charts(self) -> dict[str, Any]:
        """ポートフォリオ構成円グラフを作成"""
        return self.composition_viz.create_composition_pie_charts()

    def create_performance_charts(self) -> dict[str, Any]:
        """パフォーマンス推移グラフを作成"""
        return self.performance_viz.create_performance_charts()

    def create_heatmap(self) -> dict[str, Any]:
        """ヒートマップを作成"""
        return self.heatmap_viz.create_heatmap()

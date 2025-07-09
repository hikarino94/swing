"""ポートフォリオヒートマップの可視化"""

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.utils.logging_config import get_logger

from .base import BaseVisualizer

logger = get_logger("portfolio.visualizers.heatmap")


class HeatmapVisualizer(BaseVisualizer):
    """ポートフォリオヒートマップの可視化クラス"""

    def create_chart(self) -> dict[str, Any]:
        """ヒートマップを作成"""
        return self.create_heatmap()

    def create_heatmap(self) -> dict[str, Any]:
        """ヒートマップを作成"""
        conn = self.get_db_connection()
        try:
            # 保有銘柄データを取得
            query = """
                SELECT h.code, h.market_value, h.profit_loss, h.profit_loss_ratio,
                       li.company_name, li.sector17_name
                FROM holdings h
                LEFT JOIN listed_info li ON (h.code || '0') = li.code
                WHERE h.user_id = ? AND h.quantity > 0 AND h.market_value > 0
                      AND h.deleted_at IS NULL
                ORDER BY h.market_value DESC
            """
            df = pd.read_sql(query, conn, params=[self.user_id])

            if df.empty:
                return {"error": "保有銘柄データがありません"}

            # 各種ヒートマップを作成
            stock_heatmap = self._create_stock_heatmap(df)
            sector_heatmap = self._create_sector_heatmap(df)
            distribution_chart = self._create_distribution_chart(df)
            stats = self._calculate_performance_stats(df)

            return {
                "stock_heatmap": stock_heatmap,
                "sector_heatmap": sector_heatmap,
                "distribution_chart": distribution_chart,
                "stats": stats,
            }

        finally:
            conn.close()

    def _create_stock_heatmap(self, df: pd.DataFrame) -> go.Figure:
        """銘柄別損益率のツリーマップを作成"""
        # 上位20銘柄を対象
        top_df = df.head(20).copy()

        # 会社名がNoneの場合の処理
        top_df["display_name"] = top_df.apply(
            lambda row: f"{row['code']} {row['company_name'] or '不明'}", axis=1
        )

        # 色の設定（損益率に基づく）
        top_df["color_value"] = top_df["profit_loss_ratio"].fillna(0)

        fig = px.treemap(
            top_df,
            path=["display_name"],
            values="market_value",
            color="color_value",
            color_continuous_scale=["red", "yellow", "green"],
            color_continuous_midpoint=0,
            title="銘柄別パフォーマンス（上位20銘柄）",
            hover_data={
                "market_value": ":,.0f",
                "profit_loss": ":,.0f",
                "profit_loss_ratio": ":.2f",
            },
            labels={
                "market_value": "評価額",
                "profit_loss": "損益",
                "profit_loss_ratio": "損益率(%)",
                "color_value": "損益率(%)",
            },
        )

        fig.update_traces(
            textinfo="label+value",
            texttemplate="%{label}<br>%{value:,.0f}円<br>%{color:.1f}%",
        )

        fig.update_layout(height=600, font={"size": 12})

        return fig

    def _create_sector_heatmap(self, df: pd.DataFrame) -> go.Figure:
        """セクター別パフォーマンスのツリーマップを作成"""
        # セクター別に集計
        sector_df = (
            df.groupby("sector17_name")
            .agg(
                {
                    "market_value": "sum",
                    "profit_loss": "sum",
                }
            )
            .reset_index()
        )

        # 損益率を計算
        sector_df["profit_loss_ratio"] = (
            sector_df["profit_loss"]
            / (sector_df["market_value"] - sector_df["profit_loss"])
            * 100
        )

        # "None"を"不明"に変換
        sector_df["sector17_name"] = sector_df["sector17_name"].fillna("不明")

        fig = px.treemap(
            sector_df,
            path=["sector17_name"],
            values="market_value",
            color="profit_loss_ratio",
            color_continuous_scale=["red", "yellow", "green"],
            color_continuous_midpoint=0,
            title="セクター別パフォーマンス",
            hover_data={
                "market_value": ":,.0f",
                "profit_loss": ":,.0f",
                "profit_loss_ratio": ":.2f",
            },
            labels={
                "market_value": "評価額",
                "profit_loss": "損益",
                "profit_loss_ratio": "損益率(%)",
                "sector17_name": "セクター",
            },
        )

        fig.update_traces(
            textinfo="label+value",
            texttemplate="%{label}<br>%{value:,.0f}円<br>%{color:.1f}%",
        )

        fig.update_layout(height=600, font={"size": 12})

        return fig

    def _create_distribution_chart(self, df: pd.DataFrame) -> go.Figure:
        """損益率分布のヒストグラムを作成"""
        # 損益率のヒストグラム
        fig = go.Figure()

        # プラスとマイナスで色分け
        positive_df = df[df["profit_loss_ratio"] > 0]
        negative_df = df[df["profit_loss_ratio"] <= 0]

        if not positive_df.empty:
            fig.add_trace(
                go.Histogram(
                    x=positive_df["profit_loss_ratio"],
                    name="プラス",
                    marker_color="green",
                    opacity=0.7,
                    nbinsx=20,
                )
            )

        if not negative_df.empty:
            fig.add_trace(
                go.Histogram(
                    x=negative_df["profit_loss_ratio"],
                    name="マイナス",
                    marker_color="red",
                    opacity=0.7,
                    nbinsx=20,
                )
            )

        fig.update_layout(
            title={"text": "損益率分布", "x": 0.5, "xanchor": "center"},
            xaxis_title="損益率（%）",
            yaxis_title="銘柄数",
            height=400,
            barmode="overlay",
            showlegend=True,
        )

        # ゼロラインを追加
        fig.add_vline(x=0, line_dash="dash", line_color="gray")

        return fig

    def _calculate_performance_stats(self, df: pd.DataFrame) -> dict[str, Any]:
        """パフォーマンス統計を計算"""
        total_value = df["market_value"].sum()
        total_profit_loss = df["profit_loss"].sum()
        total_profit_loss_ratio = (
            total_profit_loss / (total_value - total_profit_loss) * 100
            if total_value > total_profit_loss
            else 0
        )

        positive_count = len(df[df["profit_loss_ratio"] > 0])
        negative_count = len(df[df["profit_loss_ratio"] <= 0])
        win_rate = positive_count / len(df) * 100 if len(df) > 0 else 0

        # 最高・最低パフォーマー
        best_performer = df.nlargest(1, "profit_loss_ratio").iloc[0]
        worst_performer = df.nsmallest(1, "profit_loss_ratio").iloc[0]

        return {
            "total_value": total_value,
            "total_profit_loss": total_profit_loss,
            "total_profit_loss_ratio": total_profit_loss_ratio,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "win_rate": win_rate,
            "best_performer": {
                "code": best_performer["code"],
                "company_name": best_performer["company_name"] or "不明",
                "profit_loss_ratio": best_performer["profit_loss_ratio"],
            },
            "worst_performer": {
                "code": worst_performer["code"],
                "company_name": worst_performer["company_name"] or "不明",
                "profit_loss_ratio": worst_performer["profit_loss_ratio"],
            },
        }

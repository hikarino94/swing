"""ポートフォリオ構成の可視化"""

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.utils.logging_config import get_logger

from .base import BaseVisualizer

logger = get_logger("portfolio.visualizers.composition")


class CompositionVisualizer(BaseVisualizer):
    """ポートフォリオ構成の可視化クラス"""

    def create_chart(self) -> dict[str, Any]:
        """ポートフォリオ構成円グラフを作成"""
        return self.create_composition_pie_charts()

    def create_composition_pie_charts(self) -> dict[str, Any]:
        """ポートフォリオ構成円グラフを作成"""
        conn = self.get_db_connection()
        try:
            # 保有銘柄データを取得（論理削除されていないもののみ）
            query = """
                SELECT h.code, h.market_value, h.account_name,
                       li.company_name, li.sector17_name, li.sector33_name,
                       li.market_name
                FROM holdings h
                LEFT JOIN listed_info li ON (h.code || '0') = li.code
                WHERE h.user_id = ? AND h.quantity > 0 AND h.market_value > 0
                      AND h.deleted_at IS NULL
            """
            df = pd.read_sql(query, conn, params=[self.user_id])

            if df.empty:
                return {"error": "保有銘柄データがありません"}

            # 総資産を計算
            total_value = df["market_value"].sum()

            # 各種円グラフを作成
            stock_fig = self._create_stock_composition_chart(df, total_value)
            sector_fig = self._create_sector_composition_chart(df, total_value)
            market_fig = self._create_market_composition_chart(df, total_value)
            account_fig = self._create_account_composition_chart(df, total_value)

            return {
                "stock_composition": stock_fig.to_json(),
                "sector_composition": sector_fig.to_json(),
                "market_composition": market_fig.to_json(),
                "account_composition": account_fig.to_json(),
                "total_value": total_value,
            }

        finally:
            conn.close()

    def _create_stock_composition_chart(
        self, df: pd.DataFrame, total_value: float
    ) -> go.Figure:
        """銘柄別構成比の円グラフを作成"""
        stock_data = (
            df.groupby(["code", "company_name"])["market_value"].sum().reset_index()
        )
        stock_data["percentage"] = (
            stock_data["market_value"] / total_value * 100
        ).round(2)
        stock_data = stock_data.sort_values("market_value", ascending=False)

        # 上位10銘柄とその他
        if len(stock_data) > 10:
            top10 = stock_data.head(10)
            others_value = stock_data.iloc[10:]["market_value"].sum()
            others_pct = (others_value / total_value * 100).round(2)
            others_row = pd.DataFrame(
                {
                    "code": ["その他"],
                    "company_name": ["その他"],
                    "market_value": [others_value],
                    "percentage": [others_pct],
                }
            )
            stock_data_display = pd.concat([top10, others_row])
        else:
            stock_data_display = stock_data

        stock_fig = go.Figure(
            data=[
                go.Pie(
                    labels=[
                        f"{row['code']} {row['company_name'] or ''}"
                        for _, row in stock_data_display.iterrows()
                    ],
                    values=stock_data_display["market_value"],
                    text=[
                        f"{row['percentage']:.1f}%"
                        for _, row in stock_data_display.iterrows()
                    ],
                    textposition="inside",
                    textinfo="text",
                    hole=0.3,
                    marker={"line": {"color": "white", "width": 2}},
                )
            ]
        )
        stock_fig.update_layout(
            title={"text": "銘柄別構成比", "x": 0.5, "xanchor": "center"},
            height=500,
            font={"size": 12},
            showlegend=True,
            legend={"orientation": "v", "yanchor": "middle", "y": 0.5, "x": 1.05},
        )

        return stock_fig

    def _create_sector_composition_chart(
        self, df: pd.DataFrame, total_value: float
    ) -> go.Figure:
        """セクター別構成比の円グラフを作成"""
        sector_data = df.groupby("sector17_name")["market_value"].sum().reset_index()
        sector_data["percentage"] = (
            sector_data["market_value"] / total_value * 100
        ).round(2)
        sector_data = sector_data.sort_values("market_value", ascending=False)

        # "None"を"不明"に変換
        sector_data["sector17_name"] = sector_data["sector17_name"].fillna("不明")

        sector_fig = go.Figure(
            data=[
                go.Pie(
                    labels=sector_data["sector17_name"],
                    values=sector_data["market_value"],
                    text=[f"{pct:.1f}%" for pct in sector_data["percentage"]],
                    textposition="inside",
                    textinfo="text",
                    hole=0.3,
                    marker={
                        "line": {"color": "white", "width": 2},
                        "colors": px.colors.qualitative.Set3,
                    },
                )
            ]
        )
        sector_fig.update_layout(
            title={"text": "セクター別構成比", "x": 0.5, "xanchor": "center"},
            height=500,
            font={"size": 12},
            showlegend=True,
            legend={"orientation": "v", "yanchor": "middle", "y": 0.5, "x": 1.05},
        )

        return sector_fig

    def _create_market_composition_chart(
        self, df: pd.DataFrame, total_value: float
    ) -> go.Figure:
        """市場別構成比の円グラフを作成"""
        market_data = df.groupby("market_name")["market_value"].sum().reset_index()
        market_data["percentage"] = (
            market_data["market_value"] / total_value * 100
        ).round(2)
        market_data = market_data.sort_values("market_value", ascending=False)

        # "None"を"不明"に変換
        market_data["market_name"] = market_data["market_name"].fillna("不明")

        market_fig = go.Figure(
            data=[
                go.Pie(
                    labels=market_data["market_name"],
                    values=market_data["market_value"],
                    text=[f"{pct:.1f}%" for pct in market_data["percentage"]],
                    textposition="inside",
                    textinfo="text",
                    hole=0.3,
                    marker={
                        "line": {"color": "white", "width": 2},
                        "colors": px.colors.qualitative.Bold,
                    },
                )
            ]
        )
        market_fig.update_layout(
            title={"text": "市場別構成比", "x": 0.5, "xanchor": "center"},
            height=500,
            font={"size": 12},
            showlegend=True,
            legend={"orientation": "v", "yanchor": "middle", "y": 0.5, "x": 1.05},
        )

        return market_fig

    def _create_account_composition_chart(
        self, df: pd.DataFrame, total_value: float
    ) -> go.Figure:
        """口座別構成比の円グラフを作成"""
        account_data = df.groupby("account_name")["market_value"].sum().reset_index()
        account_data["percentage"] = (
            account_data["market_value"] / total_value * 100
        ).round(2)
        account_data = account_data.sort_values("market_value", ascending=False)

        account_fig = go.Figure(
            data=[
                go.Pie(
                    labels=account_data["account_name"],
                    values=account_data["market_value"],
                    text=[f"{pct:.1f}%" for pct in account_data["percentage"]],
                    textposition="inside",
                    textinfo="text",
                    hole=0.3,
                    marker={
                        "line": {"color": "white", "width": 2},
                        "colors": px.colors.qualitative.Pastel,
                    },
                )
            ]
        )
        account_fig.update_layout(
            title={"text": "口座別構成比", "x": 0.5, "xanchor": "center"},
            height=500,
            font={"size": 12},
            showlegend=True,
            legend={"orientation": "v", "yanchor": "middle", "y": 0.5, "x": 1.05},
        )

        return account_fig

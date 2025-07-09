"""ポートフォリオパフォーマンスの可視化"""

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.utils.logging_config import get_logger

from .base import BaseVisualizer

logger = get_logger("portfolio.visualizers.performance")


class PerformanceVisualizer(BaseVisualizer):
    """ポートフォリオパフォーマンスの可視化クラス"""

    def create_chart(self) -> dict[str, Any]:
        """パフォーマンスチャートを作成"""
        return self.create_performance_charts()

    def create_performance_charts(self) -> dict[str, Any]:
        """パフォーマンス推移グラフを作成"""
        conn = self.get_db_connection()
        try:
            # 取引履歴を取得
            trans_query = """
                SELECT transaction_date, transaction_type, code, quantity, price,
                       commission, tax
                FROM transactions
                WHERE user_id = ?
                ORDER BY transaction_date
            """
            trans_df = pd.read_sql(trans_query, conn, params=[self.user_id])

            if trans_df.empty:
                return {"error": "取引履歴データがありません"}

            # 日付型に変換
            trans_df["transaction_date"] = pd.to_datetime(trans_df["transaction_date"])

            # 最初の取引日から現在までの日付範囲を作成
            start_date = trans_df["transaction_date"].min()
            end_date = datetime.now()
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")

            # 保有銘柄の推移を計算
            holdings_by_date = self._calculate_holdings_by_date(trans_df, date_range)

            # 価格データを取得
            codes = trans_df["code"].unique().tolist()
            prices_query = """
                SELECT code, date, adj_close
                FROM prices
                WHERE code IN ({})
                AND date >= ?
                ORDER BY code, date
            """.format(
                ",".join("?" * len(codes))
            )
            prices_df = pd.read_sql(
                prices_query,
                conn,
                params=codes + [start_date.strftime("%Y-%m-%d")],
            )

            if prices_df.empty:
                # 価格データがない場合は簡易グラフを作成
                return self._create_simple_performance_charts(trans_df)

            prices_df["date"] = pd.to_datetime(prices_df["date"])

            # 日次の資産価値を計算
            daily_values = self._calculate_daily_values(
                holdings_by_date, prices_df, date_range
            )

            # チャートを作成
            value_fig = self._create_value_chart(daily_values)
            return_fig = self._create_return_chart(daily_values)
            benchmark_fig = self._create_benchmark_chart(daily_values)

            return {
                "value_chart": value_fig.to_json(),
                "return_chart": return_fig.to_json(),
                "benchmark_chart": benchmark_fig.to_json(),
            }

        finally:
            conn.close()

    def _calculate_holdings_by_date(
        self, trans_df: pd.DataFrame, date_range: pd.DatetimeIndex
    ) -> dict[pd.Timestamp | Any, dict[str, float]]:
        """日付ごとの保有銘柄数を計算"""
        holdings_by_date = {}

        for date in date_range:
            holdings = {}
            # その日までの全取引を対象
            past_trans = trans_df[trans_df["transaction_date"] <= date]

            for code in past_trans["code"].unique():
                code_trans = past_trans[past_trans["code"] == code]
                # 買い注文の合計
                buy_qty = code_trans[code_trans["transaction_type"] == "buy"][
                    "quantity"
                ].sum()
                # 売り注文の合計
                sell_qty = code_trans[code_trans["transaction_type"] == "sell"][
                    "quantity"
                ].sum()
                # 保有数量
                qty = buy_qty - sell_qty
                if qty > 0:
                    holdings[code] = qty

            holdings_by_date[date] = holdings

        return holdings_by_date

    def _calculate_daily_values(
        self,
        holdings_by_date: dict,
        prices_df: pd.DataFrame,
        date_range: pd.DatetimeIndex,
    ) -> pd.DataFrame:
        """日次の資産価値を計算"""
        daily_data = []

        for date in date_range:
            holdings = holdings_by_date[date]
            if not holdings:
                continue

            # その日の評価額を計算
            total_value = 0
            total_cost = 0

            for code, quantity in holdings.items():
                # 最新の株価を取得
                code_prices = prices_df[
                    (prices_df["code"] == code) & (prices_df["date"] <= date)
                ]
                if not code_prices.empty:
                    latest_price = code_prices.iloc[-1]["adj_close"]
                    total_value += latest_price * quantity

                # 取得原価を計算（簡易的に平均取得価格を使用）
                # TODO: より正確な取得原価の計算
                total_cost += quantity * 1000  # 仮の取得価格

            if total_value > 0:
                daily_data.append(
                    {
                        "date": date,
                        "total_value": total_value,
                        "total_cost": total_cost,
                        "profit_loss": total_value - total_cost,
                        "return_rate": (
                            (total_value - total_cost) / total_cost * 100
                            if total_cost > 0
                            else 0
                        ),
                    }
                )

        return pd.DataFrame(daily_data)

    def _create_value_chart(self, daily_values: pd.DataFrame) -> go.Figure:
        """資産総額の推移グラフを作成"""
        fig = go.Figure()

        # 評価額の推移
        fig.add_trace(
            go.Scatter(
                x=daily_values["date"],
                y=daily_values["total_value"],
                mode="lines",
                name="評価額",
                line={"color": "blue", "width": 2},
            )
        )

        # 取得コストの推移
        fig.add_trace(
            go.Scatter(
                x=daily_values["date"],
                y=daily_values["total_cost"],
                mode="lines",
                name="取得コスト",
                line={"color": "gray", "width": 1, "dash": "dash"},
            )
        )

        fig.update_layout(
            title={"text": "資産総額の推移", "x": 0.5, "xanchor": "center"},
            xaxis_title="日付",
            yaxis_title="金額（円）",
            height=400,
            hovermode="x",
            showlegend=True,
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0.5},
        )

        return fig

    def _create_return_chart(self, daily_values: pd.DataFrame) -> go.Figure:
        """損益率の推移グラフを作成"""
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=daily_values["date"],
                y=daily_values["return_rate"],
                mode="lines",
                name="損益率",
                line={"color": "green", "width": 2},
                fill="tozeroy",
                fillcolor="rgba(0,255,0,0.1)",
            )
        )

        # ゼロラインを追加
        fig.add_hline(y=0, line_dash="dash", line_color="gray")

        fig.update_layout(
            title={"text": "損益率の推移", "x": 0.5, "xanchor": "center"},
            xaxis_title="日付",
            yaxis_title="損益率（%）",
            height=400,
            hovermode="x",
        )

        return fig

    def _create_benchmark_chart(self, daily_values: pd.DataFrame) -> go.Figure:
        """ベンチマーク比較グラフを作成"""
        fig = go.Figure()

        # ポートフォリオのリターン（初期値を100とする）
        if len(daily_values) > 0:
            initial_value = daily_values.iloc[0]["total_value"]
            portfolio_index = (
                daily_values["total_value"] / initial_value * 100
            ).reset_index(drop=True)

            fig.add_trace(
                go.Scatter(
                    x=daily_values["date"],
                    y=portfolio_index,
                    mode="lines",
                    name="ポートフォリオ",
                    line={"color": "blue", "width": 2},
                )
            )

            # ベンチマーク（仮のデータ）
            # TODO: 実際のTOPIXデータを取得
            benchmark_index = 100 + np.random.randn(len(daily_values)).cumsum() * 2
            fig.add_trace(
                go.Scatter(
                    x=daily_values["date"],
                    y=benchmark_index,
                    mode="lines",
                    name="TOPIX（仮）",
                    line={"color": "red", "width": 2, "dash": "dash"},
                )
            )

        fig.update_layout(
            title={
                "text": "ベンチマーク比較（初期値=100）",
                "x": 0.5,
                "xanchor": "center",
            },
            xaxis_title="日付",
            yaxis_title="指数",
            height=400,
            hovermode="x",
            showlegend=True,
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0.5},
        )

        return fig

    def _create_simple_performance_charts(
        self, trans_df: pd.DataFrame
    ) -> dict[str, Any]:
        """価格データがない場合の簡易グラフを作成"""
        # 取引金額の累積を計算
        trans_df = trans_df.sort_values("transaction_date")
        trans_df["cumulative_amount"] = 0

        cumulative = 0
        for idx, row in trans_df.iterrows():
            if row["transaction_type"] == "buy":
                cumulative += row["quantity"] * row["price"] + row["commission"]
            else:  # sell
                cumulative -= row["quantity"] * row["price"] - row["commission"]
            trans_df.at[idx, "cumulative_amount"] = cumulative

        # 簡易的なグラフを作成
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=trans_df["transaction_date"],
                y=trans_df["cumulative_amount"],
                mode="lines+markers",
                name="累積投資額",
                line={"color": "blue", "width": 2},
            )
        )

        fig.update_layout(
            title={
                "text": "累積投資額の推移（価格データなし）",
                "x": 0.5,
                "xanchor": "center",
            },
            xaxis_title="日付",
            yaxis_title="金額（円）",
            height=400,
        )

        return {
            "value_chart": fig.to_json(),
            "return_chart": None,
            "benchmark_chart": None,
        }

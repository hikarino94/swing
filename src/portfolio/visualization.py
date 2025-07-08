"""ポートフォリオ可視化機能"""

import sqlite3
from datetime import datetime, timedelta
from typing import Any

import japanize_matplotlib
import matplotlib
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.config import DB_PATH
from src.utils.logging_config import get_logger

# バックエンドを設定（GUIを使わない）
matplotlib.use("Agg")

# 日本語フォントの設定
japanize_matplotlib.japanize()

logger = get_logger("portfolio.visualization")


class PortfolioVisualizer:
    """ポートフォリオ可視化クラス"""

    def __init__(self, user_id: int):
        self.user_id = user_id

    def create_composition_pie_charts(self) -> dict[str, Any]:
        """ポートフォリオ構成円グラフを作成"""
        conn = sqlite3.connect(DB_PATH)
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

            # 1. 銘柄別構成比の円グラフ
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
                font={
                    "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif",
                    "size": 12,
                },
                showlegend=True,
                legend={
                    "orientation": "v",
                    "yanchor": "top",
                    "y": 0.95,
                    "xanchor": "left",
                    "x": 1.02,
                    "font": {"size": 10},
                },
                margin={"l": 20, "r": 150, "t": 60, "b": 20},
                autosize=True,
                height=400,
            )

            # 2. セクター別構成比の円グラフ（17業種）
            sector_data = (
                df.groupby("sector17_name")["market_value"].sum().reset_index()
            )
            sector_data["percentage"] = (
                sector_data["market_value"] / total_value * 100
            ).round(2)
            sector_data = sector_data.sort_values("market_value", ascending=False)

            sector_fig = go.Figure(
                data=[
                    go.Pie(
                        labels=sector_data["sector17_name"],
                        values=sector_data["market_value"],
                        text=[f"{pct:.1f}%" for pct in sector_data["percentage"]],
                        textposition="inside",
                        textinfo="text",
                        hole=0.3,
                        marker={"line": {"color": "white", "width": 2}},
                    )
                ]
            )
            sector_fig.update_layout(
                title={
                    "text": "セクター別構成比（17業種）",
                    "x": 0.5,
                    "xanchor": "center",
                },
                font={
                    "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif",
                    "size": 12,
                },
                showlegend=True,
                legend={
                    "orientation": "v",
                    "yanchor": "top",
                    "y": 0.95,
                    "xanchor": "left",
                    "x": 1.02,
                    "font": {"size": 10},
                },
                margin={"l": 20, "r": 150, "t": 60, "b": 20},
                autosize=True,
                height=400,
            )

            # 3. 資産クラス別構成比（市場別）
            market_data = df.groupby("market_name")["market_value"].sum().reset_index()
            market_data["percentage"] = (
                market_data["market_value"] / total_value * 100
            ).round(2)
            market_data = market_data.sort_values("market_value", ascending=False)

            market_fig = go.Figure(
                data=[
                    go.Pie(
                        labels=market_data["market_name"],
                        values=market_data["market_value"],
                        text=[f"{pct:.1f}%" for pct in market_data["percentage"]],
                        textposition="inside",
                        textinfo="text",
                        hole=0.3,
                        marker={"line": {"color": "white", "width": 2}},
                    )
                ]
            )
            market_fig.update_layout(
                title={"text": "市場別構成比", "x": 0.5, "xanchor": "center"},
                font={
                    "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif",
                    "size": 12,
                },
                showlegend=True,
                legend={
                    "orientation": "v",
                    "yanchor": "top",
                    "y": 0.95,
                    "xanchor": "left",
                    "x": 1.02,
                    "font": {"size": 10},
                },
                margin={"l": 20, "r": 150, "t": 60, "b": 20},
                autosize=True,
                height=400,
            )

            # 口座別構成比も追加
            account_data = (
                df.groupby("account_name")["market_value"].sum().reset_index()
            )
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
                        marker={"line": {"color": "white", "width": 2}},
                    )
                ]
            )
            account_fig.update_layout(
                title={"text": "口座別構成比", "x": 0.5, "xanchor": "center"},
                font={
                    "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif",
                    "size": 12,
                },
                showlegend=True,
                legend={
                    "orientation": "v",
                    "yanchor": "top",
                    "y": 0.95,
                    "xanchor": "left",
                    "x": 1.02,
                    "font": {"size": 10},
                },
                margin={"l": 20, "r": 150, "t": 60, "b": 20},
                autosize=True,
                height=400,
            )

            return {
                "stock_chart": stock_fig.to_json(),
                "sector_chart": sector_fig.to_json(),
                "market_chart": market_fig.to_json(),
                "account_chart": account_fig.to_json(),
                "total_value": total_value,
                "summary": {
                    "total_stocks": len(df["code"].unique()),
                    "total_sectors": len(df["sector17_name"].unique()),
                    "total_markets": len(df["market_name"].unique()),
                    "total_accounts": len(df["account_name"].unique()),
                },
            }

        except Exception as e:
            logger.error(f"円グラフ作成エラー: {str(e)}")
            return {"error": str(e)}
        finally:
            conn.close()

    def create_performance_charts(self, days: int = 180) -> dict[str, Any]:
        """パフォーマンス推移グラフを作成"""
        conn = sqlite3.connect(DB_PATH)
        try:
            # 過去の取引履歴から資産推移を計算
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # 取引履歴を取得
            trans_query = """
                SELECT transaction_date, code, transaction_type, quantity, price,
                       commission, tax
                FROM transactions
                WHERE user_id = ? AND transaction_date >= ?
                ORDER BY transaction_date
            """
            trans_df = pd.read_sql(
                trans_query,
                conn,
                params=[self.user_id, start_date.strftime("%Y-%m-%d")],
            )

            # 日次の株価データを取得
            price_query = """
                SELECT p.date, p.code, p.close
                FROM prices p
                INNER JOIN (
                    SELECT DISTINCT code FROM holdings WHERE user_id = ? AND deleted_at IS NULL
                    UNION
                    SELECT DISTINCT code FROM transactions WHERE user_id = ?
                ) h ON p.code = h.code
                WHERE p.date >= ?
                ORDER BY p.date, p.code
            """
            price_df = pd.read_sql(
                price_query,
                conn,
                params=[self.user_id, self.user_id, start_date.strftime("%Y-%m-%d")],
            )

            if price_df.empty:
                # 現在の保有銘柄情報のみでシンプルなグラフを作成
                holdings_query = """
                    SELECT h.code, h.quantity, h.average_price, h.market_value,
                           h.profit_loss, h.profit_loss_ratio, h.updated_at
                    FROM holdings h
                    WHERE h.user_id = ? AND h.quantity > 0 AND h.deleted_at IS NULL
                """
                holdings_df = pd.read_sql(holdings_query, conn, params=[self.user_id])

                if holdings_df.empty:
                    return {"error": "データがありません"}

                # 現在の情報のみで簡易グラフを作成
                total_cost = (
                    holdings_df["quantity"] * holdings_df["average_price"]
                ).sum()
                total_value = holdings_df["market_value"].sum()
                total_profit = holdings_df["profit_loss"].sum()
                total_profit_ratio = (
                    (total_profit / total_cost * 100) if total_cost > 0 else 0
                )

                # 簡易的な資産推移（現在のみ）
                fig_value = go.Figure()
                fig_value.add_trace(
                    go.Scatter(
                        x=["現在"],
                        y=[total_value],
                        mode="markers",
                        marker={"size": 10},
                        name="資産総額",
                    )
                )
                fig_value.update_layout(
                    title="資産総額（現在）",
                    xaxis_title="",
                    yaxis_title="金額（円）",
                    font={
                        "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif"
                    },
                    yaxis={"tickformat": ","},
                )

                # 損益率
                fig_profit = go.Figure()
                fig_profit.add_trace(
                    go.Bar(
                        x=["総合損益率"],
                        y=[total_profit_ratio],
                        text=[f"{total_profit_ratio:.2f}%"],
                        textposition="auto",
                    )
                )
                fig_profit.update_layout(
                    title="損益率",
                    yaxis_title="損益率（%）",
                    font={
                        "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif"
                    },
                    showlegend=False,
                )

                return {
                    "value_chart": fig_value.to_json(),
                    "profit_chart": fig_profit.to_json(),
                    "benchmark_chart": None,
                    "summary": {
                        "total_cost": total_cost,
                        "total_value": total_value,
                        "total_profit": total_profit,
                        "total_profit_ratio": total_profit_ratio,
                    },
                }

            # 日付のリストを作成
            date_range = pd.date_range(start_date, end_date, freq="D")

            # 各日の保有銘柄と評価額を計算
            daily_values = []
            daily_costs = []

            for date in date_range:
                date_str = date.strftime("%Y-%m-%d")

                # その日までの取引を反映した保有状況を計算
                holdings = {}
                costs = {}

                # 取引履歴を反映
                past_trans = trans_df[trans_df["transaction_date"] <= date_str]
                for _, trans in past_trans.iterrows():
                    code = trans["code"]
                    if code not in holdings:
                        holdings[code] = 0
                        costs[code] = 0

                    if trans["transaction_type"] == "buy":
                        holdings[code] += trans["quantity"]
                        costs[code] += (
                            trans["quantity"] * trans["price"] + trans["commission"]
                        )
                    else:  # sell
                        holdings[code] -= trans["quantity"]
                        if holdings[code] > 0:
                            # 平均取得価格を維持
                            avg_cost = costs[code] / (
                                holdings[code] + trans["quantity"]
                            )
                            costs[code] = holdings[code] * avg_cost
                        else:
                            costs[code] = 0

                # その日の評価額を計算
                day_prices = price_df[price_df["date"] == date_str]
                total_value = 0
                total_cost = 0

                for code, quantity in holdings.items():
                    if quantity > 0:
                        price_row = day_prices[day_prices["code"] == code]
                        if not price_row.empty:
                            total_value += quantity * price_row.iloc[0]["close"]
                        total_cost += costs.get(code, 0)

                daily_values.append(total_value)
                daily_costs.append(total_cost)

            # パフォーマンスグラフを作成
            performance_df = pd.DataFrame(
                {"date": date_range, "value": daily_values, "cost": daily_costs}
            )
            performance_df["profit"] = performance_df["value"] - performance_df["cost"]
            performance_df["profit_ratio"] = (
                performance_df["profit"] / performance_df["cost"] * 100
            ).fillna(0)

            # 1. 資産総額の推移
            fig_value = go.Figure()
            fig_value.add_trace(
                go.Scatter(
                    x=performance_df["date"],
                    y=performance_df["value"],
                    mode="lines",
                    name="評価額",
                    line={"width": 2},
                )
            )
            fig_value.add_trace(
                go.Scatter(
                    x=performance_df["date"],
                    y=performance_df["cost"],
                    mode="lines",
                    name="取得コスト",
                    line={"width": 2, "dash": "dash"},
                )
            )
            fig_value.update_layout(
                title=f"資産総額の推移（過去{days}日間）",
                xaxis_title="日付",
                yaxis_title="金額（円）",
                font={
                    "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif"
                },
                yaxis={"tickformat": ","},
                hovermode="x unified",
            )

            # 2. 損益率の推移
            fig_profit = go.Figure()
            fig_profit.add_trace(
                go.Scatter(
                    x=performance_df["date"],
                    y=performance_df["profit_ratio"],
                    mode="lines",
                    name="損益率",
                    line={"width": 2},
                    fill="tozeroy",
                )
            )
            fig_profit.update_layout(
                title=f"損益率の推移（過去{days}日間）",
                xaxis_title="日付",
                yaxis_title="損益率（%）",
                font={
                    "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif"
                },
                hovermode="x unified",
                yaxis={"zeroline": True, "zerolinewidth": 2, "zerolinecolor": "gray"},
            )

            # 3. ベンチマーク比較（TOPIX）
            # TOPIXデータがある場合は比較（ここでは仮実装）
            fig_benchmark = go.Figure()
            fig_benchmark.add_trace(
                go.Scatter(
                    x=performance_df["date"],
                    y=performance_df["profit_ratio"],
                    mode="lines",
                    name="ポートフォリオ",
                    line={"width": 2},
                )
            )
            # TOPIXデータがあれば追加
            fig_benchmark.update_layout(
                title="ベンチマーク比較",
                xaxis_title="日付",
                yaxis_title="リターン（%）",
                font={
                    "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif"
                },
                hovermode="x unified",
            )

            return {
                "value_chart": fig_value.to_json(),
                "profit_chart": fig_profit.to_json(),
                "benchmark_chart": fig_benchmark.to_json(),
                "summary": {
                    "start_value": daily_values[0] if daily_values else 0,
                    "end_value": daily_values[-1] if daily_values else 0,
                    "max_value": max(daily_values) if daily_values else 0,
                    "min_value": min(daily_values) if daily_values else 0,
                    "total_return": (
                        performance_df["profit_ratio"].iloc[-1]
                        if not performance_df.empty
                        else 0
                    ),
                },
            }

        except Exception as e:
            logger.error(f"パフォーマンスグラフ作成エラー: {str(e)}")
            return {"error": str(e)}
        finally:
            conn.close()

    def create_heatmap(self) -> dict[str, Any]:
        """ヒートマップを作成"""
        conn = sqlite3.connect(DB_PATH)
        try:
            # 保有銘柄データを取得
            query = """
                SELECT h.code, h.profit_loss_ratio, h.market_value,
                       li.company_name, li.sector17_name, li.sector33_name
                FROM holdings h
                LEFT JOIN listed_info li ON (h.code || '0') = li.code
                WHERE h.user_id = ? AND h.quantity > 0 AND h.deleted_at IS NULL
                ORDER BY h.profit_loss_ratio DESC
            """
            df = pd.read_sql(query, conn, params=[self.user_id])

            if df.empty:
                return {"error": "保有銘柄データがありません"}

            # 1. 銘柄別損益率ヒートマップ
            # 上位20銘柄を表示
            top_stocks = df.head(20)

            # ヒートマップ用のデータを準備
            stock_labels = [
                f"{row['code']} {row['company_name'] or ''}"
                for _, row in top_stocks.iterrows()
            ]
            # 数値型を確実にしてNaN値を処理
            stock_values_series = pd.to_numeric(
                top_stocks["profit_loss_ratio"], errors="coerce"
            )
            stock_values = np.nan_to_num(stock_values_series.to_numpy(), nan=0.0)
            stock_sizes_series = pd.to_numeric(
                top_stocks["market_value"], errors="coerce"
            )
            stock_sizes = np.nan_to_num(stock_sizes_series.to_numpy(), nan=0.0)

            # デバッグ情報をログに出力
            logger.info(
                f"ヒートマップデータ: 銘柄数={len(stock_labels)}, 損益率型={stock_values.dtype}, 最小={stock_values.min():.1f}, 最大={stock_values.max():.1f}"
            )

            # Treemapで銘柄別損益率を表示
            # Plotly Expressを使用した実装に変更
            try:
                # データフレームを準備
                treemap_data = pd.DataFrame(
                    {
                        "labels": stock_labels,
                        "values": stock_sizes,
                        "colors": stock_values,
                    }
                )

                # Plotly Expressでツリーマップを作成
                fig_stocks = px.treemap(
                    treemap_data,
                    path=["labels"],
                    values="values",
                    color="colors",
                    color_continuous_scale="RdYlGn",
                    color_continuous_midpoint=0,
                    range_color=[-20, 20],
                    title="銘柄別損益率ヒートマップ（上位20銘柄）",
                )

                # カスタマイズ
                fig_stocks.update_traces(
                    textposition="middle center",
                    texttemplate="%{label}<br>%{color:.1f}%",
                    hovertemplate="<b>%{label}</b><br>損益率: %{color:.1f}%<br>評価額: ¥%{value:,.0f}<extra></extra>",
                )

                fig_stocks.update_layout(
                    coloraxis_colorbar_title="損益率(%)",
                    font={
                        "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif"
                    },
                    margin={"t": 50, "l": 25, "r": 25, "b": 25},
                )

            except Exception as e:
                logger.warning(
                    f"Plotly Express実装でエラー: {e}. go.Treemapにフォールバック"
                )
                # フォールバック: go.Treemapを使用
                fig_stocks = go.Figure(
                    go.Treemap(
                        labels=stock_labels,
                        values=stock_sizes,
                        parents=[""] * len(stock_labels),
                        text=[f"{val:.1f}%" for val in stock_values],
                        textposition="middle center",
                        marker={
                            "colors": stock_values.tolist(),
                            "colorscale": "RdYlGn",
                            "cmid": 0,
                            "cmin": -20,
                            "cmax": 20,
                            "showscale": True,
                            "colorbar": {"title": "損益率(%)", "thickness": 15},
                            "line": {"width": 2, "color": "white"},
                        },
                        customdata=stock_values,
                        hovertemplate="<b>%{label}</b><br>損益率: %{customdata:.1f}%<br>評価額: ¥%{value:,.0f}<extra></extra>",
                    )
                )
                fig_stocks.update_traces(marker_colorbar_thickness=15)
                fig_stocks.update_layout(
                    title="銘柄別損益率ヒートマップ（上位20銘柄）",
                    font={
                        "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif"
                    },
                    margin={"t": 50, "l": 25, "r": 25, "b": 25},
                )

            # 2. セクター別パフォーマンスヒートマップ
            sector_perf = (
                df.groupby("sector17_name")
                .agg({"profit_loss_ratio": "mean", "market_value": "sum"})
                .reset_index()
            )
            sector_perf = sector_perf.sort_values("profit_loss_ratio", ascending=False)

            # 数値型を確実にしてNaN値を処理
            sector_values_series = pd.to_numeric(
                sector_perf["profit_loss_ratio"], errors="coerce"
            )
            sector_values = np.nan_to_num(sector_values_series.to_numpy(), nan=0.0)
            sector_sizes_series = pd.to_numeric(
                sector_perf["market_value"], errors="coerce"
            )
            sector_sizes = np.nan_to_num(sector_sizes_series.to_numpy(), nan=0.0)

            fig_sectors = go.Figure(
                go.Treemap(
                    labels=sector_perf["sector17_name"],
                    values=sector_sizes,
                    parents=[""] * len(sector_perf),
                    text=[f"{val:.1f}%" for val in sector_values],
                    textposition="middle center",
                    marker={
                        "colors": sector_values.tolist(),
                        "colorscale": "RdYlGn",
                        "cmid": 0,
                        "cmin": -10,
                        "cmax": 10,
                        "showscale": True,
                        "colorbar": {"title": "平均損益率(%)", "thickness": 15},
                        "line": {"width": 2, "color": "white"},
                    },
                    customdata=sector_perf["profit_loss_ratio"],
                    hovertemplate="<b>%{label}</b><br>平均損益率: %{customdata:.1f}%<br>評価額合計: ¥%{value:,.0f}<extra></extra>",
                )
            )
            fig_sectors.update_layout(
                title="セクター別パフォーマンスヒートマップ",
                font={
                    "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif"
                },
                margin={"t": 50, "l": 25, "r": 25, "b": 25},
            )

            # 3. 損益分布ヒストグラム
            fig_dist = go.Figure()
            fig_dist.add_trace(
                go.Histogram(
                    x=df["profit_loss_ratio"],
                    nbinsx=30,
                    name="銘柄数",
                    marker_color="lightblue",
                    hovertemplate="損益率: %{x:.1f}%<br>銘柄数: %{y}<extra></extra>",
                )
            )
            fig_dist.update_layout(
                title="損益率分布",
                xaxis_title="損益率（%）",
                yaxis_title="銘柄数",
                font={
                    "family": "Noto Sans JP, Hiragino Sans, Yu Gothic, Meiryo, sans-serif"
                },
                bargap=0.1,
            )
            fig_dist.add_vline(x=0, line_dash="dash", line_color="gray")

            # パフォーマンス統計
            stats = {
                "positive_stocks": len(df[df["profit_loss_ratio"] > 0]),
                "negative_stocks": len(df[df["profit_loss_ratio"] < 0]),
                "average_return": df["profit_loss_ratio"].mean(),
                "best_performer": {
                    "code": df.iloc[0]["code"] if not df.empty else "",
                    "name": df.iloc[0]["company_name"] if not df.empty else "",
                    "return": df.iloc[0]["profit_loss_ratio"] if not df.empty else 0,
                },
                "worst_performer": {
                    "code": df.iloc[-1]["code"] if not df.empty else "",
                    "name": df.iloc[-1]["company_name"] if not df.empty else "",
                    "return": df.iloc[-1]["profit_loss_ratio"] if not df.empty else 0,
                },
            }

            return {
                "stocks_heatmap": fig_stocks.to_json(),
                "sectors_heatmap": fig_sectors.to_json(),
                "distribution_chart": fig_dist.to_json(),
                "statistics": stats,
            }

        except Exception as e:
            logger.error(f"ヒートマップ作成エラー: {str(e)}")
            return {"error": str(e)}
        finally:
            conn.close()

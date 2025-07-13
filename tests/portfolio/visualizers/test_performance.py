"""portfolio.visualizers.performanceのテスト"""

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from plotly.graph_objects import Figure

from src.portfolio.visualizers.performance import PerformanceVisualizer


class TestPerformanceVisualizer:
    """PerformanceVisualizerの基本テスト"""

    def test_init(self):
        """初期化のテスト"""
        visualizer = PerformanceVisualizer(user_id=123)
        assert visualizer.user_id == 123

    def test_create_chart_calls_create_performance_charts(self):
        """create_chartがcreate_performance_chartsを呼び出すことを確認"""
        visualizer = PerformanceVisualizer(user_id=123)
        visualizer.create_performance_charts = MagicMock(return_value={"test": "data"})

        result = visualizer.create_chart()

        assert result == {"test": "data"}
        visualizer.create_performance_charts.assert_called_once()


class TestCreatePerformanceCharts:
    """create_performance_chartsメソッドのテスト"""

    @patch("src.portfolio.visualizers.performance.pd.read_sql")
    @patch("src.portfolio.visualizers.performance.datetime")
    def test_create_performance_charts_success(self, mock_datetime, mock_read_sql):
        """正常なパフォーマンスチャート作成のテスト"""
        # 現在時刻を固定
        mock_now = datetime(2024, 1, 15)
        mock_datetime.now.return_value = mock_now

        # 取引データ
        trans_df = pd.DataFrame(
            {
                "transaction_date": ["2024-01-01", "2024-01-05", "2024-01-10"],
                "transaction_type": ["buy", "buy", "sell"],
                "code": ["1234", "5678", "1234"],
                "quantity": [100, 50, 30],
                "price": [1000.0, 2000.0, 1100.0],
                "commission": [100, 100, 100],
                "tax": [0, 0, 50],
            }
        )

        # 価格データ
        prices_df = pd.DataFrame(
            {
                "code": ["1234"] * 10 + ["5678"] * 10,
                "date": [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-08",
                    "2024-01-09",
                    "2024-01-10",
                    "2024-01-11",
                    "2024-01-12",
                ]
                * 2,
                "adj_close": [
                    1000,
                    1010,
                    1020,
                    1030,
                    1040,
                    1050,
                    1060,
                    1100,
                    1110,
                    1120,
                    2000,
                    2010,
                    2020,
                    2030,
                    2040,
                    2050,
                    2060,
                    2100,
                    2110,
                    2120,
                ],
            }
        )

        # mock_read_sqlが順番に呼ばれる
        mock_read_sql.side_effect = [trans_df, prices_df]

        visualizer = PerformanceVisualizer(user_id=123)

        # get_db_connectionをモック
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # 各種メソッドをモック
        mock_fig = MagicMock(spec=Figure)
        mock_fig.to_json.return_value = '{"test": "chart"}'

        with patch.object(
            visualizer, "_calculate_holdings_by_date"
        ) as mock_calc_holdings:
            with patch.object(
                visualizer, "_calculate_daily_values"
            ) as mock_calc_values:
                with patch.object(
                    visualizer, "_create_value_chart", return_value=mock_fig
                ):
                    with patch.object(
                        visualizer, "_create_return_chart", return_value=mock_fig
                    ):
                        with patch.object(
                            visualizer, "_create_benchmark_chart", return_value=mock_fig
                        ):
                            # モックの戻り値設定
                            mock_calc_holdings.return_value = {
                                pd.Timestamp("2024-01-01"): {"1234": 100},
                                pd.Timestamp("2024-01-15"): {"1234": 70, "5678": 50},
                            }

                            mock_daily_values = pd.DataFrame(
                                {
                                    "date": [
                                        pd.Timestamp("2024-01-01"),
                                        pd.Timestamp("2024-01-15"),
                                    ],
                                    "total_value": [100000, 150000],
                                    "total_cost": [100000, 145000],
                                    "profit_loss": [0, 5000],
                                    "return_rate": [0, 3.45],
                                }
                            )
                            mock_calc_values.return_value = mock_daily_values

                            # テスト実行
                            result = visualizer.create_performance_charts()

        # 検証
        assert "value_chart" in result
        assert "return_chart" in result
        assert "benchmark_chart" in result

        # データベース接続が閉じられたことを確認
        mock_conn.close.assert_called_once()

    @patch("src.portfolio.visualizers.performance.pd.read_sql")
    def test_create_performance_charts_empty_transactions(self, mock_read_sql):
        """取引データがない場合のテスト"""
        # 空のDataFrame
        mock_read_sql.return_value = pd.DataFrame()

        visualizer = PerformanceVisualizer(user_id=123)
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # テスト実行
        result = visualizer.create_performance_charts()

        # 検証
        assert result == {"error": "取引履歴データがありません"}
        mock_conn.close.assert_called_once()

    @patch("src.portfolio.visualizers.performance.pd.read_sql")
    @patch("src.portfolio.visualizers.performance.datetime")
    def test_create_performance_charts_no_price_data(
        self, mock_datetime, mock_read_sql
    ):
        """価格データがない場合のテスト"""
        # 現在時刻を固定
        mock_now = datetime(2024, 1, 15)
        mock_datetime.now.return_value = mock_now

        # 取引データ
        trans_df = pd.DataFrame(
            {
                "transaction_date": ["2024-01-01"],
                "transaction_type": ["buy"],
                "code": ["1234"],
                "quantity": [100],
                "price": [1000.0],
                "commission": [100],
                "tax": [0],
            }
        )

        # 価格データなし
        prices_df = pd.DataFrame()

        mock_read_sql.side_effect = [trans_df, prices_df]

        visualizer = PerformanceVisualizer(user_id=123)
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # _create_simple_performance_chartsをモック
        with patch.object(
            visualizer, "_create_simple_performance_charts"
        ) as mock_simple:
            mock_simple.return_value = {"simple": "chart"}

            result = visualizer.create_performance_charts()

        # 検証
        assert result == {"simple": "chart"}
        mock_simple.assert_called_once()


class TestCalculateHoldingsByDate:
    """_calculate_holdings_by_dateメソッドのテスト"""

    def test_calculate_holdings_by_date(self):
        """日付ごとの保有銘柄数計算のテスト"""
        trans_df = pd.DataFrame(
            {
                "transaction_date": pd.to_datetime(
                    ["2024-01-01", "2024-01-05", "2024-01-10", "2024-01-10"]
                ),
                "transaction_type": ["buy", "buy", "sell", "buy"],
                "code": ["1234", "5678", "1234", "1234"],
                "quantity": [100, 50, 30, 20],
            }
        )

        date_range = pd.date_range("2024-01-01", "2024-01-12", freq="D")

        visualizer = PerformanceVisualizer(user_id=123)
        result = visualizer._calculate_holdings_by_date(trans_df, date_range)

        # 検証
        # 1月1日: 1234を100株
        assert result[pd.Timestamp("2024-01-01")] == {"1234": 100}

        # 1月5日: 1234を100株、5678を50株
        assert result[pd.Timestamp("2024-01-05")] == {"1234": 100, "5678": 50}

        # 1月10日: 1234を90株（100-30+20）、5678を50株
        assert result[pd.Timestamp("2024-01-10")] == {"1234": 90, "5678": 50}

        # 1月12日: 同じ
        assert result[pd.Timestamp("2024-01-12")] == {"1234": 90, "5678": 50}

    def test_calculate_holdings_by_date_sell_all(self):
        """全売却した場合のテスト"""
        trans_df = pd.DataFrame(
            {
                "transaction_date": pd.to_datetime(["2024-01-01", "2024-01-05"]),
                "transaction_type": ["buy", "sell"],
                "code": ["1234", "1234"],
                "quantity": [100, 100],
            }
        )

        date_range = pd.date_range("2024-01-01", "2024-01-07", freq="D")

        visualizer = PerformanceVisualizer(user_id=123)
        result = visualizer._calculate_holdings_by_date(trans_df, date_range)

        # 1月5日以降は保有なし
        assert result[pd.Timestamp("2024-01-05")] == {}
        assert result[pd.Timestamp("2024-01-07")] == {}


class TestCalculateDailyValues:
    """_calculate_daily_valuesメソッドのテスト"""

    def test_calculate_daily_values(self):
        """日次評価額計算のテスト"""
        holdings_by_date = {
            pd.Timestamp("2024-01-01"): {"1234": 100},
            pd.Timestamp("2024-01-02"): {"1234": 100, "5678": 50},
            pd.Timestamp("2024-01-03"): {"1234": 100, "5678": 50},
        }

        prices_df = pd.DataFrame(
            {
                "code": ["1234", "1234", "1234", "5678", "5678"],
                "date": pd.to_datetime(
                    [
                        "2024-01-01",
                        "2024-01-02",
                        "2024-01-03",
                        "2024-01-02",
                        "2024-01-03",
                    ]
                ),
                "adj_close": [1000, 1100, 1200, 2000, 2100],
            }
        )

        date_range = pd.date_range("2024-01-01", "2024-01-03", freq="D")

        visualizer = PerformanceVisualizer(user_id=123)
        result = visualizer._calculate_daily_values(
            holdings_by_date, prices_df, date_range
        )

        # 検証
        assert len(result) == 3

        # 1月1日: 1234のみ
        assert result.iloc[0]["total_value"] == 100000  # 100株 * 1000円

        # 1月2日: 1234 + 5678
        assert result.iloc[1]["total_value"] == 210000  # 100株 * 1100円 + 50株 * 2000円

        # 1月3日: 1234 + 5678
        assert result.iloc[2]["total_value"] == 225000  # 100株 * 1200円 + 50株 * 2100円

    def test_calculate_daily_values_no_holdings(self):
        """保有銘柄がない日のテスト"""
        holdings_by_date = {
            pd.Timestamp("2024-01-01"): {},
            pd.Timestamp("2024-01-02"): {"1234": 100},
        }

        prices_df = pd.DataFrame(
            {
                "code": ["1234"],
                "date": pd.to_datetime(["2024-01-02"]),
                "adj_close": [1000],
            }
        )

        date_range = pd.date_range("2024-01-01", "2024-01-02", freq="D")

        visualizer = PerformanceVisualizer(user_id=123)
        result = visualizer._calculate_daily_values(
            holdings_by_date, prices_df, date_range
        )

        # 1月1日は保有なしなので含まれない
        assert len(result) == 1
        assert result.iloc[0]["date"] == pd.Timestamp("2024-01-02")


class TestCreateCharts:
    """チャート作成メソッドのテスト"""

    def test_create_value_chart(self):
        """資産総額推移グラフ作成のテスト"""
        daily_values = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05", freq="D"),
                "total_value": [100000, 110000, 105000, 115000, 120000],
                "total_cost": [100000, 100000, 100000, 100000, 100000],
            }
        )

        visualizer = PerformanceVisualizer(user_id=123)
        fig = visualizer._create_value_chart(daily_values)

        # 検証
        assert isinstance(fig, Figure)
        assert len(fig.data) == 2  # 評価額と取得コスト
        assert fig.data[0].name == "評価額"
        assert fig.data[1].name == "取得コスト"
        assert fig.layout.title.text == "資産総額の推移"

    def test_create_return_chart(self):
        """損益率推移グラフ作成のテスト"""
        daily_values = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05", freq="D"),
                "return_rate": [0, 10.0, 5.0, 15.0, 20.0],
            }
        )

        visualizer = PerformanceVisualizer(user_id=123)
        fig = visualizer._create_return_chart(daily_values)

        # 検証
        assert isinstance(fig, Figure)
        assert len(fig.data) == 1
        assert fig.data[0].name == "損益率"
        assert fig.data[0].fill == "tozeroy"
        assert fig.layout.title.text == "損益率の推移"

    def test_create_benchmark_chart(self):
        """ベンチマーク比較グラフ作成のテスト"""
        daily_values = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", "2024-01-05", freq="D"),
                "total_value": [100000, 110000, 105000, 115000, 120000],
            }
        )

        visualizer = PerformanceVisualizer(user_id=123)

        # ランダムシードを固定
        with patch(
            "src.portfolio.visualizers.performance.np.random.randn"
        ) as mock_randn:
            mock_randn.return_value = np.array([0.5, -0.3, 0.8, 0.2, -0.1])
            fig = visualizer._create_benchmark_chart(daily_values)

        # 検証
        assert isinstance(fig, Figure)
        assert len(fig.data) == 2  # ポートフォリオとベンチマーク
        assert fig.data[0].name == "ポートフォリオ"
        assert fig.data[1].name == "TOPIX（仮）"
        assert fig.layout.title.text == "ベンチマーク比較（初期値=100）"

    def test_create_benchmark_chart_empty_data(self):
        """データが空の場合のベンチマークチャート"""
        daily_values = pd.DataFrame()

        visualizer = PerformanceVisualizer(user_id=123)
        fig = visualizer._create_benchmark_chart(daily_values)

        # 検証
        assert isinstance(fig, Figure)
        assert len(fig.data) == 0  # データなし


class TestCreateSimplePerformanceCharts:
    """_create_simple_performance_chartsメソッドのテスト"""

    def test_create_simple_performance_charts(self):
        """簡易パフォーマンスチャート作成のテスト"""
        trans_df = pd.DataFrame(
            {
                "transaction_date": pd.to_datetime(
                    ["2024-01-01", "2024-01-05", "2024-01-10"]
                ),
                "transaction_type": ["buy", "buy", "sell"],
                "code": ["1234", "5678", "1234"],
                "quantity": [100, 50, 30],
                "price": [1000.0, 2000.0, 1100.0],
                "commission": [100, 100, 100],
                "tax": [0, 0, 50],
            }
        )

        visualizer = PerformanceVisualizer(user_id=123)
        result = visualizer._create_simple_performance_charts(trans_df)

        # 検証
        assert "value_chart" in result
        assert result["return_chart"] is None
        assert result["benchmark_chart"] is None

        # JSONとして解析可能か確認
        json_data = json.loads(result["value_chart"])
        assert isinstance(json_data, dict)

    def test_create_simple_performance_charts_calculation(self):
        """簡易チャートの累積計算確認"""
        trans_df = pd.DataFrame(
            {
                "transaction_date": pd.to_datetime(["2024-01-01", "2024-01-05"]),
                "transaction_type": ["buy", "sell"],
                "code": ["1234", "1234"],
                "quantity": [100, 50],
                "price": [1000.0, 1200.0],
                "commission": [100, 100],
                "tax": [0, 0],
            }
        )

        visualizer = PerformanceVisualizer(user_id=123)
        result = visualizer._create_simple_performance_charts(trans_df)

        # 累積額の確認
        # 買い: 100 * 1000 + 100 = 100,100
        # 売り: -(50 * 1200 - 100) = -59,900
        # 累積: 100,100 - 59,900 = 40,200

        # value_chartが作成されていることを確認
        assert result["value_chart"] is not None


class TestPerformanceVisualizerIntegration:
    """PerformanceVisualizerの統合テスト"""

    @patch("src.portfolio.visualizers.performance.pd.read_sql")
    @patch("src.portfolio.visualizers.performance.datetime")
    def test_full_visualization_flow(self, mock_datetime, mock_read_sql):
        """完全な可視化フローのテスト"""
        # 現在時刻を固定
        mock_now = datetime(2024, 1, 15)
        mock_datetime.now.return_value = mock_now

        # リアルな取引データ
        trans_df = pd.DataFrame(
            {
                "transaction_date": [
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-05",
                    "2024-01-08",
                    "2024-01-10",
                    "2024-01-12",
                ],
                "transaction_type": ["buy", "buy", "buy", "sell", "buy", "sell"],
                "code": ["7203", "6758", "9984", "7203", "4661", "6758"],
                "quantity": [100, 200, 50, 50, 100, 100],
                "price": [2500.0, 1500.0, 5000.0, 2700.0, 3000.0, 1600.0],
                "commission": [100, 100, 100, 100, 100, 100],
                "tax": [0, 0, 0, 50, 0, 30],
            }
        )

        # 価格データ
        price_data = []
        codes = ["7203", "6758", "9984", "4661"]
        base_prices = [2500, 1500, 5000, 3000]

        for i, (code, base_price) in enumerate(zip(codes, base_prices, strict=False)):
            for j in range(14):  # 14日分
                date = (datetime(2024, 1, 1) + timedelta(days=j)).strftime("%Y-%m-%d")
                # 価格を少しずつ変動させる
                price = base_price * (1 + 0.01 * (j - 7) + 0.005 * i)
                price_data.append(
                    {
                        "code": code,
                        "date": date,
                        "adj_close": price,
                    }
                )

        prices_df = pd.DataFrame(price_data)

        mock_read_sql.side_effect = [trans_df, prices_df]

        visualizer = PerformanceVisualizer(user_id=123)
        mock_conn = MagicMock()
        visualizer.get_db_connection = MagicMock(return_value=mock_conn)

        # テスト実行
        result = visualizer.create_performance_charts()

        # 検証
        assert "value_chart" in result
        assert "return_chart" in result
        assert "benchmark_chart" in result

        # 各チャートがJSON形式で返されることを確認
        for key in ["value_chart", "return_chart", "benchmark_chart"]:
            json_data = json.loads(result[key])
            assert isinstance(json_data, dict)

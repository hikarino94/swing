"""backtest.backtest_ml モジュールのテスト"""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from backtest.backtest_ml import (
    _ascii_bar_chart,
    _fetch_price_range,
    _prepare_dataset,
    _result_paths,
    main,
    parse_args,
    run_backtest,
    show_results,
    summarize,
    to_excel,
)


class TestResultPaths:
    """結果ファイルパス生成のテスト"""

    @patch("backtest.backtest_ml.get_timestamped_output_path")
    def test_result_paths(self, mock_get_path):
        """結果ファイルパスの生成テスト"""
        mock_get_path.side_effect = [
            Path("/output/backtest_ml_20240101_120000.xlsx"),
            Path("/output/backtest_ml_20240101_120000.json"),
        ]

        excel_path, json_path = _result_paths("ml")

        assert excel_path == Path("/output/backtest_ml_20240101_120000.xlsx")
        assert json_path == Path("/output/backtest_ml_20240101_120000.json")
        assert mock_get_path.call_count == 2


class TestFetchPriceRange:
    """価格データ取得のテスト"""

    def test_fetch_price_range(self):
        """価格範囲取得のテスト"""
        # モックデータベース接続
        mock_conn = MagicMock(spec=sqlite3.Connection)
        expected_df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "adj_close": [1000.0, 1100.0],
                "adj_volume": [100000, 200000],
            }
        )

        with patch("pandas.read_sql", return_value=expected_df) as mock_read_sql:
            result = _fetch_price_range(mock_conn, "2024-01-01", "2024-01-31")

            assert result.equals(expected_df)
            mock_read_sql.assert_called_once()
            call_args = mock_read_sql.call_args
            assert "prices" in call_args[0][0]
            assert call_args[1]["params"] == ("2024-01-01", "2024-01-31")


class TestPrepareDataset:
    """データセット準備のテスト"""

    @patch("backtest.backtest_ml._fetch_price_range")
    @patch("backtest.backtest_ml._make_price_features")
    @patch("backtest.backtest_ml._fetch_stmt")
    @patch("backtest.backtest_ml._merge_features")
    @patch("backtest.backtest_ml._add_label")
    def test_prepare_dataset(
        self,
        mock_add_label,
        mock_merge,
        mock_fetch_stmt,
        mock_make_features,
        mock_fetch_price,
    ):
        """データセット準備の統合テスト"""
        # モックデータの設定
        mock_price_df = pd.DataFrame(
            {
                "code": ["1234", "1234"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "adj_close": [1000.0, 1100.0],
                "adj_volume": [100000, 200000],
            }
        )
        mock_fetch_price.return_value = mock_price_df

        mock_features_df = pd.DataFrame(
            {
                "code": ["1234", "1234"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "ret_5": [0.1, 0.05],
                "ret_10": [0.15, 0.08],
                "ret_20": [0.2, 0.1],
                "volatility_20": [0.15, 0.12],
                "turnover_norm": [1.2, 1.1],
            }
        )
        mock_make_features.return_value = mock_features_df

        mock_stmt_df = pd.DataFrame(
            {
                "code": ["1234"],
                "NetSales": [1000000.0],
                "OperatingProfit": [100000.0],
                "OrdinaryProfit": [95000.0],
                "Profit": [80000.0],
                "TotalAssets": [5000000.0],
                "Equity": [2000000.0],
                "EquityToAssetRatio": [0.4],
                "BookValuePerShare": [1000.0],
                "CashFlowsFromOperatingActivities": [150000.0],
                "CashFlowsFromInvestingActivities": [-50000.0],
                "CashFlowsFromFinancingActivities": [-30000.0],
            }
        )
        mock_fetch_stmt.return_value = mock_stmt_df

        mock_merged_df = pd.DataFrame(
            {
                "code": ["1234", "1234"],
                "date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "ret_5": [0.1, 0.05],
                "ret_10": [0.15, 0.08],
                "ret_20": [0.2, 0.1],
                "volatility_20": [0.15, 0.12],
                "turnover_norm": [1.2, 1.1],
                "NetSales": [1000000.0, 1000000.0],
                "OperatingProfit": [100000.0, 100000.0],
                "OrdinaryProfit": [95000.0, 95000.0],
                "Profit": [80000.0, 80000.0],
                "TotalAssets": [5000000.0, 5000000.0],
                "Equity": [2000000.0, 2000000.0],
                "EquityToAssetRatio": [0.4, 0.4],
                "BookValuePerShare": [1000.0, 1000.0],
                "CashFlowsFromOperatingActivities": [150000.0, 150000.0],
                "CashFlowsFromInvestingActivities": [-50000.0, -50000.0],
                "CashFlowsFromFinancingActivities": [-30000.0, -30000.0],
            }
        )
        mock_merge.return_value = mock_merged_df

        mock_labeled_df = mock_merged_df.copy()
        mock_labeled_df["label"] = [1, 0]
        mock_add_label.return_value = mock_labeled_df

        # テスト実行
        mock_conn = MagicMock(spec=sqlite3.Connection)
        result = _prepare_dataset(mock_conn, "2024-01-01", "2024-01-31")

        # 検証
        assert isinstance(result, pd.DataFrame)
        assert "future_date" in result.columns
        mock_fetch_price.assert_called_once()
        mock_make_features.assert_called_once()
        mock_fetch_stmt.assert_called_once()
        mock_merge.assert_called_once()
        mock_add_label.assert_called_once()


class TestRunBacktest:
    """バックテスト実行のテスト"""

    @patch("backtest.backtest_ml._prepare_dataset")
    @patch("backtest.backtest_ml._train_model")
    def test_run_backtest_basic(self, mock_train_model, mock_prepare_dataset):
        """基本的なバックテスト実行テスト"""
        # モックデータの準備
        mock_df = pd.DataFrame(
            {
                "code": ["1234", "1234", "5678", "5678"],
                "date": pd.to_datetime(
                    ["2023-12-30", "2024-01-01", "2023-12-30", "2024-01-01"]
                ),
                "adj_close": [1000.0, 1100.0, 2000.0, 2200.0],
                "future_close": [1100.0, 1200.0, 2200.0, 2100.0],
                "future_date": pd.to_datetime(
                    ["2024-01-30", "2024-01-31", "2024-01-30", "2024-01-31"]
                ),
                "ret_5": [0.1, 0.09, 0.15, -0.05],
                "ret_10": [0.15, 0.14, 0.2, -0.08],
                "ret_20": [0.2, 0.18, 0.25, -0.1],
                "volatility_20": [0.15, 0.14, 0.18, 0.2],
                "turnover_norm": [1.2, 1.1, 1.3, 0.9],
                "NetSales": [1000000.0, 1000000.0, 2000000.0, 2000000.0],
                "OperatingProfit": [100000.0, 100000.0, 200000.0, 200000.0],
                "OrdinaryProfit": [95000.0, 95000.0, 190000.0, 190000.0],
                "Profit": [80000.0, 80000.0, 160000.0, 160000.0],
                "TotalAssets": [5000000.0, 5000000.0, 10000000.0, 10000000.0],
                "Equity": [2000000.0, 2000000.0, 4000000.0, 4000000.0],
                "EquityToAssetRatio": [0.4, 0.4, 0.4, 0.4],
                "BookValuePerShare": [1000.0, 1000.0, 2000.0, 2000.0],
                "CashFlowsFromOperatingActivities": [
                    150000.0,
                    150000.0,
                    300000.0,
                    300000.0,
                ],
                "CashFlowsFromInvestingActivities": [
                    -50000.0,
                    -50000.0,
                    -100000.0,
                    -100000.0,
                ],
                "CashFlowsFromFinancingActivities": [
                    -30000.0,
                    -30000.0,
                    -60000.0,
                    -60000.0,
                ],
                "label": [1, 1, 1, 0],
            }
        )
        mock_prepare_dataset.return_value = mock_df

        # モックモデル
        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([[0.3, 0.7], [0.2, 0.8]])
        mock_train_model.return_value = mock_model

        # テスト実行
        mock_conn = MagicMock(spec=sqlite3.Connection)
        trades = run_backtest(
            mock_conn, "2024-01-01", "2024-01-01", top=2, capital=1_000_000
        )

        # 検証
        assert isinstance(trades, pd.DataFrame)
        assert len(trades) == 2  # top=2なので2つのトレード
        assert "code" in trades.columns
        assert "entry_date" in trades.columns
        assert "exit_date" in trades.columns
        assert "pnl_yen" in trades.columns
        assert "pnl_pct" in trades.columns
        assert "prob" in trades.columns

    @patch("backtest.backtest_ml._prepare_dataset")
    def test_run_backtest_no_history(self, mock_prepare_dataset):
        """履歴不足の場合のテスト"""
        # 日付列はあるが、訓練データが空のデータフレームを返す
        mock_df = pd.DataFrame({"date": pd.to_datetime([]), "code": []})
        mock_prepare_dataset.return_value = mock_df

        mock_conn = MagicMock(spec=sqlite3.Connection)
        with pytest.raises(ValueError, match="Not enough history"):
            run_backtest(mock_conn, "2024-01-01", "2024-01-01")


class TestSummarize:
    """サマリー生成のテスト"""

    def test_summarize_with_trades(self):
        """トレードありの場合のサマリー"""
        trades = pd.DataFrame(
            {"pnl_yen": [10000, -5000, 15000, -2000], "pnl_pct": [1.0, -0.5, 1.5, -0.2]}
        )

        summary = summarize(trades)

        assert len(summary) == 5
        assert summary[summary["metric"] == "trades"]["value"].iloc[0] == 4
        assert summary[summary["metric"] == "total_profit"]["value"].iloc[0] == 18000
        assert summary[summary["metric"] == "win_rate"]["value"].iloc[0] == 0.5
        assert summary[summary["metric"] == "avg_ret_pct"]["value"].iloc[
            0
        ] == pytest.approx(0.45)

    def test_summarize_empty_trades(self):
        """トレードなしの場合のサマリー"""
        trades = pd.DataFrame()
        summary = summarize(trades)
        assert summary.empty

    def test_summarize_all_profitable(self):
        """全て利益の場合のサマリー"""
        trades = pd.DataFrame(
            {"pnl_yen": [10000, 20000, 30000], "pnl_pct": [1.0, 2.0, 3.0]}
        )

        summary = summarize(trades)
        assert summary[summary["metric"] == "win_rate"]["value"].iloc[0] == 1.0
        assert summary[summary["metric"] == "total_profit"]["value"].iloc[0] == 60000


class TestAsciiBarChart:
    """ASCIIバーチャートのテスト"""

    def test_ascii_bar_chart_basic(self):
        """基本的なバーチャート生成"""
        values = [100, -50, 200, -150]
        chart = _ascii_bar_chart(values, width=20)

        lines = chart.split("\n")
        assert len(lines) == 4
        assert "100" in lines[0]
        assert "-50" in lines[1]
        assert "200" in lines[2]
        assert "-150" in lines[3]

    def test_ascii_bar_chart_empty(self):
        """空のリストの場合"""
        chart = _ascii_bar_chart([])
        assert chart == ""

    def test_ascii_bar_chart_single_value(self):
        """単一値の場合"""
        chart = _ascii_bar_chart([100], width=10)
        assert "100" in chart
        assert "#" * 10 in chart


class TestShowResults:
    """結果表示のテスト"""

    def test_show_results(self, capsys):
        """結果表示の出力テスト"""
        trades = pd.DataFrame({"pnl_yen": [10000, -5000, 15000]})
        summary = pd.DataFrame(
            {"metric": ["trades", "total_profit"], "value": [3, 20000]}
        )

        show_results(trades, summary)

        captured = capsys.readouterr()
        assert "=== Summary ===" in captured.out
        assert "trades" in captured.out
        assert "20000" in captured.out
        assert "=== Profit per Trade ===" in captured.out


class TestToExcel:
    """Excel出力のテスト"""

    def test_to_excel(self, tmp_path):
        """Excel出力のテスト"""
        trades = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "pnl_yen": [10000, -5000],
                "entry_date": ["2024-01-01", "2024-01-02"],
                "exit_date": ["2024-01-31", "2024-02-01"],
            }
        )
        summary = pd.DataFrame({"metric": ["trades"], "value": [2]})

        # 一時ファイルに出力
        output_file = tmp_path / "test_output.xlsx"
        to_excel(trades, summary, output_file)

        # ファイルが作成されたことを確認
        assert output_file.exists()

        # ファイルサイズが0でないことを確認
        assert output_file.stat().st_size > 0


class TestParseArgs:
    """引数解析のテスト"""

    def test_parse_args_required_only(self):
        """必須引数のみのテスト"""
        args = parse_args(["--start", "2024-01-01"])
        assert args.start == "2024-01-01"
        assert args.end is None
        assert args.top == 10
        assert args.capital == 1_000_000
        assert args.lookback == 1095
        assert not args.show

    def test_parse_args_all_options(self):
        """全オプション指定のテスト"""
        args = parse_args(
            [
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-31",
                "--top",
                "20",
                "--capital",
                "2000000",
                "--lookback",
                "365",
                "--outfile",
                "output.xlsx",
                "--json",
                "output.json",
                "--show",
            ]
        )
        assert args.start == "2024-01-01"
        assert args.end == "2024-01-31"
        assert args.top == 20
        assert args.capital == 2000000
        assert args.lookback == 365
        assert args.outfile == Path("output.xlsx")
        assert args.json == Path("output.json")
        assert args.show


class TestMain:
    """メイン関数のテスト"""

    @patch("backtest.backtest_ml.sqlite3.connect")
    @patch("backtest.backtest_ml.run_backtest")
    @patch("backtest.backtest_ml.summarize")
    @patch("backtest.backtest_ml.to_excel")
    @patch("backtest.backtest_ml.show_results")
    @patch("backtest.backtest_ml.parse_args")
    def test_main_basic(
        self,
        mock_parse_args,
        mock_show_results,
        mock_to_excel,
        mock_summarize,
        mock_run_backtest,
        mock_connect,
    ):
        """基本的なメイン関数実行"""
        # モックの設定
        mock_args = MagicMock()
        mock_args.db = "test.db"
        mock_args.start = "2024-01-01"
        mock_args.end = "2024-01-31"
        mock_args.top = 10
        mock_args.capital = 1_000_000
        mock_args.lookback = 1095
        mock_args.outfile = Path("output.xlsx")
        mock_args.json = Path("output.json")
        mock_args.show = True
        mock_parse_args.return_value = mock_args

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_trades = pd.DataFrame({"pnl_yen": [10000]})
        mock_run_backtest.return_value = mock_trades

        mock_summary = pd.DataFrame({"metric": ["trades"], "value": [1]})
        mock_summarize.return_value = mock_summary

        # 実行
        main()

        # 検証
        mock_connect.assert_called_once_with("test.db")
        mock_run_backtest.assert_called_once()
        mock_summarize.assert_called_once_with(mock_trades)
        mock_to_excel.assert_called_once()
        mock_show_results.assert_called_once()
        mock_conn.close.assert_called_once()

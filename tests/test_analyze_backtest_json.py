#!/usr/bin/env python
"""
バックテスト結果分析モジュール (backtest/analyze_backtest_json.py) のテスト

テスト対象:
- JSONファイルからのトレードデータ読み込み
- サマリー統計の計算
- フォーマット処理
- CLI引数処理
- ASCIIテーブル・チャート生成
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from backtest import analyze_backtest_json


class TestDataLoading:
    """データ読み込みのテスト"""

    def test_load_trades_single_file(self, tmp_path):
        """単一JSONファイルからのトレードデータ読み込みテスト"""
        # テストデータ作成
        trades_data = [
            {
                "code": "1234",
                "entry_date": "2024-01-15",
                "exit_date": "2024-02-15",
                "profit_jpy": 100000,
                "ret_pct": 10.0,
            },
            {
                "code": "5678",
                "entry_date": "2024-01-16",
                "exit_date": "2024-02-16",
                "profit_jpy": -30000,
                "ret_pct": -3.0,
            },
        ]

        json_file = tmp_path / "test_trades.json"
        with open(json_file, "w") as f:
            json.dump(trades_data, f)

        # データ読み込み
        df = analyze_backtest_json.load_trades([str(json_file)])

        assert len(df) == 2
        assert "code" in df.columns
        assert "profit_jpy" in df.columns
        assert "ret_pct" in df.columns
        assert df.iloc[0]["code"] == 1234
        assert df.iloc[1]["profit_jpy"] == -30000

    def test_load_trades_multiple_files(self, tmp_path):
        """複数JSONファイルからのトレードデータ読み込みテスト"""
        # 1つ目のファイル
        trades_data1 = [{"code": "1234", "profit_jpy": 100000, "ret_pct": 10.0}]
        json_file1 = tmp_path / "test_trades1.json"
        with open(json_file1, "w") as f:
            json.dump(trades_data1, f)

        # 2つ目のファイル
        trades_data2 = [
            {"code": "5678", "profit_jpy": 50000, "ret_pct": 5.0},
            {"code": "9012", "profit_jpy": -20000, "ret_pct": -2.0},
        ]
        json_file2 = tmp_path / "test_trades2.json"
        with open(json_file2, "w") as f:
            json.dump(trades_data2, f)

        # データ読み込み
        df = analyze_backtest_json.load_trades([str(json_file1), str(json_file2)])

        assert len(df) == 3
        assert df["profit_jpy"].sum() == 130000

    def test_load_trades_empty_list(self):
        """空のファイルリストのテスト"""
        df = analyze_backtest_json.load_trades([])
        assert df.empty


class TestColumnSearch:
    """カラム検索のテスト"""

    def test_find_col_success(self):
        """カラム検索成功のテスト"""
        df = pd.DataFrame({"profit_jpy": [100, -50], "other_col": [1, 2]})

        found_col = analyze_backtest_json._find_col(df, ["profit_jpy", "pnl_yen"])
        assert found_col == "profit_jpy"

    def test_find_col_alternative(self):
        """代替カラム名のテスト"""
        df = pd.DataFrame({"pnl_yen": [100, -50], "other_col": [1, 2]})

        found_col = analyze_backtest_json._find_col(df, ["profit_jpy", "pnl_yen"])
        assert found_col == "pnl_yen"

    def test_find_col_not_found(self):
        """カラムが見つからない場合のテスト"""
        df = pd.DataFrame({"other_col": [1, 2]})

        with pytest.raises(ValueError, match="Required column not found"):
            analyze_backtest_json._find_col(df, ["profit_jpy", "pnl_yen"])


class TestSummaryCalculation:
    """サマリー計算のテスト"""

    def test_summarize_profit_jpy(self):
        """profit_jpyカラムを使ったサマリー計算のテスト"""
        trades = pd.DataFrame(
            [
                {"profit_jpy": 100000, "ret_pct": 10.0},
                {"profit_jpy": 50000, "ret_pct": 5.0},
                {"profit_jpy": -30000, "ret_pct": -3.0},
                {"profit_jpy": 80000, "ret_pct": 8.0},
            ]
        )

        summary = analyze_backtest_json.summarize(trades)

        metrics_dict = dict(zip(summary["metric"], summary["value"], strict=False))
        assert metrics_dict["trades"] == 4
        assert metrics_dict["total_profit"] == 200000
        assert metrics_dict["win_rate"] == 0.75
        assert metrics_dict["avg_ret_pct"] == 5.0
        assert "sharpe" in metrics_dict

    def test_summarize_pnl_yen(self):
        """pnl_yenカラムを使ったサマリー計算のテスト"""
        trades = pd.DataFrame(
            [{"pnl_yen": 100000, "pnl_pct": 10.0}, {"pnl_yen": -50000, "pnl_pct": -5.0}]
        )

        summary = analyze_backtest_json.summarize(trades)

        metrics_dict = dict(zip(summary["metric"], summary["value"], strict=False))
        assert metrics_dict["trades"] == 2
        assert metrics_dict["total_profit"] == 50000
        assert metrics_dict["win_rate"] == 0.5

    def test_summarize_empty(self):
        """空のDataFrameのサマリー計算テスト"""
        trades = pd.DataFrame()

        summary = analyze_backtest_json.summarize(trades)

        assert summary.empty


class TestFormatting:
    """フォーマット処理のテスト"""

    def test_format_summary(self):
        """サマリーフォーマットのテスト"""
        summary = pd.DataFrame(
            {
                "metric": [
                    "trades",
                    "total_profit",
                    "win_rate",
                    "avg_ret_pct",
                    "sharpe",
                ],
                "value": [100, 1500000, 0.65, 5.25, 1.23],
            }
        )

        formatted = analyze_backtest_json.format_summary(summary)

        values_dict = dict(zip(formatted["metric"], formatted["value"], strict=False))
        assert values_dict["trades"] == "100"
        assert values_dict["total_profit"] == "1,500,000 JPY"
        assert values_dict["win_rate"] == "65.00%"
        assert values_dict["avg_ret_pct"] == "5.25%"
        assert values_dict["sharpe"] == "1.23"

    def test_format_summary_empty(self):
        """空サマリーのフォーマットテスト"""
        summary = pd.DataFrame()

        formatted = analyze_backtest_json.format_summary(summary)

        assert formatted.empty


class TestVisualization:
    """視覚化機能のテスト"""

    def test_ascii_bar_chart(self):
        """ASCIIバーチャート生成のテスト"""
        values = [100000, -50000, 200000, -30000]

        chart = analyze_backtest_json._ascii_bar_chart(values, width=20)

        lines = chart.split("\n")
        assert len(lines) == 4
        assert "1" in lines[0]  # 1番目のトレード
        assert "+100000" in lines[0]
        assert "-50000" in lines[1]  # 負の値
        assert "-" in lines[1]  # 負の符号

    def test_ascii_bar_chart_empty(self):
        """空のリストでのASCIIバーチャートテスト"""
        chart = analyze_backtest_json._ascii_bar_chart([])
        assert chart == ""

    def test_ascii_bar_chart_zero_values(self):
        """ゼロ値を含むバーチャートのテスト"""
        values = [0, 100, -100]
        chart = analyze_backtest_json._ascii_bar_chart(values, width=10)

        lines = chart.split("\n")
        assert len(lines) == 3
        assert "+0" in lines[0] or "(+0)" in lines[0]

    def test_ascii_table_basic(self):
        """基本的なASCIIテーブル生成のテスト"""
        df = pd.DataFrame({"Code": ["1234", "5678"], "Profit": [100000, -50000]})

        table = analyze_backtest_json._ascii_table(df)

        # テーブルにヘッダーとデータが含まれているか確認
        assert "Code" in table
        assert "Profit" in table
        assert "1234" in table
        assert "100000" in table

    def test_ascii_table_heavy_borders(self):
        """重い罫線でのASCIIテーブル生成のテスト"""
        df = pd.DataFrame({"Symbol": ["AAPL", "GOOGL"], "Price": [150.25, 2800.50]})

        # 重い罫線モードのテスト
        table_heavy = analyze_backtest_json._ascii_table(df, heavy=True)
        table_normal = analyze_backtest_json._ascii_table(df, heavy=False)

        # 何らかのテーブルが生成されることを確認
        assert len(table_heavy) > 0
        assert len(table_normal) > 0
        assert "Symbol" in table_heavy
        assert "Price" in table_heavy

    @mock.patch("sys.stdout")
    def test_ascii_table_encoding_fallback(self, mock_stdout):
        """エンコーディングフォールバックのテスト"""
        # エンコーディングが制限された環境を模擬
        mock_stdout.encoding = "ascii"

        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

        # エンコーディングエラーが発生してもフォールバックが動作することを確認
        table = analyze_backtest_json._ascii_table(df, heavy=True)
        assert len(table) > 0
        # ASCII文字のみ使用されることを確認（重い罫線文字は使用されない）
        assert "+" in table or "-" in table or "|" in table

    def test_ascii_table_empty_dataframe(self):
        """空のDataFrameでのASCIIテーブル生成のテスト"""
        df = pd.DataFrame()

        table = analyze_backtest_json._ascii_table(df)

        # 空のDataFrameでもエラーにならないことを確認
        assert isinstance(table, str)


class TestCLI:
    """コマンドライン引数のテスト"""

    def test_parse_args_basic(self):
        """基本的な引数パースのテスト"""
        parser = argparse.ArgumentParser()
        parser.add_argument("files", nargs="+", help="JSON files to analyze")
        parser.add_argument("--show-trades", action="store_true")
        parser.add_argument("--side", choices=["long", "short"])

        args = parser.parse_args(
            ["result1.json", "result2.json", "--show-trades", "--side", "long"]
        )

        assert args.files == ["result1.json", "result2.json"]
        assert args.show_trades is True
        assert args.side == "long"


class TestMainFunction:
    """main関数のテスト"""

    @mock.patch("backtest.analyze_backtest_json.load_trades")
    @mock.patch("builtins.print")
    def test_main_no_trades(self, mock_print, mock_load_trades):
        """取引データがない場合のmain関数テスト"""
        mock_load_trades.return_value = pd.DataFrame()

        analyze_backtest_json.main(["test.json"])

        mock_load_trades.assert_called_once_with(["test.json"])
        # print呼び出しの中に"No trades loaded."が含まれることを確認
        calls = [str(call) for call in mock_print.call_args_list]
        assert any("No trades loaded." in call for call in calls)

    @mock.patch("backtest.analyze_backtest_json.load_trades")
    @mock.patch("builtins.print")
    def test_main_with_trades_summary_only(self, mock_print, mock_load_trades):
        """取引データありでサマリーのみ表示のテスト"""
        # テストデータ
        trades_data = pd.DataFrame(
            [
                {"profit_jpy": 100000, "ret_pct": 10.0, "side": "long"},
                {"profit_jpy": -30000, "ret_pct": -3.0, "side": "short"},
            ]
        )
        mock_load_trades.return_value = trades_data

        analyze_backtest_json.main(["test.json"])

        # printが呼ばれていることを確認
        assert mock_print.call_count >= 1
        # サマリーヘッダーが表示されることを確認
        calls = [str(call) for call in mock_print.call_args_list]
        assert any("=== Summary ===" in call for call in calls)

    @mock.patch("backtest.analyze_backtest_json.load_trades")
    @mock.patch("builtins.print")
    def test_main_with_show_trades(self, mock_print, mock_load_trades):
        """--show-tradesオプション付きのmain関数テスト"""
        trades_data = pd.DataFrame(
            [
                {"profit_jpy": 100000, "ret_pct": 10.0, "code": "1234"},
                {"profit_jpy": -30000, "ret_pct": -3.0, "code": "5678"},
            ]
        )
        mock_load_trades.return_value = trades_data

        analyze_backtest_json.main(["test.json", "--show-trades"])

        # 取引テーブルが表示されることを確認
        calls = [str(call) for call in mock_print.call_args_list]
        assert any("=== Trades ===" in call for call in calls)
        assert any("=== Profit per Trade ===" in call for call in calls)

    @mock.patch("backtest.analyze_backtest_json.load_trades")
    @mock.patch("builtins.print")
    def test_main_with_side_filter_long(self, mock_print, mock_load_trades):
        """--side longオプションのテスト"""
        trades_data = pd.DataFrame(
            [
                {"profit_jpy": 100000, "ret_pct": 10.0, "side": "long"},
                {"profit_jpy": -30000, "ret_pct": -3.0, "side": "short"},
                {"profit_jpy": 50000, "ret_pct": 5.0, "side": "long"},
            ]
        )
        mock_load_trades.return_value = trades_data

        analyze_backtest_json.main(["test.json", "--side", "long"])

        # サマリーが表示されることを確認
        calls = [str(call) for call in mock_print.call_args_list]
        assert any("=== Summary ===" in call for call in calls)

    @mock.patch("backtest.analyze_backtest_json.load_trades")
    @mock.patch("builtins.print")
    def test_main_with_side_filter_no_side_column(self, mock_print, mock_load_trades):
        """sideカラムがない場合の--sideオプションテスト"""
        trades_data = pd.DataFrame(
            [
                {"profit_jpy": 100000, "ret_pct": 10.0},
                {"profit_jpy": -30000, "ret_pct": -3.0},
            ]
        )
        mock_load_trades.return_value = trades_data

        analyze_backtest_json.main(["test.json", "--side", "long"])

        # 警告メッセージが表示されることを確認
        calls = [str(call) for call in mock_print.call_args_list]
        assert any("Warning: 'side' column not found" in call for call in calls)

    @mock.patch("backtest.analyze_backtest_json.load_trades")
    @mock.patch("builtins.print")
    def test_main_with_side_filter_short(self, mock_print, mock_load_trades):
        """--side shortオプションのテスト"""
        trades_data = pd.DataFrame(
            [
                {"profit_jpy": 100000, "ret_pct": 10.0, "side": "long"},
                {"profit_jpy": -30000, "ret_pct": -3.0, "side": "short"},
                {"profit_jpy": 50000, "ret_pct": 5.0, "side": "short"},
            ]
        )
        mock_load_trades.return_value = trades_data

        analyze_backtest_json.main(["test.json", "--side", "short"])

        # サマリーが表示されることを確認
        calls = [str(call) for call in mock_print.call_args_list]
        assert any("=== Summary ===" in call for call in calls)

    @mock.patch("backtest.analyze_backtest_json.load_trades")
    @mock.patch("builtins.print")
    def test_main_full_options(self, mock_print, mock_load_trades):
        """全オプション指定のmain関数テスト"""
        trades_data = pd.DataFrame(
            [
                {"profit_jpy": 100000, "ret_pct": 10.0, "side": "long", "code": "1234"},
                {"profit_jpy": -30000, "ret_pct": -3.0, "side": "long", "code": "5678"},
            ]
        )
        mock_load_trades.return_value = trades_data

        analyze_backtest_json.main(
            ["test1.json", "test2.json", "--show-trades", "--side", "long"]
        )

        # 全ての出力セクションが表示されることを確認
        calls = [str(call) for call in mock_print.call_args_list]
        assert any("=== Summary ===" in call for call in calls)
        assert any("=== Trades ===" in call for call in calls)
        assert any("=== Profit per Trade ===" in call for call in calls)


class TestIntegration:
    """統合テスト"""

    def test_full_analysis_workflow(self, tmp_path):
        """完全な分析ワークフローのテスト"""
        # テストデータ作成（複数ファイル）
        trades_data1 = [
            {
                "code": "1234",
                "side": "long",
                "entry_date": "2024-01-15",
                "exit_date": "2024-02-15",
                "profit_jpy": 150000,
                "ret_pct": 15.0,
            },
            {
                "code": "5678",
                "side": "long",
                "entry_date": "2024-01-16",
                "exit_date": "2024-02-16",
                "profit_jpy": -40000,
                "ret_pct": -4.0,
            },
        ]

        trades_data2 = [
            {
                "code": "9012",
                "side": "short",
                "entry_date": "2024-01-17",
                "exit_date": "2024-02-17",
                "profit_jpy": 80000,
                "ret_pct": 8.0,
            }
        ]

        json_file1 = tmp_path / "backtest_result1.json"
        json_file2 = tmp_path / "backtest_result2.json"

        with open(json_file1, "w") as f:
            json.dump(trades_data1, f)
        with open(json_file2, "w") as f:
            json.dump(trades_data2, f)

        # 分析ワークフロー実行
        # 1. データ読み込み
        all_trades = analyze_backtest_json.load_trades(
            [str(json_file1), str(json_file2)]
        )

        assert len(all_trades) == 3

        # 2. 全体のサマリー計算
        summary = analyze_backtest_json.summarize(all_trades)
        assert not summary.empty

        metrics_dict = dict(zip(summary["metric"], summary["value"], strict=False))
        assert metrics_dict["trades"] == 3
        assert metrics_dict["total_profit"] == 190000
        assert metrics_dict["win_rate"] == 2 / 3  # 2勝1敗

        # 3. フォーマット処理
        formatted_summary = analyze_backtest_json.format_summary(summary)
        assert not formatted_summary.empty

        # 4. サイド別フィルタリング
        long_trades = all_trades[all_trades["side"] == "long"]
        assert len(long_trades) == 2

        short_trades = all_trades[all_trades["side"] == "short"]
        assert len(short_trades) == 1

        # 5. 視覚化
        profit_chart = analyze_backtest_json._ascii_bar_chart(
            all_trades["profit_jpy"].tolist()
        )
        assert len(profit_chart) > 0

        table = analyze_backtest_json._ascii_table(all_trades[["code", "profit_jpy"]])
        assert "1234" in table
        assert "190000" not in table  # 個別取引の表示なので合計値は含まれない


class TestErrorHandling:
    """エラー処理のテスト"""

    def test_missing_file(self):
        """存在しないファイルのテスト"""
        with pytest.raises(FileNotFoundError):
            analyze_backtest_json.load_trades(["/nonexistent/file.json"])

    def test_invalid_json(self, tmp_path):
        """無効なJSONファイルのテスト"""
        invalid_json = tmp_path / "invalid.json"
        with open(invalid_json, "w") as f:
            f.write("invalid json content")

        with pytest.raises((json.JSONDecodeError, ValueError)):
            analyze_backtest_json.load_trades([str(invalid_json)])

    def test_missing_required_columns(self):
        """必要なカラムが不足している場合のテスト"""
        trades = pd.DataFrame({"code": ["1234"], "other_col": [100]})

        with pytest.raises(ValueError):
            analyze_backtest_json.summarize(trades)

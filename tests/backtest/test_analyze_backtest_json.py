"""analyze_backtest_json.pyのテスト"""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from backtest.analyze_backtest_json import (
    _ascii_bar_chart,
    _ascii_table,
    _find_col,
    format_summary,
    load_trades,
    main,
    summarize,
)


class TestLoadTrades:
    """load_trades関数のテスト"""

    def test_load_single_file(self, tmp_path):
        """単一ファイルの読み込みテスト"""
        # テストデータ
        test_data = [
            {"trade_id": 1, "code": "1234", "profit_jpy": 1000, "ret_pct": 5.0},
            {"trade_id": 2, "code": "5678", "profit_jpy": -500, "ret_pct": -2.5},
        ]
        test_file = tmp_path / "test.json"
        test_file.write_text(json.dumps(test_data))

        # 実行
        result = load_trades([str(test_file)])

        # 検証
        assert len(result) == 2
        assert str(int(result.iloc[0]["code"])) == "1234"
        assert result.iloc[1]["profit_jpy"] == -500

    def test_load_multiple_files(self, tmp_path):
        """複数ファイルの読み込みテスト"""
        # テストデータ
        data1 = [{"trade_id": 1, "code": "1111", "profit_jpy": 1000}]
        data2 = [{"trade_id": 2, "code": "2222", "profit_jpy": 2000}]

        file1 = tmp_path / "test1.json"
        file2 = tmp_path / "test2.json"
        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        # 実行
        result = load_trades([str(file1), str(file2)])

        # 検証
        assert len(result) == 2
        assert result["profit_jpy"].sum() == 3000

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.glob")
    def test_file_not_found_with_suggestions(self, mock_glob, mock_exists):
        """ファイルが見つからない場合の提案表示テスト"""
        mock_exists.return_value = False
        mock_glob.return_value = [Path("test1.json"), Path("test2.json")]

        with pytest.raises(FileNotFoundError) as exc_info:
            load_trades(["nonexistent.json"])

        assert "nonexistent.json" in str(exc_info.value)

    def test_default_directory_fallback(self, tmp_path):
        """デフォルトディレクトリへのフォールバックテスト"""
        # デフォルトディレクトリを作成
        default_dir = tmp_path / "data" / "output" / "backtest"
        default_dir.mkdir(parents=True)

        # テストデータ
        test_data = [{"trade_id": 1, "code": "9999", "profit_jpy": 500}]
        test_file = default_dir / "result.json"
        test_file.write_text(json.dumps(test_data))

        # カレントディレクトリを一時的に変更
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = load_trades(["result.json"])
            assert len(result) == 1
            assert str(int(result.iloc[0]["code"])) == "9999"
        finally:
            os.chdir(original_cwd)


class TestFindCol:
    """_find_col関数のテスト"""

    def test_find_existing_column(self):
        """存在するカラムの検索テスト"""
        df = pd.DataFrame({"profit_jpy": [1000], "other_col": [100]})
        result = _find_col(df, ["profit_jpy", "pnl_yen"])
        assert result == "profit_jpy"

    def test_find_alternative_column(self):
        """代替カラムの検索テスト"""
        df = pd.DataFrame({"pnl_yen": [1000], "other_col": [100]})
        result = _find_col(df, ["profit_jpy", "pnl_yen"])
        assert result == "pnl_yen"

    def test_column_not_found(self):
        """カラムが見つからない場合のテスト"""
        df = pd.DataFrame({"other_col": [100]})
        with pytest.raises(ValueError, match="Required column not found"):
            _find_col(df, ["profit_jpy", "pnl_yen"])


class TestSummarize:
    """summarize関数のテスト"""

    def test_summarize_with_trades(self):
        """取引データがある場合の集計テスト"""
        df = pd.DataFrame(
            {
                "code": ["1111", "2222", "3333", "4444"],
                "profit_jpy": [1000, -500, 2000, -1000],
                "ret_pct": [5.0, -2.5, 10.0, -5.0],
            }
        )

        result = summarize(df)

        assert len(result) == 5
        assert result[result["metric"] == "trades"]["value"].iloc[0] == 4
        assert result[result["metric"] == "total_profit"]["value"].iloc[0] == 1500
        assert result[result["metric"] == "win_rate"]["value"].iloc[0] == 0.5
        assert result[result["metric"] == "avg_ret_pct"]["value"].iloc[0] == 1.875

    def test_summarize_empty_dataframe(self):
        """空のデータフレームの集計テスト"""
        df = pd.DataFrame()
        result = summarize(df)
        assert result.empty

    def test_summarize_all_wins(self):
        """全勝の場合の集計テスト"""
        df = pd.DataFrame(
            {"profit_jpy": [1000, 2000, 3000], "ret_pct": [5.0, 10.0, 15.0]}
        )

        result = summarize(df)
        win_rate = result[result["metric"] == "win_rate"]["value"].iloc[0]
        assert win_rate == 1.0


class TestFormatSummary:
    """format_summary関数のテスト"""

    def test_format_summary(self):
        """サマリーのフォーマットテスト"""
        summary = pd.DataFrame(
            {
                "metric": ["trades", "total_profit", "win_rate"],
                "value": [10, 50000, 0.6],
            }
        )

        result = format_summary(summary)

        assert len(result) == 3
        assert result[result["metric"] == "trades"]["value"].iloc[0] == "10"
        assert (
            result[result["metric"] == "total_profit"]["value"].iloc[0] == "50,000 JPY"
        )
        assert result[result["metric"] == "win_rate"]["value"].iloc[0] == "60.00%"

    def test_format_summary_empty(self):
        """空のサマリーのフォーマットテスト"""
        summary = pd.DataFrame()
        result = format_summary(summary)
        assert result.empty


class TestAsciiBarChart:
    """_ascii_bar_chart関数のテスト"""

    def test_ascii_bar_chart_positive_values(self):
        """正の値のバーチャートテスト"""
        result = _ascii_bar_chart([10, 20, 30], width=10)
        lines = result.strip().split("\n")
        assert len(lines) == 3
        assert "#" in lines[0]
        assert "(+10)" in lines[0]
        assert "(+30)" in lines[2]
        # 30のバーが10のバーより長い
        assert lines[2].count("#") > lines[0].count("#")

    def test_ascii_bar_chart_mixed_values(self):
        """正負混在のバーチャートテスト"""
        result = _ascii_bar_chart([10, -20, 30], width=20)
        lines = result.strip().split("\n")
        assert len(lines) == 3
        assert "#" in result
        assert "-#" in lines[1]  # 負の値
        assert "(-20)" in lines[1]

    def test_ascii_bar_chart_empty(self):
        """空のリストのバーチャートテスト"""
        result = _ascii_bar_chart([], width=10)
        assert result == ""


class TestAsciiTable:
    """_ascii_table関数のテスト"""

    def test_ascii_table_basic(self):
        """基本的なテーブル表示テスト"""
        df = pd.DataFrame({"col1": ["A", "B"], "col2": [1, 2]})
        result = _ascii_table(df)
        assert "col1" in result
        assert "col2" in result
        assert "A" in result
        assert "B" in result

    def test_ascii_table_heavy_borders(self):
        """太い枠線のテーブル表示テスト"""
        df = pd.DataFrame({"data": [1, 2, 3]})
        result = _ascii_table(df, heavy=True)
        # heavy=TrueでもエンコーディングによってはASCII文字にフォールバックする
        # テーブルが作成されることを確認
        assert "|" in result or "║" in result
        assert "-" in result or "═" in result

    def test_ascii_table_empty(self):
        """空のデータフレームのテーブル表示テスト"""
        df = pd.DataFrame()
        # 空のDataFrameでもテーブルが作成される
        result = _ascii_table(df)
        # 空のテーブルでもボーダーがある
        assert "+" in result


class TestMain:
    """main関数のテスト"""

    @patch("sys.argv", ["analyze_backtest_json.py", "test.json"])
    @patch("backtest.analyze_backtest_json.load_trades")
    def test_main_single_file(self, mock_load_trades, capsys):
        """単一ファイルの処理テスト"""
        # モックデータ
        mock_df = pd.DataFrame(
            {
                "code": ["1111", "2222"],
                "profit_jpy": [1000, -500],
                "ret_pct": [5.0, -2.5],
                "entry_date": ["2024-01-01", "2024-01-02"],
                "exit_date": ["2024-01-10", "2024-01-15"],
            }
        )
        mock_load_trades.return_value = mock_df

        # 実行
        main()

        # 出力を検証
        captured = capsys.readouterr()
        assert "=== Summary ===" in captured.out
        assert "trades" in captured.out
        assert "win_rate" in captured.out

    @patch("sys.argv", ["analyze_backtest_json.py", "test.json", "--show-trades"])
    @patch("backtest.analyze_backtest_json.load_trades")
    def test_main_show_trades(self, mock_load_trades, capsys):
        """取引詳細表示オプションのテスト"""
        # モックデータ
        mock_df = pd.DataFrame(
            {
                "code": ["1111"],
                "profit_jpy": [1000],
                "ret_pct": [5.0],
                "entry_date": ["2024-01-01"],
                "exit_date": ["2024-01-10"],
            }
        )
        mock_load_trades.return_value = mock_df

        # 実行
        main()

        # 出力を検証
        captured = capsys.readouterr()
        assert "code" in captured.out
        assert "1111" in captured.out

    @patch("sys.argv", ["analyze_backtest_json.py", "--side", "long", "test.json"])
    @patch("backtest.analyze_backtest_json.load_trades")
    def test_main_filter_by_side(self, mock_load_trades, capsys):
        """サイドフィルタオプションのテスト"""
        # モックデータ
        mock_df = pd.DataFrame(
            {
                "code": ["1111", "2222"],
                "side": ["long", "short"],
                "profit_jpy": [1000, -500],
                "ret_pct": [5.0, -2.5],
                "entry_date": ["2024-01-01", "2024-01-02"],
                "exit_date": ["2024-01-10", "2024-01-15"],
            }
        )
        mock_load_trades.return_value = mock_df

        # 実行
        main()

        # 出力を検証（sideフィルタリングの表示確認）
        captured = capsys.readouterr()
        assert "trades" in captured.out  # Summaryが表示される

    @patch("sys.argv", ["analyze_backtest_json.py"])
    def test_main_no_args(self, capsys):
        """引数なしの場合のテスト"""
        with pytest.raises(SystemExit):
            main()

        captured = capsys.readouterr()
        assert "usage:" in captured.err or "positional arguments" in captured.err


class TestIntegration:
    """統合テスト"""

    def test_full_workflow(self, tmp_path):
        """完全なワークフローのテスト"""
        # テストデータ作成
        test_data = [
            {
                "trade_id": 1,
                "code": "1111",
                "side": "long",
                "entry_date": "2024-01-01",
                "exit_date": "2024-01-10",
                "profit_jpy": 5000,
                "ret_pct": 10.0,
            },
            {
                "trade_id": 2,
                "code": "2222",
                "side": "short",
                "entry_date": "2024-01-05",
                "exit_date": "2024-01-15",
                "profit_jpy": -2000,
                "ret_pct": -4.0,
            },
            {
                "trade_id": 3,
                "code": "3333",
                "side": "long",
                "entry_date": "2024-01-20",
                "exit_date": "2024-01-25",
                "profit_jpy": 3000,
                "ret_pct": 6.0,
            },
        ]
        test_file = tmp_path / "backtest_result.json"
        test_file.write_text(json.dumps(test_data))

        # データ読み込み
        trades = load_trades([str(test_file)])
        assert len(trades) == 3

        # サマリー統計
        summary = summarize(trades)
        assert summary[summary["metric"] == "trades"]["value"].iloc[0] == 3
        assert summary[summary["metric"] == "total_profit"]["value"].iloc[0] == 6000
        assert summary[summary["metric"] == "win_rate"]["value"].iloc[0] == 2 / 3

        # フォーマット
        formatted = format_summary(summary)
        assert (
            formatted[formatted["metric"] == "total_profit"]["value"].iloc[0]
            == "6,000 JPY"
        )

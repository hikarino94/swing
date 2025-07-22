"""backtest.backtest_technicalのテスト"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# プロジェクトのパスを追加
sys.path.append(str(Path(__file__).resolve().parents[2]))

from backtest.backtest_technical import (
    _result_paths,
    parse_args,
    run_backtest,
    run_backtest_range,
    run_backtest_short,
)


class TestResultPaths:
    """_result_paths関数のテスト"""

    @patch("backtest.backtest_technical.get_timestamped_output_path")
    def test_result_paths(self, mock_get_path):
        """結果ファイルパスの生成"""
        excel_path = Path("/output/backtest/test_20240115_123456.xlsx")
        json_path = Path("/output/backtest/test_20240115_123456.json")

        mock_get_path.side_effect = [excel_path, json_path]

        result_excel, result_json = _result_paths("test")

        assert result_excel == excel_path
        assert result_json == json_path

        # 呼び出し引数の確認
        calls = mock_get_path.call_args_list
        assert calls[0][0] == ("backtest", "test", ".xlsx")
        assert calls[1][0] == ("backtest", "test", ".json")


class TestRunBacktest:
    """run_backtest関数のテスト"""

    def setup_method(self):
        """共通のモックデータをセットアップ"""
        self.conn = MagicMock()
        self.as_of = "2024-01-15"

    def test_no_signals(self):
        """シグナルがない場合"""
        # 空のDataFrameを返す
        with patch("pandas.read_sql", return_value=pd.DataFrame()):
            result = run_backtest(self.conn, self.as_of)

        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_successful_trade(self):
        """成功した取引のテスト"""
        # シグナルデータ
        signals_df = pd.DataFrame([{"code": "1234"}])

        # 価格データ
        prices_df = pd.DataFrame(
            [
                {"date": pd.Timestamp("2024-01-15"), "close": 1000.0},
                {"date": pd.Timestamp("2024-01-16"), "close": 1050.0},
                {"date": pd.Timestamp("2024-01-17"), "close": 1100.0},
                {"date": pd.Timestamp("2024-02-15"), "close": 1200.0},
            ]
        )

        # 会社名データ
        self.conn.execute.return_value.fetchone.return_value = ["テスト会社"]

        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [signals_df, prices_df]

            result = run_backtest(
                self.conn,
                self.as_of,
                capital=1000000,
                hold_days=60,
                stop_loss_pct=0.05,
                min_price=300,
            )

        assert len(result) == 1
        assert result.iloc[0]["code"] == "1234"
        assert result.iloc[0]["name"] == "テスト会社"
        assert result.iloc[0]["entry_price"] == 1000.0
        assert result.iloc[0]["exit_price"] == 1200.0
        assert result.iloc[0]["shares"] == 1000
        assert result.iloc[0]["side"] == "long"
        assert result.iloc[0]["pnl_pct"] == 20.0
        assert result.iloc[0]["pnl_yen"] == 200000.0

    def test_stop_loss_exit(self):
        """損切りによる終了"""
        signals_df = pd.DataFrame([{"code": "1234"}])

        # 価格が下落して損切りラインに到達
        prices_df = pd.DataFrame(
            [
                {"date": pd.Timestamp("2024-01-15"), "close": 1000.0},
                {"date": pd.Timestamp("2024-01-16"), "close": 980.0},
                {"date": pd.Timestamp("2024-01-17"), "close": 950.0},  # 5%下落
                {"date": pd.Timestamp("2024-01-18"), "close": 900.0},
            ]
        )

        self.conn.execute.return_value.fetchone.return_value = None

        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [signals_df, prices_df]

            result = run_backtest(
                self.conn, self.as_of, capital=1000000, stop_loss_pct=0.05
            )

        assert len(result) == 1
        assert result.iloc[0]["exit_price"] == 950.0
        assert result.iloc[0]["pnl_pct"] == -5.0
        assert result.iloc[0]["holding_days"] == 2

    def test_skip_conditions(self):
        """スキップ条件のテスト"""
        signals_df = pd.DataFrame(
            [
                {"code": "1234"},  # 価格データなし
                {"code": "5678"},  # 最低価格未満
                {"code": "9012"},  # エントリー日の価格なし
                {"code": "3456"},  # 正常
            ]
        )

        # 各銘柄の価格データ
        no_data = pd.DataFrame()
        low_price = pd.DataFrame(
            [{"date": pd.Timestamp("2024-01-15"), "close": 100.0}]  # min_price=300未満
        )
        no_entry_date = pd.DataFrame(
            [{"date": pd.Timestamp("2024-01-16"), "close": 1000.0}]  # 15日のデータなし
        )
        normal_price = pd.DataFrame(
            [
                {"date": pd.Timestamp("2024-01-15"), "close": 1000.0},
                {"date": pd.Timestamp("2024-02-15"), "close": 1100.0},
            ]
        )

        self.conn.execute.return_value.fetchone.return_value = None

        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [
                signals_df,
                no_data,
                low_price,
                no_entry_date,
                normal_price,
            ]

            result = run_backtest(self.conn, self.as_of, min_price=300)

        # 正常な1件のみが処理される
        assert len(result) == 1
        assert result.iloc[0]["code"] == "3456"

    def test_exception_handling(self):
        """例外処理のテスト"""
        signals_df = pd.DataFrame([{"code": "1234"}])

        with patch("pandas.read_sql") as mock_read_sql:
            # 価格データ取得時に例外発生
            mock_read_sql.side_effect = [signals_df, Exception("DB Error")]

            result = run_backtest(self.conn, self.as_of)

        # エラーが発生してもスキップして続行
        assert result.empty


class TestRunBacktestShort:
    """run_backtest_short関数のテスト"""

    def setup_method(self):
        """共通のモックデータをセットアップ"""
        self.conn = MagicMock()
        self.as_of = "2024-01-15"

    def test_successful_short_trade(self):
        """成功した空売り取引"""
        signals_df = pd.DataFrame([{"code": "1234"}])

        # 価格が下落（空売りで利益）
        prices_df = pd.DataFrame(
            [
                {"date": pd.Timestamp("2024-01-15"), "close": 1000.0},
                {"date": pd.Timestamp("2024-01-16"), "close": 950.0},
                {"date": pd.Timestamp("2024-02-15"), "close": 800.0},
            ]
        )

        self.conn.execute.return_value.fetchone.return_value = ["テスト会社"]

        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [signals_df, prices_df]

            result = run_backtest_short(
                self.conn, self.as_of, capital=1000000, hold_days=60
            )

        assert len(result) == 1
        assert result.iloc[0]["side"] == "short"
        assert result.iloc[0]["entry_price"] == 1000.0
        assert result.iloc[0]["exit_price"] == 800.0
        # 空売りの利益計算（エントリー価格 - 出口価格）
        assert result.iloc[0]["pnl_pct"] == 20.0
        assert result.iloc[0]["pnl_yen"] == 200000.0

    def test_short_stop_loss(self):
        """空売りの損切り（価格上昇）"""
        signals_df = pd.DataFrame([{"code": "1234"}])

        # 価格が上昇（空売りで損失）
        prices_df = pd.DataFrame(
            [
                {"date": pd.Timestamp("2024-01-15"), "close": 1000.0},
                {"date": pd.Timestamp("2024-01-16"), "close": 1030.0},
                {"date": pd.Timestamp("2024-01-17"), "close": 1050.0},  # 5%上昇で損切り
            ]
        )

        self.conn.execute.return_value.fetchone.return_value = None

        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = [signals_df, prices_df]

            result = run_backtest_short(self.conn, self.as_of, stop_loss_pct=0.05)

        assert len(result) == 1
        assert result.iloc[0]["exit_price"] == 1050.0
        assert result.iloc[0]["pnl_pct"] == -5.0
        assert result.iloc[0]["holding_days"] == 2


class TestRunBacktestRange:
    """run_backtest_range関数のテスト"""

    @patch("backtest.backtest_technical.run_backtest")
    @patch("backtest.backtest_technical.run_backtest_short")
    @patch("backtest.backtest_technical.pd.ExcelWriter")
    @patch("builtins.open", create=True)
    def test_run_backtest_range_success(
        self, mock_open, mock_excel_writer, mock_short, mock_long
    ):
        """複数日のバックテスト実行"""
        conn = MagicMock()
        start = "2024-01-01"
        end = "2024-01-03"

        # 各日の取引結果
        day1_long = pd.DataFrame([{"code": "1234", "pnl_yen": 10000, "side": "long"}])
        day1_short = pd.DataFrame([{"code": "5678", "pnl_yen": 5000, "side": "short"}])
        day2_long = pd.DataFrame([{"code": "9012", "pnl_yen": -3000, "side": "long"}])
        day2_short = pd.DataFrame()  # 空

        # 3日間のデータを準備
        mock_long.side_effect = [day1_long, day2_long, pd.DataFrame()]  # 3日目は空
        mock_short.side_effect = [day1_short, day2_short, pd.DataFrame()]

        # pandas.concatをモックして結果を検証
        expected_result = pd.DataFrame(
            [
                {"code": "1234", "pnl_yen": 10000, "pnl_pct": 10.0, "side": "long"},
                {"code": "5678", "pnl_yen": 5000, "pnl_pct": 5.0, "side": "short"},
                {"code": "9012", "pnl_yen": -3000, "pnl_pct": -3.0, "side": "long"},
            ]
        )

        with patch("pandas.concat", return_value=expected_result) as mock_concat:
            # 関数を実行（void関数）
            run_backtest_range(conn, start, end)

            # concatが呼ばれたことを確認
            mock_concat.assert_called_once()

            # バックテスト関数が適切に呼ばれたことを確認
            assert mock_long.call_count == 3  # 3日間
            assert mock_short.call_count == 3

    @patch("backtest.backtest_technical.run_backtest")
    @patch("backtest.backtest_technical.run_backtest_short")
    @patch("backtest.backtest_technical.logger")
    def test_run_backtest_range_no_trades(self, mock_logger, mock_short, mock_long):
        """取引がない場合"""
        conn = MagicMock()
        start = "2024-01-01"
        end = "2024-01-01"

        # 空のDataFrameを返す
        mock_long.return_value = pd.DataFrame()
        mock_short.return_value = pd.DataFrame()

        # 関数を実行
        run_backtest_range(conn, start, end)

        # ログで「No trades」が出力されることを確認
        mock_logger.info.assert_any_call("No trades in the specified period.")

    @patch("backtest.backtest_technical.run_backtest")
    @patch("backtest.backtest_technical.run_backtest_short")
    def test_run_backtest_range_all_empty(self, mock_short, mock_long):
        """全ての日で取引なし"""
        conn = MagicMock()
        start = "2024-01-01"
        end = "2024-01-02"

        mock_long.return_value = pd.DataFrame()
        mock_short.return_value = pd.DataFrame()

        # 関数を実行（void関数）
        run_backtest_range(conn, start, end)

        # バックテスト関数が2日分呼ばれたことを確認
        assert mock_long.call_count == 2
        assert mock_short.call_count == 2


class TestParseArgs:
    """parse_args関数のテスト"""

    def test_parse_args_with_all_options(self):
        """全オプション指定"""
        args = parse_args(
            [
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-31",
                "--capital",
                "2000000",
                "--hold-days",
                "30",
                "--stop-loss",
                "0.1",
                "--min-price",
                "500",
                "--show",
            ]
        )

        assert args.start == "2024-01-01"
        assert args.end == "2024-01-31"
        assert args.capital == 2000000
        assert args.hold_days == 30
        assert args.stop_loss == 0.1
        assert args.min_price == 500
        assert args.show is True

    def test_parse_args_minimal(self):
        """最小限のオプション"""
        args = parse_args(["--start", "2024-01-15"])

        assert args.start == "2024-01-15"
        assert args.end is None
        assert args.capital == 1000000  # デフォルト
        assert args.hold_days == 60  # デフォルト
        assert args.stop_loss == 0.05  # デフォルト
        assert args.min_price == 300  # デフォルト
        assert args.show is False  # デフォルト

    def test_parse_args_with_paths(self):
        """ファイルパス指定"""
        args = parse_args(
            [
                "--start",
                "2024-01-01",
                "--outfile",
                "/tmp/test.xlsx",
                "--json",
                "/tmp/test.json",
                "--db",
                "/tmp/test.db",
            ]
        )

        assert str(args.outfile) == "/tmp/test.xlsx"
        assert str(args.json) == "/tmp/test.json"
        assert args.db == "/tmp/test.db"

    def test_parse_args_invalid(self):
        """必須パラメータなしでエラー"""
        with pytest.raises(SystemExit):
            parse_args([])  # --startが必須

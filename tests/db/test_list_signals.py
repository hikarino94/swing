"""list_signals.pyのテスト"""

from datetime import date
from io import StringIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from db.list_signals import main


class TestListSignals:
    """list_signals.pyのテスト"""

    @pytest.fixture
    def mock_connect(self):
        """データベース接続のモック"""
        with patch("db.list_signals.sqlite3.connect") as mock:
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=None)
            mock.return_value = mock_conn
            yield mock, mock_conn

    @pytest.fixture
    def mock_args(self):
        """argparseのモック"""
        with patch("db.list_signals.argparse.ArgumentParser") as mock_parser:
            mock_args = MagicMock()
            mock_parser.return_value.parse_args.return_value = mock_args
            yield mock_args

    def test_main_fund_signals_with_dates(self, mock_connect, mock_args):
        """ファンダメンタルシグナル表示（日付指定）のテスト"""
        _, mock_conn = mock_connect

        # 引数設定
        mock_args.kind = "fund"
        mock_args.db = "test.db"
        mock_args.start = "2024-01-01"
        mock_args.end = "2024-01-31"
        mock_args.limit = 20

        # DataFrameのモック
        test_data = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "company_name": ["テスト株式会社A", "テスト株式会社B"],
                "DisclosedAt": ["2024-01-10", "2024-01-15"],
                "NetSales_YoY": [15.5, 20.3],
                "ROE": [12.5, 15.0],
            }
        )

        with patch("db.list_signals.pd.read_sql", return_value=test_data):
            with patch("sys.stdout", new=StringIO()) as fake_out:
                main()
                output = fake_out.getvalue()

        assert "1234" in output
        assert "テスト株式会社A" in output
        assert "5678" in output
        assert "テスト株式会社B" in output

    def test_main_tech_signals_default_date(self, mock_connect, mock_args):
        """テクニカルシグナル表示（デフォルト日付）のテスト"""
        _, mock_conn = mock_connect

        # 引数設定（日付なし）
        mock_args.kind = "tech"
        mock_args.db = "test.db"
        mock_args.start = None
        mock_args.end = None
        mock_args.limit = 20

        # 今日の日付
        today = date.today().isoformat()

        # DataFrameのモック
        test_data = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "signal_date": [today, today],
                "close_price": [1500.0, 2000.0],
                "signal_count": [4, 5],
                "composite_score": [0.8, 0.9],
            }
        )

        with patch("db.list_signals.pd.read_sql", return_value=test_data) as mock_read:
            with patch("sys.stdout", new=StringIO()) as fake_out:
                main()
                output = fake_out.getvalue()

        # デフォルトで今日の日付が設定されることを確認
        call_args = mock_read.call_args
        assert today in str(call_args)
        assert "1234" in output
        assert "5678" in output

    def test_main_tech_signals_with_filters(self, mock_connect, mock_args):
        """テクニカルシグナル表示（フィルタ付き）のテスト"""
        _, mock_conn = mock_connect

        # 引数設定
        mock_args.kind = "tech"
        mock_args.db = "test.db"
        mock_args.start = "2024-01-01"
        mock_args.end = "2024-01-31"
        mock_args.limit = 20

        # DataFrameのモック
        test_data = pd.DataFrame(
            {
                "code": ["1234"],
                "signal_date": ["2024-01-15"],
                "close_price": [1500.0],
                "signal_count": [5],
                "side": ["long"],
            }
        )

        with patch("db.list_signals.pd.read_sql", return_value=test_data) as mock_read:
            with patch("sys.stdout", new=StringIO()):
                main()

        # SQLクエリにフィルタが含まれることを確認
        sql_query = mock_read.call_args[0][0]
        assert "signals_count>=3" in sql_query
        assert "signals_first=1" in sql_query
        assert "signals_overheating=0" in sql_query
        assert "signal_date >= ?" in sql_query
        assert "signal_date <= ?" in sql_query

    def test_main_no_data(self, mock_connect, mock_args):
        """データがない場合のテスト"""
        _, mock_conn = mock_connect

        # 引数設定
        mock_args.kind = "fund"
        mock_args.db = "test.db"
        mock_args.start = "2024-01-01"
        mock_args.end = "2024-01-31"
        mock_args.limit = 20

        # 空のDataFrame
        test_data = pd.DataFrame()

        with patch("db.list_signals.pd.read_sql", return_value=test_data):
            with patch("sys.stdout", new=StringIO()) as fake_out:
                main()
                output = fake_out.getvalue()

        assert "(no rows)" in output

    def test_main_no_filters_with_limit(self, mock_connect, mock_args):
        """フィルタなし（LIMIT使用）のテスト"""
        _, mock_conn = mock_connect

        # 引数設定（日付指定なし、techでもない）
        mock_args.kind = "fund"
        mock_args.db = "test.db"
        mock_args.start = None
        mock_args.end = None
        mock_args.limit = 10

        # DataFrameのモック
        test_data = pd.DataFrame(
            {
                "code": [str(i) for i in range(10)],
                "DisclosedAt": ["2024-01-01"] * 10,
                "ROE": [10.0 + i for i in range(10)],
            }
        )

        with patch("db.list_signals.pd.read_sql", return_value=test_data) as mock_read:
            with patch("sys.stdout", new=StringIO()):
                main()

        # 日付がない場合でもデフォルトで今日の日付が設定される
        today = date.today().isoformat()
        # デフォルトで今日の日付が設定されているか確認
        params = mock_read.call_args[1]["params"]
        assert today in params

    def test_main_fund_date_range(self, mock_connect, mock_args):
        """ファンダメンタルシグナルの日付範囲テスト"""
        _, mock_conn = mock_connect

        # 引数設定
        mock_args.kind = "fund"
        mock_args.db = "test.db"
        mock_args.start = "2024-01-01"
        mock_args.end = None  # 終了日なし
        mock_args.limit = 20

        # DataFrameのモック
        test_data = pd.DataFrame(
            {"code": ["1234"], "DisclosedAt": ["2024-01-15"], "NetSales": [1000000]}
        )

        with patch("db.list_signals.pd.read_sql", return_value=test_data) as mock_read:
            with patch("sys.stdout", new=StringIO()):
                main()

        # 開始日のみの条件が設定されることを確認
        sql_query = mock_read.call_args[0][0]
        assert "DisclosedAt >= ?" in sql_query
        assert "DisclosedAt <= ?" not in sql_query
        assert mock_read.call_args[1]["params"] == ("2024-01-01",)

    def test_main_tech_date_range(self, mock_connect, mock_args):
        """テクニカルシグナルの日付範囲テスト"""
        _, mock_conn = mock_connect

        # 引数設定
        mock_args.kind = "tech"
        mock_args.db = "test.db"
        mock_args.start = None  # 開始日なし
        mock_args.end = "2024-01-31"
        mock_args.limit = 20

        # DataFrameのモック
        test_data = pd.DataFrame(
            {"code": ["5678"], "signal_date": ["2024-01-20"], "signal_count": [4]}
        )

        with patch("db.list_signals.pd.read_sql", return_value=test_data) as mock_read:
            with patch("sys.stdout", new=StringIO()):
                main()

        # 終了日のみの条件が設定されることを確認
        sql_query = mock_read.call_args[0][0]
        assert "signal_date >= ?" not in sql_query
        assert "signal_date <= ?" in sql_query
        # techのフィルタ分のパラメータも含まれる
        assert "2024-01-31" in str(mock_read.call_args[1]["params"])

    @patch("sys.argv", ["list_signals.py", "fund", "--start", "2024-01-01"])
    def test_main_with_sys_argv(self, mock_connect):
        """実際のコマンドライン引数でのテスト"""
        _, mock_conn = mock_connect

        # DataFrameのモック
        test_data = pd.DataFrame(
            {
                "code": ["1234"],
                "DisclosedAt": ["2024-01-15"],
                "company_name": ["テスト"],
            }
        )

        with patch("db.list_signals.pd.read_sql", return_value=test_data):
            with patch("sys.stdout", new=StringIO()) as fake_out:
                main()
                output = fake_out.getvalue()

        assert "1234" in output

    def test_main_custom_db_path(self, mock_connect, mock_args):
        """カスタムDBパス指定のテスト"""
        mock_connect_func, mock_conn = mock_connect

        # 引数設定
        mock_args.kind = "fund"
        mock_args.db = "/custom/path/to/database.db"
        mock_args.start = "2024-01-01"
        mock_args.end = "2024-01-31"
        mock_args.limit = 20

        # DataFrameのモック
        test_data = pd.DataFrame({"code": ["1234"]})

        with patch("db.list_signals.pd.read_sql", return_value=test_data):
            with patch("sys.stdout", new=StringIO()):
                main()

        # カスタムDBパスが使用されることを確認
        mock_connect_func.assert_called_with("/custom/path/to/database.db")

    def test_main_large_dataset(self, mock_connect, mock_args):
        """大量データ表示のテスト"""
        _, mock_conn = mock_connect

        # 引数設定
        mock_args.kind = "fund"
        mock_args.db = "test.db"
        mock_args.start = "2024-01-01"
        mock_args.end = "2024-12-31"
        mock_args.limit = 20

        # 大量のデータ
        test_data = pd.DataFrame(
            {
                "code": [f"{i:04d}" for i in range(100)],
                "company_name": [f"会社{i}" for i in range(100)],
                "DisclosedAt": ["2024-01-01"] * 100,
                "ROE": [10.0] * 100,
            }
        )

        with patch("db.list_signals.pd.read_sql", return_value=test_data):
            with patch("sys.stdout", new=StringIO()) as fake_out:
                main()
                output = fake_out.getvalue()

        # 全データが表示されることを確認
        assert "0099" in output  # 最後のデータ

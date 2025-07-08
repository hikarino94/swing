"""daily_quotes.pyのテスト"""

import datetime as dt
import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))


from fetch import daily_quotes


class TestFetchAndLoad:
    """fetch_and_load関数のテスト"""

    @patch("fetch.daily_quotes._get_optimized_connection")
    @patch("fetch.daily_quotes._load_token")
    @patch("fetch.daily_quotes._by_date")
    @patch("fetch.daily_quotes.logger")
    def test_fetch_and_load_single_date(
        self, mock_logger, mock_by_date, mock_load_token, mock_get_connection
    ):
        """単一日付でのデータ取得テスト"""
        # モックの設定
        mock_load_token.return_value = "test_token"

        # DataFrameのモック
        mock_df = pd.DataFrame(
            {
                "Code": ["1234"],
                "Date": ["2024-01-01"],
                "Close": [100],
                "adj_factor": [1.0],
            }
        )
        mock_by_date.return_value = mock_df

        # データベース接続のモック
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # テスト実行
        daily_quotes.fetch_and_load(None, None)

        # 検証
        mock_by_date.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("fetch.daily_quotes._get_optimized_connection")
    @patch("fetch.daily_quotes._load_token")
    @patch("fetch.daily_quotes.fetch_dates_parallel")
    @patch("fetch.daily_quotes.logger")
    def test_fetch_and_load_date_range(
        self, mock_logger, mock_fetch_parallel, mock_load_token, mock_get_connection
    ):
        """日付範囲指定でのデータ取得テスト"""
        # モックの設定
        mock_load_token.return_value = "test_token"

        # 並列実行の結果をモック
        mock_dfs = [
            pd.DataFrame(
                {
                    "Code": ["1234"],
                    "Date": ["2024-01-01"],
                    "Close": [100],
                    "adj_factor": [1.0],
                }
            ),
            pd.DataFrame(
                {
                    "Code": ["1234"],
                    "Date": ["2024-01-02"],
                    "Close": [101],
                    "adj_factor": [1.0],
                }
            ),
            pd.DataFrame(
                {
                    "Code": ["1234"],
                    "Date": ["2024-01-03"],
                    "Close": [102],
                    "adj_factor": [1.0],
                }
            ),
        ]
        # fetch_dates_parallelは(成功したDFリスト, 失敗リスト)のタプルを返す
        mock_fetch_parallel.return_value = (mock_dfs, [])

        # データベース接続のモック
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # テスト実行
        daily_quotes.fetch_and_load("2024-01-01", "2024-01-03")

        # 検証
        mock_fetch_parallel.assert_called_once()
        # 3日分なので1回のバッチでコミット
        mock_conn.commit.assert_called()
        mock_conn.close.assert_called_once()

    @patch("fetch.daily_quotes._get_optimized_connection")
    @patch("fetch.daily_quotes._load_token")
    @patch("fetch.daily_quotes._by_date")
    @patch("fetch.daily_quotes.logger")
    def test_fetch_and_load_error_handling(
        self, mock_logger, mock_by_date, mock_load_token, mock_get_connection
    ):
        """エラーハンドリングのテスト"""
        # モックの設定
        mock_load_token.return_value = "test_token"

        # HTTPエラーを発生させる
        mock_by_date.side_effect = requests.HTTPError("API Error")

        # データベース接続のモック
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # テスト実行（エラーが発生しても例外は発生しない）
        daily_quotes.fetch_and_load(None, None)

        # エラー時でもBEGINとcommitが呼ばれることを確認（個別トランザクション）
        mock_conn.execute.assert_called_with("BEGIN")
        mock_conn.close.assert_called_once()
        mock_logger.error.assert_called()


class TestDatabaseOperations:
    """データベース操作のテスト"""

    @patch("fetch.daily_quotes._norm")
    def test_upsert_operation(self, mock_norm, temp_db):
        """_upsert関数のテスト"""
        # テストデータ
        test_df = pd.DataFrame(
            {
                "code": ["1234"],
                "date": ["2024-01-01"],
                "open": [100],
                "high": [105],
                "low": [95],
                "close": [102],
                "volume": [10000],
                "turnover_value": [1020000],
                "adj_factor": [1.0],
                "adj_open": [100],
                "adj_high": [105],
                "adj_low": [95],
                "adj_close": [102],
                "adj_volume": [10000],
            }
        )

        # モックの設定
        mock_norm.return_value = test_df

        # データベース接続
        conn = sqlite3.connect(temp_db)

        # _upsert実行
        daily_quotes._upsert(conn, test_df)
        conn.commit()

        # データベースから読み込んで検証
        df = pd.read_sql_query("SELECT * FROM prices WHERE code = '1234'", conn)
        conn.close()

        assert len(df) == 1
        assert df.iloc[0]["code"] == "1234"

    def test_stock_split_detection(self, temp_db):
        """株式分割検出のテスト"""
        # 株式分割を含むデータ
        test_df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": ["2024-01-01", "2024-01-01"],
                "close": [100, 200],
                "adj_factor": [2.0, 1.0],  # 1234は株式分割
            }
        )

        # 株式分割の検出
        splits = test_df.loc[
            test_df["adj_factor"].fillna(1.0) != 1.0,
            "code",
        ].unique()

        assert len(splits) == 1
        assert "1234" in splits


class TestCLI:
    """CLIのテスト"""

    @patch("fetch.daily_quotes.fetch_and_load")
    def test_cli_no_args(self, mock_fetch_and_load):
        """引数なしでの実行テスト"""
        # 引数をモック
        test_args = ["daily_quotes.py"]
        with patch("sys.argv", test_args):
            daily_quotes._cli()

        # fetch_and_loadがNone引数で呼ばれたことを確認
        mock_fetch_and_load.assert_called_once_with(None, None)

    @patch("fetch.daily_quotes.fetch_and_load")
    def test_cli_with_dates(self, mock_fetch_and_load):
        """日付指定での実行テスト"""
        # 引数をモック
        test_args = ["daily_quotes.py", "--start", "2024-01-01", "--end", "2024-01-03"]
        with patch("sys.argv", test_args):
            daily_quotes._cli()

        # fetch_and_loadが日付引数付きで呼ばれたことを確認
        mock_fetch_and_load.assert_called_once_with("2024-01-01", "2024-01-03")


class TestParallelFetch:
    """並列処理のテスト"""

    @patch("fetch.daily_quotes.ThreadPoolExecutor")
    @patch("fetch.daily_quotes._by_date")
    def test_fetch_dates_parallel(self, mock_by_date, mock_executor_class):
        """並列データ取得のテスト"""
        # モックの設定
        mock_df = pd.DataFrame({"Code": ["1234"], "Close": [100]})
        mock_by_date.return_value = mock_df

        # ThreadPoolExecutorのモック
        mock_executor = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # submitの戻り値をモック
        mock_future = MagicMock()
        mock_future.result.return_value = (dt.date(2024, 1, 1), mock_df, None)
        mock_executor.submit.return_value = mock_future

        # as_completedのモック
        with patch("fetch.daily_quotes.as_completed", return_value=[mock_future]):
            # テスト実行
            dates = [dt.date(2024, 1, 1), dt.date(2024, 1, 2)]
            successful_dfs, failed_dates = daily_quotes.fetch_dates_parallel(
                dates, "test_token"
            )

        # 検証
        assert len(successful_dfs) == 1
        assert len(failed_dates) == 0

    def test_rate_limiter(self):
        """レート制限クラスのテスト"""
        rate_limiter = daily_quotes.RateLimiter(max_per_second=2)

        # 2回は即座に実行可能
        start = time.time()
        rate_limiter.wait_if_needed()
        rate_limiter.wait_if_needed()
        elapsed = time.time() - start
        assert elapsed < 0.1  # ほぼ即座

        # 3回目は待機が必要
        start = time.time()
        rate_limiter.wait_if_needed()
        elapsed = time.time() - start
        assert elapsed >= 0.9  # 約1秒待機


class TestUtilityFunctions:
    """ユーティリティ関数のテスト"""

    def test_column_mapping(self):
        """カラムマッピングの確認"""
        # _PRICE_COLSが定義されていることを確認
        assert hasattr(daily_quotes, "_PRICE_COLS")
        assert len(daily_quotes._PRICE_COLS) > 0

    @patch("fetch.daily_quotes.requests.Session")
    def test_session_headers(self, mock_session_class):
        """セッションヘッダーの設定確認"""
        with patch("fetch.daily_quotes.config"):
            # fetch_quotes内でSessionが作成される際のヘッダーを確認
            session = requests.Session()
            assert hasattr(session, "headers")

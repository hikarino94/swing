"""Tests for fetch/daily_quotes.py"""

import datetime as dt
import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from fetch.daily_quotes import (
    RateLimiter,
    _by_code,
    _by_date,
    _call,
    _daterange,
    _fetch_all,
    _fetch_date_with_limiter,
    _get_optimized_connection,
    _load_token,
    _norm,
    _upsert,
    fetch_dates_parallel,
)


class TestRateLimiter:
    """RateLimiterクラスのテスト"""

    def test_initialization(self):
        """初期化のテスト"""
        limiter = RateLimiter(max_per_second=3)
        assert limiter.max_per_second == 3
        assert len(limiter.last_request_times) == 0

    def test_wait_if_needed_no_wait(self):
        """待機が不要な場合のテスト"""
        limiter = RateLimiter(max_per_second=3)
        start_time = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start_time
        assert elapsed < 0.1  # ほぼ待機しない

    def test_wait_if_needed_with_wait(self):
        """待機が必要な場合のテスト"""
        limiter = RateLimiter(max_per_second=2)

        # 最初のリクエスト
        limiter.wait_if_needed()

        # 2回目のリクエスト（待機が必要）
        start_time = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start_time

        # 待機時間は約0.5秒（2リクエスト/秒の場合）
        assert 0.3 < elapsed < 0.7

    def test_request_times_cleanup(self):
        """古いリクエスト時刻がクリーンアップされることを確認"""
        limiter = RateLimiter(max_per_second=10)

        # 3回リクエスト
        for _ in range(3):
            limiter.wait_if_needed()

        # 時間が経過してから再度リクエスト
        time.sleep(1.2)
        limiter.wait_if_needed()

        # 古い時刻はクリーンアップされている（1秒以上経過した分は削除）
        assert len(limiter.last_request_times) <= 1


class TestLoadToken:
    """トークン読み込み関数のテスト"""

    @patch("fetch.daily_quotes.get_idtoken")
    def test_load_token(self, mock_get_idtoken):
        """トークンが正しく読み込まれることを確認"""
        mock_get_idtoken.return_value = "test_token_12345"
        token = _load_token()
        assert token == "test_token_12345"
        mock_get_idtoken.assert_called_once()


class TestDateRange:
    """日付範囲生成関数のテスト"""

    def test_daterange_single_day(self):
        """1日のみの場合"""
        start = dt.date(2024, 1, 15)  # 月曜日
        end = dt.date(2024, 1, 15)
        result = _daterange(start, end)
        assert len(result) == 1
        assert result[0] == start

    def test_daterange_weekdays_only(self):
        """平日のみ抽出されることを確認"""
        start = dt.date(2024, 1, 13)  # 土曜日
        end = dt.date(2024, 1, 15)  # 月曜日
        result = _daterange(start, end)
        assert len(result) == 1
        assert result[0] == dt.date(2024, 1, 15)

    def test_daterange_full_week(self):
        """1週間の場合"""
        start = dt.date(2024, 1, 15)  # 月曜日
        end = dt.date(2024, 1, 19)  # 金曜日
        result = _daterange(start, end)
        assert len(result) == 5
        assert all(d.weekday() < 5 for d in result)


class TestAPICall:
    """API呼び出し関数のテスト"""

    @patch("fetch.daily_quotes.time.sleep")
    def test_call_success(self, mock_sleep):
        """正常なAPI呼び出し"""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"daily_quotes": [{"Code": "1234"}]}
        mock_session.get.return_value = mock_response

        result = _call(mock_session, {"date": "2024-01-15"}, "test_token")

        assert result == {"daily_quotes": [{"Code": "1234"}]}
        mock_session.get.assert_called_once()
        mock_sleep.assert_called_once_with(0.35)

    @patch("fetch.daily_quotes.time.sleep")
    def test_call_with_retry(self, mock_sleep):
        """リトライが必要な場合"""
        mock_session = MagicMock()

        # 最初は失敗、2回目で成功
        mock_response_fail = MagicMock()
        mock_response_fail.status_code = 429

        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"daily_quotes": []}

        mock_session.get.side_effect = [mock_response_fail, mock_response_success]

        result = _call(mock_session, {}, "test_token")

        assert result == {"daily_quotes": []}
        assert mock_session.get.call_count == 2

    def test_call_max_retries_exceeded(self):
        """最大リトライ回数を超えた場合"""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.HTTPError()
        mock_session.get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            _call(mock_session, {}, "test_token", retries=1)


class TestFetchAll:
    """ページネーション処理のテスト"""

    @patch("fetch.daily_quotes._call")
    def test_fetch_all_single_page(self, mock_call):
        """単一ページの場合"""
        mock_call.return_value = {
            "daily_quotes": [{"Code": "1234", "Date": "2024-01-15", "Close": 1000}]
        }

        session = MagicMock()
        result = _fetch_all(session, {"date": "2024-01-15"}, "test_token")

        assert len(result) == 1
        assert result.iloc[0]["Code"] == "1234"
        mock_call.assert_called_once()

    @patch("fetch.daily_quotes._call")
    def test_fetch_all_multiple_pages(self, mock_call):
        """複数ページの場合"""
        mock_call.side_effect = [
            {
                "daily_quotes": [{"Code": "1234", "Close": 1000}],
                "pagination_key": "next_page_1",
            },
            {
                "daily_quotes": [{"Code": "5678", "Close": 2000}],
                "pagination_key": "next_page_2",
            },
            {"daily_quotes": [{"Code": "9999", "Close": 3000}]},
        ]

        session = MagicMock()
        result = _fetch_all(session, {"date": "2024-01-15"}, "test_token")

        assert len(result) == 3
        assert mock_call.call_count == 3

    @patch("fetch.daily_quotes._call")
    def test_fetch_all_empty_response(self, mock_call):
        """空のレスポンスの場合"""
        mock_call.return_value = {"daily_quotes": []}

        session = MagicMock()
        result = _fetch_all(session, {"date": "2024-01-15"}, "test_token")

        assert len(result) == 0
        assert isinstance(result, pd.DataFrame)


class TestFetchByDateAndCode:
    """日付・銘柄別の取得関数のテスト"""

    @patch("fetch.daily_quotes._fetch_all")
    def test_by_date(self, mock_fetch_all):
        """日付指定での取得"""
        expected_df = pd.DataFrame([{"Code": "1234", "Close": 1000}])
        mock_fetch_all.return_value = expected_df

        session = MagicMock()
        result = _by_date(session, "test_token", dt.date(2024, 1, 15))

        mock_fetch_all.assert_called_once_with(
            session, {"date": "2024-01-15"}, "test_token"
        )
        assert result.equals(expected_df)

    @patch("fetch.daily_quotes._fetch_all")
    def test_by_code(self, mock_fetch_all):
        """銘柄コード指定での取得"""
        expected_df = pd.DataFrame([{"Code": "1234", "Close": 1000}])
        mock_fetch_all.return_value = expected_df

        session = MagicMock()
        result = _by_code(session, "test_token", "1234")

        mock_fetch_all.assert_called_once_with(session, {"code": "1234"}, "test_token")
        assert result.equals(expected_df)


class TestFetchDateWithLimiter:
    """レート制限付き取得関数のテスト"""

    @patch("fetch.daily_quotes._by_date")
    @patch("fetch.daily_quotes.requests.Session")
    def test_fetch_date_with_limiter_success(self, mock_session_class, mock_by_date):
        """正常な取得"""
        mock_df = pd.DataFrame([{"Code": "1234"}])
        mock_by_date.return_value = mock_df

        rate_limiter = RateLimiter()
        date = dt.date(2024, 1, 15)

        result_date, result_df, error = _fetch_date_with_limiter(
            (date, "test_token", rate_limiter)
        )

        assert result_date == date
        assert result_df.equals(mock_df)
        assert error is None

    @patch("fetch.daily_quotes._by_date")
    @patch("fetch.daily_quotes.requests.Session")
    def test_fetch_date_with_limiter_http_error(self, mock_session_class, mock_by_date):
        """HTTPエラーの場合"""
        mock_by_date.side_effect = requests.HTTPError("Server error")

        rate_limiter = RateLimiter()
        date = dt.date(2024, 1, 15)

        result_date, result_df, error = _fetch_date_with_limiter(
            (date, "test_token", rate_limiter)
        )

        assert result_date == date
        assert result_df is None
        assert "Server error" in error


class TestFetchDatesParallel:
    """並列取得関数のテスト"""

    @patch("fetch.daily_quotes._fetch_date_with_limiter")
    def test_fetch_dates_parallel_success(self, mock_fetch):
        """正常な並列取得"""
        dates = [dt.date(2024, 1, 15), dt.date(2024, 1, 16)]
        df1 = pd.DataFrame([{"Code": "1234", "Date": "2024-01-15"}])
        df2 = pd.DataFrame([{"Code": "5678", "Date": "2024-01-16"}])

        mock_fetch.side_effect = [(dates[0], df1, None), (dates[1], df2, None)]

        dfs, errors = fetch_dates_parallel(dates, "test_token", max_workers=2)

        assert len(dfs) == 2
        assert len(errors) == 0

    @patch("fetch.daily_quotes._fetch_date_with_limiter")
    def test_fetch_dates_parallel_with_errors(self, mock_fetch):
        """一部エラーがある場合"""
        dates = [dt.date(2024, 1, 15), dt.date(2024, 1, 16)]
        df1 = pd.DataFrame([{"Code": "1234"}])

        mock_fetch.side_effect = [
            (dates[0], df1, None),
            (dates[1], None, "Error occurred"),
        ]

        dfs, errors = fetch_dates_parallel(dates, "test_token", max_workers=2)

        assert len(dfs) == 1
        assert len(errors) == 1
        assert errors[0] == (dates[1], "Error occurred")


class TestNormalization:
    """データ正規化のテスト"""

    def test_norm_basic(self):
        """基本的なカラム名変換"""
        df = pd.DataFrame(
            {"Code": ["1234"], "Date": ["2024-01-15"], "Open": [1000], "Close": [1010]}
        )

        result = _norm(df)

        assert "code" in result.columns
        assert "date" in result.columns
        assert "open" in result.columns
        assert "close" in result.columns
        assert "Code" not in result.columns

    def test_norm_with_adjustment(self):
        """調整値カラムの変換"""
        df = pd.DataFrame(
            {
                "Code": ["1234"],
                "Date": ["2024-01-15"],
                "AdjustmentOpen": [1000],
                "AdjustmentClose": [1010],
                "AdjustmentVolume": [100000],
            }
        )

        result = _norm(df)

        assert "code" in result.columns
        assert "date" in result.columns
        assert "adj_open" in result.columns
        assert "adj_close" in result.columns
        assert "adj_volume" in result.columns


class TestDatabaseOperations:
    """データベース操作のテスト"""

    @patch("fetch.daily_quotes.get_db_path")
    def test_get_optimized_connection(self, mock_get_db_path):
        """最適化された接続の取得"""
        mock_get_db_path.return_value = ":memory:"

        conn = _get_optimized_connection()

        assert conn is not None
        # テストでは:memory:データベースを使用
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode")
        result = cursor.fetchone()
        # :memory:データベースではmemoryモードになる
        assert result[0] in ["wal", "memory"]
        conn.close()

    @patch("fetch.daily_quotes.sqlite3.connect")
    def test_upsert_success(self, mock_connect):
        """正常なupsert操作"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": ["2024-01-15", "2024-01-15"],
                "close": [1000, 2000],
            }
        )

        _upsert(mock_conn, df)

        # executemanyが呼ばれたことを確認
        assert mock_conn.executemany.called

    def test_upsert_empty_dataframe(self):
        """空のDataFrameの場合"""
        mock_conn = MagicMock()
        df = pd.DataFrame()

        # エラーが発生しないことを確認
        _upsert(mock_conn, df)

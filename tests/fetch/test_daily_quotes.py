"""fetch/daily_quotes.pyのテスト"""

import datetime as dt

# sys.pathの設定をモック
import sys
import time
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))

from fetch.daily_quotes import (
    RateLimiter,
    _by_code,
    _by_date,
    _call,
    _daterange,
    _fetch_all,
    _fetch_date_with_limiter,
    _load_token,
    _norm,
    _upsert,
    fetch_and_load,
    fetch_dates_parallel,
)


class TestRateLimiter:
    """RateLimiterクラスのテスト"""

    def test_init(self):
        """初期化のテスト"""
        limiter = RateLimiter(max_per_second=5)
        assert limiter.max_per_second == 5
        assert hasattr(limiter, "lock")
        assert hasattr(limiter.lock, "acquire")  # Lock-like object
        assert limiter.last_request_times == []

    def test_wait_if_needed_no_wait(self):
        """待機不要な場合のテスト"""
        limiter = RateLimiter(max_per_second=3)
        start_time = time.time()

        # 1回目のリクエスト
        limiter.wait_if_needed()

        # ほぼ待機なしで完了することを確認
        assert time.time() - start_time < 0.1
        assert len(limiter.last_request_times) == 1

    def test_wait_if_needed_with_wait(self):
        """待機が必要な場合のテスト"""
        limiter = RateLimiter(max_per_second=2)

        # レート制限まで到達
        limiter.last_request_times = [time.time(), time.time()]

        start_time = time.time()
        limiter.wait_if_needed()
        wait_time = time.time() - start_time

        # 待機が発生したことを確認（誤差を考慮）
        assert wait_time > 0.3  # CI環境での誤差を考慮してしきい値を下げる

    def test_cleanup_old_timestamps(self):
        """古いタイムスタンプがクリーンアップされることを確認"""
        limiter = RateLimiter(max_per_second=3)

        # 2秒前のタイムスタンプを追加
        old_time = time.time() - 2.0
        limiter.last_request_times = [old_time, time.time()]

        limiter.wait_if_needed()

        # 古いタイムスタンプが削除されていることを確認
        assert len(limiter.last_request_times) == 2  # 新しいものと既存の1つ
        assert all(
            time.time() - t < 1.5 for t in limiter.last_request_times
        )  # CI環境での誤差を考慮


class TestHelpers:
    """ヘルパー関数のテスト"""

    @patch("fetch.daily_quotes.get_idtoken")
    def test_load_token(self, mock_get_idtoken):
        """_load_token関数のテスト"""
        mock_get_idtoken.return_value = "test-jwt-token"

        token = _load_token()

        assert token == "test-jwt-token"
        mock_get_idtoken.assert_called_once()

    def test_daterange_weekdays_only(self):
        """_daterange関数が平日のみを返すことを確認"""
        # 2024年1月1日（月）から1月7日（日）
        start = dt.date(2024, 1, 1)
        end = dt.date(2024, 1, 7)

        dates = _daterange(start, end)

        # 月〜金の5日間のみ
        assert len(dates) == 5
        assert dates[0] == dt.date(2024, 1, 1)  # 月
        assert dates[-1] == dt.date(2024, 1, 5)  # 金

        # 土日が含まれていないことを確認
        assert dt.date(2024, 1, 6) not in dates  # 土
        assert dt.date(2024, 1, 7) not in dates  # 日

    def test_daterange_single_day(self):
        """単一日の場合"""
        date = dt.date(2024, 1, 15)  # 月曜日
        dates = _daterange(date, date)

        assert len(dates) == 1
        assert dates[0] == date

    def test_daterange_weekend_only(self):
        """週末のみの期間"""
        start = dt.date(2024, 1, 6)  # 土
        end = dt.date(2024, 1, 7)  # 日

        dates = _daterange(start, end)

        assert len(dates) == 0


class TestApiCalls:
    """API呼び出し関数のテスト"""

    @patch(
        "fetch.daily_quotes.API_URL",
        "https://api.jpx-jquants.com/v2/prices/daily_quotes",
    )
    @patch("fetch.daily_quotes.time.sleep")
    def test_call_success(self, mock_sleep):
        """_call関数の成功ケース"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_session.get.return_value = mock_response

        result = _call(mock_session, {"param": "value"}, "test-token")

        assert result == {"data": "test"}
        mock_session.get.assert_called_once_with(
            "https://api.jpx-jquants.com/v2/prices/daily_quotes",
            headers={"Authorization": "Bearer test-token"},
            params={"param": "value"},
            timeout=60,
        )
        mock_sleep.assert_called_once_with(0.35)

    @patch("fetch.daily_quotes.time.sleep")
    @patch("fetch.daily_quotes.logger")
    def test_call_with_message(self, mock_logger, mock_sleep):
        """APIメッセージがある場合"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": "test",
            "message": "API notice",
        }
        mock_session.get.return_value = mock_response

        result = _call(mock_session, {}, "token")

        assert result == {"data": "test", "message": "API notice"}
        mock_logger.info.assert_called_once_with("API message: %s", "API notice")

    @patch("fetch.daily_quotes.time.sleep")
    @patch("fetch.daily_quotes.logger")
    def test_call_retry_on_error(self, mock_logger, mock_sleep):
        """エラー時のリトライ"""
        mock_session = Mock()

        # 1回目と2回目は失敗、3回目で成功
        responses = [
            Mock(status_code=503),
            Mock(status_code=429),
            Mock(status_code=200, json=lambda: {"data": "success"}),
        ]
        mock_session.get.side_effect = responses

        result = _call(mock_session, {}, "token", retries=3)

        assert result == {"data": "success"}
        assert mock_session.get.call_count == 3
        assert mock_logger.warning.call_count == 2

    def test_call_max_retries_exceeded(self):
        """最大リトライ回数超過"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.raise_for_status.side_effect = requests.HTTPError()
        mock_session.get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            _call(mock_session, {}, "token", retries=2)

        assert mock_session.get.call_count == 2


class TestFetchAll:
    """_fetch_all関数のテスト"""

    @patch("fetch.daily_quotes._call")
    def test_fetch_all_single_page(self, mock_call):
        """単一ページの取得"""
        mock_call.return_value = {
            "daily_quotes": [
                {"code": "1234", "date": "2024-01-15"},
                {"code": "5678", "date": "2024-01-15"},
            ]
        }

        df = _fetch_all(Mock(), {"date": "2024-01-15"}, "token")

        assert len(df) == 2
        assert df.iloc[0]["code"] == "1234"
        mock_call.assert_called_once()

    @patch("fetch.daily_quotes._call")
    def test_fetch_all_pagination(self, mock_call):
        """ページネーションのテスト"""
        mock_call.side_effect = [
            {
                "daily_quotes": [{"code": "1234"}],
                "pagination_key": "next-page",
            },
            {
                "daily_quotes": [{"code": "5678"}],
                "page_key": "page-2",  # 旧形式のキー
            },
            {
                "daily_quotes": [{"code": "9012"}],
            },
        ]

        df = _fetch_all(Mock(), {"date": "2024-01-15"}, "token")

        assert len(df) == 3
        assert mock_call.call_count == 3

    @patch("fetch.daily_quotes._call")
    @patch("fetch.daily_quotes.logger")
    def test_fetch_all_empty_data(self, mock_logger, mock_call):
        """空データの場合"""
        mock_call.return_value = {"daily_quotes": []}

        df = _fetch_all(Mock(), {"date": "2024-01-15"}, "token")

        assert df.empty
        mock_logger.debug.assert_called_once()

    @patch("fetch.daily_quotes._call")
    def test_fetch_all_duplicate_key_protection(self, mock_call):
        """重複キーからの保護"""
        mock_call.side_effect = [
            {
                "daily_quotes": [{"code": "1234"}],
                "pagination_key": "key1",
            },
            {
                "daily_quotes": [{"code": "5678"}],
                "pagination_key": "key1",  # 重複キー
            },
        ]

        df = _fetch_all(Mock(), {}, "token")

        assert len(df) == 2
        assert mock_call.call_count == 2  # 重複キーで停止


class TestFetchByDateAndCode:
    """日付・コード別取得関数のテスト"""

    @patch("fetch.daily_quotes._fetch_all")
    def test_by_date(self, mock_fetch_all):
        """_by_date関数のテスト"""
        mock_df = pd.DataFrame([{"code": "1234"}])
        mock_fetch_all.return_value = mock_df

        date = dt.date(2024, 1, 15)
        result = _by_date(Mock(), "token", date)

        assert result.equals(mock_df)
        mock_fetch_all.assert_called_once()
        # 日付フォーマットを確認
        call_args = mock_fetch_all.call_args[0][1]
        assert call_args["date"] == "2024-01-15"

    @patch("fetch.daily_quotes._fetch_all")
    def test_by_code(self, mock_fetch_all):
        """_by_code関数のテスト"""
        mock_df = pd.DataFrame([{"date": "2024-01-15"}])
        mock_fetch_all.return_value = mock_df

        result = _by_code(Mock(), "token", "1234")

        assert result.equals(mock_df)
        mock_fetch_all.assert_called_once()
        call_args = mock_fetch_all.call_args[0][1]
        assert call_args["code"] == "1234"


class TestParallelFetch:
    """並列取得関数のテスト"""

    def test_fetch_date_with_limiter_success(self):
        """_fetch_date_with_limiter成功ケース"""
        date = dt.date(2024, 1, 15)
        token = "test-token"
        rate_limiter = Mock()

        mock_df = pd.DataFrame([{"code": "1234"}])

        with patch("fetch.daily_quotes.requests.Session"):
            with patch("fetch.daily_quotes._by_date", return_value=mock_df):
                result_date, df, error = _fetch_date_with_limiter(
                    (date, token, rate_limiter)
                )

        assert result_date == date
        assert df.equals(mock_df)
        assert error is None
        rate_limiter.wait_if_needed.assert_called_once()

    def test_fetch_date_with_limiter_http_error(self):
        """HTTP エラーの場合"""
        date = dt.date(2024, 1, 15)
        token = "test-token"
        rate_limiter = Mock()

        with patch("fetch.daily_quotes.requests.Session"):
            with patch(
                "fetch.daily_quotes._by_date",
                side_effect=requests.HTTPError("404 Not Found"),
            ):
                result_date, df, error = _fetch_date_with_limiter(
                    (date, token, rate_limiter)
                )

        assert result_date == date
        assert df is None
        assert "404 Not Found" in error

    def test_fetch_date_with_limiter_unexpected_error(self):
        """予期しないエラーの場合"""
        date = dt.date(2024, 1, 15)
        token = "test-token"
        rate_limiter = Mock()

        with patch("fetch.daily_quotes.requests.Session"):
            with patch(
                "fetch.daily_quotes._by_date",
                side_effect=ValueError("Unexpected"),
            ):
                result_date, df, error = _fetch_date_with_limiter(
                    (date, token, rate_limiter)
                )

        assert result_date == date
        assert df is None
        assert "Unexpected error: Unexpected" in error

    @patch("fetch.daily_quotes.ThreadPoolExecutor")
    @patch("fetch.daily_quotes.logger")
    def test_fetch_dates_parallel(self, mock_logger, mock_executor_class):
        """fetch_dates_parallel関数のテスト"""
        dates = [dt.date(2024, 1, 15), dt.date(2024, 1, 16)]

        # ThreadPoolExecutorのモック設定
        mock_executor = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor

        # Futureのモック
        future1 = Mock(spec=Future)
        future1.result.return_value = (
            dates[0],
            pd.DataFrame([{"code": "1234"}]),
            None,
        )

        future2 = Mock(spec=Future)
        future2.result.return_value = (dates[1], None, "Error occurred")

        # submitの戻り値を設定
        mock_executor.submit.side_effect = [future1, future2]

        # as_completedのモック
        with patch("fetch.daily_quotes.as_completed", return_value=[future1, future2]):
            dfs, failed = fetch_dates_parallel(dates, "token", max_workers=2)

        assert len(dfs) == 1
        assert len(failed) == 1
        assert failed[0] == (dates[1], "Error occurred")

        mock_logger.info.assert_called()
        mock_logger.error.assert_called()


class TestNormFunction:
    """_norm関数のテスト"""

    def test_norm_empty_dataframe(self):
        """空のDataFrameの場合"""
        df = pd.DataFrame()
        result = _norm(df)
        assert result.empty

    def test_norm_rename_columns(self):
        """カラム名の変換"""
        df = pd.DataFrame(
            {
                "Code": ["1234"],
                "Date": ["2024-01-15"],
                "Open": [100.0],
                "UpperLimit": [110.0],
                "LowerLimit": [90.0],
                "TurnoverValue": [1000000],
                "AdjustmentFactor": [1.0],
                "AdjustmentOpen": [100.0],
            }
        )

        result = _norm(df)

        # カラム名が変換されていることを確認
        assert "code" in result.columns
        assert "date" in result.columns
        assert "open" in result.columns
        assert "upper_limit" in result.columns
        assert "lower_limit" in result.columns
        assert "turnover_value" in result.columns
        assert "adj_factor" in result.columns
        assert "adj_open" in result.columns

    def test_norm_fill_missing_values(self):
        """欠損値の処理"""
        df = pd.DataFrame(
            {
                "Code": ["1234"],
                "Date": ["2024-01-15"],
                "Open": [None],  # 欠損値
                "Close": [100.0],
            }
        )

        result = _norm(df)

        # 欠損値がNaNとして処理されることを確認
        assert pd.isna(result.iloc[0]["open"])

    def test_norm_column_selection(self):
        """必要なカラムのみ選択"""
        df = pd.DataFrame(
            {
                "Code": ["1234"],
                "Date": ["2024-01-15"],
                "Open": [100.0],
                "Close": [101.0],
                "ExtraColumn": ["extra"],  # 不要なカラム
            }
        )

        # _PRICE_COLSのモック
        with patch("fetch.daily_quotes._PRICE_COLS", ["code", "date", "open", "close"]):
            result = _norm(df)

        assert "ExtraColumn" not in result.columns
        assert len(result.columns) == 4


class TestUpsertFunction:
    """_upsert関数のテスト"""

    def test_upsert_success(self):
        """正常なupsert"""
        mock_conn = MagicMock()

        df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": ["2024-01-15", "2024-01-15"],
                "close": [100.0, 200.0],
            }
        )

        _upsert(mock_conn, df)

        # executemanyが呼ばれたことを確認
        mock_conn.executemany.assert_called_once()
        sql = mock_conn.executemany.call_args[0][0]
        assert "INSERT OR REPLACE INTO prices" in sql
        assert "code" in sql
        assert "date" in sql
        assert "close" in sql

    def test_upsert_empty_dataframe(self):
        """空のDataFrameの場合"""
        mock_conn = MagicMock()
        df = pd.DataFrame()

        _upsert(mock_conn, df)

        # 何も実行されないことを確認
        mock_conn.executemany.assert_not_called()

    def test_upsert_with_all_columns(self):
        """全カラムでのupsert"""
        mock_conn = MagicMock()

        # _PRICE_COLSの一部をモック
        df = pd.DataFrame(
            {
                "code": ["1234"],
                "date": ["2024-01-15"],
                "open": [100.0],
                "high": [105.0],
                "low": [95.0],
                "close": [102.0],
                "volume": [10000],
            }
        )

        _upsert(mock_conn, df)

        sql = mock_conn.executemany.call_args[0][0]
        assert "code, date, open, high, low, close, volume" in sql


class TestFetchAndLoad:
    """fetch_and_load関数のテスト"""

    @patch("fetch.daily_quotes._load_token")
    @patch("fetch.daily_quotes._get_optimized_connection")
    @patch("fetch.daily_quotes.fetch_dates_parallel")
    @patch("fetch.daily_quotes._upsert")
    @patch("fetch.daily_quotes.logger")
    def test_fetch_and_load_with_dates(
        self,
        mock_logger,
        mock_upsert,
        mock_fetch_parallel,
        mock_get_conn,
        mock_load_token,
    ):
        """日付範囲指定での取得"""
        mock_load_token.return_value = "test-token"
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        # 並列取得の結果
        mock_dfs = [pd.DataFrame([{"code": "1234", "date": "2024-01-15"}])]
        mock_fetch_parallel.return_value = (mock_dfs, [])

        fetch_and_load("2024-01-15", "2024-01-16")

        mock_fetch_parallel.assert_called()
        mock_upsert.assert_called()
        mock_conn.commit.assert_called()
        mock_conn.close.assert_called()

    @patch("fetch.daily_quotes._load_token")
    @patch("fetch.daily_quotes._get_optimized_connection")
    @patch("fetch.daily_quotes._by_date")
    @patch("fetch.daily_quotes._upsert")
    def test_fetch_and_load_today_only(
        self, mock_upsert, mock_by_date, mock_get_conn, mock_load_token
    ):
        """今日のみの取得（日付指定なし）"""
        mock_load_token.return_value = "test-token"
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        mock_df = pd.DataFrame(
            [{"code": "1234", "date": "2024-01-15", "adj_factor": 1.0}]
        )
        mock_by_date.return_value = mock_df

        fetch_and_load(None, None)

        mock_by_date.assert_called_once()
        mock_upsert.assert_called_once()
        mock_conn.commit.assert_called()
        mock_conn.close.assert_called()


class TestGetOptimizedConnection:
    """_get_optimized_connection関数のテスト"""

    @patch("fetch.daily_quotes.sqlite3.connect")
    @patch("fetch.daily_quotes.get_db_path")
    def test_get_optimized_connection(self, mock_get_db_path, mock_connect):
        """最適化された接続の取得"""
        mock_get_db_path.return_value = "/path/to/db"
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        from fetch.daily_quotes import _get_optimized_connection

        conn = _get_optimized_connection()

        assert conn == mock_conn
        mock_connect.assert_called_once_with("/path/to/db")

        # PRAGMA設定が実行されることを確認
        pragma_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
        assert "PRAGMA cache_size = -64000" in pragma_calls
        assert "PRAGMA temp_store = MEMORY" in pragma_calls
        assert "PRAGMA mmap_size = 268435456" in pragma_calls

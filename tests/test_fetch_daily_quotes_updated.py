"""Test suite for fetch/daily_quotes.py"""

import sys
from datetime import date
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

# モジュールをインポート
sys.path.insert(0, str(Path(__file__).parent.parent))
from fetch.daily_quotes import (
    _by_date,
    _call,
    _daterange,
    _fetch_all,
    _load_token,
    _norm,
    _upsert,
    fetch_and_load,
    fetch_dates_parallel,
)


class TestHelperFunctions:
    """Test helper functions"""

    def test_daterange(self):
        """日付範囲生成のテスト"""
        start = date(2024, 1, 1)
        end = date(2024, 1, 5)

        dates = _daterange(start, end)

        assert len(dates) == 5
        assert dates[0] == date(2024, 1, 1)
        assert dates[4] == date(2024, 1, 5)

    def test_daterange_single_day(self):
        """単一日の日付範囲"""
        start = end = date(2024, 1, 1)

        dates = _daterange(start, end)

        assert len(dates) == 1
        assert dates[0] == date(2024, 1, 1)

    @mock.patch("fetch.daily_quotes.config")
    def test_load_token_success(self, mock_config):
        """トークン読み込み成功"""
        # モックファイルオブジェクトを作成
        mock_file = mock.MagicMock()
        mock_file.read.return_value = '{"idToken": "test_token_123"}'
        mock_file.__enter__.return_value = mock_file

        # get_file_pathのモックを設定
        mock_path = mock.MagicMock()
        mock_path.open.return_value = mock_file
        mock_config.get_file_path.return_value = mock_path

        # json.loadが正しく動作するようにモックを設定
        with mock.patch("json.load", return_value={"idToken": "test_token_123"}):
            token = _load_token()

        assert token == "test_token_123"

    @mock.patch("fetch.daily_quotes.config")
    def test_load_token_failure(self, mock_config):
        """トークン読み込み失敗（idTokenがない場合）"""
        # モックファイルオブジェクトを作成
        mock_file = mock.MagicMock()
        mock_file.__enter__.return_value = mock_file

        # get_file_pathのモックを設定
        mock_path = mock.MagicMock()
        mock_path.open.return_value = mock_file
        mock_config.get_file_path.return_value = mock_path

        # json.loadが空の辞書を返すようにモック
        with mock.patch("json.load", return_value={}):
            with pytest.raises(RuntimeError, match="idToken not found"):
                _load_token()


class TestAPICall:
    """Test API call function"""

    @mock.patch("time.sleep")
    def test_call_success(self, mock_sleep):
        """API呼び出し成功"""
        mock_session = mock.MagicMock()
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "daily_quotes": [{"Code": "1234", "Close": 1000}],
            "pagination_key": "next_key",
        }
        mock_session.post.return_value = mock_response

        result = _call(mock_session, {"date": "2024-01-01"}, "test_token")

        assert "daily_quotes" in result
        assert result["pagination_key"] == "next_key"

    @mock.patch("time.sleep")
    def test_call_retry_on_429(self, mock_sleep):
        """429エラーでリトライ"""
        mock_session = mock.MagicMock()

        # 最初は429、次に成功
        mock_response_429 = mock.MagicMock()
        mock_response_429.status_code = 429

        mock_response_success = mock.MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"daily_quotes": []}

        mock_session.post.side_effect = [mock_response_429, mock_response_success]

        result = _call(mock_session, {"date": "2024-01-01"}, "test_token", retries=2)

        assert result == {"daily_quotes": []}
        assert mock_sleep.call_count >= 1  # リトライ時のsleep

    def test_call_max_retries_exceeded(self):
        """最大リトライ回数超過"""
        mock_session = mock.MagicMock()
        mock_response = mock.MagicMock()
        mock_response.status_code = 500
        mock_session.post.return_value = mock_response

        with pytest.raises(Exception) as exc_info:  # APIError is not imported
            _call(mock_session, {"date": "2024-01-01"}, "test_token", retries=1)
        assert exc_info.type is Exception


class TestFetchAll:
    """Test _fetch_all function"""

    @mock.patch("fetch.daily_quotes._call")
    def test_fetch_all_single_page(self, mock_call):
        """単一ページの取得"""
        mock_call.return_value = {
            "daily_quotes": [
                {"Code": "1234", "Date": "2024-01-01", "Close": 1000},
                {"Code": "5678", "Date": "2024-01-01", "Close": 2000},
            ],
            "pagination_key": None,  # 最終ページ
        }

        session = mock.MagicMock()
        df = _fetch_all(session, {"date": "2024-01-01"}, "test_token")

        assert len(df) == 2
        assert "Code" in df.columns
        assert df.iloc[0]["Code"] == "1234"

    @mock.patch("fetch.daily_quotes._call")
    @mock.patch("time.sleep")
    def test_fetch_all_multiple_pages(self, mock_sleep, mock_call):
        """複数ページの取得"""
        mock_call.side_effect = [
            {
                "daily_quotes": [{"Code": "1234", "Close": 1000}],
                "pagination_key": "key2",
            },
            {"daily_quotes": [{"Code": "5678", "Close": 2000}], "pagination_key": None},
        ]

        session = mock.MagicMock()
        df = _fetch_all(session, {"date": "2024-01-01"}, "test_token")

        assert len(df) == 2
        assert mock_call.call_count == 2

    @mock.patch("fetch.daily_quotes._call")
    def test_fetch_all_empty_response(self, mock_call):
        """空のレスポンス処理"""
        mock_call.return_value = {"daily_quotes": [], "pagination_key": None}

        session = mock.MagicMock()
        df = _fetch_all(session, {"date": "2024-01-01"}, "test_token")

        assert df.empty


class TestFetchByDate:
    """Test _by_date function"""

    @mock.patch("fetch.daily_quotes._fetch_all")
    def test_by_date(self, mock_fetch_all):
        """日付指定での取得"""
        mock_fetch_all.return_value = pd.DataFrame(
            {"Code": ["1234"], "Date": ["2024-01-01"], "Close": [1000]}
        )

        session = mock.MagicMock()
        df = _by_date(session, "test_token", date(2024, 1, 1))

        assert len(df) == 1
        mock_fetch_all.assert_called_once()
        # 日付がYYYYMMDD形式で渡されることを確認
        call_args = mock_fetch_all.call_args[0][1]
        assert call_args["date"] == "20240101"


class TestNormFunction:
    """Test _norm function"""

    def test_norm_basic(self):
        """基本的な正規化処理"""
        df = pd.DataFrame(
            {
                "Code": ["1234", "5678"],
                "Date": ["2024-01-01", "2024-01-02"],
                "Close": [1000, 2000],
                "Open": [990, 1990],
                "High": [1010, 2010],
                "Low": [980, 1980],
                "Volume": [100000, 200000],
                "TurnoverValue": [99000000, 398000000],
                "AdjustmentFactor": [1.0, 1.0],
                "AdjustmentOpen": [990, 1990],
                "AdjustmentHigh": [1010, 2010],
                "AdjustmentLow": [980, 1980],
                "AdjustmentClose": [1000, 2000],
                "AdjustmentVolume": [100000, 200000],
            }
        )

        result = _norm(df)

        # カラム名が変換されていることを確認
        assert "code" in result.columns
        assert "date" in result.columns
        assert "adj_close" in result.columns
        assert "adj_volume" in result.columns

        # 元のカラムが削除されていることを確認
        assert "Code" not in result.columns
        assert "AdjustmentClose" not in result.columns

    def test_norm_empty_dataframe(self):
        """空のDataFrameの処理"""
        df = pd.DataFrame()
        result = _norm(df)
        assert result.empty


class TestDatabaseOperations:
    """Test database operations"""

    @mock.patch("fetch.daily_quotes._get_optimized_connection")
    def test_upsert_operation(self, mock_get_conn):
        """UPSERT操作のテスト"""
        # モックカーソルとコネクション
        mock_cursor = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        # テストデータ
        df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": ["2024-01-01", "2024-01-01"],
                "open": [990, 1990],
                "high": [1010, 2010],
                "low": [980, 1980],
                "close": [1000, 2000],
                "volume": [100000, 200000],
                "turnover_value": [99000000, 398000000],
                "adj_factor": [1.0, 1.0],
                "adj_open": [990, 1990],
                "adj_high": [1010, 2010],
                "adj_low": [980, 1980],
                "adj_close": [1000, 2000],
                "adj_volume": [100000, 200000],
            }
        )

        _upsert(mock_conn, df)

        # executemanyが呼ばれたことを確認
        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()


class TestFetchDatesParallel:
    """Test parallel fetching"""

    @mock.patch("fetch.daily_quotes.ThreadPoolExecutor")
    @mock.patch("fetch.daily_quotes._by_date")
    def test_fetch_dates_parallel(self, mock_by_date, mock_executor_class):
        """並列日付取得のテスト"""
        # モックデータ
        mock_by_date.side_effect = [
            pd.DataFrame({"code": ["1234"], "date": ["2024-01-01"], "close": [1000]}),
            pd.DataFrame({"code": ["5678"], "date": ["2024-01-02"], "close": [2000]}),
        ]

        # ThreadPoolExecutorのモック
        mock_executor = mock.MagicMock()
        mock_future1 = mock.MagicMock()
        mock_future1.result.return_value = mock_by_date.side_effect[0]
        mock_future2 = mock.MagicMock()
        mock_future2.result.return_value = mock_by_date.side_effect[1]

        mock_executor.submit.side_effect = [mock_future1, mock_future2]
        mock_executor.__enter__.return_value = mock_executor
        mock_executor_class.return_value = mock_executor

        # as_completedのモック
        with mock.patch(
            "fetch.daily_quotes.as_completed", return_value=[mock_future1, mock_future2]
        ):
            session = mock.MagicMock()
            dates = [date(2024, 1, 1), date(2024, 1, 2)]

            dfs = fetch_dates_parallel(session, "test_token", dates, max_workers=2)

            assert len(dfs) == 2


class TestFetchAndLoad:
    """Test main fetch_and_load function"""

    @mock.patch("fetch.daily_quotes._load_token")
    @mock.patch("fetch.daily_quotes.fetch_dates_parallel")
    @mock.patch("fetch.daily_quotes._upsert")
    @mock.patch("fetch.daily_quotes._get_optimized_connection")
    def test_fetch_and_load_default_dates(
        self, mock_get_conn, mock_upsert, mock_fetch, mock_load_token
    ):
        """デフォルト日付でのfetch_and_load"""
        mock_load_token.return_value = "test_token"
        mock_conn = mock.MagicMock()
        mock_get_conn.return_value = mock_conn

        # 昨日のデータを返す
        mock_fetch.return_value = [
            pd.DataFrame({"code": ["1234"], "date": ["2024-01-01"], "close": [1000]})
        ]

        fetch_and_load(None, None)

        # トークンが読み込まれたことを確認
        mock_load_token.assert_called_once()
        # データが保存されたことを確認
        mock_upsert.assert_called_once()

    @mock.patch("fetch.daily_quotes._load_token")
    @mock.patch("fetch.daily_quotes.fetch_dates_parallel")
    @mock.patch("fetch.daily_quotes._upsert")
    @mock.patch("fetch.daily_quotes._get_optimized_connection")
    def test_fetch_and_load_with_date_range(
        self, mock_get_conn, mock_upsert, mock_fetch, mock_load_token
    ):
        """日付範囲指定でのfetch_and_load"""
        mock_load_token.return_value = "test_token"
        mock_conn = mock.MagicMock()
        mock_get_conn.return_value = mock_conn

        mock_fetch.return_value = [
            pd.DataFrame({"code": ["1234"], "date": ["2024-01-01"], "close": [1000]}),
            pd.DataFrame({"code": ["5678"], "date": ["2024-01-02"], "close": [2000]}),
        ]

        fetch_and_load("2024-01-01", "2024-01-02")

        # 正しい日付範囲で呼ばれたことを確認
        call_args = mock_fetch.call_args[0][2]  # dates引数
        assert len(call_args) == 2
        assert call_args[0] == date(2024, 1, 1)
        assert call_args[1] == date(2024, 1, 2)

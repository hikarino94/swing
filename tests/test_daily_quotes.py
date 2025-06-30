#!/usr/bin/env python
"""
日次株価データ取得モジュール (fetch/daily_quotes.py) のテスト

テスト対象:
- API呼び出しのモック
- ページネーション処理
- レート制限の遵守
- エラーハンドリング
- データ正規化
- SQLiteへのアップサート
"""

from __future__ import annotations

import datetime as dt
import sqlite3
import sys
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from fetch import daily_quotes


class TestHelpers:
    """ヘルパー関数のテスト"""

    def test_load_token_success(self, tmp_path):
        """正常なトークン読み込みのテスト"""
        token_file = tmp_path / "idtoken.json"
        token_file.write_text('{"idToken": "test_token_123"}')

        with mock.patch("config.config.get_file_path") as mock_path:
            mock_path.return_value = token_file
            token = daily_quotes._load_token()
            assert token == "test_token_123"

    def test_load_token_missing(self, tmp_path):
        """トークンが存在しない場合のテスト"""
        token_file = tmp_path / "idtoken.json"
        token_file.write_text("{}")

        with mock.patch("config.config.get_file_path") as mock_path:
            mock_path.return_value = token_file
            with pytest.raises(RuntimeError, match="idToken not found"):
                daily_quotes._load_token()

    def test_daterange(self):
        """日付範囲生成のテスト（週末を除く）"""
        start = dt.date(2024, 1, 1)  # 月曜日
        end = dt.date(2024, 1, 7)  # 日曜日

        dates = daily_quotes._daterange(start, end)
        # 月〜金の5日間のみ
        assert len(dates) == 5
        assert dates[0] == dt.date(2024, 1, 1)
        assert dates[-1] == dt.date(2024, 1, 5)


class TestAPI:
    """API呼び出し関連のテスト"""

    @mock.patch("time.sleep")
    def test_call_success(self, mock_sleep):
        """正常なAPI呼び出しのテスト"""
        mock_session = mock.Mock()
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"daily_quotes": [{"Code": "1234"}]}
        mock_session.get.return_value = mock_response

        result = daily_quotes._call(mock_session, {"date": "2024-01-01"}, "test_token")

        assert result == {"daily_quotes": [{"Code": "1234"}]}
        mock_session.get.assert_called_once()
        mock_sleep.assert_called_once_with(daily_quotes.RATE_SLEEP)

    @mock.patch("time.sleep")
    def test_call_with_message(self, mock_sleep):
        """APIメッセージ付きレスポンスのテスト"""
        mock_session = mock.Mock()
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "データ取得成功", "daily_quotes": []}
        mock_session.get.return_value = mock_response

        with mock.patch("fetch.daily_quotes.logger") as mock_logger:
            result = daily_quotes._call(
                mock_session, {"date": "2024-01-01"}, "test_token"
            )

            assert result["message"] == "データ取得成功"
            mock_logger.info.assert_called_with("API message: %s", "データ取得成功")

    @mock.patch("time.sleep")
    def test_call_retry_on_429(self, mock_sleep):
        """レート制限エラー時のリトライテスト"""
        mock_session = mock.Mock()

        # 最初は429、次は成功
        mock_response_429 = mock.Mock()
        mock_response_429.status_code = 429

        mock_response_ok = mock.Mock()
        mock_response_ok.status_code = 200
        mock_response_ok.json.return_value = {"daily_quotes": []}

        mock_session.get.side_effect = [mock_response_429, mock_response_ok]

        result = daily_quotes._call(
            mock_session, {"date": "2024-01-01"}, "test_token", retries=2
        )

        assert result == {"daily_quotes": []}
        assert mock_session.get.call_count == 2
        # レート制限スリープ + リトライ待機
        assert mock_sleep.call_count == 2

    @mock.patch("time.sleep")
    def test_fetch_all_pagination(self, mock_sleep):
        """ページネーション処理のテスト"""
        mock_session = mock.Mock()
        mock_token = "test_token"

        # 3ページ分のレスポンスをモック
        responses = [
            {
                "daily_quotes": [{"Code": "1234", "Date": "2024-01-01"}],
                "pagination_key": "key1",
            },
            {
                "daily_quotes": [{"Code": "5678", "Date": "2024-01-01"}],
                "pagination_key": "key2",
            },
            {
                "daily_quotes": [{"Code": "9012", "Date": "2024-01-01"}],
                # 最後のページにはpagination_keyなし
            },
        ]

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.side_effect = responses

        with mock.patch.object(daily_quotes, "_call", side_effect=responses):
            result = daily_quotes._fetch_all(
                mock_session, {"date": "2024-01-01"}, mock_token
            )

        assert len(result) == 3
        assert result["Code"].tolist() == ["1234", "5678", "9012"]

    @mock.patch("time.sleep")
    def test_fetch_all_empty_page(self, mock_sleep):
        """空ページでの適切な終了テスト"""
        mock_session = mock.Mock()
        mock_token = "test_token"

        # 2ページ目が空
        responses = [
            {
                "daily_quotes": [{"Code": "1234", "Date": "2024-01-01"}],
                "pagination_key": "key1",
            },
            {"daily_quotes": [], "pagination_key": "key2"},  # 空のデータ  # キーはあるが無視すべき
        ]

        with mock.patch.object(daily_quotes, "_call", side_effect=responses):
            result = daily_quotes._fetch_all(
                mock_session, {"date": "2024-01-01"}, mock_token
            )

        assert len(result) == 1
        assert result["Code"].tolist() == ["1234"]


class TestDataProcessing:
    """データ処理関連のテスト"""

    def test_norm_basic(self):
        """基本的なデータ正規化のテスト"""
        df = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-01",
                    "Open": "1000",
                    "High": "1100",
                    "Low": "990",
                    "Close": "1050",
                    "Volume": "100000",
                    "TurnoverValue": "105000000",
                    "AdjustmentFactor": "1.0",
                    "AdjustmentClose": "1050",
                }
            ]
        )

        result = daily_quotes._norm(df)

        # カラム名の変換確認
        assert "code" in result.columns
        assert "date" in result.columns
        assert "adj_close" in result.columns

        # 型変換の確認
        assert pd.api.types.is_numeric_dtype(result["open"])
        assert pd.api.types.is_numeric_dtype(result["volume"])

        # 日付フォーマットの確認
        assert result["date"].iloc[0] == "2024-01-01"

    def test_norm_with_splits(self):
        """株式分割データの正規化テスト"""
        df = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-01",
                    "Close": "1000",
                    "AdjustmentFactor": "2.0",  # 株式分割
                    "AdjustmentClose": "500",
                }
            ]
        )

        result = daily_quotes._norm(df)

        assert result["adj_factor"].iloc[0] == 2.0
        assert result["adj_close"].iloc[0] == 500

    def test_norm_empty_dataframe(self):
        """空のDataFrameの処理テスト"""
        df = pd.DataFrame()
        result = daily_quotes._norm(df)
        assert result.empty


class TestDatabase:
    """データベース操作のテスト"""

    def test_upsert_new_records(self, test_db):
        """新規レコードの挿入テスト"""
        conn = sqlite3.connect(test_db)

        df = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "date": "2024-01-01",
                    "open": 1000,
                    "high": 1100,
                    "low": 990,
                    "close": 1050,
                    "volume": 100000,
                }
            ]
        )

        daily_quotes._upsert(conn, df)
        conn.commit()

        # 挿入確認
        cursor = conn.execute("SELECT * FROM prices WHERE code = '1234'")
        rows = cursor.fetchall()
        assert len(rows) == 1

        conn.close()

    def test_upsert_update_existing(self, test_db):
        """既存レコードの更新テスト"""
        conn = sqlite3.connect(test_db)

        # 初回挿入
        df1 = pd.DataFrame(
            [{"code": "1234", "date": "2024-01-01", "close": 1000, "volume": 100000}]
        )
        daily_quotes._upsert(conn, df1)
        conn.commit()

        # 更新
        df2 = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "date": "2024-01-01",
                    "close": 1050,  # 変更
                    "volume": 200000,  # 変更
                }
            ]
        )
        daily_quotes._upsert(conn, df2)
        conn.commit()

        # 更新確認
        cursor = conn.execute("SELECT close, volume FROM prices WHERE code = '1234'")
        row = cursor.fetchone()
        assert row[0] == 1050  # close
        assert row[1] == 200000  # volume

        conn.close()


class TestIntegration:
    """統合テスト"""

    @mock.patch("fetch.daily_quotes._load_token")
    @mock.patch("fetch.daily_quotes._by_date")
    @mock.patch("fetch.daily_quotes._by_code")
    @mock.patch("time.sleep")
    def test_fetch_and_load_today(
        self, mock_sleep, mock_by_code, mock_by_date, mock_load_token, test_db
    ):
        """本日データ取得の統合テスト"""
        mock_load_token.return_value = "test_token"

        # 通常の銘柄と株式分割銘柄
        mock_by_date.return_value = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-01",
                    "Close": "1000",
                    "AdjustmentFactor": "1.0",
                },
                {
                    "Code": "5678",
                    "Date": "2024-01-01",
                    "Close": "2000",
                    "AdjustmentFactor": "2.0",  # 株式分割
                },
            ]
        )

        # 株式分割銘柄の全履歴
        mock_by_code.return_value = pd.DataFrame(
            [
                {
                    "Code": "5678",
                    "Date": "2023-12-01",
                    "Close": "4000",
                    "AdjustmentFactor": "1.0",
                }
            ]
        )

        with mock.patch("fetch.daily_quotes.DB_PATH", test_db):
            daily_quotes.fetch_and_load(None, None)

        # 株式分割銘柄の全履歴が取得されたか確認
        mock_by_code.assert_called_once()

        # DB確認
        conn = sqlite3.connect(test_db)
        cursor = conn.execute("SELECT COUNT(*) FROM prices")
        count = cursor.fetchone()[0]
        assert count > 0
        conn.close()

    @mock.patch("fetch.daily_quotes._load_token")
    @mock.patch("fetch.daily_quotes._by_date")
    @mock.patch("time.sleep")
    def test_fetch_and_load_date_range(
        self, mock_sleep, mock_by_date, mock_load_token, test_db
    ):
        """期間指定データ取得の統合テスト"""
        mock_load_token.return_value = "test_token"

        # 平日のみデータを返す
        def mock_data(sess, tok, date):
            if date.weekday() < 5:  # 平日
                return pd.DataFrame(
                    [
                        {
                            "Code": "1234",
                            "Date": date.strftime("%Y-%m-%d"),
                            "Close": "1000",
                            "AdjustmentFactor": "1.0",
                        }
                    ]
                )
            return pd.DataFrame()  # 週末は空

        mock_by_date.side_effect = mock_data

        with mock.patch("fetch.daily_quotes.DB_PATH", test_db):
            daily_quotes.fetch_and_load("2024-01-01", "2024-01-07")

        # 平日5日分のAPI呼び出しを確認
        assert mock_by_date.call_count == 5

#!/usr/bin/env python
"""
財務諸表データ取得モジュール (fetch/statements.py) のテスト

テスト対象:
- API呼び出し（コード別・日付別）
- ページネーション処理
- 並行処理（複数銘柄の同時取得）
- データ正規化
- SQLiteへのアップサート
- モード別処理（モード1：銘柄別、モード2：日付別）
"""

from __future__ import annotations

import datetime as dt
import sqlite3
import sys
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
import requests

sys.path.append(str(Path(__file__).resolve().parents[1]))
from fetch import statements


class TestHelpers:
    """ヘルパー関数のテスト"""

    def test_load_token_success(self, tmp_path):
        """正常なトークン読み込みのテスト"""
        token_file = tmp_path / "idtoken.json"
        token_file.write_text('{"idToken": "test_token_789"}')

        with mock.patch("src.config.config.get_file_path") as mock_path:
            mock_path.return_value = token_file
            token = statements._load_token()
            assert token == "test_token_789"

    def test_load_token_missing(self, tmp_path):
        """トークンが存在しない場合のテスト"""
        token_file = tmp_path / "idtoken.json"
        token_file.write_text("{}")

        with mock.patch("src.config.config.get_file_path") as mock_path:
            mock_path.return_value = token_file
            with pytest.raises(RuntimeError, match="idToken not found"):
                statements._load_token()

    def test_daterange(self):
        """日付範囲生成のテスト"""
        start = dt.date(2024, 1, 1)
        end = dt.date(2024, 1, 5)

        dates = statements._daterange(start, end)
        assert len(dates) == 5
        assert dates[0] == dt.date(2024, 1, 1)
        assert dates[-1] == dt.date(2024, 1, 5)


class TestAPI:
    """API呼び出し関連のテスト"""

    def test_fetch_statements_by_code_success(self):
        """コード別取得の正常系テスト"""
        mock_session = mock.Mock()

        # ページネーションを含むレスポンス
        responses = [
            {
                "statements": [
                    {
                        "LocalCode": "1234",
                        "DisclosureNumber": "12345678901234",
                        "NetSales": 1000000,
                    }
                ],
                "pagination_key": "key1",
            },
            {
                "statements": [
                    {
                        "LocalCode": "1234",
                        "DisclosureNumber": "12345678901235",
                        "NetSales": 2000000,
                    }
                ],
                # 最後のページにはpagination_keyなし
            },
        ]

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.side_effect = responses

        result = statements._fetch_statements_by_code(
            mock_session, "test_token", "1234"
        )

        assert len(result) == 2
        assert result[0]["NetSales"] == 1000000
        assert result[1]["NetSales"] == 2000000
        assert mock_session.get.call_count == 2

    def test_fetch_statements_by_code_error(self):
        """コード別取得のエラー処理テスト"""
        mock_session = mock.Mock()
        mock_session.get.return_value.status_code = 401
        mock_session.get.return_value.text = "Unauthorized"
        mock_session.get.return_value.raise_for_status.side_effect = (
            requests.HTTPError()
        )

        with pytest.raises(requests.HTTPError):
            statements._fetch_statements_by_code(mock_session, "invalid_token", "1234")

    def test_fetch_statements_by_date_success(self):
        """日付別取得の正常系テスト"""
        mock_session = mock.Mock()

        response = {
            "statements": [
                {"LocalCode": "1234", "DisclosedDate": "2024-01-01"},
                {"LocalCode": "5678", "DisclosedDate": "2024-01-01"},
            ]
        }

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = response

        result = statements._fetch_statements_by_date(
            mock_session, "test_token", "2024-01-01"
        )

        assert len(result) == 2
        assert result[0]["LocalCode"] == "1234"
        assert result[1]["LocalCode"] == "5678"

    def test_fetch_statements_by_date_with_message(self):
        """APIメッセージ付きレスポンスのテスト"""
        mock_session = mock.Mock()

        response = {"message": "データ取得成功", "statements": [{"LocalCode": "1234"}]}

        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.return_value = response

        with mock.patch("fetch.statements.logger") as mock_logger:
            result = statements._fetch_statements_by_date(
                mock_session, "test_token", "2024-01-01"
            )

            assert len(result) == 1
            mock_logger.info.assert_called_with("API message: %s", "データ取得成功")

    def test_fetch_statements_by_period(self):
        """期間指定取得のテスト"""
        mock_session = mock.Mock()

        # 各日付に対するレスポンスを設定
        def mock_response(date_str):
            return {"statements": [{"LocalCode": "1234", "DisclosedDate": date_str}]}

        with mock.patch.object(statements, "_fetch_statements_by_date") as mock_fetch:
            mock_fetch.side_effect = lambda s, t, d: mock_response(d)["statements"]

            result = statements._fetch_statements_by_period(
                mock_session, "test_token", "2024-01-01", "2024-01-03"
            )

            assert len(result) == 3
            assert mock_fetch.call_count == 3

    def test_fetch_multiple_codes(self):
        """複数銘柄の並行取得テスト"""
        codes = ["1234", "5678", "9012"]

        def mock_fetch_by_code(session, token, code):
            return [
                {"LocalCode": code, "DisclosureNumber": f"{code}0001"},
                {"LocalCode": code, "DisclosureNumber": f"{code}0002"},
            ]

        with mock.patch.object(
            statements, "_fetch_statements_by_code", side_effect=mock_fetch_by_code
        ):
            result = statements._fetch_multiple_codes("test_token", codes, workers=2)

            assert len(result) == 6  # 3銘柄 × 2件ずつ
            # 全ての銘柄のデータが含まれているか確認
            local_codes = {stmt["LocalCode"] for stmt in result}
            assert local_codes == {"1234", "5678", "9012"}


class TestDataProcessing:
    """データ処理関連のテスト"""

    def test_normalize_basic(self):
        """基本的なデータ正規化のテスト"""
        df = pd.DataFrame(
            [
                {
                    "LocalCode": "1234",
                    "DisclosureNumber": "12345678901234",
                    "NetSales": 1000000,
                    "OperatingProfit": 100000,
                    "OrdinaryProfit": 90000,
                    "Profit": 60000,
                }
            ]
        )

        result = statements._normalize(df)

        # 全てのスキーマカラムが存在することを確認
        assert len(result.columns) == len(statements.SCHEMA_COLUMNS)
        assert list(result.columns) == statements.SCHEMA_COLUMNS

        # 存在するカラムの値が保持されていることを確認
        assert result["LocalCode"].iloc[0] == "1234"
        assert result["NetSales"].iloc[0] == 1000000

        # 存在しないカラムはNAになることを確認
        assert pd.isna(result["EarningsPerShare"].iloc[0])

    def test_normalize_empty_dataframe(self):
        """空のDataFrameの正規化テスト"""
        df = pd.DataFrame()
        result = statements._normalize(df)

        assert result.empty
        assert list(result.columns) == statements.SCHEMA_COLUMNS


@pytest.fixture
def statements_db():
    """statements用のテストデータベース"""
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    # statements テーブルの作成（簡略版）
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS statements (
            {', '.join([f'{col} TEXT' for col in statements.SCHEMA_COLUMNS])},
            PRIMARY KEY (DisclosureNumber)
        )
    """
    )
    # listed_info テーブルも作成（モード1のテスト用）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listed_info (
            code TEXT PRIMARY KEY,
            delete_flag INTEGER DEFAULT 0
        )
    """
    )
    conn.commit()
    conn.close()

    yield db_path

    os.unlink(db_path)


class TestDatabase:
    """データベース操作のテスト"""

    def test_upsert_new_records(self, statements_db):
        """新規レコードの挿入テスト"""
        conn = sqlite3.connect(statements_db)

        records = [
            {
                "LocalCode": "1234",
                "DisclosureNumber": "12345678901234",
                "NetSales": 1000000,
                "OperatingProfit": 100000,
            },
            {
                "LocalCode": "5678",
                "DisclosureNumber": "56789012345678",
                "NetSales": 2000000,
                "OperatingProfit": 200000,
            },
        ]

        statements._upsert(conn, records)
        conn.commit()

        # 挿入確認
        cursor = conn.execute("SELECT COUNT(*) FROM statements")
        count = cursor.fetchone()[0]
        assert count == 2

        conn.close()

    def test_upsert_update_existing(self, statements_db):
        """既存レコードの更新テスト"""
        conn = sqlite3.connect(statements_db)

        # 初回挿入
        records1 = [
            {
                "LocalCode": "1234",
                "DisclosureNumber": "12345678901234",
                "NetSales": 1000000,
            }
        ]
        statements._upsert(conn, records1)
        conn.commit()

        # 更新
        records2 = [
            {
                "LocalCode": "1234",
                "DisclosureNumber": "12345678901234",
                "NetSales": 1500000,  # 変更
            }
        ]
        statements._upsert(conn, records2)
        conn.commit()

        # 更新確認
        cursor = conn.execute(
            "SELECT NetSales FROM statements WHERE DisclosureNumber = '12345678901234'"
        )
        row = cursor.fetchone()
        assert row[0] == "1500000"

        conn.close()

    def test_upsert_empty_records(self, statements_db):
        """空のレコードリストの処理テスト"""
        conn = sqlite3.connect(statements_db)

        statements._upsert(conn, [])

        # 何も挿入されていないことを確認
        cursor = conn.execute("SELECT COUNT(*) FROM statements")
        count = cursor.fetchone()[0]
        assert count == 0

        conn.close()


class TestIntegration:
    """統合テスト"""

    @mock.patch("fetch.statements._load_token")
    @mock.patch("fetch.statements._fetch_multiple_codes")
    def test_main_mode1(self, mock_fetch_codes, mock_load_token, statements_db):
        """モード1（銘柄別一括取得）の統合テスト"""
        mock_load_token.return_value = "test_token"

        # listed_infoにテストデータを挿入
        conn = sqlite3.connect(statements_db)
        conn.executemany(
            "INSERT INTO listed_info (code, delete_flag) VALUES (?, ?)",
            [("1234", 0), ("5678", 0), ("9012", 1)],  # 9012は無効
        )
        conn.commit()
        conn.close()

        # モックの戻り値設定
        mock_fetch_codes.return_value = [
            {"LocalCode": "1234", "DisclosureNumber": "12340001"},
            {"LocalCode": "5678", "DisclosureNumber": "56780001"},
        ]

        with mock.patch("fetch.statements.DB_PATH", statements_db):
            statements.main("1", None, None)

        # 有効な銘柄のみ取得されたか確認
        mock_fetch_codes.assert_called_once()
        args = mock_fetch_codes.call_args[0]
        assert len(args[1]) == 2  # codesの数
        assert "1234" in args[1]
        assert "5678" in args[1]
        assert "9012" not in args[1]  # 無効な銘柄は含まれない

        # DBに挿入されたか確認
        conn = sqlite3.connect(statements_db)
        cursor = conn.execute("SELECT COUNT(*) FROM statements")
        count = cursor.fetchone()[0]
        assert count == 2
        conn.close()

    @mock.patch("fetch.statements._load_token")
    @mock.patch("fetch.statements._fetch_statements_by_date")
    @mock.patch("datetime.date")
    def test_main_mode2_today(
        self, mock_date, mock_fetch_date, mock_load_token, statements_db
    ):
        """モード2（本日分取得）の統合テスト"""
        mock_load_token.return_value = "test_token"

        # 本日を2024-01-01に固定
        mock_today = mock.Mock()
        mock_today.strftime.return_value = "2024-01-01"
        mock_date.today.return_value = mock_today

        mock_fetch_date.return_value = [
            {"LocalCode": "1234", "DisclosureNumber": "12340001"}
        ]

        with mock.patch("fetch.statements.DB_PATH", statements_db):
            statements.main("2", None, None)

        mock_fetch_date.assert_called_once()
        # 本日の日付で呼ばれたか確認
        args = mock_fetch_date.call_args[0]
        assert args[2] == "2024-01-01"

    @mock.patch("fetch.statements._load_token")
    @mock.patch("fetch.statements._fetch_statements_by_period")
    def test_main_mode2_period(self, mock_fetch_period, mock_load_token, statements_db):
        """モード2（期間指定取得）の統合テスト"""
        mock_load_token.return_value = "test_token"

        mock_fetch_period.return_value = [
            {"LocalCode": "1234", "DisclosureNumber": "12340001"},
            {"LocalCode": "5678", "DisclosureNumber": "56780001"},
        ]

        with mock.patch("fetch.statements.DB_PATH", statements_db):
            statements.main("2", "2024-01-01", "2024-01-03")

        mock_fetch_period.assert_called_once()
        args = mock_fetch_period.call_args[0]
        assert args[2] == "2024-01-01"
        assert args[3] == "2024-01-03"

    def test_main_invalid_mode(self, statements_db):
        """無効なモードのテスト"""
        with mock.patch("fetch.statements._load_token") as mock_load_token:
            mock_load_token.return_value = "test_token"

            with mock.patch("fetch.statements.DB_PATH", statements_db):
                with mock.patch("fetch.statements.logger") as mock_logger:
                    statements.main("3", None, None)  # 無効なモード

                    mock_logger.error.assert_called_once()
                    assert "無効なモード" in mock_logger.error.call_args[0][0]

#!/usr/bin/env python
"""
上場企業情報取得モジュール (fetch/listed_info.py) のテスト

テスト対象:
- API呼び出しとレスポンス処理
- データの正規化とカラムマッピング
- SQLiteへのアップサート処理
- delete_flagの更新ロジック
- エラーハンドリング
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
from fetch import listed_info


class TestHelpers:
    """ヘルパー関数のテスト"""

    def test_load_token_success(self, tmp_path):
        """正常なトークン読み込みのテスト"""
        token_file = tmp_path / "idtoken.json"
        token_file.write_text('{"idToken": "test_token_456"}')

        with mock.patch("config.config.get_file_path") as mock_path:
            mock_path.return_value = token_file
            token = listed_info._load_token()
            assert token == "test_token_456"

    def test_load_token_missing(self, tmp_path):
        """トークンが存在しない場合のテスト"""
        token_file = tmp_path / "idtoken.json"
        token_file.write_text("{}")

        with mock.patch("config.config.get_file_path") as mock_path:
            mock_path.return_value = token_file
            with pytest.raises(RuntimeError, match="idToken not found"):
                listed_info._load_token()


class TestAPI:
    """API呼び出し関連のテスト"""

    @mock.patch("requests.get")
    def test_fetch_listed_info_success(self, mock_get):
        """正常なAPI呼び出しのテスト"""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "info": [
                {
                    "Code": "1234",
                    "Date": "2024-01-01",
                    "CompanyName": "テスト株式会社",
                    "CompanyNameEnglish": "Test Corp",
                    "Sector17Code": "1",
                    "Sector17CodeName": "食品",
                    "Sector33Code": "1050",
                    "Sector33CodeName": "水産・農林業",
                    "ScaleCategory": "TOPIX Large70",
                    "MarketCode": "0111",
                    "MarketCodeName": "プライム",
                    "MarginCode": "1",
                    "MarginCodeName": "信用",
                }
            ]
        }
        mock_get.return_value = mock_response

        result = listed_info._fetch_listed_info("test_token")

        assert len(result) == 1
        assert result["Code"].iloc[0] == "1234"
        assert result["CompanyName"].iloc[0] == "テスト株式会社"
        mock_get.assert_called_once_with(
            listed_info.API_ENDPOINT,
            headers={"Authorization": "Bearer test_token"},
            timeout=30,
        )

    @mock.patch("requests.get")
    def test_fetch_listed_info_api_error(self, mock_get):
        """APIエラー時のテスト"""
        mock_response = mock.Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_get.return_value = mock_response

        with pytest.raises(RuntimeError, match="API error 401"):
            listed_info._fetch_listed_info("invalid_token")

    @mock.patch("requests.get")
    def test_fetch_listed_info_empty_response(self, mock_get):
        """空のレスポンス時のテスト"""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}  # 'info'キーなし
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="no 'info' key"):
            listed_info._fetch_listed_info("test_token")

    @mock.patch("requests.get")
    def test_fetch_listed_info_with_message(self, mock_get):
        """APIメッセージ付きレスポンスのテスト"""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "message": "データ取得成功",
            "info": [{"Code": "1234", "CompanyName": "テスト"}],
        }
        mock_get.return_value = mock_response

        with mock.patch("fetch.listed_info.logger") as mock_logger:
            result = listed_info._fetch_listed_info("test_token")

            assert len(result) == 1
            mock_logger.info.assert_called_with("API message: %s", "データ取得成功")


class TestDatabase:
    """データベース操作のテスト"""

    @pytest.fixture
    def listed_info_db(self):
        """listed_info用のテストデータベース"""
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS listed_info (
                code TEXT PRIMARY KEY,
                date TEXT,
                company_name TEXT,
                company_name_en TEXT,
                sector17_code TEXT,
                sector17_name TEXT,
                sector33_code TEXT,
                sector33_name TEXT,
                scale_category TEXT,
                market_code TEXT,
                market_name TEXT,
                margin_code TEXT,
                margin_name TEXT,
                delete_flag INTEGER DEFAULT 0
            )
        """
        )
        conn.commit()
        conn.close()

        yield db_path

        os.unlink(db_path)

    def test_to_db_basic(self, listed_info_db):
        """基本的なデータ挿入のテスト"""
        df = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-01",
                    "CompanyName": "テスト株式会社",
                    "CompanyNameEnglish": "Test Corp",
                    "Sector17Code": "1",
                    "Sector17CodeName": "食品",
                    "Sector33Code": "1050",
                    "Sector33CodeName": "水産・農林業",
                    "ScaleCategory": "TOPIX Large70",
                    "MarketCode": "0111",
                    "MarketCodeName": "プライム",
                    "MarginCode": "1",
                    "MarginCodeName": "信用",
                }
            ]
        )

        conn = sqlite3.connect(listed_info_db)
        listed_info._to_db(df, conn)

        # 挿入確認
        cursor = conn.execute("SELECT * FROM listed_info WHERE code = '1234'")
        row = cursor.fetchone()
        assert row is not None

        # カラムインデックスで値を確認
        assert row[0] == "1234"  # code
        assert row[1] == "2024-01-01"  # date
        assert row[2] == "テスト株式会社"  # company_name
        assert row[3] == "Test Corp"  # company_name_en
        assert row[5] == "食品"  # sector17_name
        assert row[7] == "水産・農林業"  # sector33_name
        assert row[10] == "プライム"  # market_name

        conn.close()

    @mock.patch("datetime.date")
    def test_to_db_delete_flag_update(self, mock_date, listed_info_db):
        """delete_flag更新ロジックのテスト"""
        # 本日を2024-01-02に固定
        mock_today = mock.Mock()
        mock_today.strftime.return_value = "2024-01-02"
        mock_date.today.return_value = mock_today

        # 古いデータと新しいデータ
        df = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-01",
                    "CompanyName": "古い会社",
                },  # 昨日
                {
                    "Code": "5678",
                    "Date": "2024-01-02",
                    "CompanyName": "新しい会社",
                },  # 本日
            ]
        )

        conn = sqlite3.connect(listed_info_db)
        listed_info._to_db(df, conn)

        # delete_flagの確認
        cursor = conn.execute("SELECT code, delete_flag FROM listed_info ORDER BY code")
        rows = cursor.fetchall()

        assert rows[0][0] == "1234"
        assert rows[0][1] == 1  # 古いデータはdelete_flag=1

        assert rows[1][0] == "5678"
        assert rows[1][1] == 0  # 本日のデータはdelete_flag=0

        conn.close()

    def test_to_db_update_existing(self, listed_info_db):
        """既存レコードの更新テスト"""
        conn = sqlite3.connect(listed_info_db)

        # 初回挿入
        df1 = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-01",
                    "CompanyName": "旧名称",
                    "MarketCodeName": "スタンダード",
                }
            ]
        )
        listed_info._to_db(df1, conn)

        # 更新
        df2 = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-02",
                    "CompanyName": "新名称",
                    "MarketCodeName": "プライム",
                }
            ]
        )
        listed_info._to_db(df2, conn)

        # 更新確認
        cursor = conn.execute(
            "SELECT company_name, market_name FROM listed_info WHERE code = '1234'"
        )
        row = cursor.fetchone()
        assert row[0] == "新名称"
        assert row[1] == "プライム"

        conn.close()

    def test_to_db_empty_dataframe(self, listed_info_db):
        """空のDataFrameの処理テスト"""
        df = pd.DataFrame()
        conn = sqlite3.connect(listed_info_db)

        with mock.patch("fetch.listed_info.logger") as mock_logger:
            listed_info._to_db(df, conn)
            mock_logger.warning.assert_called_once()

        conn.close()

    def test_to_db_missing_columns(self, listed_info_db):
        """一部カラムが欠けているデータのテスト"""
        df = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": "2024-01-01",
                    "CompanyName": "テスト株式会社",
                    # 他のカラムは欠落
                }
            ]
        )

        conn = sqlite3.connect(listed_info_db)
        listed_info._to_db(df, conn)

        # 挿入確認
        cursor = conn.execute("SELECT * FROM listed_info WHERE code = '1234'")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "1234"
        assert row[2] == "テスト株式会社"
        # 欠落カラムはNULL（None）になるはず

        conn.close()


class TestIntegration:
    """統合テスト"""

    @mock.patch("fetch.listed_info._load_token")
    @mock.patch("fetch.listed_info._fetch_listed_info")
    def test_update_listed_info(self, mock_fetch, mock_load_token, tmp_path):
        """update_listed_info関数の統合テスト"""
        # テスト用DB作成
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS listed_info (
                code TEXT PRIMARY KEY,
                date TEXT,
                company_name TEXT,
                company_name_en TEXT,
                sector17_code TEXT,
                sector17_name TEXT,
                sector33_code TEXT,
                sector33_name TEXT,
                scale_category TEXT,
                market_code TEXT,
                market_name TEXT,
                margin_code TEXT,
                margin_name TEXT,
                delete_flag INTEGER DEFAULT 0
            )
        """
        )
        conn.commit()
        conn.close()

        mock_load_token.return_value = "test_token"
        mock_fetch.return_value = pd.DataFrame(
            [
                {
                    "Code": "1234",
                    "Date": dt.date.today().strftime("%Y-%m-%d"),
                    "CompanyName": "統合テスト株式会社",
                    "MarketCodeName": "プライム",
                }
            ]
        )

        # DB_PATHを直接モック
        with mock.patch("fetch.listed_info.DB_PATH", str(db_path)):
            # 実行
            listed_info.update_listed_info()

        # 確認
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT company_name FROM listed_info WHERE code = '1234'"
        )
        row = cursor.fetchone()
        assert row[0] == "統合テスト株式会社"
        conn.close()

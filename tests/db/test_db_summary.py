"""Tests for db/db_summary.py"""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from db.db_summary import TABLES, main


class TestDbSummary:
    """db_summary のテスト"""

    def test_tables_constant(self):
        """TABLES定数の確認"""
        assert isinstance(TABLES, dict)
        assert "prices" in TABLES
        assert "listed_info" in TABLES
        assert "statements" in TABLES
        assert "fundamental_signals" in TABLES
        assert "technical_indicators" in TABLES

        # 各テーブルの日付カラムが正しいか確認
        assert TABLES["prices"] == "date"
        assert TABLES["listed_info"] == "date"
        assert TABLES["statements"] == "DisclosedDate"
        assert TABLES["fundamental_signals"] == "created_at"
        assert TABLES["technical_indicators"] == "signal_date"

    @patch("db.db_summary.get_db_connection")
    @patch("builtins.print")
    def test_main_with_data(self, mock_print, mock_get_db_connection):
        """データがある場合のmain関数のテスト"""
        # モックデータベース接続を作成
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # 各テーブルのクエリ結果を設定
        query_results = [
            (100, "2024-01-01", "2024-01-31"),  # prices
            (50, "2024-01-01", "2024-01-31"),  # listed_info
            (200, "2024-01-01", "2024-01-31"),  # statements
            (30, "2024-01-01 00:00:00", "2024-01-31 23:59:59"),  # fundamental_signals
            (150, "2024-01-01", "2024-01-31"),  # technical_indicators
        ]

        # executeの戻り値を設定
        mock_cursor.fetchone.side_effect = query_results
        mock_conn.execute.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # main関数を実行
        main()

        # 各テーブルに対してクエリが実行されたことを確認
        assert mock_conn.execute.call_count == 5

        # print関数が正しく呼ばれたことを確認
        assert mock_print.call_count == 5

        # 出力内容を確認
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        assert any("prices" in call and "rows=    100" in call for call in print_calls)
        assert any(
            "listed_info" in call and "rows=     50" in call for call in print_calls
        )
        assert any(
            "statements" in call and "rows=    200" in call for call in print_calls
        )
        assert any(
            "fundamental_signals" in call and "rows=     30" in call
            for call in print_calls
        )
        assert any(
            "technical_indicators" in call and "rows=    150" in call
            for call in print_calls
        )

    @patch("db.db_summary.get_db_connection")
    @patch("builtins.print")
    def test_main_with_empty_tables(self, mock_print, mock_get_db_connection):
        """空のテーブルの場合のテスト"""
        # モックデータベース接続を作成
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # 空のテーブルの結果
        mock_cursor.fetchone.return_value = (0, None, None)
        mock_conn.execute.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # main関数を実行
        main()

        # print関数が呼ばれたことを確認
        assert mock_print.call_count == 5

        # 空のテーブルの出力を確認
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        for call in print_calls:
            assert "rows=      0" in call
            assert "range=[None .. None]" in call

    def test_main_with_real_db(self):
        """実際のデータベースでのテスト"""
        # テンポラリデータベースを作成
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            # テスト用のテーブルとデータを作成
            with sqlite3.connect(db_path) as conn:
                # テーブルを作成
                conn.execute(
                    """
                    CREATE TABLE prices (
                        code TEXT,
                        date TEXT,
                        adj_close REAL,
                        PRIMARY KEY (code, date)
                    )
                """
                )

                conn.execute(
                    """
                    CREATE TABLE listed_info (
                        code TEXT PRIMARY KEY,
                        date TEXT,
                        company_name TEXT
                    )
                """
                )

                conn.execute(
                    """
                    CREATE TABLE statements (
                        DisclosureNumber TEXT PRIMARY KEY,
                        DisclosedDate TEXT,
                        code TEXT
                    )
                """
                )

                conn.execute(
                    """
                    CREATE TABLE fundamental_signals (
                        code TEXT,
                        DisclosedAt TEXT,
                        created_at TEXT,
                        PRIMARY KEY (code, DisclosedAt)
                    )
                """
                )

                conn.execute(
                    """
                    CREATE TABLE technical_indicators (
                        code TEXT,
                        signal_date TEXT,
                        signals_count INTEGER,
                        PRIMARY KEY (code, signal_date)
                    )
                """
                )

                # テストデータを挿入
                conn.execute("INSERT INTO prices VALUES ('1234', '2024-01-15', 1000.0)")
                conn.execute(
                    "INSERT INTO listed_info VALUES ('1234', '2024-01-01', 'Test Company')"
                )
                conn.execute(
                    "INSERT INTO statements VALUES ('12345', '2024-01-20', '1234')"
                )
                conn.execute(
                    "INSERT INTO fundamental_signals VALUES ('1234', '2024-01-15 10:00:00', '2024-01-15 10:00:00')"
                )
                conn.execute(
                    "INSERT INTO technical_indicators VALUES ('1234', '2024-01-15', 5)"
                )
                conn.commit()

            # get_db_connectionをモックして、テスト用DBを使用
            with patch("db.db_summary.get_db_connection") as mock_get_db_connection:
                mock_get_db_connection.return_value.__enter__.return_value = (
                    sqlite3.connect(db_path)
                )

                # 出力をキャプチャ
                with patch("builtins.print") as mock_print:
                    main()

                    # 各テーブルの出力を確認
                    assert mock_print.call_count == 5
                    print_calls = [call[0][0] for call in mock_print.call_args_list]

                    # 各テーブルに1行ずつあることを確認
                    assert any(
                        "prices" in call and "rows=      1" in call
                        for call in print_calls
                    )
                    assert any(
                        "listed_info" in call and "rows=      1" in call
                        for call in print_calls
                    )
                    assert any(
                        "statements" in call and "rows=      1" in call
                        for call in print_calls
                    )

        finally:
            # クリーンアップ
            Path(db_path).unlink(missing_ok=True)

    @patch("db.db_summary.get_db_connection")
    def test_main_with_db_error(self, mock_get_db_connection):
        """データベースエラーの場合のテスト"""
        # エラーを発生させる
        mock_get_db_connection.side_effect = sqlite3.Error("Database error")

        # エラーが発生することを確認
        with pytest.raises(sqlite3.Error):
            main()

    @patch("db.db_summary.get_db_connection")
    @patch("builtins.print")
    def test_main_output_format(self, mock_print, mock_get_db_connection):
        """出力フォーマットのテスト"""
        # モックデータベース接続を作成
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # 長いテーブル名と大きな数値でテスト
        mock_cursor.fetchone.return_value = (1234567, "2020-01-01", "2024-12-31")
        mock_conn.execute.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # main関数を実行
        main()

        # 出力フォーマットを確認
        print_calls = [call[0][0] for call in mock_print.call_args_list]

        # technical_indicators（最も長いテーブル名）の出力を確認
        tech_output = next(
            call for call in print_calls if "technical_indicators" in call
        )
        # technical_indicatorsの後にはコロンではなくスペースのみ
        assert "technical_indicators" in tech_output
        assert "rows=1234567" in tech_output  # 7桁の数値フォーマット
        assert "range=[2020-01-01 .. 2024-12-31]" in tech_output

"""Tests for db_summary module"""

import sys
from io import StringIO
from unittest.mock import MagicMock, patch


class TestDbSummary:
    """Tests for db_summary module"""

    @patch("db.db_summary.get_db_connection")
    def test_main_displays_summary(self, mock_get_db_connection):
        """main関数がデータベースのサマリーを表示することを確認"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # fetchone()の戻り値を設定
        mock_cursor.fetchone.side_effect = [
            (1000, "2023-01-01", "2023-12-31"),  # prices
            (500, "2023-01-01", "2023-12-31"),  # listed_info
            (2000, "2023-01-01", "2023-12-31"),  # statements
            (300, "2023-01-01", "2023-12-31"),  # fundamental_signals
            (1500, "2023-01-01", "2023-12-31"),  # technical_indicators
        ]

        mock_conn.execute.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # 標準出力をキャプチャ
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            from db.db_summary import main

            main()

            # 出力を確認
            output = captured_output.getvalue()
            assert "prices" in output
            assert "rows=   1000" in output
            assert "range=[2023-01-01 .. 2023-12-31]" in output

            assert "listed_info" in output
            assert "rows=    500" in output

            assert "statements" in output
            assert "rows=   2000" in output

            assert "fundamental_signals" in output
            assert "rows=    300" in output

            assert "technical_indicators" in output
            assert "rows=   1500" in output

        finally:
            sys.stdout = sys.__stdout__

    @patch("db.db_summary.get_db_connection")
    def test_empty_tables(self, mock_get_db_connection):
        """空のテーブルの場合の処理を確認"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # 空のテーブルをシミュレート
        mock_cursor.fetchone.side_effect = [
            (0, None, None),  # prices
            (0, None, None),  # listed_info
            (0, None, None),  # statements
            (0, None, None),  # fundamental_signals
            (0, None, None),  # technical_indicators
        ]

        mock_conn.execute.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            from db.db_summary import main

            main()

            output = captured_output.getvalue()
            assert "rows=      0" in output
            assert "range=[None .. None]" in output

        finally:
            sys.stdout = sys.__stdout__

    def test_tables_constant(self):
        """TABLES定数が正しく定義されていることを確認"""
        from db.db_summary import TABLES

        expected_tables = {
            "prices": "date",
            "listed_info": "date",
            "statements": "DisclosedDate",
            "fundamental_signals": "created_at",
            "technical_indicators": "signal_date",
        }

        assert TABLES == expected_tables

    @patch("db.db_summary.get_db_connection")
    def test_sql_queries(self, mock_get_db_connection):
        """正しいSQLクエリが実行されることを確認"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0, None, None)

        mock_conn.execute.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # 標準出力を無効化
        sys.stdout = StringIO()

        try:
            from db.db_summary import main

            main()

            # 各テーブルに対してクエリが実行されたことを確認
            expected_calls = [
                "SELECT COUNT(*), MIN(date), MAX(date) FROM prices",
                "SELECT COUNT(*), MIN(date), MAX(date) FROM listed_info",
                "SELECT COUNT(*), MIN(DisclosedDate), MAX(DisclosedDate) FROM statements",
                "SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM fundamental_signals",
                "SELECT COUNT(*), MIN(signal_date), MAX(signal_date) FROM technical_indicators",
            ]

            # execute()の呼び出しを確認
            assert mock_conn.execute.call_count == 5

            for i, expected_query in enumerate(expected_calls):
                actual_query = mock_conn.execute.call_args_list[i][0][0]
                assert actual_query == expected_query

        finally:
            sys.stdout = sys.__stdout__

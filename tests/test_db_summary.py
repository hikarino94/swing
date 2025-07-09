"""Test suite for db/db_summary.py module."""

import sqlite3
import sys
from pathlib import Path
from unittest import mock

import pytest

# プロジェクトルートをPYTHONPATHに追加
sys.path.insert(0, str(Path(__file__).parent.parent))
from db.db_summary import TABLES, main


class TestConstants:
    """Test module constants."""

    def test_tables_constant(self):
        """TABLESの定義を確認"""
        assert TABLES == {
            "prices": "date",
            "listed_info": "date",
            "statements": "DisclosedDate",
            "fundamental_signals": "created_at",
            "technical_indicators": "signal_date",
        }


class TestMain:
    """Test main function."""

    @pytest.fixture
    def test_db(self, tmp_path):
        """テスト用のデータベースを作成"""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)

        # テーブルを作成
        conn.execute(
            """
            CREATE TABLE prices (
                id INTEGER PRIMARY KEY,
                date TEXT,
                price REAL
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE listed_info (
                id INTEGER PRIMARY KEY,
                date TEXT,
                code TEXT
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE statements (
                id INTEGER PRIMARY KEY,
                DisclosedDate TEXT,
                revenue REAL
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE fundamental_signals (
                id INTEGER PRIMARY KEY,
                created_at TEXT,
                signal TEXT
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE technical_indicators (
                id INTEGER PRIMARY KEY,
                signal_date TEXT,
                indicator TEXT
            )
        """
        )

        # テストデータを挿入
        conn.execute(
            "INSERT INTO prices (date, price) VALUES (?, ?), (?, ?), (?, ?)",
            ("2024-01-01", 100.0, "2024-01-02", 101.0, "2024-01-03", 102.0),
        )

        conn.execute(
            "INSERT INTO listed_info (date, code) VALUES (?, ?), (?, ?)",
            ("2024-01-01", "1234", "2024-01-05", "5678"),
        )

        conn.execute(
            "INSERT INTO statements (DisclosedDate, revenue) VALUES (?, ?)",
            (
                "2024-01-15",
                1000000.0,
            ),
        )

        conn.execute(
            "INSERT INTO fundamental_signals (created_at, signal) VALUES (?, ?), (?, ?), (?, ?), (?, ?)",
            (
                "2024-01-01",
                "BUY",
                "2024-01-02",
                "HOLD",
                "2024-01-03",
                "SELL",
                "2024-01-04",
                "BUY",
            ),
        )

        conn.execute(
            "INSERT INTO technical_indicators (signal_date, indicator) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)",
            (
                "2024-01-01",
                "RSI",
                "2024-01-02",
                "MACD",
                "2024-01-03",
                "BB",
                "2024-01-04",
                "RSI",
                "2024-01-05",
                "MACD",
            ),
        )

        conn.commit()
        conn.close()

        return db_path

    def test_main_with_data(self, test_db, capsys):
        """データありのデータベースでのテスト"""
        with mock.patch("db.db_summary.get_db_path", return_value=str(test_db)):
            main()

        captured = capsys.readouterr()
        output_lines = captured.out.strip().split("\n")

        # 出力行数の確認
        assert len(output_lines) == 5

        # 各テーブルの出力を確認
        expected = [
            ("prices", 3, "2024-01-01", "2024-01-03"),
            ("listed_info", 2, "2024-01-01", "2024-01-05"),
            ("statements", 1, "2024-01-15", "2024-01-15"),
            ("fundamental_signals", 4, "2024-01-01", "2024-01-04"),
            ("technical_indicators", 5, "2024-01-01", "2024-01-05"),
        ]

        for i, (table_name, row_count, min_date, max_date) in enumerate(expected):
            line = output_lines[i]
            assert table_name in line
            assert f"rows={row_count:7d}" in line
            assert f"range=[{min_date} .. {max_date}]" in line

    def test_main_empty_tables(self, tmp_path, capsys):
        """空のテーブルでのテスト"""
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(db_path)

        # 空のテーブルを作成
        for table, date_col in TABLES.items():
            conn.execute(f"CREATE TABLE {table} ({date_col} TEXT)")

        conn.commit()
        conn.close()

        with mock.patch("db.db_summary.get_db_path", return_value=str(db_path)):
            main()

        captured = capsys.readouterr()
        output_lines = captured.out.strip().split("\n")

        # すべてのテーブルが0行であることを確認
        for line in output_lines:
            assert "rows=      0" in line
            assert "range=[None .. None]" in line

    def test_main_partial_data(self, tmp_path, capsys):
        """一部のテーブルにのみデータがある場合"""
        db_path = tmp_path / "partial.db"
        conn = sqlite3.connect(db_path)

        # テーブルを作成
        for table, date_col in TABLES.items():
            conn.execute(f"CREATE TABLE {table} ({date_col} TEXT, data TEXT)")

        # pricesテーブルのみにデータを挿入
        conn.execute(
            "INSERT INTO prices (date, data) VALUES (?, ?), (?, ?)",
            ("2024-01-10", "data1", "2024-01-20", "data2"),
        )

        conn.commit()
        conn.close()

        with mock.patch("db.db_summary.get_db_path", return_value=str(db_path)):
            main()

        captured = capsys.readouterr()
        output_lines = captured.out.strip().split("\n")

        # pricesテーブルの出力を確認
        prices_line = output_lines[0]
        assert "prices" in prices_line
        assert "rows=      2" in prices_line
        assert "range=[2024-01-10 .. 2024-01-20]" in prices_line

        # 他のテーブルは空
        for i in range(1, 5):
            assert "rows=      0" in output_lines[i]

    def test_output_format(self, test_db, capsys):
        """出力フォーマットの詳細テスト"""
        with mock.patch("db.db_summary.get_db_path", return_value=str(test_db)):
            main()

        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")

        # 各行のフォーマットを確認
        for line in lines:
            # テーブル名が20文字幅で左寄せ
            parts = line.split(":")
            assert len(parts[0]) == 20

            # コロンで区切られている
            assert ":" in line

            # rows=とrange=が含まれている
            assert "rows=" in line
            assert "range=[" in line
            assert ".." in line
            assert "]" in line

    def test_sql_query_structure(self, test_db, capsys):
        """SQLクエリが正しく実行されることを確認"""
        # このテストは主にSQL文の構文エラーがないことを確認
        with mock.patch("db.db_summary.get_db_path", return_value=str(test_db)):
            # エラーなく実行できることを確認
            try:
                main()
            except sqlite3.Error as e:
                pytest.fail(f"SQL error occurred: {e}")

        # 出力があることを確認
        captured = capsys.readouterr()
        assert captured.out.strip() != ""

    @mock.patch("db.db_summary.sqlite3.connect")
    def test_database_connection_handling(self, mock_connect):
        """データベース接続が適切に処理されることを確認"""
        # モックの設定
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchone.return_value = (10, "2024-01-01", "2024-01-31")
        mock_conn.execute.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connect.return_value = mock_conn

        # 実行
        with mock.patch("db.db_summary.get_db_path", return_value="dummy.db"):
            main()

        # コンテキストマネージャーとして使用されたことを確認
        mock_conn.__enter__.assert_called_once()
        mock_conn.__exit__.assert_called_once()

        # 各テーブルに対してクエリが実行されたことを確認
        assert mock_conn.execute.call_count == len(TABLES)


class TestIntegration:
    """Integration tests for db_summary module."""

    def test_real_world_scenario(self, tmp_path, capsys):
        """実際の使用シナリオをシミュレート"""
        db_path = tmp_path / "real_world.db"
        conn = sqlite3.connect(db_path)

        # より現実的なスキーマとデータ
        conn.execute(
            """
            CREATE TABLE prices (
                code TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (code, date)
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE listed_info (
                code TEXT PRIMARY KEY,
                date TEXT,
                company_name TEXT,
                market TEXT
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE statements (
                code TEXT,
                DisclosedDate TEXT,
                revenue REAL,
                profit REAL,
                PRIMARY KEY (code, DisclosedDate)
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE fundamental_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT,
                created_at TEXT,
                signal_type TEXT
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE technical_indicators (
                code TEXT,
                signal_date TEXT,
                rsi REAL,
                macd REAL,
                signal_count INTEGER,
                PRIMARY KEY (code, signal_date)
            )
        """
        )

        # 大量のデータを挿入
        # 価格データ（複数銘柄、複数日）
        for code in ["1234", "5678", "9012"]:
            for day in range(1, 31):
                date = f"2024-01-{day:02d}"
                conn.execute(
                    "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (code, date, 100.0, 105.0, 99.0, 102.0, 100000),
                )

        # その他のテーブルにもデータを挿入
        conn.execute(
            "INSERT INTO listed_info VALUES ('1234', '2024-01-01', 'Company A', 'TSE')"
        )
        conn.execute(
            "INSERT INTO statements VALUES ('1234', '2024-01-15', 1000000, 100000)"
        )

        conn.commit()
        conn.close()

        with mock.patch("db.db_summary.get_db_path", return_value=str(db_path)):
            main()

        captured = capsys.readouterr()

        # 価格データの確認
        assert "prices" in captured.out
        assert "rows=     90" in captured.out  # 3銘柄 × 30日
        assert "range=[2024-01-01 .. 2024-01-30]" in captured.out


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

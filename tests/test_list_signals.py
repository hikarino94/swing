"""Test suite for db/list_signals.py module."""

import sqlite3
import sys
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

# プロジェクトルートをPYTHONPATHに追加
sys.path.insert(0, str(Path(__file__).parent.parent))
from db.list_signals import TABLES, main


class TestConstants:
    """Test module constants."""

    def test_tables_constant(self):
        """TABLESの定義を確認"""
        assert TABLES == {
            "fund": ("fundamental_signals", "DisclosedAt"),
            "tech": ("technical_indicators", "signal_date"),
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
            CREATE TABLE fundamental_signals (
                code TEXT,
                DisclosedAt TEXT,
                eps_yoy REAL,
                cf_quality REAL
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE technical_indicators (
                code TEXT,
                signal_date TEXT,
                signals_count INTEGER,
                signals_first INTEGER,
                signals_overheating INTEGER
            )
        """
        )

        # テストデータを挿入
        conn.execute(
            """
            INSERT INTO fundamental_signals (code, DisclosedAt, eps_yoy, cf_quality)
            VALUES
                ('1234', '2024-01-15', 0.35, 0.85),
                ('5678', '2024-01-16', 0.40, 0.90),
                ('9012', '2024-01-17', 0.25, 0.75)
        """
        )

        conn.execute(
            """
            INSERT INTO technical_indicators
            (code, signal_date, signals_count, signals_first, signals_overheating)
            VALUES
                ('1234', '2024-01-15', 4, 1, 0),
                ('5678', '2024-01-16', 3, 1, 0),
                ('9012', '2024-01-17', 2, 0, 0),
                ('3456', '2024-01-18', 5, 1, 1)
        """
        )

        conn.commit()
        conn.close()
        return db_path

    def test_fund_signals_default(self, test_db, capsys):
        """ファンダメンタルシグナルのデフォルト表示"""
        with mock.patch("sys.argv", ["list_signals.py", "fund", "--db", str(test_db)]):
            # 今日の日付をモック
            with mock.patch("db.list_signals.dt.date") as mock_date:
                mock_date.today.return_value.isoformat.return_value = "2024-01-16"
                main()

        captured = capsys.readouterr()
        assert "5678" in captured.out
        assert "2024-01-16" in captured.out
        assert "0.40" in captured.out

    def test_tech_signals_with_filters(self, test_db, capsys):
        """テクニカルシグナルのフィルタ付き表示"""
        with mock.patch(
            "sys.argv",
            [
                "list_signals.py",
                "tech",
                "--db",
                str(test_db),
                "--start",
                "2024-01-15",
                "--end",
                "2024-01-16",
            ],
        ):
            main()

        captured = capsys.readouterr()
        # フィルタ条件を満たすものだけ表示
        assert "1234" in captured.out  # signals_count=4, first=1, overheating=0
        assert "5678" in captured.out  # signals_count=3, first=1, overheating=0
        assert "9012" not in captured.out  # signals_count=2 (条件外)
        assert "3456" not in captured.out  # overheating=1 (条件外)

    def test_empty_result(self, test_db, capsys):
        """結果が空の場合"""
        with mock.patch(
            "sys.argv",
            [
                "list_signals.py",
                "fund",
                "--db",
                str(test_db),
                "--start",
                "2025-01-01",
                "--end",
                "2025-12-31",
            ],
        ):
            main()

        captured = capsys.readouterr()
        assert "(no rows)" in captured.out

    def test_limit_option(self, test_db, capsys):
        """limit オプションのテスト"""
        with mock.patch(
            "sys.argv",
            ["list_signals.py", "fund", "--db", str(test_db), "--limit", "1"],
        ):
            # 日付範囲を指定しない場合はLIMITが使われる
            with mock.patch("argparse.ArgumentParser.parse_args") as mock_parse:
                mock_parse.return_value = mock.MagicMock(
                    kind="fund", db=str(test_db), start=None, end=None, limit=1
                )
                main()

        captured = capsys.readouterr()
        # 1件のみ表示されることを確認
        lines = [line for line in captured.out.strip().split("\n") if line]
        # ヘッダー行を除いてデータ行をカウント
        data_lines = [
            line for line in lines if "code" not in line and "(no rows)" not in line
        ]
        assert len(data_lines) <= 1

    def test_date_range_query(self, test_db, capsys):
        """日付範囲指定のテスト"""
        with mock.patch(
            "sys.argv",
            [
                "list_signals.py",
                "fund",
                "--db",
                str(test_db),
                "--start",
                "2024-01-16",
                "--end",
                "2024-01-17",
            ],
        ):
            main()

        captured = capsys.readouterr()
        assert "1234" not in captured.out  # 2024-01-15 (範囲外)
        assert "5678" in captured.out  # 2024-01-16 (範囲内)
        assert "9012" in captured.out  # 2024-01-17 (範囲内)

    @mock.patch("db.list_signals.pd.read_sql")
    def test_sql_query_construction(self, mock_read_sql, test_db):
        """SQLクエリの構築をテスト"""
        mock_read_sql.return_value = pd.DataFrame()

        # テクニカル指標でフィルタ付き
        with mock.patch(
            "sys.argv",
            [
                "list_signals.py",
                "tech",
                "--db",
                str(test_db),
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-31",
            ],
        ):
            main()

        # 呼び出されたSQLを確認
        call_args = mock_read_sql.call_args
        sql = call_args[0][0]
        params = call_args[1]["params"]

        assert "technical_indicators" in sql
        assert "WHERE" in sql
        assert "signals_count>=3" in sql
        assert "signals_first=1" in sql
        assert "signals_overheating=0" in sql
        assert "signal_date >= ?" in sql
        assert "signal_date <= ?" in sql
        assert params == ("2024-01-01", "2024-01-31")

    def test_database_connection_error(self, capsys):
        """データベース接続エラーのテスト"""
        with mock.patch(
            "sys.argv", ["list_signals.py", "fund", "--db", "/nonexistent/database.db"]
        ):
            with pytest.raises(sqlite3.OperationalError):
                main()


class TestIntegration:
    """Integration tests for list_signals module."""

    def test_full_workflow(self, tmp_path, capsys):
        """完全なワークフローのテスト"""
        # データベース作成
        db_path = tmp_path / "integration.db"
        conn = sqlite3.connect(db_path)

        # より現実的なスキーマ
        conn.execute(
            """
            CREATE TABLE fundamental_signals (
                id INTEGER PRIMARY KEY,
                code TEXT,
                DisclosedAt TEXT,
                eps_yoy REAL,
                cf_quality REAL,
                eta_delta REAL,
                treasury_delta REAL,
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
                adx REAL,
                macd REAL,
                bb_position REAL,
                signals_count INTEGER,
                signals_first INTEGER,
                signals_overheating INTEGER,
                signals_oversold INTEGER,
                PRIMARY KEY (code, signal_date)
            )
        """
        )

        # 複数日のデータを挿入
        for i in range(10):
            date = f"2024-01-{i+10:02d}"
            conn.execute(
                """
                INSERT INTO fundamental_signals
                (code, DisclosedAt, eps_yoy, cf_quality, signal_type)
                VALUES (?, ?, ?, ?, ?)
            """,
                (f"{1000+i}", date, 0.3 + i * 0.05, 0.8 + i * 0.02, "BUY"),
            )

            conn.execute(
                """
                INSERT INTO technical_indicators
                (code, signal_date, rsi, adx, signals_count, signals_first, signals_overheating)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (f"{1000+i}", date, 50 + i, 25 + i, 3 + i % 2, 1, 0),
            )

        conn.commit()
        conn.close()

        # ファンダメンタルシグナルの確認
        with mock.patch(
            "sys.argv",
            [
                "list_signals.py",
                "fund",
                "--db",
                str(db_path),
                "--start",
                "2024-01-15",
                "--end",
                "2024-01-19",
            ],
        ):
            main()

        captured = capsys.readouterr()
        assert "1005" in captured.out  # 2024-01-15
        assert "1009" in captured.out  # 2024-01-19
        assert "1004" not in captured.out  # 2024-01-14 (範囲外)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

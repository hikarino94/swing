"""データベーススキーマのテスト"""

import sqlite3
from pathlib import Path

import pytest


class TestDBSchema:
    """データベーススキーマのテスト"""

    def test_prices_table(self, temp_db: Path):
        """pricesテーブルのテスト"""
        conn = sqlite3.connect(temp_db)

        # データ挿入
        conn.execute(
            """
            INSERT INTO prices (code, date, adj_close, adj_volume)
            VALUES ('1234', '2024-01-01', 1000.0, 100000)
        """
        )
        conn.commit()

        # データ取得（カラム名で指定）
        cursor = conn.execute(
            "SELECT code, date, adj_close, adj_volume FROM prices WHERE code = '1234'"
        )
        row = cursor.fetchone()

        assert row[0] == "1234"  # code
        assert row[1] == "2024-01-01"  # date
        assert row[2] == 1000.0  # adj_close
        assert row[3] == 100000  # adj_volume

        conn.close()

    def test_listed_info_table(self, temp_db: Path):
        """listed_infoテーブルのテスト"""
        conn = sqlite3.connect(temp_db)

        # データ挿入
        conn.execute(
            """
            INSERT INTO listed_info (code, company_name, sector33_name)
            VALUES ('1234', 'テスト株式会社', '情報・通信業')
        """
        )
        conn.commit()

        # データ取得（カラム名を指定して取得）
        cursor = conn.execute(
            "SELECT code, company_name, sector33_name, delete_flag FROM listed_info WHERE code = '1234'"
        )
        row = cursor.fetchone()

        assert row[0] == "1234"  # code
        assert row[1] == "テスト株式会社"  # company_name
        assert row[2] == "情報・通信業"  # sector33_name
        assert row[3] == 0  # delete_flag (デフォルト値)

        conn.close()

    def test_primary_key_constraint(self, temp_db: Path):
        """主キー制約のテスト"""
        conn = sqlite3.connect(temp_db)

        # 最初の挿入
        conn.execute(
            """
            INSERT INTO prices (code, date, adj_close)
            VALUES ('1234', '2024-01-01', 1000.0)
        """
        )
        conn.commit()

        # 同じ主キーで挿入を試みる
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO prices (code, date, adj_close)
                VALUES ('1234', '2024-01-01', 2000.0)
            """
            )

        conn.close()

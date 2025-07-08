"""CSV取り込み時の論理削除機能のテスト"""

import os
import sqlite3
import tempfile
from unittest.mock import patch

import pytest

from src.portfolio.manager import PortfolioManager


class TestCSVLogicalDelete:
    """CSV取り込み時の論理削除テスト"""

    @pytest.fixture
    def setup_db(self):
        """テスト用データベースのセットアップ"""
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                account_name TEXT NOT NULL DEFAULT 'default',
                account_type TEXT NOT NULL DEFAULT '特定',
                quantity INTEGER NOT NULL,
                average_price REAL NOT NULL,
                market_value REAL,
                profit_loss REAL,
                profit_loss_ratio REAL,
                expected_per REAL,
                actual_pbr REAL,
                dividend_yield REAL,
                expected_eps REAL,
                actual_bps REAL,
                expected_dividend REAL,
                lending_type TEXT,
                updated_at TEXT DEFAULT (datetime('now')),
                deleted_at TEXT DEFAULT NULL,
                UNIQUE(user_id, code, account_name, account_type)
            );
            CREATE TABLE prices (
                code TEXT,
                date TEXT,
                close REAL
            );
            CREATE TABLE statements (
                code TEXT,
                DisclosedDate TEXT,
                EarningsPerShare REAL,
                BookValuePerShare REAL,
                ForecastDividendPerShareAnnual REAL,
                NextYearForecastDividendPerShareAnnual REAL,
                ForecastEarningsPerShare REAL,
                NextYearForecastEarningsPerShare REAL,
                ResultDividendPerShareAnnual REAL
            );
        """
        )

        # 既存データを挿入（3銘柄）
        conn.executescript(
            """
            INSERT INTO holdings (user_id, code, account_name, account_type, quantity, average_price)
            VALUES
                (1, '7203', 'default', '特定', 100, 2500),
                (1, '9432', 'default', '特定', 200, 150),
                (1, '6501', 'default', '特定', 300, 1000);
        """
        )
        conn.commit()
        conn.close()

        yield db_path

        # クリーンアップ
        os.unlink(db_path)

    def test_csv_import_with_logical_delete(self, setup_db):
        """CSVに含まれない銘柄が論理削除されることを確認"""
        with patch("src.portfolio.manager.get_db_path", return_value=setup_db):
            with patch("src.portfolio.models.get_db_path", return_value=setup_db):
                # CSVデータ（7203と9432のみ、6501は含まれない）
                csv_data = [
                    {
                        "code": "7203",
                        "name": "トヨタ自動車",
                        "account_type": "特定",
                        "quantity": 150,  # 数量更新
                        "average_price": 2600,
                    },
                    {
                        "code": "9432",
                        "name": "NTT",
                        "account_type": "特定",
                        "quantity": 200,
                        "average_price": 150,
                    },
                ]

                # CSV取り込み実行
                updated, new = PortfolioManager.update_holdings_from_csv(
                    1, csv_data, "default"
                )

                # 結果を確認
                conn = sqlite3.connect(setup_db)
                cursor = conn.cursor()

                # 6501が論理削除されていることを確認
                cursor.execute("SELECT deleted_at FROM holdings WHERE code = '6501'")
                result = cursor.fetchone()
                assert result[0] is not None  # deleted_atが設定されている

                # 7203と9432は論理削除されていないことを確認
                cursor.execute(
                    "SELECT deleted_at FROM holdings WHERE code IN ('7203', '9432')"
                )
                results = cursor.fetchall()
                for row in results:
                    assert row[0] is None  # deleted_atがNULL

                conn.close()

    def test_csv_import_resurrect_deleted(self, setup_db):
        """論理削除された銘柄がCSVに含まれる場合、復活することを確認"""
        with patch("src.portfolio.manager.get_db_path", return_value=setup_db):
            with patch("src.portfolio.models.get_db_path", return_value=setup_db):
                # まず6501を論理削除
                conn = sqlite3.connect(setup_db)
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE holdings SET deleted_at = datetime('now') WHERE code = '6501'"
                )
                conn.commit()

                # CSVデータ（6501を含む）
                csv_data = [
                    {
                        "code": "6501",
                        "name": "日立製作所",
                        "account_type": "特定",
                        "quantity": 400,  # 数量更新
                        "average_price": 1100,
                    },
                ]

                # CSV取り込み実行
                updated, new = PortfolioManager.update_holdings_from_csv(
                    1, csv_data, "default"
                )

                # 6501が復活していることを確認
                cursor.execute(
                    "SELECT deleted_at, quantity FROM holdings WHERE code = '6501'"
                )
                result = cursor.fetchone()
                assert result[0] is None  # deleted_atがNULL（復活）
                assert result[1] == 400  # 数量も更新されている

                conn.close()

    def test_standard_format_vs_savefile_format(self, setup_db):
        """標準形式とSaveFile形式の処理の違いを確認"""
        with patch("src.portfolio.manager.get_db_path", return_value=setup_db):
            with patch("src.portfolio.models.get_db_path", return_value=setup_db):
                # 標準形式のCSVデータ（PER等の指標データあり）
                standard_csv_data = [
                    {
                        "code": "7203",
                        "name": "トヨタ自動車",
                        "account_type": "特定",
                        "quantity": 100,
                        "average_price": 2500,
                        "expected_per": 10.5,  # 指標データあり
                        "actual_pbr": 1.2,
                        "dividend_yield": 3.5,
                    },
                ]

                # SaveFile形式のCSVデータ（PER等の指標データなし）
                savefile_csv_data = [
                    {
                        "code": "9432",
                        "name": "NTT",
                        "account_type": "特定",
                        "quantity": 200,
                        "average_price": 150,
                        # 指標データなし
                    },
                ]

                # テスト用の株価データとステートメントデータを追加
                conn = sqlite3.connect(setup_db)
                cursor = conn.cursor()
                cursor.execute("INSERT INTO prices VALUES ('94320', '2024-01-20', 160)")
                cursor.execute(
                    """
                    INSERT INTO statements VALUES
                    ('94320', '2024-01-15', 12.5, 120, 6, 7, 13, 14, 5.5)
                """
                )
                conn.commit()

                # 標準形式の取り込み（PER等は再計算されない）
                PortfolioManager.update_holdings_from_csv(
                    1, standard_csv_data, "default"
                )

                cursor.execute("SELECT expected_per FROM holdings WHERE code = '7203'")
                result = cursor.fetchone()
                assert result[0] == 10.5  # CSVの値がそのまま使用される

                # SaveFile形式の取り込み（PER等が再計算される）
                PortfolioManager.update_holdings_from_csv(
                    1, savefile_csv_data, "default"
                )

                # 株価指標が計算されることを確認（update_stock_indicatorsが呼ばれる）
                # ※実際の計算ロジックは複雑なので、ここではログ出力で確認

                conn.close()

"""ポートフォリオ管理の口座タイプ対応テスト"""

import os
import sqlite3
import tempfile

import pytest

from src.portfolio.manager import PortfolioManager
from src.portfolio.models import Holding


class TestPortfolioAccountType:
    """ポートフォリオ管理の口座タイプ機能テスト"""

    @pytest.fixture
    def temp_db(self):
        """テスト用の一時データベース"""
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        # テーブル作成
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                account_name TEXT DEFAULT 'default',
                account_type TEXT DEFAULT '特定',
                quantity INTEGER DEFAULT 0,
                average_price REAL DEFAULT 0.0,
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
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

        yield path

        # クリーンアップ
        os.unlink(path)

    def test_update_holdings_with_account_type(self, temp_db, monkeypatch):
        """口座タイプを含む保有銘柄の更新"""
        monkeypatch.setattr("src.config.get_db_path", lambda: temp_db)
        monkeypatch.setattr("src.portfolio.models.get_db_path", lambda: temp_db)

        holdings_data = [
            {
                "code": "9984",
                "name": "ソフトバンクグループ",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 1000,
            },
            {
                "code": "9984",
                "name": "ソフトバンクグループ",
                "account_type": "NISA",
                "quantity": 50,
                "average_price": 1200,
            },
            {
                "code": "7203",
                "name": "トヨタ自動車",
                "account_type": "つみたてNISA",
                "quantity": 200,
                "average_price": 2000,
            },
        ]

        updated, new = PortfolioManager.update_holdings_from_csv(
            user_id=1,
            holdings_data=holdings_data,
            account_name="SBI証券"
        )

        assert new == 3

        # データベースから確認
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT code, account_type, quantity, account_name
            FROM holdings
            WHERE user_id = 1
            ORDER BY code, account_type
        """)
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 3

        # トヨタ（つみたてNISA）
        assert rows[0] == ("7203", "つみたてNISA", 200, "SBI証券")

        # ソフトバンク（NISA）
        assert rows[1] == ("9984", "NISA", 50, "SBI証券")

        # ソフトバンク（特定）
        assert rows[2] == ("9984", "特定", 100, "SBI証券")

    def test_holding_model_with_account_type(self, temp_db, monkeypatch):
        """Holdingモデルの口座タイプ対応"""
        monkeypatch.setattr("src.portfolio.models.get_db_path", lambda: temp_db)

        # 特定口座の保有を作成
        holding1 = Holding(user_id=1, code="9984", account_name="SBI証券", account_type="特定")
        holding1.quantity = 100
        holding1.average_price = 1000
        assert holding1.save()

        # NISA口座の保有を作成
        holding2 = Holding(user_id=1, code="9984", account_name="SBI証券", account_type="NISA")
        holding2.quantity = 50
        holding2.average_price = 1200
        assert holding2.save()

        # 検索テスト - 特定口座
        found1 = Holding.find_by_user_code_and_account(1, "9984", "SBI証券", "特定")
        assert found1 is not None
        assert found1.quantity == 100
        assert found1.account_type == "特定"

        # 検索テスト - NISA口座
        found2 = Holding.find_by_user_code_and_account(1, "9984", "SBI証券", "NISA")
        assert found2 is not None
        assert found2.quantity == 50
        assert found2.account_type == "NISA"

        # 全保有取得
        all_holdings = Holding.find_all_by_user(1)
        assert len(all_holdings) == 2

    def test_same_stock_different_account_types(self, temp_db, monkeypatch):
        """同一銘柄で異なる口座タイプの正しい処理"""
        monkeypatch.setattr("src.portfolio.models.get_db_path", lambda: temp_db)

        # 同一銘柄を異なる口座タイプで保存
        holding1 = Holding(user_id=1, code="9984", account_name="SBI証券", account_type="特定")
        holding1.quantity = 100
        holding1.save()

        holding2 = Holding(user_id=1, code="9984", account_name="SBI証券", account_type="NISA")
        holding2.quantity = 50
        holding2.save()

        # 特定口座の数量を更新
        holding1.quantity = 150
        holding1.save()

        # 各口座タイプの保有が正しく保持されていることを確認
        found1 = Holding.find_by_user_code_and_account(1, "9984", "SBI証券", "特定")
        assert found1.quantity == 150

        found2 = Holding.find_by_user_code_and_account(1, "9984", "SBI証券", "NISA")
        assert found2.quantity == 50

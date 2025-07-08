"""財務情報の上書き防止機能のテスト"""

import tempfile

import pytest

from src.portfolio.models import Holding


class TestFinancialDataPreservation:
    """財務情報の上書き防止テスト"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """テスト用の一時データベースを設定"""
        import sqlite3

        # 一時データベースファイルを作成
        self.db_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".db")
        self.db_file.close()

        # テスト用DBパスを設定
        import src.config

        self._original_db_path = src.config.DB_PATH
        src.config.DB_PATH = self.db_file.name

        # テーブルを作成
        conn = sqlite3.connect(self.db_file.name)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                account_name TEXT NOT NULL DEFAULT 'default',
                account_type TEXT DEFAULT '特定',
                quantity INTEGER NOT NULL,
                average_price REAL,
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
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, code, account_name)
            )
        """
        )
        conn.commit()
        conn.close()

        yield

        # クリーンアップ
        import os

        os.unlink(self.db_file.name)
        src.config.DB_PATH = self._original_db_path

    def test_preserve_financial_data_on_update(self):
        """既存の財務情報が保持されることを確認"""
        # 初期データを作成（財務情報あり）
        holding = Holding(user_id=1, code="7203", account_name="default")
        holding.quantity = 100
        holding.average_price = 2500
        holding.actual_pbr = 1.5
        holding.dividend_yield = 2.3
        holding.expected_per = 12.5
        assert holding.save()

        # 同じ銘柄を財務情報なしで更新
        holding2 = Holding(user_id=1, code="7203", account_name="default")
        holding2.quantity = 150  # 数量を変更
        holding2.average_price = 2600  # 平均価格を変更
        # 財務情報は設定しない（None）
        assert holding2.save()

        # データベースから再読み込み
        result = Holding.find_by_user_and_code(1, "7203")
        assert result is not None

        # 基本情報は更新されている
        assert result.quantity == 150
        assert result.average_price == 2600

        # 財務情報は保持されている
        assert result.actual_pbr == 1.5
        assert result.dividend_yield == 2.3
        assert result.expected_per == 12.5

    def test_update_financial_data_with_new_values(self):
        """明示的に新しい値を設定した場合は更新される"""
        # 初期データを作成
        holding = Holding(user_id=1, code="6758", account_name="default")
        holding.quantity = 50
        holding.average_price = 13000
        holding.actual_pbr = 2.0
        holding.dividend_yield = 1.5
        assert holding.save()

        # 財務情報を新しい値で更新
        holding2 = Holding(user_id=1, code="6758", account_name="default")
        holding2.quantity = 50
        holding2.average_price = 13000
        holding2.actual_pbr = 2.5  # 新しい値
        holding2.dividend_yield = 1.8  # 新しい値
        assert holding2.save()

        # データベースから再読み込み
        result = Holding.find_by_user_and_code(1, "6758")
        assert result is not None

        # 財務情報が更新されている
        assert result.actual_pbr == 2.5
        assert result.dividend_yield == 1.8

    def test_preserve_financial_data_with_account_type(self):
        """口座タイプありでも財務情報が保持される"""
        # 初期データを作成（NISA口座）
        holding = Holding(
            user_id=1, code="9984", account_name="default", account_type="NISA"
        )
        holding.quantity = 100
        holding.average_price = 10000
        holding.actual_pbr = 3.0
        holding.expected_eps = 500
        holding.actual_bps = 3333
        assert holding.save()

        # 同じ銘柄を財務情報なしで更新
        holding2 = Holding(
            user_id=1, code="9984", account_name="default", account_type="NISA"
        )
        holding2.quantity = 200
        holding2.average_price = 10500
        # 財務情報は設定しない
        assert holding2.save()

        # データベースから再読み込み
        result = Holding.find_by_user_code_and_account(1, "9984", "default", "NISA")
        assert result is not None

        # 基本情報は更新されている
        assert result.quantity == 200
        assert result.average_price == 10500

        # 財務情報は保持されている
        assert result.actual_pbr == 3.0
        assert result.expected_eps == 500
        assert result.actual_bps == 3333

    def test_new_holding_with_null_financial_data(self):
        """新規作成時にNoneの財務情報は正しく保存される"""
        holding = Holding(user_id=1, code="1234", account_name="default")
        holding.quantity = 100
        holding.average_price = 1000
        # 財務情報は設定しない（None）
        assert holding.save()

        # データベースから再読み込み
        result = Holding.find_by_user_and_code(1, "1234")
        assert result is not None

        # 財務情報はNone
        assert result.actual_pbr is None
        assert result.dividend_yield is None
        assert result.expected_per is None

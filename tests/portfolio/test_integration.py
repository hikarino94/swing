"""ポートフォリオ管理機能の統合テスト"""

import sqlite3
import tempfile
from unittest.mock import patch

import pytest

from src.portfolio.holdings_manager import HoldingsManager
from src.portfolio.manager import PortfolioManager
from src.portfolio.models.holding import Holding
from src.portfolio.models.transaction import Transaction
from src.portfolio.transaction_manager import TransactionManager


class TestPortfolioIntegration:
    """ポートフォリオ管理機能の統合テスト"""

    @pytest.fixture(scope="function")
    def temp_db(self):
        """テスト用の一時データベース（各テストごとに新規作成）"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        # テスト用のテーブルを作成
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # holdingsテーブル
        cursor.execute(
            """
            CREATE TABLE holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                account_name TEXT NOT NULL DEFAULT 'default',
                account_type TEXT DEFAULT '特定',
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                deleted_at TIMESTAMP,
                UNIQUE(user_id, code, account_name, account_type)
            )
        """
        )

        # transactionsテーブル
        cursor.execute(
            """
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                transaction_date DATE NOT NULL,
                transaction_type TEXT NOT NULL,
                detailed_type TEXT,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL,
                commission REAL DEFAULT 0,
                tax REAL DEFAULT 0,
                total_amount REAL NOT NULL,
                realized_profit REAL,
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # pricesテーブル（時価評価用）
        cursor.execute(
            """
            CREATE TABLE prices (
                code TEXT NOT NULL,
                date DATE NOT NULL,
                close REAL NOT NULL,
                PRIMARY KEY (code, date)
            )
        """
        )

        # listed_infoテーブル（会社名取得用）
        cursor.execute(
            """
            CREATE TABLE listed_info (
                code TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                delete_flag BOOLEAN DEFAULT 0
            )
        """
        )

        conn.commit()
        conn.close()

        yield db_path

        # クリーンアップ
        import os

        os.unlink(db_path)

    @patch("src.config.get_db_path")
    def test_holdings_crud_operations(self, mock_get_db_path, temp_db):
        """保有銘柄のCRUD操作の統合テスト"""
        mock_get_db_path.return_value = temp_db
        print(f"Test DB Path: {temp_db}")

        # 1. 新規保有銘柄の作成
        holding = Holding(
            user_id=1,
            code="1234",
            account_name="default",
            account_type="特定",
            quantity=100,
            average_price=1000.0,
        )

        # 保存
        assert holding.save() is True
        assert holding.id is not None

        # 2. 検索
        found = Holding.find_by_user_code_and_account(
            user_id=1, code="1234", account_name="default", account_type="特定"
        )
        assert found is not None
        assert found.quantity == 100
        assert found.average_price == 1000.0

        # 3. 更新
        found.quantity = 150
        found.average_price = 1100.0
        assert found.save() is True

        # 4. 再検索して更新を確認
        updated = Holding.find_by_user_code_and_account(
            user_id=1, code="1234", account_name="default", account_type="特定"
        )
        assert updated.quantity == 150
        assert updated.average_price == 1100.0

        # 5. 全件取得
        all_holdings = Holding.find_all_by_user(user_id=1)
        print(f"Found {len(all_holdings)} holdings:")
        for h in all_holdings:
            print(f"  - Code: {h.code}, Quantity: {h.quantity}, User: {h.user_id}")
        assert len(all_holdings) == 1
        assert all_holdings[0].code == "1234"

    @patch("src.config.get_db_path")
    def test_transaction_to_holdings_flow(self, mock_get_db_path, temp_db):
        """取引履歴から保有銘柄への反映フロー"""
        mock_get_db_path.return_value = temp_db

        # 1. 取引履歴を作成
        transactions_data = [
            {
                "user_id": 1,
                "code": "5678",
                "transaction_date": "2024-01-01",
                "transaction_type": "buy",
                "quantity": 100,
                "price": 2000.0,
                "commission": 200,
                "total_amount": 200200.0,
            },
            {
                "user_id": 1,
                "code": "5678",
                "transaction_date": "2024-01-15",
                "transaction_type": "buy",
                "quantity": 50,
                "price": 2100.0,
                "commission": 100,
                "total_amount": 105100.0,
            },
            {
                "user_id": 1,
                "code": "5678",
                "transaction_date": "2024-02-01",
                "transaction_type": "sell",
                "quantity": 30,
                "price": 2200.0,
                "commission": 100,
                "total_amount": 65900.0,
            },
        ]

        # 一括挿入
        inserted = Transaction.bulk_insert(transactions_data)
        assert inserted == 3

        # 2. 保有銘柄を再計算
        TransactionManager.recalculate_holdings(user_id=1)

        # 3. 保有銘柄を確認
        holding = Holding.find_by_user_and_code(user_id=1, code="5678")
        assert holding is not None
        assert holding.quantity == 120  # 100 + 50 - 30
        # 平均取得価格の計算: (200200 + 105100) / 150 = 2035.33...
        # 売却後: 残り120株の取得コスト = 2035.33 * 120 = 244240
        assert holding.average_price == pytest.approx(2035.33, rel=1e-2)

    @patch("src.config.get_db_path")
    def test_portfolio_manager_csv_import(self, mock_get_db_path, temp_db):
        """PortfolioManager経由のCSVインポートテスト"""
        mock_get_db_path.return_value = temp_db

        # CSVデータ（標準形式）
        holdings_data = [
            {
                "code": "1111",
                "quantity": 100,
                "average_price": 1000.0,
                "expected_per": 15.5,
                "actual_pbr": 1.2,
                "dividend_yield": 2.5,
                "is_fund": False,
                "account_type": "特定",
            },
            {
                "code": "2222",
                "quantity": 200,
                "average_price": 2000.0,
                "expected_per": 20.0,
                "actual_pbr": 1.5,
                "dividend_yield": 3.0,
                "is_fund": False,
                "account_type": "NISA",
            },
        ]

        # FundManagerとIndicatorsManagerをモック
        with patch("src.portfolio.manager.FundManager") as mock_fund_manager:
            with patch("src.portfolio.manager.IndicatorsManager"):
                mock_fund_manager.update_funds_from_csv.return_value = (0, 0)
                mock_fund_manager.delete_funds_not_in_csv.return_value = 0

                # インポート実行
                updated, new = PortfolioManager.update_holdings_from_csv(
                    user_id=1, holdings_data=holdings_data
                )

                # 検証
                assert updated == 0
                assert new == 2

                # 保有銘柄を確認
                holdings = Holding.find_all_by_user(user_id=1)
                assert len(holdings) == 2

                # 銘柄1の確認
                holding1 = next(h for h in holdings if h.code == "1111")
                assert holding1.quantity == 100
                assert holding1.expected_per == 15.5
                assert holding1.account_type == "特定"

                # 銘柄2の確認
                holding2 = next(h for h in holdings if h.code == "2222")
                assert holding2.quantity == 200
                assert holding2.expected_per == 20.0
                assert holding2.account_type == "NISA"

    @patch("src.config.get_db_path")
    def test_market_value_update(self, mock_get_db_path, temp_db):
        """時価評価更新のテスト"""
        mock_get_db_path.return_value = temp_db

        # 別のユーザーIDを使用
        test_user_id = 50

        # 1. 保有銘柄を作成
        holding = Holding(
            user_id=test_user_id, code="9999", quantity=100, average_price=1000.0
        )
        holding.save()

        # 2. 株価データを挿入
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO prices (code, date, close) VALUES (?, ?, ?)",
            ("99990", "2024-01-10", 1200.0),  # 5桁コード
        )
        conn.commit()

        # デバッグ: 挿入されたデータを確認
        cursor.execute("SELECT * FROM prices")
        print("Prices data:", cursor.fetchall())

        conn.close()

        # 3. 時価評価を更新
        updated_count = HoldingsManager.update_market_values(user_id=test_user_id)
        assert updated_count == 1

        # 4. 更新後の値を確認
        updated_holding = Holding.find_by_user_and_code(
            user_id=test_user_id, code="9999"
        )
        assert updated_holding.market_value == 120000.0  # 100 * 1200
        assert updated_holding.profit_loss == 20000.0  # 120000 - 100000
        assert updated_holding.profit_loss_ratio == 20.0  # 20%

    @patch("src.config.get_db_path")
    def test_holdings_deletion_recovery(self, mock_get_db_path, temp_db):
        """保有銘柄の論理削除と復活のテスト"""
        mock_get_db_path.return_value = temp_db

        # 別のユーザーIDを使用してアイソレーション
        test_user_id = 100

        # 1. 保有銘柄を作成
        holdings_data = [
            {
                "code": "3333",
                "quantity": 100,
                "average_price": 3000.0,
                "is_fund": False,
                "account_type": "特定",
            }
        ]

        with patch("src.portfolio.manager.FundManager") as mock_fund:
            mock_fund.update_funds_from_csv.return_value = (0, 0)
            mock_fund.delete_funds_not_in_csv.return_value = 0

            # 初回インポート
            PortfolioManager.update_holdings_from_csv(
                user_id=test_user_id, holdings_data=holdings_data
            )

        # 2. CSVから除外して論理削除
        with patch("src.portfolio.manager.FundManager") as mock_fund:
            mock_fund.update_funds_from_csv.return_value = (0, 0)
            mock_fund.delete_funds_not_in_csv.return_value = 0

            # 空のデータでインポート（論理削除される）
            PortfolioManager.update_holdings_from_csv(
                user_id=test_user_id, holdings_data=[], account_name="default"
            )

        # 論理削除されたことを確認（find_all_by_userは数量>0のものしか返さないため、削除処理を直接確認）
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM holdings WHERE user_id=? AND deleted_at IS NOT NULL",
            (test_user_id,),
        )
        deleted_count = cursor.fetchone()[0]
        conn.close()
        assert deleted_count == 1  # 1件論理削除されている

        # 3. 再度インポートして復活
        with patch("src.portfolio.manager.FundManager") as mock_fund:
            mock_fund.update_funds_from_csv.return_value = (0, 0)
            mock_fund.delete_funds_not_in_csv.return_value = 0

            updated, new = PortfolioManager.update_holdings_from_csv(
                user_id=test_user_id, holdings_data=holdings_data
            )

        # 復活したことを確認
        holdings = Holding.find_all_by_user(user_id=test_user_id)
        assert len(holdings) == 1
        assert holdings[0].code == "3333"

    @patch("src.config.get_db_path")
    def test_transaction_with_company_name(self, mock_get_db_path, temp_db):
        """会社名を含む取引履歴の取得テスト"""
        mock_get_db_path.return_value = temp_db

        # 1. 会社情報を登録
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO listed_info (code, company_name) VALUES (?, ?)",
            ("44440", "テスト株式会社"),  # 5桁コード
        )
        conn.commit()
        conn.close()

        # 2. 取引を作成（異なるユーザーIDを使用してアイソレーション）
        transaction = Transaction(
            user_id=999,  # 他のテストと被らないユーザーID
            code="4444",
            transaction_date="2024-01-01",
            transaction_type="buy",
            quantity=100,
            price=4000.0,
        )
        transaction.save()

        # 3. 取引履歴を取得
        transactions = Transaction.find_all_by_user(user_id=999)
        assert len(transactions) == 1
        assert transactions[0].company_name == "テスト株式会社"

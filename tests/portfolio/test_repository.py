"""portfolio.repositoryのテスト"""

from unittest.mock import MagicMock, patch

import pytest

from src.portfolio.models import Holding, Transaction
from src.portfolio.repository import PortfolioRepository


class TestGetHolding:
    """get_holdingメソッドのテスト"""

    @patch("src.portfolio.repository.get_db_connection")
    def test_get_holding_found(self, mock_get_db_connection):
        """保有銘柄が見つかった場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor

        # DBから返されるデータ
        mock_row = (
            1,  # id
            123,  # user_id
            "1234",  # code
            "特定口座",  # account_name
            "specific",  # account_type
            100,  # quantity
            1000.0,  # average_price
            110000.0,  # market_value
            10000.0,  # profit_loss
            10.0,  # profit_loss_ratio
            15.0,  # expected_per
            1.2,  # actual_pbr
            2.5,  # dividend_yield
            70.0,  # expected_eps
            900.0,  # actual_bps
            25.0,  # expected_dividend
            None,  # lending_type
            "2024-01-01 00:00:00",  # created_at
            "2024-01-15 00:00:00",  # updated_at
            None,  # deleted_at
        )
        mock_cursor.fetchone.return_value = mock_row
        mock_cursor.description = [
            ("id",),
            ("user_id",),
            ("code",),
            ("account_name",),
            ("account_type",),
            ("quantity",),
            ("average_price",),
            ("market_value",),
            ("profit_loss",),
            ("profit_loss_ratio",),
            ("expected_per",),
            ("actual_pbr",),
            ("dividend_yield",),
            ("expected_eps",),
            ("actual_bps",),
            ("expected_dividend",),
            ("lending_type",),
            ("created_at",),
            ("updated_at",),
            ("deleted_at",),
        ]

        # テスト実行
        result = PortfolioRepository.get_holding(123, "1234", "特定口座", "specific")

        # 検証
        assert isinstance(result, Holding)
        assert result.user_id == 123
        assert result.code == "1234"
        assert result.account_name == "特定口座"
        assert result.quantity == 100
        assert result.average_price == 1000.0

        # SQL実行の確認
        mock_conn.execute.assert_called_once()
        sql = mock_conn.execute.call_args[0][0]
        params = mock_conn.execute.call_args[0][1]
        assert "SELECT * FROM holdings" in sql
        assert "deleted_at IS NULL" in sql
        assert params == (123, "1234", "特定口座", "specific")

    @patch("src.portfolio.repository.get_db_connection")
    def test_get_holding_not_found(self, mock_get_db_connection):
        """保有銘柄が見つからない場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None

        # テスト実行
        result = PortfolioRepository.get_holding(123, "9999", "特定口座", "specific")

        # 検証
        assert result is None


class TestUpsertHolding:
    """upsert_holdingメソッドのテスト"""

    @patch("src.portfolio.repository.PortfolioRepository.get_holding")
    @patch("src.portfolio.repository.get_db_connection")
    def test_upsert_holding_new(self, mock_get_db_connection, mock_get_holding):
        """新規保有銘柄の追加テスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_get_holding.return_value = None  # 既存なし

        # テストデータ
        holding = Holding(
            user_id=123,
            code="1234",
            account_name="特定口座",
            account_type="specific",
            quantity=100,
            average_price=1000.0,
            market_value=110000.0,
            profit_loss=10000.0,
            profit_loss_ratio=10.0,
        )

        # テスト実行
        is_updated, is_new = PortfolioRepository.upsert_holding(holding)

        # 検証
        assert is_updated is False
        assert is_new is True

        # INSERT文の実行確認
        mock_conn.execute.assert_called()
        sql = mock_conn.execute.call_args[0][0]
        assert "INSERT INTO holdings" in sql
        mock_conn.commit.assert_called_once()

    @patch("src.portfolio.repository.PortfolioRepository.get_holding")
    @patch("src.portfolio.repository.get_db_connection")
    def test_upsert_holding_update(self, mock_get_db_connection, mock_get_holding):
        """既存保有銘柄の更新テスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # 既存のホールディング
        existing_holding = Holding(
            id=1,
            user_id=123,
            code="1234",
            account_name="特定口座",
            account_type="specific",
            quantity=50,
            average_price=900.0,
        )
        mock_get_holding.return_value = existing_holding

        # 更新データ
        holding = Holding(
            user_id=123,
            code="1234",
            account_name="特定口座",
            account_type="specific",
            quantity=100,
            average_price=1000.0,
            market_value=110000.0,
            profit_loss=10000.0,
            profit_loss_ratio=10.0,
        )

        # テスト実行
        is_updated, is_new = PortfolioRepository.upsert_holding(holding)

        # 検証
        assert is_updated is True
        assert is_new is False

        # UPDATE文の実行確認
        mock_conn.execute.assert_called()
        sql = mock_conn.execute.call_args[0][0]
        assert "UPDATE holdings" in sql
        assert "updated_at = datetime('now')" in sql
        mock_conn.commit.assert_called_once()

    @patch("src.portfolio.repository.PortfolioRepository.get_holding")
    @patch("src.portfolio.repository.get_db_connection")
    def test_upsert_holding_exception(self, mock_get_db_connection, mock_get_holding):
        """例外発生時のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_get_holding.return_value = None
        mock_conn.execute.side_effect = Exception("DB Error")

        # テストデータ
        holding = Holding(
            user_id=123,
            code="1234",
            account_name="特定口座",
            account_type="specific",
            quantity=100,
        )

        # テスト実行（例外が発生することを確認）
        with pytest.raises(Exception, match="DB Error"):
            PortfolioRepository.upsert_holding(holding)


class TestSoftDeleteHolding:
    """soft_delete_holdingメソッドのテスト"""

    @patch("src.portfolio.repository.get_db_connection")
    def test_soft_delete_holding_success(self, mock_get_db_connection):
        """保有銘柄の削除成功テスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # テスト実行
        result = PortfolioRepository.soft_delete_holding(
            123, "1234", "特定口座", "specific"
        )

        # 検証
        assert result is True

        # SQL実行の確認
        mock_conn.execute.assert_called_once()
        sql = mock_conn.execute.call_args[0][0]
        params = mock_conn.execute.call_args[0][1]
        assert "UPDATE holdings" in sql
        assert "SET deleted_at = datetime('now')" in sql
        assert params == (123, "1234", "特定口座", "specific")
        mock_conn.commit.assert_called_once()

    @patch("src.portfolio.repository.get_db_connection")
    def test_soft_delete_holding_not_found(self, mock_get_db_connection):
        """削除対象が見つからない場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor
        mock_cursor.rowcount = 0

        # テスト実行
        result = PortfolioRepository.soft_delete_holding(
            123, "9999", "特定口座", "specific"
        )

        # 検証
        assert result is False


class TestGetHoldings:
    """get_holdingsメソッドのテスト"""

    @patch("src.portfolio.repository.get_db_connection")
    def test_get_holdings_all(self, mock_get_db_connection):
        """全保有銘柄取得のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor

        # DBから返されるデータ
        mock_rows = [
            (
                1,
                123,
                "1234",
                "特定口座",
                "specific",
                100,
                1000.0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "2024-01-01",
                "2024-01-01",
                None,
                "会社A",
                "東証プライム",
            ),
            (
                2,
                123,
                "5678",
                "NISA",
                "nisa",
                50,
                2000.0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "2024-01-01",
                "2024-01-01",
                None,
                "会社B",
                "東証グロース",
            ),
        ]
        mock_cursor.__iter__ = lambda self: iter(mock_rows)
        mock_cursor.description = [
            ("id",),
            ("user_id",),
            ("code",),
            ("account_name",),
            ("account_type",),
            ("quantity",),
            ("average_price",),
            ("market_value",),
            ("profit_loss",),
            ("profit_loss_ratio",),
            ("expected_per",),
            ("actual_pbr",),
            ("dividend_yield",),
            ("expected_eps",),
            ("actual_bps",),
            ("expected_dividend",),
            ("lending_type",),
            ("created_at",),
            ("updated_at",),
            ("deleted_at",),
            ("company_name",),
            ("market_code",),
        ]

        # テスト実行
        results = PortfolioRepository.get_holdings(123)

        # 検証
        assert len(results) == 2
        assert all(isinstance(h, dict) for h in results)
        assert results[0]["code"] == "1234"
        assert results[1]["code"] == "5678"

        # SQL実行の確認
        mock_conn.execute.assert_called_once()
        sql = mock_conn.execute.call_args[0][0]
        assert "holdings h" in sql
        assert "LEFT JOIN listed_info" in sql
        assert "deleted_at IS NULL" in sql


class TestInsertTransaction:
    """insert_transactionメソッドのテスト"""

    @patch("src.portfolio.repository.get_db_connection")
    def test_insert_transaction_success(self, mock_get_db_connection):
        """取引追加成功のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor
        mock_cursor.lastrowid = 999

        # テストデータ
        transaction = Transaction(
            user_id=123,
            code="1234",
            transaction_date="2024-01-15",
            transaction_type="buy",
            quantity=100,
            price=1000.0,
        )
        # 追加属性を設定
        transaction.commission = 100.0
        transaction.tax = 50.0
        transaction.total_amount = 100150.0

        # テスト実行
        transaction_id = PortfolioRepository.insert_transaction(transaction)

        # 検証
        assert transaction_id == 999

        # SQL実行の確認
        mock_conn.execute.assert_called_once()
        sql = mock_conn.execute.call_args[0][0]
        assert "INSERT INTO transactions" in sql
        mock_conn.commit.assert_called_once()


class TestGetTransactions:
    """get_transactionsメソッドのテスト"""

    @patch("src.portfolio.repository.get_db_connection")
    def test_get_transactions_with_filters(self, mock_get_db_connection):
        """フィルター付き取引取得のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor

        # DBから返されるデータ
        mock_rows = [
            (
                1,
                123,
                "1234",
                "テスト銘柄",
                "特定口座",
                "specific",
                "2024-01-15",
                "buy",
                100,
                1000.0,
                100.0,
                50.0,
                100150.0,
                None,
                "2024-01-15",
                None,
            ),
        ]
        mock_cursor.__iter__ = lambda self: iter(mock_rows)
        mock_cursor.description = [
            ("id",),
            ("user_id",),
            ("code",),
            ("name",),
            ("account_name",),
            ("account_type",),
            ("trade_date",),
            ("transaction_type",),
            ("quantity",),
            ("price",),
            ("fees",),
            ("taxes",),
            ("net_amount",),
            ("memo",),
            ("created_at",),
            ("deleted_at",),
        ]

        # テスト実行
        results = PortfolioRepository.get_transactions(
            user_id=123, code="1234", start_date="2024-01-01", end_date="2024-01-31"
        )

        # 検証
        assert len(results) == 1
        assert isinstance(results[0], dict)
        assert results[0]["code"] == "1234"
        assert results[0]["transaction_type"] == "buy"

        # SQL実行の確認
        sql = mock_conn.execute.call_args[0][0]
        params = mock_conn.execute.call_args[0][1]
        assert "SELECT t.*, li.company_name" in sql
        assert "FROM transactions t" in sql
        assert "t.code = ?" in sql
        assert "t.transaction_date >= ?" in sql
        assert "t.transaction_date <= ?" in sql
        assert len(params) == 4  # user_id, code, start_date, end_date

    @patch("src.portfolio.repository.get_db_connection")
    def test_get_transactions_no_filters(self, mock_get_db_connection):
        """フィルターなし取引取得のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = []

        # テスト実行
        results = PortfolioRepository.get_transactions(user_id=123)

        # 検証
        assert results == []

        # SQL実行の確認
        sql = mock_conn.execute.call_args[0][0]
        params = mock_conn.execute.call_args[0][1]
        assert "SELECT t.*, li.company_name" in sql
        assert params == [123]  # user_idのみ


class TestGetPortfolioSummary:
    """get_portfolio_summaryメソッドのテスト"""

    @patch("src.portfolio.repository.get_db_connection")
    def test_get_portfolio_summary(self, mock_get_db_connection):
        """ポートフォリオサマリー取得のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_cursor

        # DBから返されるデータ
        mock_rows = [
            ("特定口座", "specific", 3, 300000.0, 20000.0),
            ("NISA", "nisa", 2, 200000.0, 15000.0),
        ]
        mock_cursor.fetchall.return_value = iter(mock_rows)

        # テスト実行
        result = PortfolioRepository.get_portfolio_summary(123)

        # 検証
        assert "accounts" in result
        assert len(result["accounts"]) == 2
        assert result["accounts"][0]["account_name"] == "特定口座"
        assert result["accounts"][0]["stock_count"] == 3
        assert result["total_value"] == 500000.0
        assert result["total_profit_loss"] == 35000.0
        assert result["profit_loss_ratio"] == pytest.approx(7.5269, rel=1e-3)

        # SQL実行の確認
        sql = mock_conn.execute.call_args[0][0]
        assert "account_name" in sql
        assert "COUNT(DISTINCT code)" in sql
        assert "SUM(market_value)" in sql
        assert "GROUP BY account_name, account_type" in sql

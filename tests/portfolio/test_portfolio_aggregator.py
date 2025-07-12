"""PortfolioAggregatorクラスのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.portfolio.portfolio_aggregator import PortfolioAggregator


class TestPortfolioAggregator:
    """PortfolioAggregatorクラスのテスト"""

    @pytest.fixture
    def mock_connect(self):
        """データベース接続のモック"""
        with patch("src.portfolio.portfolio_aggregator.sqlite3.connect") as mock:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock.return_value = mock_conn
            yield mock, mock_conn, mock_cursor

    def test_aggregate_holdings_by_code_single_account(self, mock_connect):
        """単一口座の保有銘柄集約テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [
            (
                "1234",  # code
                "テスト株式会社",  # company_name
                100,  # total_quantity
                1500.0,  # weighted_avg_price
                155000.0,  # total_market_value
                5000.0,  # total_profit_loss
                1,  # account_count
                "default",  # account_names
                "特定",  # account_types
                15.5,  # expected_per
                1.2,  # actual_pbr
                2.5,  # dividend_yield
                100.0,  # expected_eps
                1250.0,  # actual_bps
                37.5,  # expected_dividend
                "一般貸",  # lending_type
            ),
        ]

        result = PortfolioAggregator.aggregate_holdings_by_code(user_id=1)

        assert len(result) == 1
        holding = result[0]
        assert holding["type"] == "stock"
        assert holding["code"] == "1234"
        assert holding["company_name"] == "テスト株式会社"
        assert holding["total_quantity"] == 100
        assert holding["weighted_avg_price"] == 1500.0
        assert holding["total_market_value"] == 155000.0
        assert holding["total_profit_loss"] == 5000.0
        assert holding["account_count"] == 1
        assert holding["account_names"] == "default"
        assert holding["profit_loss_ratio"] == pytest.approx(3.33, rel=0.01)
        assert holding["expected_per"] == 15.5
        assert holding["actual_pbr"] == 1.2
        assert holding["dividend_yield"] == 2.5
        mock_conn.close.assert_called_once()

    def test_aggregate_holdings_by_code_multiple_accounts(self, mock_connect):
        """複数口座の保有銘柄集約テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [
            (
                "5678",  # code
                "複数口座株式会社",  # company_name
                300,  # total_quantity (100+200)
                2000.0,  # weighted_avg_price
                660000.0,  # total_market_value
                60000.0,  # total_profit_loss
                2,  # account_count
                "特定,NISA",  # account_names
                "特定,NISA",  # account_types
                18.0,  # expected_per
                1.5,  # actual_pbr
                3.0,  # dividend_yield
                120.0,  # expected_eps
                1450.0,  # actual_bps
                60.0,  # expected_dividend
                None,  # lending_type
            ),
        ]

        result = PortfolioAggregator.aggregate_holdings_by_code(user_id=1)

        assert len(result) == 1
        holding = result[0]
        assert holding["total_quantity"] == 300
        assert holding["account_count"] == 2
        assert holding["account_names"] == "特定,NISA"
        assert holding["profit_loss_ratio"] == pytest.approx(10.0, rel=0.01)
        mock_conn.close.assert_called_once()

    def test_aggregate_holdings_by_code_no_holdings(self, mock_connect):
        """保有銘柄がない場合のテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = []

        result = PortfolioAggregator.aggregate_holdings_by_code(user_id=1)

        assert result == []
        mock_conn.close.assert_called_once()

    def test_aggregate_holdings_by_code_zero_quantity_cost(self, mock_connect):
        """数量ゼロまたはコストゼロの場合の処理テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [
            (
                "9999",
                "ゼロ株式会社",
                100,
                0,  # weighted_avg_price が 0
                10000.0,
                10000.0,
                1,
                "default",
                "特定",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ]

        result = PortfolioAggregator.aggregate_holdings_by_code(user_id=1)

        assert len(result) == 1
        holding = result[0]
        assert holding["weighted_avg_price"] == 0
        assert holding["profit_loss_ratio"] == 0  # ゼロ除算を避ける
        mock_conn.close.assert_called_once()

    def test_aggregate_holdings_by_code_null_values(self, mock_connect):
        """NULL値を含むデータの処理テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [
            (
                "1111",
                None,  # company_name がNULL
                100,
                1000.0,
                None,  # market_value がNULL
                None,  # profit_loss がNULL
                1,
                "default",
                "特定",
                None,  # 以下、株価指標がすべてNULL
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ]

        result = PortfolioAggregator.aggregate_holdings_by_code(user_id=1)

        assert len(result) == 1
        holding = result[0]
        assert holding["company_name"] is None
        assert holding["total_market_value"] is None
        assert holding["total_profit_loss"] is None
        assert holding["expected_per"] is None
        assert holding["actual_pbr"] is None
        mock_conn.close.assert_called_once()

    def test_get_portfolio_summary_with_data(self, mock_connect):
        """ポートフォリオサマリー取得の成功テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchone.side_effect = [
            # 株式の集計結果
            (10, 1500000.0, 1650000.0, 150000.0),
            # 投資信託の集計結果
            (5, 500000.0, 550000.0, 50000.0),
            # 取引履歴の集計結果
            (100, "2023-01-01", "2024-01-10"),
        ]

        result = PortfolioAggregator.get_portfolio_summary(user_id=1)

        assert result["stock_count"] == 15  # 10 + 5
        assert result["total_cost"] == 2000000.0  # 1500000 + 500000
        assert result["total_market_value"] == 2200000.0  # 1650000 + 550000
        assert result["total_profit_loss"] == 200000.0  # 150000 + 50000
        assert result["total_profit_loss_ratio"] == pytest.approx(10.0, rel=0.01)
        assert result["transaction_count"] == 100
        assert result["first_transaction_date"] == "2023-01-01"
        assert result["last_transaction_date"] == "2024-01-10"
        mock_conn.close.assert_called_once()

    def test_get_portfolio_summary_no_data(self, mock_connect):
        """データがない場合のポートフォリオサマリーテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchone.side_effect = [
            # 株式の集計結果（データなし）
            (0, None, None, None),
            # 投資信託の集計結果（データなし）
            (0, None, None, None),
            # 取引履歴の集計結果（データなし）
            (0, None, None),
        ]

        result = PortfolioAggregator.get_portfolio_summary(user_id=1)

        assert result["stock_count"] == 0
        assert result["total_cost"] == 0
        assert result["total_market_value"] == 0
        assert result["total_profit_loss"] == 0
        assert result["total_profit_loss_ratio"] == 0
        assert result["transaction_count"] == 0
        assert result["first_transaction_date"] is None
        assert result["last_transaction_date"] is None
        mock_conn.close.assert_called_once()

    def test_get_portfolio_summary_partial_data(self, mock_connect):
        """一部のデータのみある場合のテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchone.side_effect = [
            # 株式のみデータあり
            (5, 1000000.0, 1100000.0, 100000.0),
            # 投資信託はデータなし
            (0, None, None, None),
            # 取引履歴あり
            (50, "2023-06-01", "2024-01-10"),
        ]

        result = PortfolioAggregator.get_portfolio_summary(user_id=1)

        assert result["stock_count"] == 5
        assert result["total_cost"] == 1000000.0
        assert result["total_market_value"] == 1100000.0
        assert result["total_profit_loss"] == 100000.0
        assert result["total_profit_loss_ratio"] == pytest.approx(10.0, rel=0.01)
        assert result["transaction_count"] == 50
        mock_conn.close.assert_called_once()

    def test_get_portfolio_summary_zero_cost(self, mock_connect):
        """コストがゼロの場合の損益率計算テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchone.side_effect = [
            # 株式（コストゼロ）
            (1, 0, 50000.0, 50000.0),
            # 投資信託
            (0, None, None, None),
            # 取引履歴
            (0, None, None),
        ]

        result = PortfolioAggregator.get_portfolio_summary(user_id=1)

        assert result["total_cost"] == 0
        assert result["total_market_value"] == 50000.0
        assert result["total_profit_loss"] == 50000.0
        assert result["total_profit_loss_ratio"] == 0  # ゼロ除算を避ける
        mock_conn.close.assert_called_once()

    def test_aggregate_holdings_by_code_database_error(self, mock_connect):
        """集約時のデータベースエラーテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.execute.side_effect = sqlite3.Error("Database error")

        with pytest.raises(sqlite3.Error):
            PortfolioAggregator.aggregate_holdings_by_code(user_id=1)

        mock_conn.close.assert_called_once()

    def test_get_portfolio_summary_database_error(self, mock_connect):
        """サマリー取得時のデータベースエラーテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.execute.side_effect = sqlite3.Error("Database error")

        with pytest.raises(sqlite3.Error):
            PortfolioAggregator.get_portfolio_summary(user_id=1)

        mock_conn.close.assert_called_once()

    def test_aggregate_holdings_by_code_multiple_rows(self, mock_connect):
        """複数銘柄の集約テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [
            (
                "1234",
                "株式A",
                100,
                1000.0,
                110000.0,
                10000.0,
                1,
                "default",
                "特定",
                15.0,
                1.2,
                2.0,
                66.7,
                833.3,
                20.0,
                None,
            ),
            (
                "5678",
                "株式B",
                200,
                2000.0,
                420000.0,
                20000.0,
                2,
                "default,NISA",
                "特定,NISA",
                20.0,
                1.5,
                3.0,
                100.0,
                1333.3,
                60.0,
                "一般貸",
            ),
            (
                "9012",
                "株式C",
                50,
                3000.0,
                145000.0,
                -5000.0,
                1,
                "NISA",
                "NISA",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ]

        result = PortfolioAggregator.aggregate_holdings_by_code(user_id=1)

        assert len(result) == 3
        assert result[0]["code"] == "1234"
        assert result[1]["code"] == "5678"
        assert result[2]["code"] == "9012"
        assert result[2]["total_profit_loss"] == -5000.0  # 損失の場合
        assert result[2]["profit_loss_ratio"] == pytest.approx(-3.33, rel=0.01)
        mock_conn.close.assert_called_once()

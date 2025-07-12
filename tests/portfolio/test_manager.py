"""src.portfolio.manager モジュールのテスト"""

from unittest.mock import patch

from src.portfolio.manager import PortfolioManager


class TestPortfolioManagerUpdateHoldingsFromCSV:
    """PortfolioManager.update_holdings_from_csv のテスト"""

    @patch("src.portfolio.manager.HoldingsManager.update_holdings_from_csv")
    @patch("src.portfolio.manager.FundManager.update_funds_from_csv")
    @patch("src.portfolio.manager.HoldingsManager.delete_stocks_not_in_csv")
    @patch("src.portfolio.manager.FundManager.delete_funds_not_in_csv")
    @patch("src.portfolio.manager.IndicatorsManager.get_codes_needing_update")
    @patch("src.portfolio.manager.IndicatorsManager.update_stock_indicators")
    def test_update_holdings_standard_format(
        self,
        mock_update_indicators,
        mock_get_codes_needing,
        mock_delete_funds,
        mock_delete_stocks,
        mock_update_funds,
        mock_update_holdings,
    ):
        """標準形式のCSVデータ更新テスト"""
        # モックの設定
        mock_update_holdings.return_value = (2, 1, True)  # 更新2件、新規1件、標準形式
        mock_update_funds.return_value = (1, 0)  # 更新1件、新規0件
        mock_delete_stocks.return_value = 1  # 1件削除
        mock_delete_funds.return_value = 0  # 0件削除

        # テストデータ
        holdings_data = [
            {"code": "1234", "quantity": 100, "is_fund": False},
            {"code": "FUND001", "quantity": 10000, "is_fund": True},
        ]

        # 実行
        updated, new = PortfolioManager.update_holdings_from_csv(
            user_id=1, holdings_data=holdings_data, account_name="default"
        )

        # 検証
        assert updated == 3  # 株式2件 + 投資信託1件
        assert new == 1  # 株式1件
        mock_update_holdings.assert_called_once_with(1, holdings_data, "default")
        mock_update_funds.assert_called_once_with(1, holdings_data, "default")
        mock_delete_stocks.assert_called_once_with(1, holdings_data, "default")
        mock_delete_funds.assert_called_once_with(1, holdings_data, "default")
        # 標準形式なので株価指標の再計算は行われない
        mock_get_codes_needing.assert_not_called()
        mock_update_indicators.assert_not_called()

    @patch("src.portfolio.manager.HoldingsManager.update_holdings_from_csv")
    @patch("src.portfolio.manager.FundManager.update_funds_from_csv")
    @patch("src.portfolio.manager.HoldingsManager.delete_stocks_not_in_csv")
    @patch("src.portfolio.manager.FundManager.delete_funds_not_in_csv")
    @patch("src.portfolio.manager.IndicatorsManager.get_codes_needing_update")
    @patch("src.portfolio.manager.IndicatorsManager.update_stock_indicators")
    def test_update_holdings_savefile_format(
        self,
        mock_update_indicators,
        mock_get_codes_needing,
        mock_delete_funds,
        mock_delete_stocks,
        mock_update_funds,
        mock_update_holdings,
    ):
        """SaveFile形式のCSVデータ更新テスト"""
        # モックの設定
        mock_update_holdings.return_value = (
            1,
            2,
            False,
        )  # 更新1件、新規2件、SaveFile形式
        mock_update_funds.return_value = (0, 0)  # 更新0件、新規0件
        mock_delete_stocks.return_value = 0
        mock_delete_funds.return_value = 0
        mock_get_codes_needing.return_value = ["1234", "5678"]
        mock_update_indicators.return_value = 2

        # テストデータ
        holdings_data = [
            {"code": "1234", "quantity": 100, "is_fund": False},
            {"code": "5678", "quantity": 200, "is_fund": False},
        ]

        # 実行
        updated, new = PortfolioManager.update_holdings_from_csv(
            user_id=1, holdings_data=holdings_data
        )

        # 検証
        assert updated == 1
        assert new == 2
        # SaveFile形式なので株価指標の再計算が行われる
        mock_get_codes_needing.assert_called_once_with(1)
        mock_update_indicators.assert_called_once_with(1, ["1234", "5678"])

    @patch("src.portfolio.manager.HoldingsManager.update_holdings_from_csv")
    @patch("src.portfolio.manager.FundManager.update_funds_from_csv")
    def test_update_holdings_empty_data(self, mock_update_funds, mock_update_holdings):
        """空データの処理テスト"""
        mock_update_holdings.return_value = (0, 0, False)
        mock_update_funds.return_value = (0, 0)

        # 空のリスト
        updated, new = PortfolioManager.update_holdings_from_csv(
            user_id=1, holdings_data=[]
        )

        # 検証
        assert updated == 0
        assert new == 0
        mock_update_holdings.assert_called_once_with(1, [], "default")
        mock_update_funds.assert_called_once_with(1, [], "default")


class TestPortfolioManagerTransactions:
    """取引履歴関連メソッドのテスト"""

    @patch("src.portfolio.manager.TransactionManager.import_transactions_from_csv")
    def test_import_transactions_from_csv(self, mock_import):
        """取引履歴インポートのテスト"""
        mock_import.return_value = 5

        transactions_data = [
            {"code": "1234", "transaction_type": "buy", "quantity": 100}
        ]

        result = PortfolioManager.import_transactions_from_csv(
            user_id=1, transactions_data=transactions_data
        )

        assert result == 5
        mock_import.assert_called_once_with(1, transactions_data)

    @patch("src.portfolio.manager.TransactionManager.recalculate_holdings")
    def test_recalculate_holdings(self, mock_recalculate):
        """保有銘柄再計算のテスト"""
        PortfolioManager.recalculate_holdings(user_id=1)
        mock_recalculate.assert_called_once_with(1)


class TestPortfolioManagerMarketValues:
    """時価評価関連メソッドのテスト"""

    @patch("src.portfolio.manager.HoldingsManager.update_market_values")
    def test_update_market_values(self, mock_update):
        """時価評価更新のテスト"""
        mock_update.return_value = 10

        result = PortfolioManager.update_market_values(user_id=1)

        assert result == 10
        mock_update.assert_called_once_with(1)


class TestPortfolioManagerAggregation:
    """集約関連メソッドのテスト"""

    @patch("src.portfolio.manager.PortfolioAggregator.aggregate_holdings_by_code")
    def test_aggregate_holdings_by_code(self, mock_aggregate):
        """銘柄コード集約のテスト"""
        expected_data = [
            {"code": "1234", "total_quantity": 300, "average_price": 1000.0}
        ]
        mock_aggregate.return_value = expected_data

        result = PortfolioManager.aggregate_holdings_by_code(user_id=1)

        assert result == expected_data
        mock_aggregate.assert_called_once_with(1)

    @patch("src.portfolio.manager.PortfolioAggregator.get_portfolio_summary")
    def test_get_portfolio_summary(self, mock_summary):
        """ポートフォリオサマリー取得のテスト"""
        expected_summary = {
            "total_market_value": 1000000,
            "total_profit_loss": 50000,
            "stock_count": 5,
        }
        mock_summary.return_value = expected_summary

        result = PortfolioManager.get_portfolio_summary(user_id=1)

        assert result == expected_summary
        mock_summary.assert_called_once_with(1)


class TestPortfolioManagerDeletion:
    """削除関連メソッドのテスト"""

    @patch("src.portfolio.manager.HoldingsManager.delete_all_holdings")
    @patch("src.portfolio.manager.FundManager.delete_all_funds")
    def test_delete_all_holdings(self, mock_delete_funds, mock_delete_holdings):
        """全保有銘柄削除のテスト"""
        mock_delete_holdings.return_value = 5
        mock_delete_funds.return_value = 3

        result = PortfolioManager.delete_all_holdings(user_id=1)

        assert result == 8  # 5 + 3
        mock_delete_holdings.assert_called_once_with(1)
        mock_delete_funds.assert_called_once_with(1)

    @patch("src.portfolio.manager.HoldingsManager.delete_holdings_by_account")
    @patch("src.portfolio.manager.FundManager.delete_funds_by_account")
    def test_delete_holdings_by_account(self, mock_delete_funds, mock_delete_holdings):
        """口座別保有銘柄削除のテスト"""
        mock_delete_holdings.return_value = 3
        mock_delete_funds.return_value = 2

        result = PortfolioManager.delete_holdings_by_account(
            user_id=1, account_name="特定"
        )

        assert result == 5  # 3 + 2
        mock_delete_holdings.assert_called_once_with(1, "特定")
        mock_delete_funds.assert_called_once_with(1, "特定")


class TestPortfolioManagerIndicators:
    """株価指標関連メソッドのテスト"""

    @patch("src.portfolio.manager.IndicatorsManager.update_stock_indicators")
    def test_update_stock_indicators_with_codes(self, mock_update):
        """指定銘柄の株価指標更新テスト"""
        mock_update.return_value = 3

        result = PortfolioManager.update_stock_indicators(
            user_id=1, codes=["1234", "5678"]
        )

        assert result == 3
        mock_update.assert_called_once_with(1, ["1234", "5678"])

    @patch("src.portfolio.manager.IndicatorsManager.update_stock_indicators")
    def test_update_stock_indicators_all(self, mock_update):
        """全保有銘柄の株価指標更新テスト"""
        mock_update.return_value = 10

        result = PortfolioManager.update_stock_indicators(user_id=1, codes=None)

        assert result == 10
        mock_update.assert_called_once_with(1, None)

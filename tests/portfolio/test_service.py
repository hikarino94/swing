"""portfolio.serviceのテスト"""

from unittest.mock import MagicMock, patch

import pytest

from src.portfolio.models import Holding
from src.portfolio.service import PortfolioService


class TestPortfolioServiceInit:
    """初期化のテスト"""

    def test_init(self):
        """正しく初期化されることを確認"""
        service = PortfolioService()
        assert service.repo is not None


class TestUpdateHoldingsFromCsv:
    """update_holdings_from_csvメソッドのテスト"""

    @patch("src.portfolio.service.logger")
    def test_update_holdings_from_csv_standard_format(self, mock_logger):
        """標準形式CSVの処理テスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        # 標準形式のデータ
        holdings_data = [
            {
                "code": "1234",
                "quantity": 100,
                "average_price": 1000.0,
                "market_value": 110000.0,
                "profit_loss": 10000.0,
                "profit_loss_ratio": 10.0,
                "expected_per": 15.0,
                "actual_pbr": 1.2,
                "dividend_yield": 2.5,
            },
            {
                "code": "5678",
                "quantity": 50,
                "average_price": 2000.0,
                "expected_per": None,  # 欠損値のテスト
            },
        ]

        # upsert_holdingのモック設定
        service.repo.upsert_holding.side_effect = [
            (True, False),  # 更新 (is_updated=True, is_new=False)
            (False, True),  # 新規 (is_updated=False, is_new=True)
        ]

        # テスト実行
        updated, new = service.update_holdings_from_csv(123, holdings_data, "特定口座")

        # 検証
        assert updated == 1
        assert new == 1
        assert service.repo.upsert_holding.call_count == 2

        # 作成されたHoldingを検証
        first_call = service.repo.upsert_holding.call_args_list[0]
        holding = first_call[0][0]
        assert isinstance(holding, Holding)
        assert holding.code == "1234"
        assert holding.quantity == 100
        assert holding.expected_per == 15.0

    @patch("src.portfolio.service.logger")
    def test_update_holdings_from_csv_savefile_format(self, mock_logger):
        """SaveFile形式CSVの処理テスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        # SaveFile形式のデータ
        holdings_data = [
            {
                "code": "1234",
                "quantity": 100,
                "average_cost": 1000.0,  # average_priceではなくaverage_cost
                "account_type": "NISA",
            }
        ]

        service.repo.upsert_holding.return_value = (False, True)  # 新規作成

        # テスト実行
        updated, new = service.update_holdings_from_csv(123, holdings_data, "NISA口座")

        # 検証
        assert updated == 0
        assert new == 1

        # 作成されたHoldingを検証
        holding = service.repo.upsert_holding.call_args[0][0]
        assert holding.average_price == 1000.0
        assert holding.account_type == "NISA"

    def test_update_holdings_from_csv_skip_invalid(self):
        """無効なデータがスキップされることを確認"""
        service = PortfolioService()
        service.repo = MagicMock()

        holdings_data = [
            {"code": "", "quantity": 100},  # コードなし
            {"code": "1234", "quantity": 0},  # 数量0
            {"code": "5678", "quantity": 50, "average_price": 1000},  # 有効
        ]

        service.repo.upsert_holding.return_value = (True, False)

        # テスト実行
        updated, new = service.update_holdings_from_csv(123, holdings_data)

        # 有効なデータのみ処理されることを確認
        assert service.repo.upsert_holding.call_count == 1
        assert updated == 1

    @patch("src.portfolio.service.logger")
    def test_update_holdings_from_csv_exception_handling(self, mock_logger):
        """例外処理のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        holdings_data = [
            {"code": "1234", "quantity": "invalid"},  # 不正な数量
        ]

        # テスト実行
        updated, new = service.update_holdings_from_csv(123, holdings_data)

        # エラーがログに記録され、処理が続行されることを確認
        assert updated == 0
        assert new == 0
        mock_logger.error.assert_called()


class TestRecalculateHoldings:
    """recalculate_holdingsメソッドのテスト"""

    @patch("src.portfolio.service.logger")
    def test_recalculate_holdings_buy_only(self, mock_logger):
        """買い取引のみの再計算テスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        # 既存保有銘柄の削除
        service.repo.delete_all_holdings.return_value = 2

        # 取引履歴（買いのみ）
        service.repo.get_transactions.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "transaction_type": "buy",
                "quantity": 100,
                "total_amount": 100000,
                "commission": 100,
                "tax": 50,
            },
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "transaction_type": "buy",
                "quantity": 50,
                "total_amount": 55000,
                "commission": 50,
                "tax": 25,
            },
        ]

        # update_market_valuesのモック
        service.update_market_values = MagicMock()

        # テスト実行
        service.recalculate_holdings(123)

        # 検証
        service.repo.delete_all_holdings.assert_called_once_with(123)
        service.repo.get_transactions.assert_called_once_with(123)

        # 作成されたHoldingを検証
        assert service.repo.upsert_holding.call_count == 1
        holding = service.repo.upsert_holding.call_args[0][0]
        assert holding.code == "1234"
        assert holding.quantity == 150  # 100 + 50
        assert holding.average_price == pytest.approx(1033.33, rel=1e-3)  # 155000 / 150

        service.update_market_values.assert_called_once_with(123)

    @patch("src.portfolio.service.logger")
    def test_recalculate_holdings_buy_and_sell(self, mock_logger):
        """買いと売り取引の再計算テスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.delete_all_holdings.return_value = 1

        # 取引履歴（買いと売り）
        service.repo.get_transactions.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "transaction_type": "buy",
                "quantity": 100,
                "total_amount": 100000,
            },
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "transaction_type": "sell",
                "quantity": 30,
                "total_amount": 33000,
            },
        ]

        service.update_market_values = MagicMock()

        # テスト実行
        service.recalculate_holdings(123)

        # 検証
        holding = service.repo.upsert_holding.call_args[0][0]
        assert holding.quantity == 70  # 100 - 30
        assert holding.average_price == 1000.0  # (100000 * 0.7) / 70

    def test_recalculate_holdings_no_transactions(self):
        """取引履歴がない場合のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.delete_all_holdings.return_value = 0
        service.repo.get_transactions.return_value = []
        service.update_market_values = MagicMock()

        # テスト実行
        service.recalculate_holdings(123)

        # 保有銘柄が作成されないことを確認
        service.repo.upsert_holding.assert_not_called()
        service.update_market_values.assert_called_once()

    def test_recalculate_holdings_multiple_accounts(self):
        """複数口座の再計算テスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.delete_all_holdings.return_value = 0

        # 異なる口座の取引
        service.repo.get_transactions.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "transaction_type": "buy",
                "quantity": 100,
                "total_amount": 100000,
            },
            {
                "code": "1234",
                "account_name": "NISA口座",
                "account_type": "NISA",
                "transaction_type": "buy",
                "quantity": 50,
                "total_amount": 50000,
            },
        ]

        service.update_market_values = MagicMock()

        # テスト実行
        service.recalculate_holdings(123)

        # 2つの保有銘柄が作成されることを確認
        assert service.repo.upsert_holding.call_count == 2


class TestUpdateMarketValues:
    """update_market_valuesメソッドのテスト"""

    def test_update_market_values_success(self):
        """市場価値更新成功のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        # 保有銘柄
        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 1000.0,
            },
            {
                "code": "5678",
                "account_name": "NISA口座",
                "account_type": "NISA",
                "quantity": 50,
                "average_price": 2000.0,
            },
        ]

        # 最新株価
        service.repo.get_latest_prices.return_value = {
            "1234": 1100.0,
            "5678": 1900.0,
        }

        # テスト実行
        updated_count = service.update_market_values(123)

        # 検証
        assert updated_count == 2
        assert service.repo.upsert_holding.call_count == 2

        # 1つ目の保有銘柄の更新を検証
        first_holding = service.repo.upsert_holding.call_args_list[0][0][0]
        assert first_holding.market_value == 110000.0  # 1100 * 100
        assert first_holding.profit_loss == 10000.0  # (1100 - 1000) * 100
        assert first_holding.profit_loss_ratio == 10.0  # 10%

    def test_update_market_values_no_holdings(self):
        """保有銘柄がない場合のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.get_holdings.return_value = []

        # テスト実行
        updated_count = service.update_market_values(123)

        # 検証
        assert updated_count == 0
        service.repo.get_latest_prices.assert_not_called()

    def test_update_market_values_no_price_data(self):
        """株価データがない場合のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 1000.0,
            }
        ]

        # 株価データなし
        service.repo.get_latest_prices.return_value = {}

        # テスト実行
        updated_count = service.update_market_values(123)

        # 検証
        assert updated_count == 0
        service.repo.upsert_holding.assert_not_called()

    def test_update_market_values_zero_average_price(self):
        """平均取得価格が0の場合のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 0,
            }
        ]

        service.repo.get_latest_prices.return_value = {"1234": 1000.0}

        # テスト実行
        service.update_market_values(123)

        # 検証
        holding = service.repo.upsert_holding.call_args[0][0]
        assert holding.profit_loss_ratio == 0  # ゼロ除算を回避


class TestUpdateStockIndicators:
    """update_stock_indicatorsメソッドのテスト"""

    def test_update_stock_indicators_all_holdings(self):
        """全保有銘柄の指標更新テスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 1000.0,
            },
            {
                "code": "5678",
                "account_name": "NISA口座",
                "account_type": "NISA",
                "quantity": 50,
                "average_price": 2000.0,
            },
        ]

        # _calculate_stock_indicatorsのモック
        service._calculate_stock_indicators = MagicMock()
        service._calculate_stock_indicators.side_effect = [
            {"expected_per": 15.0, "actual_pbr": 1.2},
            {"expected_per": 20.0, "actual_pbr": 1.5},
        ]

        # テスト実行
        updated_count = service.update_stock_indicators(123)

        # 検証
        assert updated_count == 2
        assert service._calculate_stock_indicators.call_count == 2

    def test_update_stock_indicators_specific_codes(self):
        """特定銘柄の指標更新テスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 1000.0,
            },
            {
                "code": "5678",
                "account_name": "NISA口座",
                "account_type": "NISA",
                "quantity": 50,
                "average_price": 2000.0,
            },
        ]

        service._calculate_stock_indicators = MagicMock(
            return_value={"expected_per": 15.0}
        )

        # テスト実行（1234のみ更新）
        updated_count = service.update_stock_indicators(123, codes=["1234"])

        # 検証
        assert updated_count == 1
        service._calculate_stock_indicators.assert_called_once_with("1234")

    def test_update_stock_indicators_no_indicators(self):
        """指標が計算できない場合のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 1000.0,
            }
        ]

        # 指標なし
        service._calculate_stock_indicators = MagicMock(return_value={})

        # テスト実行
        service.update_stock_indicators(123)

        # 検証 - updated_count変数を削除
        service.repo.upsert_holding.assert_not_called()


class TestAggregateHoldingsByCode:
    """aggregate_holdings_by_codeメソッドのテスト"""

    def test_aggregate_holdings_by_code(self):
        """銘柄別集約のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        # 同じ銘柄が複数口座にある
        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "company_name": "テスト会社",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 1000.0,
                "market_value": 110000.0,
                "profit_loss": 10000.0,
                "profit_loss_ratio": 10.0,
            },
            {
                "code": "1234",
                "company_name": "テスト会社",
                "account_name": "NISA口座",
                "account_type": "NISA",
                "quantity": 50,
                "average_price": 1100.0,
                "market_value": 55000.0,
                "profit_loss": 0.0,
                "profit_loss_ratio": 0.0,
            },
            {
                "code": "5678",
                "company_name": "別会社",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 200,
                "average_price": 500.0,
                "market_value": 120000.0,
                "profit_loss": 20000.0,
                "profit_loss_ratio": 20.0,
            },
        ]

        # テスト実行
        result = service.aggregate_holdings_by_code(123)

        # 検証
        assert len(result) == 2

        # 1234が先（評価額が大きい: 165000）
        assert result[0]["code"] == "1234"
        assert result[0]["total_quantity"] == 150  # 100 + 50
        assert result[0]["total_value"] == 165000.0  # 110000 + 55000

        # 5678は2番目（評価額: 120000）
        assert result[1]["code"] == "5678"
        assert result[1]["total_quantity"] == 200
        assert result[1]["total_value"] == 120000.0
        assert result[0]["total_cost"] == 155000.0  # 100*1000 + 50*1100
        assert result[0]["average_price"] == pytest.approx(
            1033.33, rel=1e-3
        )  # 155000 / 150
        assert result[0]["profit_loss"] == 10000.0  # 165000 - 155000
        assert len(result[0]["accounts"]) == 2

    def test_aggregate_holdings_by_code_empty(self):
        """保有銘柄がない場合のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.get_holdings.return_value = []

        # テスト実行
        result = service.aggregate_holdings_by_code(123)

        # 検証
        assert result == []

    def test_aggregate_holdings_by_code_zero_cost(self):
        """コストが0の場合のテスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "company_name": "テスト会社",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 0,
                "market_value": 10000.0,
            }
        ]

        # テスト実行
        result = service.aggregate_holdings_by_code(123)

        # 検証
        assert len(result) == 1
        assert result[0]["profit_loss_ratio"] == 0  # ゼロ除算を回避


class TestIsStandardFormat:
    """_is_standard_formatメソッドのテスト"""

    def test_is_standard_format_true(self):
        """標準形式と判定される場合のテスト"""
        service = PortfolioService()

        holdings_data = [
            {"code": "1234", "quantity": 100, "expected_per": 15.0},
            {"code": "5678", "quantity": 50, "actual_pbr": 1.2},
        ]

        assert service._is_standard_format(holdings_data) is True

    def test_is_standard_format_false(self):
        """SaveFile形式と判定される場合のテスト"""
        service = PortfolioService()

        holdings_data = [
            {"code": "1234", "quantity": 100, "average_cost": 1000},
            {"code": "5678", "quantity": 50, "average_cost": 2000},
        ]

        assert service._is_standard_format(holdings_data) is False

    def test_is_standard_format_empty(self):
        """空のデータの場合のテスト"""
        service = PortfolioService()

        assert service._is_standard_format([]) is False


class TestCreateHoldingFromCsvData:
    """_create_holding_from_csv_dataメソッドのテスト"""

    def test_create_holding_standard_format(self):
        """標準形式のHolding作成テスト"""
        service = PortfolioService()

        data = {
            "code": "1234",
            "quantity": 100,
            "average_price": 1000.0,
            "market_value": 110000.0,
            "profit_loss": 10000.0,
            "profit_loss_ratio": 10.0,
            "expected_per": 15.0,
            "actual_pbr": 1.2,
            "dividend_yield": 2.5,
            "expected_eps": 70.0,
            "actual_bps": 900.0,
            "expected_dividend": 25.0,
            "lending_type": "general",
        }

        holding = service._create_holding_from_csv_data(123, data, "特定口座", True)

        assert holding is not None
        assert holding.code == "1234"
        assert holding.expected_per == 15.0
        assert holding.lending_type == "general"

    def test_create_holding_savefile_format(self):
        """SaveFile形式のHolding作成テスト"""
        service = PortfolioService()

        data = {
            "code": "1234",
            "quantity": 100,
            "average_cost": 1000.0,
            "account_type": "NISA",
        }

        holding = service._create_holding_from_csv_data(123, data, "NISA口座", False)

        assert holding is not None
        assert holding.code == "1234"
        assert holding.average_price == 1000.0
        assert holding.account_type == "NISA"

    def test_create_holding_with_missing_values(self):
        """欠損値がある場合のテスト"""
        service = PortfolioService()

        data = {
            "code": "1234",
            "quantity": 100,
            "average_price": 1000.0,
            "expected_per": "-",  # 欠損値
            "actual_pbr": "N/A",  # 欠損値
            "dividend_yield": "",  # 欠損値
        }

        holding = service._create_holding_from_csv_data(123, data, "特定口座", True)

        assert holding is not None
        assert holding.expected_per is None
        assert holding.actual_pbr is None
        assert holding.dividend_yield is None

    def test_create_holding_short_code(self):
        """短いコードの0埋めテスト"""
        service = PortfolioService()

        data = {"code": "123", "quantity": 100, "average_price": 1000}

        holding = service._create_holding_from_csv_data(123, data, "特定口座", True)

        assert holding is not None
        assert holding.code == "0123"

    def test_create_holding_invalid_data(self):
        """無効なデータの場合のテスト"""
        service = PortfolioService()

        # コードなし
        data1 = {"quantity": 100}
        assert (
            service._create_holding_from_csv_data(123, data1, "特定口座", True) is None
        )

        # 数量0
        data2 = {"code": "1234", "quantity": 0}
        assert (
            service._create_holding_from_csv_data(123, data2, "特定口座", True) is None
        )

        # 数量が文字列（変換エラー）
        data3 = {"code": "1234", "quantity": "invalid"}
        assert (
            service._create_holding_from_csv_data(123, data3, "特定口座", True) is None
        )


class TestCalculateStockIndicators:
    """_calculate_stock_indicatorsメソッドのテスト"""

    def test_calculate_stock_indicators_empty(self):
        """現在の実装（空の辞書を返す）のテスト"""
        service = PortfolioService()

        result = service._calculate_stock_indicators("1234")

        assert result == {}


class TestPortfolioServiceIntegration:
    """PortfolioServiceの統合テスト"""

    @patch("src.portfolio.service.logger")
    def test_csv_import_and_update_flow(self, mock_logger):
        """CSV取り込みと市場価値更新の統合テスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        # CSVデータ
        csv_data = [
            {
                "code": "1234",
                "quantity": 100,
                "average_price": 1000.0,
                "expected_per": 15.0,
            }
        ]

        # 保有銘柄の追加
        service.repo.upsert_holding.return_value = (False, True)  # 新規作成
        updated, new = service.update_holdings_from_csv(123, csv_data)
        assert new == 1

        # 市場価値の更新
        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "account_name": "default",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 1000.0,
            }
        ]
        service.repo.get_latest_prices.return_value = {"1234": 1100.0}

        updated_count = service.update_market_values(123)
        assert updated_count == 1

    def test_recalculation_and_aggregation_flow(self):
        """再計算と集約の統合テスト"""
        service = PortfolioService()
        service.repo = MagicMock()

        # 取引履歴からの再計算
        service.repo.delete_all_holdings.return_value = 1
        service.repo.get_transactions.return_value = [
            {
                "code": "1234",
                "account_name": "特定口座",
                "account_type": "特定",
                "transaction_type": "buy",
                "quantity": 100,
                "total_amount": 100000,
            }
        ]

        # update_market_valuesをモック
        service.update_market_values = MagicMock()

        service.recalculate_holdings(123)

        # 集約のテスト
        service.repo.get_holdings.return_value = [
            {
                "code": "1234",
                "company_name": "テスト会社",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 100,
                "average_price": 1000.0,
                "market_value": 110000.0,
            }
        ]

        aggregated = service.aggregate_holdings_by_code(123)
        assert len(aggregated) == 1
        assert aggregated[0]["total_quantity"] == 100

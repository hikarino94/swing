"""ui.blueprints.portfolio.holdingsのテスト"""

import json
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

from src.auth.models import User
from src.ui.blueprints.portfolio.holdings import holdings_bp


@pytest.fixture
def app():
    """テスト用のFlaskアプリケーション"""
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-secret-key"
    app.config["TESTING"] = True
    app.register_blueprint(holdings_bp)

    return app


@pytest.fixture
def client(app):
    """テスト用のクライアント"""
    return app.test_client()


@pytest.fixture
def user():
    """テスト用ユーザー"""
    user = MagicMock(spec=User)
    user.id = 1
    user.username = "testuser"
    return user


class TestGetHoldings:
    """get_holdings関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.ui.blueprints.portfolio.holdings.Holding")
    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    @patch("src.auth.decorators.get_current_user")
    def test_get_holdings_normal(
        self, mock_get_user, mock_connect, mock_holding, mock_manager, client, user
    ):
        """通常の保有銘柄一覧取得"""
        mock_get_user.return_value = user

        # 株式の保有データ
        holding1 = MagicMock()
        holding1.code = "1234"
        holding1.company_name = "テスト会社A"
        holding1.account_name = "特定口座"
        holding1.quantity = 100
        holding1.average_price = 1000.0
        holding1.market_value = 110000
        holding1.profit_loss = 10000
        holding1.profit_loss_ratio = 10.0
        holding1.updated_at = "2024-01-15"
        holding1.expected_per = 15.0
        holding1.actual_pbr = 1.2
        holding1.dividend_yield = 2.5
        holding1.expected_eps = 70.0
        holding1.actual_bps = 800.0
        holding1.expected_dividend = 25.0
        holding1.lending_type = None

        mock_holding.find_all_by_user.return_value = [holding1]

        # 投資信託のデータ
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            {
                "fund_id": "JP90C0001234",
                "fund_name": "テスト投信",
                "account_name": "特定口座",
                "account_type": "特定",
                "quantity": 10000,
                "average_price": 1.5,
                "market_value": 16000,
                "profit_loss": 1000,
                "profit_loss_ratio": 6.67,
                "updated_at": "2024-01-15",
                "current_nav": 1.6,
                "nav_date": "2024-01-15",
                "management_fee": 0.5,
                "trust_fee": 0.1,
                "sales_fee": 0.0,
                "category": "国内株式",
            }
        ]

        response = client.get("/holdings")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 2  # 株式1件 + 投信1件

        # 株式データの確認
        stock = data[0]
        assert stock["type"] == "stock"
        assert stock["code"] == "1234"
        assert stock["company_name"] == "テスト会社A"
        assert stock["quantity"] == 100
        assert stock["average_price"] == 1000.0

        # 投信データの確認
        fund = data[1]
        assert fund["type"] == "fund"
        assert fund["fund_id"] == "JP90C0001234"
        assert fund["fund_name"] == "テスト投信"
        assert fund["quantity"] == 10000

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    @patch("src.auth.decorators.get_current_user")
    def test_get_holdings_aggregate(
        self, mock_get_user, mock_connect, mock_manager, client, user
    ):
        """集約表示での保有銘柄一覧取得"""
        mock_get_user.return_value = user

        # 集約された株式データ
        mock_manager.aggregate_holdings_by_code.return_value = [
            {
                "type": "stock",
                "code": "1234",
                "company_name": "テスト会社A",
                "total_quantity": 200,
                "weighted_avg_price": 1050.0,
                "total_market_value": 220000,
                "total_profit_loss": 10000,
                "profit_loss_ratio": 4.76,
                "account_count": 2,
                "account_names": "特定口座,NISA",
                "account_types": "特定,NISA",
                "updated_at": "2024-01-15",
                "expected_per": 15.0,
                "actual_pbr": 1.2,
                "dividend_yield": 2.5,
                "expected_eps": 70.0,
                "actual_bps": 800.0,
                "expected_dividend": 25.0,
            }
        ]

        # 集約された投信データ
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [
            {
                "fund_id": "JP90C0001234",
                "fund_name": "テスト投信",
                "total_quantity": 20000,
                "weighted_avg_price": 1.45,
                "total_market_value": 32000,
                "total_profit_loss": 3000,
                "account_count": 2,
                "account_names": "特定口座,NISA",
                "account_types": "特定,NISA",
                "updated_at": "2024-01-15",
                "current_nav": 1.6,
                "nav_date": "2024-01-15",
                "management_fee": 0.5,
                "trust_fee": 0.1,
                "sales_fee": 0.0,
                "category": "国内株式",
            }
        ]

        response = client.get("/holdings?aggregate=true")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 2

        # 集約データの確認
        stock = data[0]
        assert stock["total_quantity"] == 200
        assert stock["account_count"] == 2
        assert "特定口座" in stock["account_names"]
        assert "NISA" in stock["account_names"]

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.ui.blueprints.portfolio.holdings.Holding")
    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    @patch("src.auth.decorators.get_current_user")
    def test_get_holdings_exception(
        self, mock_get_user, mock_connect, mock_holding, mock_manager, client, user
    ):
        """例外発生時のエラーハンドリング"""
        mock_get_user.return_value = user
        mock_holding.find_all_by_user.side_effect = Exception("DB Error")

        response = client.get("/holdings")

        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data
        assert "DB Error" in data["error"]

    @patch("src.auth.decorators.get_current_user")
    def test_get_holdings_not_logged_in(self, mock_get_user, client):
        """ログインしていない場合"""
        mock_get_user.return_value = None

        response = client.get("/holdings")

        assert response.status_code == 401


class TestUploadSbiCsv:
    """upload_sbi_csv関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.ui.blueprints.portfolio.holdings.SBICSVParser")
    @patch("src.auth.decorators.get_current_user")
    def test_upload_sbi_csv_success(
        self, mock_get_user, mock_parser, mock_manager, client, user
    ):
        """CSVアップロード成功"""
        mock_get_user.return_value = user

        # パーサーのモック
        mock_parser_instance = MagicMock()
        mock_parser.return_value = mock_parser_instance
        mock_parser_instance.is_valid.return_value = True
        mock_parser_instance.has_data.return_value = True
        mock_parser_instance.get_holdings.return_value = [
            {
                "code": "1234",
                "company_name": "テスト会社",
                "account_name": "特定口座",
                "quantity": 100,
                "average_price": 1000.0,
            }
        ]

        # ポートフォリオマネージャーのモック
        mock_manager_instance = MagicMock()
        mock_manager.return_value = mock_manager_instance
        mock_manager_instance.update_holdings_from_csv.return_value = (1, 0)

        # ファイルアップロードのモック
        data = {
            "file": (b"csv,data,here", "holdings.csv"),
            "auto_register": "true",
            "keep_zero_quantity": "false",
        }

        response = client.post(
            "/holdings/upload/sbi", data=data, content_type="multipart/form-data"
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["updated"] == 1
        assert data["new"] == 0

        # パーサーとマネージャーの呼び出し確認
        mock_parser_instance.is_valid.assert_called_once()
        mock_parser_instance.get_holdings.assert_called_once()
        mock_manager_instance.update_holdings_from_csv.assert_called_once()

    @patch("src.ui.blueprints.portfolio.holdings.SBICSVParser")
    @patch("src.auth.decorators.get_current_user")
    def test_upload_sbi_csv_no_file(self, mock_get_user, mock_parser, client, user):
        """ファイルが指定されていない場合"""
        mock_get_user.return_value = user

        response = client.post("/holdings/upload/sbi")

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "ファイルが選択されていません" in data["error"]

    @patch("src.ui.blueprints.portfolio.holdings.SBICSVParser")
    @patch("src.auth.decorators.get_current_user")
    def test_upload_sbi_csv_invalid_format(
        self, mock_get_user, mock_parser, client, user
    ):
        """無効なCSVフォーマット"""
        mock_get_user.return_value = user

        # パーサーのモック
        mock_parser_instance = MagicMock()
        mock_parser.return_value = mock_parser_instance
        mock_parser_instance.is_valid.return_value = False

        data = {"file": (b"invalid,csv,data", "holdings.csv")}

        response = client.post(
            "/holdings/upload/sbi", data=data, content_type="multipart/form-data"
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "CSVファイルの形式が正しくありません" in data["error"]

    @patch("src.ui.blueprints.portfolio.holdings.SBICSVParser")
    @patch("src.auth.decorators.get_current_user")
    def test_upload_sbi_csv_empty_data(self, mock_get_user, mock_parser, client, user):
        """データが含まれていない場合"""
        mock_get_user.return_value = user

        # パーサーのモック
        mock_parser_instance = MagicMock()
        mock_parser.return_value = mock_parser_instance
        mock_parser_instance.is_valid.return_value = True
        mock_parser_instance.has_data.return_value = False

        data = {"file": (b"header,only\n", "holdings.csv")}

        response = client.post(
            "/holdings/upload/sbi", data=data, content_type="multipart/form-data"
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "保有銘柄データが含まれていません" in data["error"]


class TestUpdateMarketValues:
    """update_market_values関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.auth.decorators.get_current_user")
    def test_update_market_values_success(
        self, mock_get_user, mock_manager, client, user
    ):
        """時価更新成功"""
        mock_get_user.return_value = user

        mock_manager_instance = MagicMock()
        mock_manager.return_value = mock_manager_instance
        mock_manager_instance.update_market_values.return_value = 5

        response = client.post("/holdings/update-market-values")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["updated"] == 5
        assert data["message"] == "5件の時価情報を更新しました"

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.auth.decorators.get_current_user")
    def test_update_market_values_exception(
        self, mock_get_user, mock_manager, client, user
    ):
        """時価更新中の例外"""
        mock_get_user.return_value = user

        mock_manager_instance = MagicMock()
        mock_manager.return_value = mock_manager_instance
        mock_manager_instance.update_market_values.side_effect = Exception("API Error")

        response = client.post("/holdings/update-market-values")

        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data
        assert "API Error" in data["error"]


class TestUpdateStockIndicators:
    """update_stock_indicators関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.auth.decorators.get_current_user")
    def test_update_stock_indicators_success(
        self, mock_get_user, mock_manager, client, user
    ):
        """株価指標更新成功"""
        mock_get_user.return_value = user

        mock_manager_instance = MagicMock()
        mock_manager.return_value = mock_manager_instance
        mock_manager_instance.update_stock_indicators.return_value = 3

        response = client.post("/holdings/update-stock-indicators")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["updated"] == 3
        assert data["message"] == "3件の株価指標を更新しました"

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.auth.decorators.get_current_user")
    def test_update_stock_indicators_no_updates(
        self, mock_get_user, mock_manager, client, user
    ):
        """更新対象がない場合"""
        mock_get_user.return_value = user

        mock_manager_instance = MagicMock()
        mock_manager.return_value = mock_manager_instance
        mock_manager_instance.update_stock_indicators.return_value = 0

        response = client.post("/holdings/update-stock-indicators")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["updated"] == 0
        assert data["message"] == "更新対象の銘柄はありませんでした"


class TestDeleteHolding:
    """delete_holding関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.Holding")
    @patch("src.auth.decorators.get_current_user")
    def test_delete_holding_success(self, mock_get_user, mock_holding, client, user):
        """保有銘柄削除成功"""
        mock_get_user.return_value = user

        # 保有銘柄のモック
        holding = MagicMock()
        holding.code = "1234"
        holding.company_name = "テスト会社"
        mock_holding.find_by_code_and_account.return_value = holding

        response = client.delete("/holdings/1234?account_name=特定口座")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["message"] == "保有銘柄を削除しました"

        # 削除メソッドが呼ばれたことを確認
        holding.delete.assert_called_once()

    @patch("src.ui.blueprints.portfolio.holdings.Holding")
    @patch("src.auth.decorators.get_current_user")
    def test_delete_holding_not_found(self, mock_get_user, mock_holding, client, user):
        """削除対象が見つからない場合"""
        mock_get_user.return_value = user
        mock_holding.find_by_code_and_account.return_value = None

        response = client.delete("/holdings/9999?account_name=特定口座")

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data
        assert "保有銘柄が見つかりません" in data["error"]

    @patch("src.auth.decorators.get_current_user")
    def test_delete_holding_missing_account(self, mock_get_user, client, user):
        """口座名が指定されていない場合"""
        mock_get_user.return_value = user

        response = client.delete("/holdings/1234")

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data
        assert "口座名が指定されていません" in data["error"]

"""ui.blueprints.portfolio.holdingsのテスト"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

# プロジェクトのパスを追加
sys.path.append(str(Path(__file__).resolve().parents[4]))

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
    @patch("src.portfolio.models.holding.Holding")
    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    def test_get_holdings_normal(
        self, mock_connect, mock_holding, mock_manager, client
    ):
        """通常の保有銘柄一覧取得"""
        # TESTINGモードでは自動的にcurrent_userが設定される

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
                "average_price": 15000,
                "current_price": 16000,
                "market_value": 160000,
                "profit_loss": 10000,
                "profit_loss_ratio": 6.67,
                "updated_at": "2024-01-15",
            }
        ]

        response = client.get("/holdings")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["holdings"]) == 1
        assert len(data["funds"]) == 1

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.portfolio.models.holding.Holding")
    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    def test_get_holdings_aggregated(
        self, mock_connect, mock_holding, mock_manager, client
    ):
        """銘柄別集約表示"""
        # 集約データ
        aggregated_data = [
            {
                "code": "1234",
                "company_name": "テスト会社A",
                "total_quantity": 150,
                "total_value": 165000,
                "average_price": 1033.33,
                "profit_loss": 10500,
                "profit_loss_ratio": 6.8,
                "accounts": [
                    {
                        "account_name": "特定口座",
                        "quantity": 100,
                        "average_price": 1000,
                        "market_value": 110000,
                    },
                    {
                        "account_name": "NISA口座",
                        "quantity": 50,
                        "average_price": 1100,
                        "market_value": 55000,
                    },
                ],
            }
        ]

        mock_manager.aggregate_holdings_by_code.return_value = aggregated_data
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []

        response = client.get("/holdings?aggregate=true")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["holdings"]) == 1
        assert data["holdings"][0]["total_quantity"] == 150


class TestImportHoldings:
    """import_holdings関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    def test_import_holdings_success(self, mock_manager, client):
        """CSV取り込み成功"""
        mock_manager.update_holdings_from_csv.return_value = (5, 3)  # 更新5件、新規3件
        mock_manager.update_market_values.return_value = None

        csv_content = """銘柄コード,銘柄名,保有数量,取得単価
1234,テスト会社A,100,1000
5678,テスト会社B,50,2000"""

        response = client.post(
            "/holdings/upload",
            data={"csv_content": csv_content, "account_name": "特定口座"},
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["new"] == 3
        assert data["updated"] == 5

    def test_import_holdings_no_csv(self, client):
        """CSVデータなしエラー"""
        response = client.post("/holdings/upload", data={})

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["success"] is False
        assert "CSVデータ" in data["error"]


class TestAddHolding:
    """add_holding関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.portfolio.models.holding.Holding")
    def test_add_holding_new(self, mock_holding_class, mock_manager, client):
        """新規保有銘柄追加"""
        # 既存レコードなし
        mock_holding_class.find_by_user_code_and_account.return_value = None

        # 新規インスタンス作成
        new_holding = MagicMock()
        new_holding.save.return_value = True
        mock_holding_class.return_value = new_holding

        mock_manager.update_market_values.return_value = None
        mock_manager.update_stock_indicators.return_value = None

        response = client.post(
            "/holdings/add",
            json={
                "code": "1234",
                "quantity": 100,
                "average_price": 1000,
                "account_name": "特定口座",
            },
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert "追加" in data["message"]

    @patch("src.portfolio.models.holding.Holding")
    def test_add_holding_update_existing(self, mock_holding_class, client):
        """既存保有銘柄の更新"""
        # 既存レコードあり
        existing = MagicMock()
        existing.quantity = 50
        existing.average_price = 900
        existing.save.return_value = True
        mock_holding_class.find_by_user_code_and_account.return_value = existing

        response = client.post(
            "/holdings/add",
            json={
                "code": "1234",
                "quantity": 100,
                "average_price": 1000,
                "account_name": "特定口座",
            },
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert "更新" in data["message"]
        # 数量と平均価格が更新されたことを確認
        assert existing.quantity == 150  # 50 + 100
        assert existing.average_price == 966.67  # (50*900 + 100*1000) / 150

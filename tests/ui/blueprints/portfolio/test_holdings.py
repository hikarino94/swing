"""ui.blueprints.portfolio.holdingsのテスト"""

import json
from io import BytesIO
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

    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    @patch("src.portfolio.models.holding.Holding")
    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    def test_get_holdings_normal(
        self, mock_manager, mock_holding, mock_connect, client
    ):
        """通常の保有銘柄一覧取得"""

        # 株式の保有データ
        class MockHolding:
            def __init__(self):
                self.code = "1234"
                self.company_name = "テスト会社A"
                self.account_name = "特定口座"
                self.account_type = "特定"
                self.quantity = 100
                self.average_price = 1000.0
                self.market_value = 110000
                self.profit_loss = 10000
                self.profit_loss_ratio = 10.0
                self.updated_at = "2024-01-15"
                self.expected_per = 15.0
                self.actual_pbr = 1.2
                self.dividend_yield = 2.5
                self.expected_eps = 70.0
                self.actual_bps = 800.0
                self.expected_dividend = 25.0
                self.lending_type = None

        holding1 = MockHolding()

        mock_holding.find_all_by_user.return_value = [holding1]

        # 投資信託のデータ
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # fetchallはタプルのリストを返す
        mock_cursor.fetchall.return_value = [
            (
                "JP90C0001234",  # fund_id
                "テスト投信",  # fund_name
                "特定口座",  # account_name
                "特定",  # account_type
                10000,  # quantity
                15000,  # average_price
                160000,  # market_value
                10000,  # profit_loss
                6.67,  # profit_loss_ratio
                "再投資",  # dividend_method
                "2024-01-15",  # updated_at
                16000,  # current_nav
                "2024-01-15",  # nav_date
            )
        ]

        response = client.get("/holdings")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert len(data["holdings"]) == 2  # 株式1件と投資信託1件
        assert data["aggregated"] is False

        # 株式データの確認
        stock = next(h for h in data["holdings"] if h["type"] == "stock")
        assert stock["code"] == "1234"
        assert stock["company_name"] == "テスト会社A"
        assert stock["quantity"] == 100

        # 投資信託データの確認
        fund = next(h for h in data["holdings"] if h["type"] == "fund")
        assert fund["fund_id"] == "JP90C0001234"
        assert fund["fund_name"] == "テスト投信"
        assert fund["quantity"] == 10000

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
                "type": "stock",
                "code": "1234",
                "company_name": "テスト会社A",
                "quantity": 150,
                "market_value": 165000,
                "average_price": 1033.33,
                "profit_loss": 10500,
                "profit_loss_ratio": 6.8,
                "account_name": None,
                "account_type": None,
                "updated_at": "2024-01-15",
                "expected_per": 15.0,
                "actual_pbr": 1.2,
                "dividend_yield": 2.5,
                "expected_eps": None,
                "actual_bps": None,
                "expected_dividend": None,
                "lending_type": None,
            }
        ]

        mock_manager.aggregate_holdings_by_code.return_value = aggregated_data

        # 投資信託の集約データ
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (
                "JP90C0001234",  # fund_id
                "テスト投信",  # fund_name
                20000,  # total_quantity
                14500,  # weighted_avg_price
                310000,  # total_market_value
                20000,  # total_profit_loss
                2,  # account_count
                "特定口座,NISA口座",  # account_names
                "特定,NISA",  # account_types
                "2024-01-15",  # updated_at
                15500,  # current_nav
                "2024-01-15",  # nav_date
            )
        ]

        response = client.get("/holdings?aggregate=true")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert data["aggregated"] is True
        assert len(data["holdings"]) == 2  # 株式1件と投資信託1件の集約

    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    @patch("src.portfolio.models.holding.Holding")
    @patch("src.ui.blueprints.portfolio.holdings.logger")
    def test_get_holdings_error(self, mock_logger, mock_holding, mock_connect, client):
        """エラー処理のテスト"""
        mock_holding.find_all_by_user.side_effect = Exception("Database error")

        response = client.get("/holdings")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is False
        assert "Database error" in data["error"]
        mock_logger.error.assert_called()


class TestUploadHoldings:
    """upload_holdings関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.ui.blueprints.portfolio.holdings.SBICSVParser")
    def test_upload_holdings_success(self, mock_parser, mock_manager, client):
        """CSV取り込み成功"""
        mock_parser.parse_holdings_csv.return_value = [
            {
                "code": "1234",
                "name": "テスト会社A",
                "quantity": 100,
                "average_price": 1000,
            }
        ]
        mock_manager.update_holdings_from_csv.return_value = (5, 3)  # 更新5件、新規3件
        mock_manager.update_market_values.return_value = None

        csv_content = """銘柄コード,銘柄名,保有数量,取得単価
1234,テスト会社A,100,1000
5678,テスト会社B,50,2000"""

        # ファイルオブジェクトを作成
        file_data = BytesIO(csv_content.encode("utf-8"))

        response = client.post(
            "/holdings/upload",
            data={"file": (file_data, "test.csv"), "account_name": "特定口座"},
            content_type="multipart/form-data",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["new"] == 3
        assert data["updated"] == 5
        assert data["account_name"] == "特定口座"

    def test_upload_holdings_no_file(self, client):
        """ファイルなしエラー"""
        response = client.post("/holdings/upload", data={})

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "ファイルが選択されていません" in data["error"]

    @patch("src.ui.blueprints.portfolio.holdings.SBICSVParser")
    def test_upload_holdings_parse_error(self, mock_parser, client):
        """CSV解析エラー"""
        mock_parser.parse_holdings_csv.side_effect = ValueError("Invalid CSV format")

        file_data = BytesIO(b"invalid,csv,data")

        response = client.post(
            "/holdings/upload",
            data={"file": (file_data, "test.csv")},
            content_type="multipart/form-data",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "Invalid CSV format" in data["error"]


class TestDeleteHoldings:
    """delete_holdings関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    def test_delete_all_holdings(self, mock_manager, client):
        """全保有銘柄削除"""
        mock_manager.delete_all_holdings.return_value = 10

        response = client.post("/holdings/delete", json={"type": "all"})

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["deleted"] == 10
        assert "全ての保有銘柄を削除しました" in data["message"]

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    def test_delete_holdings_by_account(self, mock_manager, client):
        """特定口座の保有銘柄削除"""
        mock_manager.delete_holdings_by_account.return_value = 5

        response = client.post(
            "/holdings/delete", json={"type": "account", "account_name": "特定口座"}
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["deleted"] == 5
        assert "口座 '特定口座' の保有銘柄を削除しました" in data["message"]

    def test_delete_holdings_missing_account_name(self, client):
        """口座名なしエラー"""
        response = client.post("/holdings/delete", json={"type": "account"})

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "口座名が指定されていません" in data["error"]


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
        new_holding.quantity = 100
        new_holding.average_price = 1000
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
                "company_name": "テスト会社A",
            },
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert "追加" in data["message"]
        assert data["holding"]["code"] == "1234"
        assert data["holding"]["quantity"] == 100

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.portfolio.models.holding.Holding")
    def test_add_holding_update_existing(
        self, mock_holding_class, mock_manager, client
    ):
        """既存保有銘柄の更新"""
        # 既存レコードあり
        existing = MagicMock()
        existing.quantity = 50
        existing.average_price = 900
        existing.save.return_value = True
        mock_holding_class.find_by_user_code_and_account.return_value = existing

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
        # 数量と平均価格が更新されたことを確認
        assert existing.quantity == 150  # 50 + 100
        assert abs(existing.average_price - 966.67) < 0.01  # (50*900 + 100*1000) / 150

    def test_add_holding_5digit_code(self, client):
        """5桁コードの変換テスト"""
        with patch("src.portfolio.models.holding.Holding") as mock_holding_class:
            mock_holding_class.find_by_user_code_and_account.return_value = None

            new_holding = MagicMock()
            new_holding.save.return_value = True
            new_holding.quantity = 100
            new_holding.average_price = 1000
            mock_holding_class.return_value = new_holding

            response = client.post(
                "/holdings/add",
                json={
                    "code": "12345",  # 5桁
                    "quantity": 100,
                    "average_price": 1000,
                },
            )

            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["success"] is True
            # 4桁に変換されていることを確認
            assert data["holding"]["code"] == "1234"

    def test_add_holding_validation_errors(self, client):
        """バリデーションエラー"""
        # コードなし
        response = client.post(
            "/holdings/add",
            json={"quantity": 100, "average_price": 1000},
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "銘柄コードは必須です" in data["error"]

        # 数量が0以下
        response = client.post(
            "/holdings/add",
            json={"code": "1234", "quantity": 0, "average_price": 1000},
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "数量は正の数を入力してください" in data["error"]

        # 価格が0以下
        response = client.post(
            "/holdings/add",
            json={"code": "1234", "quantity": 100, "average_price": 0},
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "平均取得価格は正の数を入力してください" in data["error"]


class TestUpdateHolding:
    """update_holding関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.portfolio.models.holding.Holding")
    def test_update_holding_success(self, mock_holding_class, mock_manager, client):
        """保有銘柄更新成功"""
        existing = MagicMock()
        existing.quantity = 100
        existing.average_price = 1000
        existing.save.return_value = True
        mock_holding_class.find_by_user_code_and_account.return_value = existing

        mock_manager.update_market_values.return_value = None

        response = client.post(
            "/holdings/update",
            json={
                "code": "1234",
                "account_name": "特定口座",
                "quantity": 150,
                "average_price": 1100,
            },
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert "更新" in data["message"]
        assert existing.quantity == 150
        assert existing.average_price == 1100

    @patch("src.portfolio.models.holding.Holding")
    def test_update_holding_not_found(self, mock_holding_class, client):
        """更新対象が見つからない"""
        mock_holding_class.find_by_user_code_and_account.return_value = None

        response = client.post(
            "/holdings/update",
            json={
                "code": "1234",
                "account_name": "特定口座",
                "quantity": 150,
            },
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "指定された保有銘柄が見つかりません" in data["error"]

    def test_update_holding_validation_error(self, client):
        """バリデーションエラー"""
        # 必須項目なし
        response = client.post(
            "/holdings/update",
            json={"quantity": 150},
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "銘柄コードと口座名は必須です" in data["error"]

        # 数量が負
        response = client.post(
            "/holdings/update",
            json={
                "code": "1234",
                "account_name": "特定口座",
                "quantity": -10,
            },
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "数量は0以上を入力してください" in data["error"]


class TestDeleteSingleHolding:
    """delete_single_holding関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    def test_delete_single_holding_success(self, mock_connect, client):
        """単一保有銘柄削除成功"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        response = client.delete("/holdings/delete/1234/特定口座")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert "削除しました" in data["message"]

        # SQL実行確認
        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "DELETE FROM holdings" in sql
        assert "user_id = ?" in sql
        assert "code = ?" in sql
        assert "account_name = ?" in sql

    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    def test_delete_single_holding_not_found(self, mock_connect, client):
        """削除対象が見つからない"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 0

        response = client.delete("/holdings/delete/9999/特定口座")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is False
        assert "指定された保有銘柄が見つかりません" in data["error"]


class TestDeleteSingleFundHolding:
    """delete_single_fund_holding関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    def test_delete_single_fund_holding_success(self, mock_connect, client):
        """単一投資信託削除成功"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        response = client.delete("/holdings/delete/fund/JP90C0001234/特定口座")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is True
        assert "削除しました" in data["message"]

        # SQL実行確認
        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "DELETE FROM fund_holdings" in sql
        assert "user_id = ?" in sql
        assert "fund_id = ?" in sql
        assert "account_name = ?" in sql

    @patch("src.ui.blueprints.portfolio.holdings.sqlite3.connect")
    def test_delete_single_fund_holding_not_found(self, mock_connect, client):
        """削除対象が見つからない"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 0

        response = client.delete("/holdings/delete/fund/JP90C0009999/特定口座")
        assert response.status_code == 200

        data = json.loads(response.data)
        assert data["success"] is False
        assert "指定された投資信託が見つかりません" in data["error"]


class TestIndicatorsUpdate:
    """indicators_update関数のテスト"""

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    def test_indicators_update_success(self, mock_manager, client):
        """株価指標更新成功"""
        mock_manager.update_stock_indicators.return_value = 5

        response = client.post("/indicators/update", json={"codes": ["1234", "5678"]})

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["updated"] == 5
        assert data["refresh"] is True
        assert "5件の株価指標を更新しました" in data["message"]

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    def test_indicators_update_no_targets(self, mock_manager, client):
        """更新対象なし"""
        mock_manager.update_stock_indicators.return_value = 0

        response = client.post("/indicators/update", json={})

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is True
        assert data["updated"] == 0
        assert data["refresh"] is False
        assert "更新対象の銘柄がありませんでした" in data["message"]

    @patch("src.ui.blueprints.portfolio.holdings.PortfolioManager")
    @patch("src.ui.blueprints.portfolio.holdings.logger")
    def test_indicators_update_error(self, mock_logger, mock_manager, client):
        """更新エラー"""
        mock_manager.update_stock_indicators.side_effect = Exception("Update error")

        response = client.post("/indicators/update", json={})

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["success"] is False
        assert "Update error" in data["error"]
        mock_logger.error.assert_called()


class TestHoldingsIntegration:
    """holdings_bpの統合テスト"""

    def test_all_endpoints_require_login(self, app, client):
        """全エンドポイントがログイン必須であることを確認"""
        # TESTINGモードを一時的に無効化
        app.config["TESTING"] = False

        endpoints = [
            ("/holdings", "GET"),
            ("/holdings/upload", "POST"),
            ("/holdings/delete", "POST"),
            ("/holdings/add", "POST"),
            ("/holdings/update", "POST"),
            ("/holdings/delete/1234/test", "DELETE"),
            ("/holdings/delete/fund/JP90C0001234/test", "DELETE"),
            ("/indicators/update", "POST"),
        ]

        with patch(
            "src.auth.decorators.AuthManager.get_user_by_session"
        ) as mock_get_user:
            # ユーザーが存在しない（ログインしていない）状態
            mock_get_user.return_value = None

            # url_forとredirectをモック
            with patch("src.auth.decorators.url_for") as mock_url_for:
                mock_url_for.return_value = "/login"

                with patch("src.auth.decorators.redirect") as mock_redirect:
                    from werkzeug.wrappers import Response

                    # redirectの戻り値をモック（401レスポンスを返す）
                    mock_response = Response("Unauthorized", status=401)
                    mock_redirect.return_value = mock_response

                    for endpoint, method in endpoints:
                        if method == "GET":
                            response = client.get(endpoint)
                        elif method == "POST":
                            response = client.post(endpoint, json={})
                        elif method == "DELETE":
                            response = client.delete(endpoint)

                        assert response.status_code == 401

                    # 各エンドポイントでredirectが呼ばれたことを確認
                    assert mock_redirect.call_count == len(endpoints)

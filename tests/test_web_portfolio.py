"""ポートフォリオ管理のWeb APIテスト - 実際のデータベース操作を含む統合テスト"""

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from db.db_schema import init_schema
from src.ui.web import app


@pytest.fixture
def test_db():
    """テスト用の一時データベース"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    init_schema(db_path)

    # テストユーザーを追加
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO users (username, email, password_hash) VALUES ('test_user', 'test@example.com', 'dummy_hash')"
    )
    # テスト用の銘柄情報を追加
    conn.execute(
        """INSERT INTO listed_info (code, company_name, company_name_en, sector17_code,
           sector17_name, sector33_code, sector33_name, scale_category,
           market_code, market_name, delete_flag)
           VALUES ('12340', 'テスト株式会社', 'Test Corp', '0001', 'プライム市場',
                   '01', '食品', '0100', '水産・農林業', 'PRIME', 0)"""
    )
    conn.commit()
    conn.close()

    yield db_path

    # クリーンアップ
    db_path.unlink()


@pytest.fixture
def client(test_db, monkeypatch):
    """Flask テストクライアントを作成（認証済み）"""
    # データベースパスをモックする前に、web.pyもインポート

    # データベースパスをモック
    monkeypatch.setattr("src.config.get_db_path", lambda: str(test_db))

    # web.py内で使われるget_db_pathもモック
    monkeypatch.setattr("src.ui.web.get_db_path", lambda: str(test_db))

    # モジュールごとにも設定
    monkeypatch.setattr("src.portfolio.models.get_db_path", lambda: str(test_db))
    monkeypatch.setattr("src.auth.models.get_db_path", lambda: str(test_db))

    # アプリケーションインスタンスを設定
    app.config["TESTING"] = True
    app.config["DATABASE"] = str(test_db)

    # テストクライアントを作成
    with app.test_client() as client:
        with app.app_context():
            yield client


class TestPortfolioAPI:
    """ポートフォリオ管理APIのテスト"""

    def test_search_stocks(self, client, test_db):
        """銘柄検索APIのテスト - 実際のデータベースを使用"""
        # デバッグ: データベースの内容を確認
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute("SELECT code, company_name, delete_flag FROM listed_info")
        debug_results = cursor.fetchall()
        print(f"Debug - Database contents: {debug_results}")
        conn.close()

        # リクエスト - 5桁のコードで検索
        response = client.get("/api/portfolio/stocks/search?q=12340")

        # レスポンスの確認
        assert (
            response.status_code == 200
        ), f"Unexpected status code: {response.status_code}, data: {response.data}"
        data = json.loads(response.data)
        print(f"Debug - Response data: {data}")

        # 検証
        assert response.status_code == 200
        assert data["success"] is True
        assert len(data["stocks"]) == 1
        assert data["stocks"][0]["code"] == "1234"
        assert data["stocks"][0]["full_code"] == "12340"
        assert data["stocks"][0]["company_name"] == "テスト株式会社"

    def test_add_holding(self, client, test_db):
        """保有銘柄追加APIのテスト - 実際のデータベースを使用"""
        # リクエストデータ
        request_data = {
            "code": "1234",
            "account_name": "test_account",
            "quantity": 100,
            "average_price": 1500.0,
        }

        # リクエスト
        response = client.post(
            "/api/portfolio/holdings/add",
            data=json.dumps(request_data),
            content_type="application/json",
        )
        data = json.loads(response.data)

        # 検証
        assert response.status_code == 200
        assert data["success"] is True
        assert data["message"] == "保有銘柄を追加しました"

        # データベースで実際に保存されたか確認
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT quantity, average_price FROM holdings WHERE user_id = 1 AND code = '1234' AND account_name = 'test_account'"
        )
        result = cursor.fetchone()
        conn.close()

        assert result is not None
        assert result[0] == 100  # quantity
        assert result[1] == 1500.0  # average_price

    def test_update_holding(self, client, test_db):
        """保有銘柄更新APIのテスト - 実際のデータベースを使用"""
        # まず保有銘柄を追加
        conn = sqlite3.connect(test_db)
        conn.execute(
            """INSERT INTO holdings (user_id, code, account_name, account_type, quantity, average_price)
               VALUES (1, '1234', 'test_account', '特定', 100, 1500.0)"""
        )
        conn.commit()
        conn.close()

        # リクエストデータ
        request_data = {
            "code": "1234",
            "account_name": "test_account",
            "quantity": 200,
            "average_price": 1600.0,
        }

        # リクエスト
        response = client.post(
            "/api/portfolio/holdings/update",
            data=json.dumps(request_data),
            content_type="application/json",
        )
        data = json.loads(response.data)

        # 検証
        assert response.status_code == 200
        assert data["success"] is True
        assert data["message"] == "保有銘柄を更新しました"

        # データベースで実際に更新されたか確認
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT quantity, average_price FROM holdings WHERE user_id = 1 AND code = '1234'"
        )
        result = cursor.fetchone()
        conn.close()

        assert result[0] == 200  # quantity
        assert result[1] == 1600.0  # average_price

    def test_delete_single_holding(self, client, test_db):
        """保有銘柄削除APIのテスト - 実際のデータベースを使用"""
        # まず保有銘柄を追加
        conn = sqlite3.connect(test_db)
        conn.execute(
            """INSERT INTO holdings (user_id, code, account_name, account_type, quantity, average_price)
               VALUES (1, '1234', 'test_account', '特定', 100, 1500.0)"""
        )
        conn.commit()
        conn.close()

        # リクエスト
        response = client.delete("/api/portfolio/holdings/delete/1234/test_account")
        data = json.loads(response.data)

        # 検証
        assert response.status_code == 200
        assert data["success"] is True
        assert data["message"] == "保有銘柄を削除しました"

        # データベースで実際に削除されたか確認
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM holdings WHERE user_id = 1 AND code = '1234' AND deleted_at IS NULL"
        )
        result = cursor.fetchone()
        conn.close()

        assert result[0] == 0  # 削除されている

    def test_add_transaction(self, client, test_db):
        """取引履歴追加APIのテスト - 実際のデータベースを使用"""
        # リクエストデータ
        request_data = {
            "code": "1234",
            "transaction_date": "2024-01-01",
            "transaction_type": "buy",
            "quantity": 100,
            "price": 1500.0,
            "commission": 100,
            "tax": 0,
            "remarks": "テスト取引",
        }

        # リクエスト
        response = client.post(
            "/api/portfolio/transactions/add",
            data=json.dumps(request_data),
            content_type="application/json",
        )
        data = json.loads(response.data)

        # 検証
        assert response.status_code == 200
        assert data["success"] is True
        assert data["message"] == "取引を追加しました"

        # データベースで実際に保存されたか確認
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute(
            """SELECT transaction_type, quantity, price, commission, tax, remarks
               FROM transactions WHERE user_id = 1 AND code = '1234'"""
        )
        result = cursor.fetchone()
        conn.close()

        assert result is not None
        assert result[0] == "buy"
        assert result[1] == 100
        assert result[2] == 1500.0
        assert result[3] == 100
        assert result[4] == 0
        assert result[5] == "テスト取引"

    def test_update_transaction(self, client, test_db):
        """取引履歴更新APIのテスト - 実際のデータベースを使用"""
        # まず取引履歴を追加
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO transactions (user_id, code, transaction_date, transaction_type,
               quantity, price, commission, tax, total_amount)
               VALUES (1, '1234', '2024-01-01', 'buy', 100, 1500.0, 100, 0, 150100)"""
        )
        transaction_id = cursor.lastrowid
        conn.commit()
        conn.close()

        # リクエストデータ
        request_data = {"quantity": 200, "price": 1600.0}

        # リクエスト
        response = client.post(
            f"/api/portfolio/transactions/update/{transaction_id}",
            data=json.dumps(request_data),
            content_type="application/json",
        )
        data = json.loads(response.data)

        # 検証
        assert response.status_code == 200
        assert data["success"] is True
        assert data["message"] == "取引を更新しました"

        # データベースで実際に更新されたか確認
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT quantity, price FROM transactions WHERE id = {transaction_id}"
        )
        result = cursor.fetchone()
        conn.close()

        assert result[0] == 200  # quantity
        assert result[1] == 1600.0  # price

    def test_delete_transaction(self, client, test_db):
        """取引履歴削除APIのテスト - 実際のデータベースを使用"""
        # まず取引履歴を追加
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO transactions (user_id, code, transaction_date, transaction_type,
               quantity, price, commission, tax, total_amount)
               VALUES (1, '1234', '2024-01-01', 'buy', 100, 1500.0, 100, 0, 150100)"""
        )
        transaction_id = cursor.lastrowid
        conn.commit()
        conn.close()

        # リクエスト
        response = client.delete(f"/api/portfolio/transactions/delete/{transaction_id}")
        data = json.loads(response.data)

        # 検証
        assert response.status_code == 200
        assert data["success"] is True
        assert data["message"] == "取引を削除しました"

        # データベースで実際に削除されたか確認
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM transactions WHERE id = {transaction_id}")
        result = cursor.fetchone()
        conn.close()

        assert result[0] == 0  # 削除されている

    def test_add_holding_validation(self, client):
        """保有銘柄追加のバリデーションテスト"""
        # 銘柄コードなし
        response = client.post(
            "/api/portfolio/holdings/add",
            data=json.dumps({"quantity": 100, "average_price": 1500}),
            content_type="application/json",
        )
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["error"] == "銘柄コードは必須です"

        # 数量が0以下
        response = client.post(
            "/api/portfolio/holdings/add",
            data=json.dumps({"code": "1234", "quantity": 0, "average_price": 1500}),
            content_type="application/json",
        )
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["error"] == "数量は正の数を入力してください"

    def test_add_transaction_validation(self, client):
        """取引履歴追加のバリデーションテスト"""
        # 取引種別が不正
        response = client.post(
            "/api/portfolio/transactions/add",
            data=json.dumps(
                {
                    "code": "1234",
                    "transaction_date": "2024-01-01",
                    "transaction_type": "invalid",
                    "quantity": 100,
                    "price": 1500,
                }
            ),
            content_type="application/json",
        )
        data = json.loads(response.data)
        assert data["success"] is False
        assert data["error"] == "取引種別は buy または sell を指定してください"

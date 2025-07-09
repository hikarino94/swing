"""エンドツーエンド（E2E）ワークフローテスト - 実際の業務フローを検証"""

import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from db.db_schema import init_schema
from src.portfolio.csv_parser import SBICSVParser
from src.portfolio.manager import PortfolioManager
from src.portfolio.models import Holding, Transaction


@pytest.fixture
def test_environment():
    """テスト環境のセットアップ"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        db_path = tmpdir_path / "test.db"

        # データベース初期化
        init_schema(db_path)

        # テストユーザー作成
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)",
            ("test_user", "test@example.com", "dummy_hash"),
        )
        user_id = cursor.lastrowid

        # テスト用の銘柄情報を追加
        conn.execute(
            """
            INSERT INTO listed_info (code, company_name, company_name_en,
                sector17_code, sector17_name, sector33_code, sector33_name,
                scale_category, market_code, market_name)
            VALUES
                ('72030', 'トヨタ自動車', 'TOYOTA MOTOR', '0001', 'プライム市場',
                 '16', '輸送用機器', '3300', '輸送用機器', 'PRIME'),
                ('92010', '日本電信電話', 'NTT', '0001', 'プライム市場',
                 '12', '情報・通信業', '2200', '情報・通信', 'PRIME')
        """
        )

        # テスト用の価格データを追加
        today = datetime.now().date()
        for i in range(30):
            date = today - timedelta(days=i)
            conn.execute(
                """
                INSERT INTO prices (code, date, open, high, low, close, volume)
                VALUES
                    ('72030', ?, 2500, 2550, 2480, 2520, 1000000),
                    ('92010', ?, 150, 155, 148, 152, 2000000)
            """,
                (date.isoformat(), date.isoformat()),
            )

        conn.commit()
        conn.close()

        yield {"db_path": db_path, "tmpdir": tmpdir_path, "user_id": user_id}


class TestE2EWorkflow:
    """エンドツーエンドワークフローのテスト"""

    def test_portfolio_management_workflow(self, test_environment, monkeypatch):
        """ポートフォリオ管理の完全なワークフロー"""
        db_path = test_environment["db_path"]
        user_id = test_environment["user_id"]

        # データベースパスをモック
        monkeypatch.setattr("src.config.get_db_path", lambda: str(db_path))
        monkeypatch.setattr("src.portfolio.models.get_db_path", lambda: str(db_path))
        monkeypatch.setattr("src.portfolio.manager.get_db_path", lambda: str(db_path))

        # Step 1: CSVファイルから保有銘柄を読み込む
        csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,注文株数,取得単価,現在値,現在値,評価損益,評価損益(%),買付金額,評価額
,,,,7203,トヨタ自動車,東P,特定,100,--,"2,500","2,520",↑,"+2,000",+0.80%,"250,000","252,000"
,,,,9201,日本電信電話,東P,特定,1000,--,"150","152",↑,"+2,000",+1.33%,"150,000","152,000"
"""

        holdings_data = SBICSVParser.parse_holdings_csv(csv_content)
        assert len(holdings_data) == 2

        # Step 2: データベースに保存
        updated, new = PortfolioManager.update_holdings_from_csv(
            user_id, holdings_data, "test_account"
        )
        assert new == 2
        assert updated == 0

        # Step 3: 保有銘柄を確認
        holdings = Holding.find_all_by_user(user_id)
        assert len(holdings) == 2

        toyota = next(h for h in holdings if h.code == "7203")
        assert toyota.quantity == 100
        assert toyota.average_price == 2500
        assert toyota.account_name == "test_account"

        # Step 4: 取引履歴を追加
        trans = Transaction(
            user_id=user_id,
            code="7203",
            transaction_date=datetime.now().date().isoformat(),
            transaction_type="buy",
            quantity=50,
            price=2550,
        )
        trans.commission = 275
        trans.tax = 0
        trans.save()

        # Step 5: 取引履歴を確認
        transactions = Transaction.find_all_by_user(user_id, code="7203")
        assert len(transactions) == 1
        assert transactions[0].quantity == 50

        # Step 6: 保有銘柄を更新（追加購入を反映）
        toyota.quantity = 150
        toyota.average_price = (100 * 2500 + 50 * 2550) / 150
        toyota.save()

        # Step 7: 更新後の保有銘柄を確認
        updated_holding = Holding.find_by_user_code_and_account(
            user_id, "7203", "test_account"
        )
        assert updated_holding.quantity == 150
        assert round(updated_holding.average_price, 2) == 2516.67

        # Step 8: ポートフォリオ全体の評価額を計算
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT h.code, h.quantity, h.average_price, p.close
            FROM holdings h
            LEFT JOIN prices p ON h.code = SUBSTR(p.code, 1, 4)
            WHERE h.user_id = ? AND h.deleted_at IS NULL
            AND p.date = (SELECT MAX(date) FROM prices WHERE code = p.code)
        """,
            (user_id,),
        )

        portfolio_data = cursor.fetchall()
        conn.close()

        total_cost = sum(row[1] * row[2] for row in portfolio_data)
        total_value = sum(row[1] * row[3] for row in portfolio_data)
        total_value - total_cost

        assert total_cost > 0
        assert total_value > 0

    def test_csv_update_workflow(self, test_environment, monkeypatch):
        """CSV更新による論理削除を含むワークフロー"""
        db_path = test_environment["db_path"]
        user_id = test_environment["user_id"]

        # データベースパスをモック
        monkeypatch.setattr("src.config.get_db_path", lambda: str(db_path))
        monkeypatch.setattr("src.portfolio.models.get_db_path", lambda: str(db_path))
        monkeypatch.setattr("src.portfolio.manager.get_db_path", lambda: str(db_path))

        # 初回CSV読み込み（2銘柄）
        csv_content1 = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,注文株数,取得単価,現在値,現在値,評価損益,評価損益(%),買付金額,評価額
,,,,7203,トヨタ自動車,東P,特定,100,--,"2,500","2,520",↑,"+2,000",+0.80%,"250,000","252,000"
,,,,9201,日本電信電話,東P,特定,1000,--,"150","152",↑,"+2,000",+1.33%,"150,000","152,000"
"""

        holdings_data1 = SBICSVParser.parse_holdings_csv(csv_content1)
        PortfolioManager.update_holdings_from_csv(user_id, holdings_data1, "default")

        # 保有銘柄を確認
        holdings = Holding.find_all_by_user(user_id)
        assert len(holdings) == 2

        # 2回目のCSV読み込み（1銘柄のみ - NTTが削除される）
        csv_content2 = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,注文株数,取得単価,現在値,現在値,評価損益,評価損益(%),買付金額,評価額
,,,,7203,トヨタ自動車,東P,特定,150,--,"2,600","2,520",↓,"-12,000",-3.08%,"390,000","378,000"
"""

        holdings_data2 = SBICSVParser.parse_holdings_csv(csv_content2)
        updated, new = PortfolioManager.update_holdings_from_csv(
            user_id, holdings_data2, "default"
        )

        assert updated == 1  # トヨタが更新
        assert new == 0

        # アクティブな保有銘柄は1つだけ
        active_holdings = Holding.find_all_by_user(user_id)
        assert len(active_holdings) == 1
        assert active_holdings[0].code == "7203"
        assert active_holdings[0].quantity == 150

        # 論理削除された銘柄を確認
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT code, deleted_at FROM holdings WHERE user_id = ? AND deleted_at IS NOT NULL",
            (user_id,),
        )
        deleted = cursor.fetchall()
        conn.close()

        assert len(deleted) == 1
        assert deleted[0][0] == "9201"
        assert deleted[0][1] is not None

    def test_transaction_csv_workflow(self, test_environment, monkeypatch):
        """取引履歴CSVの読み込みワークフロー"""
        db_path = test_environment["db_path"]
        user_id = test_environment["user_id"]

        # データベースパスをモック
        monkeypatch.setattr("src.config.get_db_path", lambda: str(db_path))
        monkeypatch.setattr("src.portfolio.models.get_db_path", lambda: str(db_path))

        # 取引履歴CSV（注文一覧形式）
        csv_content = """
"銘柄（コード）","銘柄（名前）","銘柄（市場）","取引区分","期限","預り区分","約定日","注文株数","約定株数","約定単価","手数料","消費税","約定代金","入金額"
"7203","トヨタ自動車","東証プライム","現物買","2024/01/15","特定預り","2024/01/15 09:00:00","100","100","2500.00","250","25","250275",""
"9201","日本電信電話","東証プライム","現物買","2024/01/16","特定預り","2024/01/16 10:00:00","1000","1000","150.00","100","10","150110",""
"7203","トヨタ自動車","東証プライム","現物売","2024/01/20","特定預り","2024/01/20 13:00:00","50","50","2600.00","125","12","","129863"
"""

        transactions = SBICSVParser.parse_transactions_csv(csv_content)
        assert len(transactions) == 3

        # データベースに保存
        for trans_data in transactions:
            trans = Transaction(
                user_id=user_id,
                code=trans_data["code"],
                transaction_date=trans_data["transaction_date"],
                transaction_type=trans_data["transaction_type"],
                quantity=trans_data["quantity"],
                price=trans_data["price"],
            )
            trans.commission = trans_data["commission"]
            trans.tax = trans_data["tax"]
            trans.save()

        # 取引履歴を確認
        all_transactions = Transaction.find_all_by_user(user_id)
        assert len(all_transactions) == 3

        # トヨタの取引履歴
        toyota_trans = Transaction.find_all_by_user(user_id, code="7203")
        assert len(toyota_trans) == 2

        buy_trans = next(t for t in toyota_trans if t.transaction_type == "buy")
        sell_trans = next(t for t in toyota_trans if t.transaction_type == "sell")

        assert buy_trans.quantity == 100
        assert sell_trans.quantity == 50

        # 売却益の計算
        profit = (sell_trans.price - buy_trans.price) * sell_trans.quantity
        profit -= sell_trans.commission + sell_trans.tax
        assert profit > 0  # 利益が出ている


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

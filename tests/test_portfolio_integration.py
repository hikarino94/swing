"""ポートフォリオ管理の統合テスト"""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from src.portfolio.csv_parser import SBICSVParser
from src.portfolio.manager import PortfolioManager
from src.portfolio.models import Holding


@pytest.fixture
def test_db():
    """テスト用の一時データベース"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    # テーブル作成
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            email TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            account_name TEXT NOT NULL DEFAULT 'default',
            quantity INTEGER NOT NULL,
            average_price REAL NOT NULL,
            market_value REAL,
            profit_loss REAL,
            profit_loss_ratio REAL,
            updated_at TEXT DEFAULT (datetime('now')),
            expected_per REAL,
            actual_pbr REAL,
            dividend_yield REAL,
            expected_eps REAL,
            actual_bps REAL,
            expected_dividend REAL,
            lending_type TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS listed_info (
            code TEXT PRIMARY KEY,
            date TEXT,
            company_name TEXT,
            company_name_en TEXT,
            sector17_code TEXT,
            sector17_name TEXT,
            sector33_code TEXT,
            sector33_name TEXT,
            scale_category TEXT,
            market_code TEXT,
            market_name TEXT,
            margin_code TEXT,
            margin_name TEXT,
            delete_flag INTEGER
        );

        INSERT INTO users (username, password_hash) VALUES ('test_user', 'dummy_hash');
    """
    )
    conn.commit()
    conn.close()

    yield db_path

    # クリーンアップ
    db_path.unlink()


def test_csv_to_db_integration(test_db, monkeypatch):
    """CSVから読み込んだデータがデータベースに正しく保存されることを確認"""

    # DB_PATHをモック
    monkeypatch.setattr("src.config.DB_PATH", str(test_db))
    monkeypatch.setattr("src.portfolio.models.DB_PATH", str(test_db))
    monkeypatch.setattr("src.portfolio.manager.DB_PATH", str(test_db))

    # サンプルCSVデータ
    csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,注文株数,取得単価,現在値,現在値,評価損益,評価損益(%),買付金額,評価額,基準値,基準値比,基準値比(%),決算月,貸株金利,始値,高値,安値,売買代金(千円),出来高,予想PER(倍),実績PBR(倍),予想配当利回り(%),予想EPS,実績BPS,予想1株配当,貸借区分,騰落チャート(日足)
,,,,7203,トヨタ自動車,東P,特定,100,--,"2,500","2,800",↑,"+30,000",+12.00%,"250,000","280,000","2,750",+50,+1.82%,3月,0.10%,"2,750","2,810","2,740","5,000","1,800",9.50,1.20,3.50,294.7,"2,333.33",98,貸借,
,,,,9984,ソフトバンクグループ,東P,NISA,50,--,"8,000","7,500",↓,"-25,000",-6.25%,"400,000","375,000","7,600",-100,-1.32%,3月,0.10%,"7,600","7,650","7,450","3,000",400,15.25,2.10,0.50,491.8,"3,571.43",37.5,貸借,"""

    # CSVを解析
    holdings_data = SBICSVParser.parse_holdings_csv(csv_content)
    assert len(holdings_data) == 2

    # PortfolioManagerを使って保存
    user_id = 1
    updated, new = PortfolioManager.update_holdings_from_csv(
        user_id, holdings_data, "test_account"
    )

    assert new == 2
    assert updated == 0

    # データベースから読み込んで確認
    holdings = Holding.find_all_by_user(user_id)
    assert len(holdings) == 2

    # トヨタの確認
    toyota = next(h for h in holdings if h.code == "7203")
    assert toyota.quantity == 100
    assert toyota.average_price == 2500
    assert toyota.expected_per == 9.50
    assert toyota.actual_pbr == 1.20
    assert toyota.dividend_yield == 3.50
    assert toyota.expected_eps == 294.7
    assert toyota.actual_bps == 2333.33
    assert toyota.expected_dividend == 98
    assert toyota.lending_type == "貸借"

    # ソフトバンクグループの確認
    sbg = next(h for h in holdings if h.code == "9984")
    assert sbg.quantity == 50
    assert sbg.average_price == 8000
    assert sbg.expected_per == 15.25
    assert sbg.actual_pbr == 2.10
    assert sbg.dividend_yield == 0.50


def test_holding_model_with_indicators(test_db, monkeypatch):
    """Holdingモデルが株価指標データを正しく保存・読み込みできることを確認"""

    monkeypatch.setattr("src.portfolio.models.DB_PATH", str(test_db))

    # 新規作成
    holding = Holding(user_id=1, code="7203", account_name="test")
    holding.quantity = 100
    holding.average_price = 2500
    holding.expected_per = 9.50
    holding.actual_pbr = 1.20
    holding.dividend_yield = 3.50
    holding.expected_eps = 294.7
    holding.actual_bps = 2333.33
    holding.expected_dividend = 98
    holding.lending_type = "貸借"

    # 保存
    assert holding.save()
    assert holding.id is not None

    # 読み込み
    loaded = Holding.find_by_user_code_and_account(1, "7203", "test")
    assert loaded is not None
    assert loaded.expected_per == 9.50
    assert loaded.actual_pbr == 1.20
    assert loaded.dividend_yield == 3.50
    assert loaded.expected_eps == 294.7
    assert loaded.actual_bps == 2333.33
    assert loaded.expected_dividend == 98
    assert loaded.lending_type == "貸借"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

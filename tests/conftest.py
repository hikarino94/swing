"""共通のテストフィクスチャと設定"""

import os
import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path

import pandas as pd
import pytest

# テスト用の環境変数を設定
os.environ["TESTING"] = "1"
os.environ["FLASK_ENV"] = "testing"


@pytest.fixture
def temp_db() -> Generator[Path, None, None]:
    """テスト用の一時データベースを作成"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    # テーブルを作成
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS prices (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            upper_limit REAL,
            lower_limit REAL,
            volume INTEGER,
            turnover_value REAL,
            adj_factor REAL,
            adj_open REAL,
            adj_high REAL,
            adj_low REAL,
            adj_close REAL,
            adj_volume INTEGER,
            PRIMARY KEY (code, date)
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
            delete_flag INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS statements (
            Code TEXT,
            DisclosureNumber TEXT PRIMARY KEY,
            DisclosedDate TEXT,
            NetSales REAL,
            OperatingProfit REAL,
            OrdinaryProfit REAL,
            Profit REAL
        );
    """
    )
    conn.close()

    yield db_path

    # クリーンアップ
    os.unlink(db_path)


@pytest.fixture
def authenticated_client(tmp_path):
    """認証済みのテストクライアント"""
    # テスト用の一時データベースパスを設定
    test_db_path = tmp_path / "test_stock.db"
    os.environ["DATABASE_PATH"] = str(test_db_path)
    
    from src.ui.web import app
    from werkzeug.security import generate_password_hash
    
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    
    with app.test_client() as client:
        # テスト用データベースを初期化
        from db.db_schema import init_schema
        init_schema(test_db_path)
        
        # テストユーザーを作成
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()
        
        # 既存のユーザーを削除（念のため）
        cursor.execute("DELETE FROM users WHERE email = ?", ("test@example.com",))
        
        cursor.execute(
            """
            INSERT INTO users (username, email, password_hash, role)
            VALUES (?, ?, ?, ?)
            """,
            (
                "testuser",
                "test@example.com",
                generate_password_hash("testpass123"),
                "admin"
            )
        )
        user_id = cursor.lastrowid
        
        # セッションを作成
        session_id = "test-session-id"
        cursor.execute(
            """
            INSERT INTO sessions (id, user_id, expires_at)
            VALUES (?, ?, datetime('now', '+1 day'))
            """,
            (session_id, user_id)
        )
        conn.commit()
        conn.close()
        
        # セッションクッキーを設定
        with client.session_transaction() as sess:
            sess["session_id"] = session_id
            sess["_user_id"] = str(user_id)
        
        # before_requestで適切にユーザーがセットされるようにする
        @app.before_request
        def set_test_user():
            from flask import session, request
            if "session_id" in session and session["session_id"] == session_id:
                from src.auth import AuthManager
                request.current_user = AuthManager.get_user_by_session(session_id)
        
        yield client


@pytest.fixture
def sample_prices_df():
    """テスト用の価格データフレーム"""
    return pd.DataFrame(
        {
            "code": ["1234", "1234", "1234", "5678", "5678"],
            "date": [
                "2024-01-01",
                "2024-01-02",
                "2024-01-03",
                "2024-01-01",
                "2024-01-02",
            ],
            "open": [100.0, 101.0, 102.0, 200.0, 201.0],
            "high": [105.0, 106.0, 107.0, 205.0, 206.0],
            "low": [95.0, 96.0, 97.0, 195.0, 196.0],
            "close": [102.0, 103.0, 104.0, 202.0, 203.0],
            "volume": [10000, 11000, 12000, 20000, 21000],
            "adjustment_close": [102.0, 103.0, 104.0, 202.0, 203.0],
        }
    )


@pytest.fixture
def sample_statements_df():
    """テスト用の財務諸表データフレーム"""
    return pd.DataFrame(
        {
            "code": ["1234", "1234", "5678"],
            "disclosure_date": ["2024-01-10", "2023-10-10", "2024-01-10"],
            "type_of_document": ["1Q", "3Q", "1Q"],
            "net_sales": [1000000, 900000, 2000000],
            "operating_profit": [100000, 90000, 200000],
            "ordinary_profit": [110000, 95000, 210000],
            "profit_attributable_to_owners_of_parent": [80000, 70000, 150000],
            "total_assets": [5000000, 4500000, 8000000],
            "net_assets": [2000000, 1800000, 3000000],
            "equity_to_asset_ratio": [0.4, 0.4, 0.375],
            "book_value_per_share": [200.0, 180.0, 300.0],
        }
    )


@pytest.fixture
def mock_jquants_response():
    """J-Quants APIレスポンスのモック"""

    def _create_response(data_type):
        if data_type == "daily_quotes":
            return {
                "daily_quotes": [
                    {
                        "Code": "1234",
                        "Date": "2024-01-01",
                        "Open": 100,
                        "High": 105,
                        "Low": 95,
                        "Close": 102,
                        "Volume": 10000,
                        "TurnoverValue": 1020000,
                        "AdjustmentFactor": 1.0,
                        "AdjustmentOpen": 100,
                        "AdjustmentHigh": 105,
                        "AdjustmentLow": 95,
                        "AdjustmentClose": 102,
                        "AdjustmentVolume": 10000,
                    }
                ]
            }
        elif data_type == "listed_info":
            return {
                "info": [
                    {
                        "Code": "1234",
                        "CompanyName": "テスト会社",
                        "CompanyNameEnglish": "Test Company",
                        "Sector17Code": "1",
                        "Sector17CodeName": "食品",
                        "Sector33Code": "1050",
                        "Sector33CodeName": "電気機器",
                        "ScaleCategory": "TOPIX Core30",
                        "MarketCode": "0111",
                        "MarketCodeName": "プライム",
                    }
                ]
            }
        elif data_type == "statements":
            return {
                "statements": [
                    {
                        "Code": "1234",
                        "DisclosureDate": "2024-01-10",
                        "TypeOfDocument": "1Q",
                        "NetSales": 1000000,
                        "OperatingProfit": 100000,
                        "OrdinaryProfit": 110000,
                        "ProfitAttributableToOwnersOfParent": 80000,
                        "TotalAssets": 5000000,
                        "NetAssets": 2000000,
                        "EquityToAssetRatio": 0.4,
                        "BookValuePerShare": 200.0,
                    }
                ]
            }
        elif data_type == "idtoken":
            return {"idToken": "test_id_token_12345"}
        return {}

    return _create_response


@pytest.fixture
def sample_config(tmp_path: Path) -> Path:
    """テスト用の設定ファイルを作成"""
    config_path = tmp_path / "config.json"
    config_path.write_text(
        """
    {
        "database": {
            "path": "test.db"
        },
        "api": {
            "base_url": "https://api.example.com/v1",
            "endpoints": {
                "auth": "/auth",
                "daily_quotes": "/quotes"
            },
            "rate_limit": {
                "sleep_seconds": 0.1
            }
        },
        "files": {
            "account": "account.json",
            "idtoken": "idtoken.json",
            "thresholds": "thresholds.json"
        },
        "logging": {
            "level": "DEBUG",
            "format": "%(message)s"
        }
    }
    """
    )
    return config_path


@pytest.fixture
def mock_idtoken(tmp_path: Path) -> Path:
    """テスト用のIDトークンファイルを作成"""
    token_path = tmp_path / "idtoken.json"
    token_path.write_text('{"idToken": "test-token-12345"}')
    return token_path


@pytest.fixture
def test_db() -> Generator[str, None, None]:
    """test_daily_quotes.py用のテストデータベース"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # pricesテーブルを作成
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            upper_limit REAL,
            lower_limit REAL,
            volume INTEGER,
            turnover_value REAL,
            adj_factor REAL,
            adj_open REAL,
            adj_high REAL,
            adj_low REAL,
            adj_close REAL,
            adj_volume INTEGER,
            PRIMARY KEY (code, date)
        )
    """
    )
    conn.commit()
    conn.close()

    yield db_path

    # クリーンアップ
    os.unlink(db_path)

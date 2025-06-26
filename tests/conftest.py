"""Pytest共通設定とフィクスチャ"""
import pytest
import tempfile
import sqlite3
from pathlib import Path
from datetime import date, datetime
import pandas as pd
import json
import logging

# テスト用のログレベル設定
logging.getLogger().setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def test_data_dir():
    """テストデータディレクトリ"""
    return Path(__file__).parent / "data"


@pytest.fixture
def temp_dir():
    """一時ディレクトリ"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_db():
    """一時的なSQLiteデータベース"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = Path(tmp.name)
    
    yield db_path
    
    # クリーンアップ
    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def sample_config_data():
    """サンプル設定データ"""
    return {
        "idToken": "test_token_12345",
        "mailaddress": "test@example.com",
        "password": "test_password"
    }


@pytest.fixture
def sample_price_data():
    """サンプル株価データ"""
    return pd.DataFrame({
        "code": ["1301", "1301", "1332", "1332"],
        "date": ["2023-01-01", "2023-01-02", "2023-01-01", "2023-01-02"],
        "open": [100.0, 102.0, 200.0, 205.0],
        "high": [105.0, 108.0, 210.0, 215.0],
        "low": [98.0, 100.0, 195.0, 200.0],
        "close": [103.0, 106.0, 208.0, 212.0],
        "volume": [1000000, 1200000, 800000, 900000],
        "adj_close": [103.0, 106.0, 208.0, 212.0]
    })


@pytest.fixture
def sample_signal_data():
    """サンプルシグナルデータ"""
    return pd.DataFrame({
        "code": ["1301", "1332", "1333"],
        "signal_date": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
        "signal_type": ["buy", "buy", "sell"],
        "strength": [0.8, 0.7, 0.6]
    })


@pytest.fixture
def initialized_test_db(temp_db, sample_price_data):
    """初期化済みテストデータベース"""
    # 基本的なテーブル構造を作成
    with sqlite3.connect(temp_db) as conn:
        # pricesテーブル
        conn.execute("""
            CREATE TABLE prices (
                code TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                adj_close REAL,
                PRIMARY KEY (code, date)
            )
        """)
        
        # listed_infoテーブル
        conn.execute("""
            CREATE TABLE listed_info (
                code TEXT PRIMARY KEY,
                name TEXT,
                market_code TEXT,
                sector TEXT
            )
        """)
        
        # fundamental_signalsテーブル
        conn.execute("""
            CREATE TABLE fundamental_signals (
                LocalCode TEXT,
                DisclosedAt TEXT,
                TypeOfCurrentPeriod TEXT,
                eps_yoy_fy REAL,
                eps_yoy_q REAL,
                cf_quality REAL,
                created_at TEXT
            )
        """)
        
        # technical_indicatorsテーブル
        conn.execute("""
            CREATE TABLE technical_indicators (
                code TEXT,
                signal_date TEXT,
                signal_type TEXT,
                strength REAL,
                rsi REAL,
                bb_position REAL,
                created_at TEXT
            )
        """)
        
        # サンプルデータを挿入
        sample_price_data.to_sql("prices", conn, if_exists="append", index=False)
        
        # listed_infoにサンプルデータ
        sample_listed = pd.DataFrame({
            "code": ["1301", "1332", "1333"],
            "name": ["極洋", "日本水産", "マルハニチロ"],
            "market_code": ["0101", "0101", "0101"],
            "sector": ["水産・農林業", "水産・農林業", "水産・農林業"]
        })
        sample_listed.to_sql("listed_info", conn, if_exists="append", index=False)
    
    return temp_db


@pytest.fixture
def mock_jquants_response():
    """J-Quants APIレスポンスのモック"""
    return {
        "daily_quotes": [
            {
                "Code": "1301",
                "Date": "2023-01-01",
                "Open": 100.0,
                "High": 105.0,
                "Low": 98.0,
                "Close": 103.0,
                "Volume": 1000000,
                "AdjustmentClose": 103.0
            }
        ],
        "pagination_key": None
    }


@pytest.fixture(autouse=True)
def reset_logging():
    """各テスト後にロギング設定をリセット"""
    yield
    # テスト後のクリーンアップ
    logger = logging.getLogger()
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)


@pytest.fixture
def mock_config_files(temp_dir, sample_config_data):
    """モック設定ファイル群"""
    # idtoken.json
    idtoken_file = temp_dir / "idtoken.json"
    with open(idtoken_file, "w") as f:
        json.dump({"idToken": sample_config_data["idToken"]}, f)
    
    # account.json
    account_file = temp_dir / "account.json"
    with open(account_file, "w") as f:
        json.dump({
            "mailaddress": sample_config_data["mailaddress"],
            "password": sample_config_data["password"]
        }, f)
    
    return {
        "idtoken": idtoken_file,
        "account": account_file,
        "base_dir": temp_dir
    }
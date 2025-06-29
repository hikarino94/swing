"""共通のテストフィクスチャと設定"""
import os
import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest


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
            adj_close REAL,
            adj_volume INTEGER,
            PRIMARY KEY (code, date)
        );

        CREATE TABLE IF NOT EXISTS listed_info (
            code TEXT PRIMARY KEY,
            company_name TEXT,
            sector33_name TEXT,
            delete_flag INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS statements (
            LocalCode TEXT,
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

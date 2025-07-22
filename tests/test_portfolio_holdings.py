"""
保有銘柄管理機能のテスト
"""

import sqlite3
from unittest.mock import patch

import pytest

from src.portfolio.holdings import (
    add_holding,
    bulk_delete_holdings,
    delete_holding,
    get_holdings,
    search_listed_info,
    update_holding,
)


@pytest.fixture
def test_db(tmp_path):
    """テスト用データベースのセットアップ"""
    db_path = tmp_path / "test.db"

    # テーブル作成
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                account_name TEXT NOT NULL,
                account_type TEXT NOT NULL,
                stock_type TEXT DEFAULT '現物',
                trade_position TEXT,
                margin_term TEXT,
                quantity INTEGER NOT NULL,
                average_price REAL NOT NULL,
                current_price REAL,
                market_value REAL,
                profit_loss REAL,
                profit_loss_ratio REAL,
                expected_per REAL,
                actual_pbr REAL,
                dividend_yield REAL,
                expected_eps REAL,
                actual_bps REAL,
                expected_dividend REAL,
                lending_type TEXT,
                acquisition_date TEXT,
                updated_at TEXT DEFAULT (datetime('now')),
                deleted_at TEXT DEFAULT NULL
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE listed_info (
                code TEXT PRIMARY KEY,
                company_name TEXT,
                market_name TEXT,
                sector17_name TEXT,
                sector33_name TEXT,
                delete_flag INTEGER DEFAULT 0
            )
        """
        )

        # テストデータ挿入（5桁コード形式）
        conn.execute(
            """
            INSERT INTO listed_info (code, company_name, market_name, sector17_name, sector33_name, delete_flag)
            VALUES ('72030', 'トヨタ自動車', '東証プライム', '輸送用機器', '輸送用機器', 0)
        """
        )

        conn.execute(
            """
            INSERT INTO holdings (user_id, code, account_name, account_type,
                                quantity, average_price, current_price)
            VALUES (1, '7203', 'default', '特定', 100, 2000, 2100)
        """
        )

        # pricesテーブル（get_holdingsで参照される）
        conn.execute(
            """
            CREATE TABLE prices (
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                upper_limit REAL,
                lower_limit REAL,
                volume INTEGER,
                turnover_value INTEGER,
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

        # pricesテストデータ挿入（5桁コード形式）
        conn.execute(
            """
            INSERT INTO prices (code, date, close)
            VALUES ('72030', '2025-07-22', 2100)
        """
        )

    return db_path


@patch("src.portfolio.holdings.get_db_path")
def test_get_holdings(mock_db_path, test_db):
    """保有銘柄取得のテスト"""
    mock_db_path.return_value = str(test_db)

    # 全件取得
    holdings = get_holdings(user_id=1)
    assert len(holdings) == 1
    assert holdings[0]["code"] == "7203"
    assert holdings[0]["quantity"] == 100

    # 口座名でフィルタ
    holdings = get_holdings(user_id=1, account_name="default")
    assert len(holdings) == 1

    # 存在しない口座
    holdings = get_holdings(user_id=1, account_name="unknown")
    assert len(holdings) == 0


@patch("src.portfolio.holdings.get_db_path")
def test_add_holding(mock_db_path, test_db):
    """保有銘柄追加のテスト"""
    mock_db_path.return_value = str(test_db)

    # 新規追加
    holding_id = add_holding(
        user_id=1,
        code="6758",
        account_name="test",
        account_type="NISA",
        stock_type="現物",
        quantity=200,
        average_price=5000,
        current_price=5500,
    )

    assert holding_id > 0

    # 追加確認
    holdings = get_holdings(user_id=1)
    assert len(holdings) == 2

    # 追加したデータの確認
    new_holding = next(h for h in holdings if h["code"] == "6758")
    assert new_holding["account_name"] == "test"
    assert new_holding["account_type"] == "NISA"
    assert new_holding["quantity"] == 200


@patch("src.portfolio.holdings.get_db_path")
def test_update_holding(mock_db_path, test_db):
    """保有銘柄更新のテスト"""
    mock_db_path.return_value = str(test_db)

    # 更新
    success = update_holding(
        holding_id=1, quantity=150, average_price=2050, current_price=2200
    )

    assert success is True

    # 更新確認
    holdings = get_holdings(user_id=1)
    assert holdings[0]["quantity"] == 150
    assert holdings[0]["average_price"] == 2050
    assert holdings[0]["current_price"] == 2100  # pricesテーブルの値が使われるため

    # 存在しないIDの更新
    success = update_holding(holding_id=999, quantity=100)
    assert success is False


@patch("src.portfolio.holdings.get_db_path")
def test_delete_holding(mock_db_path, test_db):
    """保有銘柄削除のテスト"""
    mock_db_path.return_value = str(test_db)

    # 削除
    success = delete_holding(holding_id=1)
    assert success is True

    # 削除確認（論理削除なので物理的には残っている）
    holdings = get_holdings(user_id=1)
    assert len(holdings) == 0

    # 既に削除済みのものを再度削除
    success = delete_holding(holding_id=1)
    assert success is False


@patch("src.portfolio.holdings.get_db_path")
def test_bulk_delete_holdings(mock_db_path, test_db):
    """一括削除のテスト"""
    mock_db_path.return_value = str(test_db)

    # テストデータ追加
    add_holding(
        user_id=1,
        code="6758",
        account_name="test",
        account_type="特定",
        stock_type="現物",
        quantity=100,
        average_price=5000,
    )
    add_holding(
        user_id=1,
        code="9984",
        account_name="test",
        account_type="特定",
        stock_type="現物",
        quantity=200,
        average_price=3000,
    )

    # 特定口座の一括削除
    deleted_count = bulk_delete_holdings(user_id=1, account_name="test")
    assert deleted_count == 2

    # 削除確認
    holdings = get_holdings(user_id=1, account_name="test")
    assert len(holdings) == 0

    # 他の口座は残っている
    holdings = get_holdings(user_id=1, account_name="default")
    assert len(holdings) == 1


@patch("src.portfolio.holdings.get_db_path")
def test_search_listed_info(mock_db_path, test_db):
    """銘柄検索のテスト"""
    mock_db_path.return_value = str(test_db)

    # 銘柄コードで検索
    results = search_listed_info("7203")
    assert len(results) == 1
    assert results[0]["code"] == "7203"
    assert results[0]["company_name"] == "トヨタ自動車"

    # 銘柄名の一部で検索
    results = search_listed_info("トヨタ")
    assert len(results) == 1

    # 存在しない銘柄
    results = search_listed_info("9999")
    assert len(results) == 0


@patch("src.portfolio.holdings.get_db_path")
def test_aggregate_holdings(mock_db_path, test_db):
    """合算表示のテスト"""
    mock_db_path.return_value = str(test_db)

    # 同一銘柄を複数口座に追加
    add_holding(
        user_id=1,
        code="7203",
        account_name="test",
        account_type="NISA",
        stock_type="現物",
        quantity=50,
        average_price=2100,
    )

    # 通常表示
    holdings = get_holdings(user_id=1, aggregate=False)
    assert len(holdings) == 2

    # 合算表示
    holdings = get_holdings(user_id=1, aggregate=True)
    assert len(holdings) == 1
    assert holdings[0]["total_quantity"] == 150  # 100 + 50


@patch("src.portfolio.holdings.get_db_path")
def test_margin_holdings(mock_db_path, test_db):
    """信用取引銘柄のテスト"""
    mock_db_path.return_value = str(test_db)

    # 信用買建を追加
    add_holding(
        user_id=1,
        code="6758",
        account_name="default",
        account_type="特定",
        stock_type="信用",
        trade_position="買建",
        margin_term="6ヵ月",
        quantity=100,
        average_price=5000,
    )

    # 信用売建を追加
    add_holding(
        user_id=1,
        code="6758",
        account_name="default",
        account_type="特定",
        stock_type="信用",
        trade_position="売建",
        margin_term="無期限",
        quantity=50,
        average_price=5200,
    )

    holdings = get_holdings(user_id=1)

    # 現物と信用が別レコードとして管理されているか確認
    assert len(holdings) == 3

    # 信用取引の詳細確認
    margin_buy = next(h for h in holdings if h["trade_position"] == "買建")
    assert margin_buy["stock_type"] == "信用"
    assert margin_buy["margin_term"] == "6ヵ月"

    margin_sell = next(h for h in holdings if h["trade_position"] == "売建")
    assert margin_sell["stock_type"] == "信用"
    assert margin_sell["margin_term"] == "無期限"

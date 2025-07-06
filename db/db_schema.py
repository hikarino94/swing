#!/usr/bin/env python
"""
SQLite schema initializer for the swing‑trade project.

Usage:
    python db_schema.py

This file intentionally contains **only ASCII characters** to avoid the
`unicodeescape` issue on Windows.
"""

import logging
import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import DB_PATH

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("db_schema")

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;

-- prices -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prices (
    code            TEXT NOT NULL,
    date            TEXT NOT NULL,  -- YYYY-MM-DD
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    upper_limit     REAL,
    lower_limit     REAL,
    volume          INTEGER,
    turnover_value  INTEGER,
    adj_factor      REAL,
    adj_open        REAL,
    adj_high        REAL,
    adj_low         REAL,
    adj_close       REAL,
    adj_volume      INTEGER,
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);
CREATE INDEX IF NOT EXISTS idx_prices_code ON prices(code);
-- 複合インデックス（日付範囲検索の高速化）
CREATE INDEX IF NOT EXISTS idx_prices_code_date ON prices(code, date);
CREATE INDEX IF NOT EXISTS idx_prices_date_code ON prices(date, code);

-- listed_info (master) ----------------------------------------------
CREATE TABLE IF NOT EXISTS listed_info (
    code            TEXT PRIMARY KEY,
    date            TEXT,
    company_name    TEXT,
    company_name_en TEXT,
    sector17_code   TEXT,
    sector17_name   TEXT,
    sector33_code   TEXT,
    sector33_name   TEXT,
    scale_category  TEXT,
    market_code     TEXT,
    market_name     TEXT,
    margin_code     TEXT,
    margin_name     TEXT,
    delete_flag     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_listed_date ON listed_info(code);
-- 市場コードでのフィルタリング高速化
CREATE INDEX IF NOT EXISTS idx_listed_market_code ON listed_info(market_code);

-- statements -------------------------------------------------------
CREATE TABLE IF NOT EXISTS statements (
    DisclosedDate                                 TEXT,
    DisclosedTime                                 TEXT,
    code                                          TEXT,
    DisclosureNumber                              TEXT    PRIMARY KEY,
    TypeOfDocument                                TEXT,
    TypeOfCurrentPeriod                           TEXT,
    CurrentPeriodStartDate                        TEXT,
    CurrentPeriodEndDate                          TEXT,
    CurrentFiscalYearStartDate                    TEXT,
    CurrentFiscalYearEndDate                      TEXT,
    NextFiscalYearStartDate                       TEXT,
    NextFiscalYearEndDate                         TEXT,

    NetSales                                      REAL,
    OperatingProfit                               REAL,
    OrdinaryProfit                                REAL,
    Profit                                        REAL,
    EarningsPerShare                              REAL,
    DilutedEarningsPerShare                       REAL,
    TotalAssets                                   REAL,
    Equity                                        REAL,
    EquityToAssetRatio                            REAL,
    BookValuePerShare                             REAL,
    CashFlowsFromOperatingActivities              REAL,
    CashFlowsFromInvestingActivities              REAL,
    CashFlowsFromFinancingActivities              REAL,
    CashAndEquivalents                            REAL,

    ResultDividendPerShare1stQuarter              REAL,
    ResultDividendPerShare2ndQuarter              REAL,
    ResultDividendPerShare3rdQuarter              REAL,
    ResultDividendPerShareFiscalYearEnd           REAL,
    ResultDividendPerShareAnnual                  REAL,
    DistributionsPerUnit_REIT                    REAL,
    ResultTotalDividendPaidAnnual                 REAL,
    ResultPayoutRatioAnnual                       REAL,

    ForecastDividendPerShare1stQuarter            REAL,
    ForecastDividendPerShare2ndQuarter            REAL,
    ForecastDividendPerShare3rdQuarter            REAL,
    ForecastDividendPerShareFiscalYearEnd         REAL,
    ForecastDividendPerShareAnnual                REAL,
    ForecastDistributionsPerUnit_REIT            REAL,
    ForecastTotalDividendPaidAnnual               REAL,
    ForecastPayoutRatioAnnual                     REAL,

    NextYearForecastDividendPerShare1stQuarter    REAL,
    NextYearForecastDividendPerShare2ndQuarter    REAL,
    NextYearForecastDividendPerShare3rdQuarter    REAL,
    NextYearForecastDividendPerShareFiscalYearEnd REAL,
    NextYearForecastDividendPerShareAnnual        REAL,
    NextYearForecastDistributionsPerUnit_REIT    REAL,
    NextYearForecastPayoutRatioAnnual             REAL,

    ForecastNetSales2ndQuarter                    REAL,
    ForecastOperatingProfit2ndQuarter             REAL,
    ForecastOrdinaryProfit2ndQuarter              REAL,
    ForecastProfit2ndQuarter                      REAL,
    ForecastEarningsPerShare2ndQuarter            REAL,
    NextYearForecastNetSales2ndQuarter            REAL,
    NextYearForecastOperatingProfit2ndQuarter     REAL,
    NextYearForecastOrdinaryProfit2ndQuarter      REAL,
    NextYearForecastProfit2ndQuarter              REAL,
    NextYearForecastEarningsPerShare2ndQuarter    REAL,

    ForecastNetSales                              REAL,
    ForecastOperatingProfit                       REAL,
    ForecastOrdinaryProfit                        REAL,
    ForecastProfit                                REAL,
    ForecastEarningsPerShare                      REAL,
    NextYearForecastNetSales                      REAL,
    NextYearForecastOperatingProfit               REAL,
    NextYearForecastOrdinaryProfit                REAL,
    NextYearForecastProfit                        REAL,
    NextYearForecastEarningsPerShare              REAL,

    MaterialChangesInSubsidiaries                  TEXT,
    SignificantChangesInTheScopeOfConsolidation    TEXT,
    ChangesBasedOnRevisionsOfAccountingStandard    TEXT,
    ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard TEXT,
    ChangesInAccountingEstimates                   TEXT,
    RetrospectiveRestatement                      TEXT,

    NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock REAL,
    NumberOfTreasuryStockAtTheEndOfFiscalYear     REAL,
    AverageNumberOfShares                         REAL,

    NonConsolidatedNetSales                       REAL,
    NonConsolidatedOperatingProfit                 REAL,
    NonConsolidatedOrdinaryProfit                  REAL,
    NonConsolidatedProfit                         REAL,
    NonConsolidatedEarningsPerShare                REAL,
    NonConsolidatedTotalAssets                    REAL,
    NonConsolidatedEquity                         REAL,
    NonConsolidatedEquityToAssetRatio             REAL,
    NonConsolidatedBookValuePerShare               REAL,

    ForecastNonConsolidatedNetSales2ndQuarter     REAL,
    ForecastNonConsolidatedOperatingProfit2ndQuarter REAL,
    ForecastNonConsolidatedOrdinaryProfit2ndQuarter REAL,
    ForecastNonConsolidatedProfit2ndQuarter       REAL,
    ForecastNonConsolidatedEarningsPerShare2ndQuarter REAL,
    NextYearForecastNonConsolidatedNetSales2ndQuarter REAL,
    NextYearForecastNonConsolidatedOperatingProfit2ndQuarter REAL,
    NextYearForecastNonConsolidatedOrdinaryProfit2ndQuarter REAL,
    NextYearForecastNonConsolidatedProfit2ndQuarter REAL,
    NextYearForecastNonConsolidatedEarningsPerShare2ndQuarter REAL,

    ForecastNonConsolidatedNetSales               REAL,
    ForecastNonConsolidatedOperatingProfit         REAL,
    ForecastNonConsolidatedOrdinaryProfit          REAL,
    ForecastNonConsolidatedProfit                 REAL,
    ForecastNonConsolidatedEarningsPerShare        REAL,
    NextYearForecastNonConsolidatedNetSales       REAL,
    NextYearForecastNonConsolidatedOperatingProfit REAL,
    NextYearForecastNonConsolidatedOrdinaryProfit  REAL,
    NextYearForecastNonConsolidatedProfit         REAL,
    NextYearForecastNonConsolidatedEarningsPerShare REAL
);

CREATE INDEX IF NOT EXISTS idx_statements_code  ON statements(code);
CREATE INDEX IF NOT EXISTS idx_statements_disclosure_no ON statements(DisclosureNumber);
CREATE INDEX IF NOT EXISTS idx_statements_disclosed_date
    ON statements(DisclosedDate);
-- fundamental_signals ----------------------------------------------
-- スクリーニング結果を永続化し、後から検証・可視化できるようにする
CREATE TABLE IF NOT EXISTS fundamental_signals (
    code                TEXT NOT NULL,
    DisclosedAt         TEXT NOT NULL,  -- ISO8601 (YYYY‑MM‑DD HH:MM:SS)
    TypeOfCurrentPeriod TEXT,

    eps_yoy_fy          REAL,
    eps_yoy_q           REAL,
    op_margin_delta     REAL,
    feps_revision       REAL,
    cf_quality          REAL,
    eta_delta           REAL,
    leverage            REAL,
    turnaround          INTEGER,  -- 0/1
    treasury_delta      REAL,

    created_at          TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (code, DisclosedAt)
);
CREATE INDEX IF NOT EXISTS idx_fsignals_code ON fundamental_signals(code);
CREATE INDEX IF NOT EXISTS idx_fsignals_created  ON fundamental_signals(created_at);

-- technical_indicators ----------------------------------------------
CREATE TABLE IF NOT EXISTS technical_indicators (
    code TEXT       NOT NULL,
    signal_date TEXT       NOT NULL,  -- YYYY-MM-DD
    signal_ma INTEGER,
    signal_rsi INTEGER,
    signal_adx INTEGER,
    signal_bb INTEGER,
    signal_macd INTEGER,
    signal_ma_short INTEGER,
    signal_rsi_short INTEGER,
    signal_bb_short INTEGER,
    signal_macd_short INTEGER,
    signals_count INTEGER,
    signals_short_count INTEGER,
    signals_overheating INTEGER,
    signals_oversold INTEGER,
    signals_short_first INTEGER,
    signals_first INTEGER,

    PRIMARY KEY (code, signal_date)
);
CREATE INDEX IF NOT EXISTS idx_tindicators_code ON technical_indicators(code);
CREATE INDEX IF NOT EXISTS idx_tindicators_date ON technical_indicators(signal_date);
-- 複合インデックスの追加（シグナル検索高速化）
CREATE INDEX IF NOT EXISTS idx_tech_date_count ON technical_indicators(signal_date, signals_count);
CREATE INDEX IF NOT EXISTS idx_tech_date_short_count ON technical_indicators(signal_date, signals_short_count);

-- users --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT DEFAULT 'admin',  -- 'admin' or 'portfolio_only'
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- sessions -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    remember_me INTEGER DEFAULT 0,  -- 0: false, 1: true
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);

-- holdings -----------------------------------------------------------
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
    expected_per REAL,
    actual_pbr REAL,
    dividend_yield REAL,
    expected_eps REAL,
    actual_bps REAL,
    expected_dividend REAL,
    lending_type TEXT,
    updated_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(user_id, code, account_name)
);
CREATE INDEX IF NOT EXISTS idx_holdings_user_id ON holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_holdings_code ON holdings(code);
CREATE INDEX IF NOT EXISTS idx_holdings_account ON holdings(account_name);

-- transactions -------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    transaction_date TEXT NOT NULL,  -- YYYY-MM-DD
    transaction_type TEXT NOT NULL,  -- 'buy' or 'sell'
    quantity INTEGER NOT NULL,
    price REAL NOT NULL,
    commission REAL DEFAULT 0,
    tax REAL DEFAULT 0,
    total_amount REAL NOT NULL,
    remarks TEXT,
    detailed_type TEXT,
    realized_profit REAL,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_code ON transactions(code);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_user_code ON transactions(user_id, code);


"""


def init_schema(db_path: str | Path) -> None:
    """Create tables and indexes if they do not exist.

    Args:
        db_path: Database file path (str or Path object)

    TODO: 将来的にはプロジェクト全体でPath型に統一することを検討
          現在はconfig.pyがstr型を返すため、互換性のために両方を受け入れる
    """
    with sqlite3.connect(db_path) as conn:
        conn.executescript(DDL)


def main() -> None:  # pragma: no cover
    init_schema(DB_PATH)
    logger.info("Schema created or verified at %s", DB_PATH)


if __name__ == "__main__":
    # • 必要なテーブルとインデックスを作成
    main()

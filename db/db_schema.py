#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SQLite schema initializer for the swing‑trade project.

Usage:
    python db_schema.py

This file intentionally contains **only ASCII characters** to avoid the
`unicodeescape` issue on Windows.
"""

import sqlite3
import sys
from pathlib import Path

DB_PATH = "./stock.db"

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;

-- prices -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prices (
    code            TEXT NOT NULL,
    date            TEXT NOT NULL,
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

-- statements -------------------------------------------------------
CREATE TABLE IF NOT EXISTS statements (
    DisclosedDate                                 TEXT,
    DisclosedTime                                 TEXT,
    LocalCode                                     TEXT,
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

CREATE INDEX IF NOT EXISTS idx_statements_localcode  ON statements(LocalCode);
CREATE INDEX IF NOT EXISTS idx_statements_disclosure_no ON statements(DisclosureNumber);
-- fundamental_signals ----------------------------------------------
-- スクリーニング結果を永続化し、後から検証・可視化できるようにする
CREATE TABLE IF NOT EXISTS fundamental_signals (
    LocalCode           TEXT NOT NULL,
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
    PRIMARY KEY (LocalCode, DisclosedAt)
);
CREATE INDEX IF NOT EXISTS idx_fsignals_localcode ON fundamental_signals(LocalCode);
CREATE INDEX IF NOT EXISTS idx_fsignals_created  ON fundamental_signals(created_at);

-- technical_indicators ----------------------------------------------
CREATE TABLE IF NOT EXISTS technical_indicators (
    code TEXT       NOT NULL,
    signal_date TEXT       NOT NULL,
    signal_ma INTEGER,
    signal_rsi INTEGER,
    signal_adx INTEGER,
    signal_bb INTEGER,
    signal_macd INTEGER,
    signals_count INTEGER,
    signals_overheating INTEGER,
    signals_first INTEGER,

    PRIMARY KEY (code, signal_date)
);
CREATE INDEX IF NOT EXISTS idx_tindicators_code ON technical_indicators(code);
CREATE INDEX IF NOT EXISTS idx_tindicators_date ON technical_indicators(signal_date);


"""


def init_schema(db_path: Path) -> None:
    """Create tables and indexes if they do not exist."""
    with sqlite3.connect(db_path) as conn:
        conn.executescript(DDL)


def main() -> None:  # pragma: no cover
    init_schema(DB_PATH)
    print("Schema created or verified at", DB_PATH)


if __name__ == "__main__":
    # • 必要なテーブルとインデックスを作成
    main()

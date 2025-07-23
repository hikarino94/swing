#!/usr/bin/env python
"""
PostgreSQL schema initializer for the swing-trade project.

Usage:
    python db_schema_postgres.py

PostgreSQL用のスキーマ定義。SQLiteからの主な変更点：
- AUTOINCREMENT → SERIAL
- TEXT型でもPRIMARY KEYが使用可能
- datetime('now') → CURRENT_TIMESTAMP
- PRAGMA文は削除
"""

import logging
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import get_postgres_config
from src.database import get_database_adapter

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("db_schema_postgres")

# PostgreSQL用DDL
DDL = """
-- prices -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prices (
    code            VARCHAR(10) NOT NULL,
    date            DATE NOT NULL,
    open            DECIMAL(10, 2),
    high            DECIMAL(10, 2),
    low             DECIMAL(10, 2),
    close           DECIMAL(10, 2),
    upper_limit     DECIMAL(10, 2),
    lower_limit     DECIMAL(10, 2),
    volume          BIGINT,
    turnover_value  BIGINT,
    adj_factor      DECIMAL(10, 4),
    adj_open        DECIMAL(10, 2),
    adj_high        DECIMAL(10, 2),
    adj_low         DECIMAL(10, 2),
    adj_close       DECIMAL(10, 2),
    adj_volume      BIGINT,
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);
CREATE INDEX IF NOT EXISTS idx_prices_code ON prices(code);
CREATE INDEX IF NOT EXISTS idx_prices_code_date ON prices(code, date);
CREATE INDEX IF NOT EXISTS idx_prices_date_code ON prices(date, code);

-- listed_info (master) ----------------------------------------------
CREATE TABLE IF NOT EXISTS listed_info (
    code            VARCHAR(10) PRIMARY KEY,
    date            DATE,
    company_name    VARCHAR(255),
    company_name_en VARCHAR(255),
    sector17_code   VARCHAR(10),
    sector17_name   VARCHAR(100),
    sector33_code   VARCHAR(10),
    sector33_name   VARCHAR(100),
    scale_category  VARCHAR(50),
    market_code     VARCHAR(10),
    market_name     VARCHAR(50),
    margin_code     VARCHAR(10),
    margin_name     VARCHAR(50),
    delete_flag     SMALLINT
);
CREATE INDEX IF NOT EXISTS idx_listed_date ON listed_info(code);
CREATE INDEX IF NOT EXISTS idx_listed_market_code ON listed_info(market_code);

-- statements -------------------------------------------------------
CREATE TABLE IF NOT EXISTS statements (
    DisclosedDate                                 DATE,
    DisclosedTime                                 TIME,
    code                                          VARCHAR(10),
    DisclosureNumber                              VARCHAR(50) PRIMARY KEY,
    TypeOfDocument                                VARCHAR(100),
    TypeOfCurrentPeriod                           VARCHAR(50),
    CurrentPeriodStartDate                        DATE,
    CurrentPeriodEndDate                          DATE,
    CurrentFiscalYearStartDate                    DATE,
    CurrentFiscalYearEndDate                      DATE,
    NextFiscalYearStartDate                       DATE,
    NextFiscalYearEndDate                         DATE,

    NetSales                                      DECIMAL(20, 2),
    OperatingProfit                               DECIMAL(20, 2),
    OrdinaryProfit                                DECIMAL(20, 2),
    Profit                                        DECIMAL(20, 2),
    EarningsPerShare                              DECIMAL(10, 2),
    DilutedEarningsPerShare                       DECIMAL(10, 2),
    TotalAssets                                   DECIMAL(20, 2),
    Equity                                        DECIMAL(20, 2),
    EquityToAssetRatio                            DECIMAL(5, 2),
    BookValuePerShare                             DECIMAL(10, 2),
    CashFlowsFromOperatingActivities              DECIMAL(20, 2),
    CashFlowsFromInvestingActivities              DECIMAL(20, 2),
    CashFlowsFromFinancingActivities              DECIMAL(20, 2),
    CashAndEquivalents                            DECIMAL(20, 2),

    ResultDividendPerShare1stQuarter              DECIMAL(10, 2),
    ResultDividendPerShare2ndQuarter              DECIMAL(10, 2),
    ResultDividendPerShare3rdQuarter              DECIMAL(10, 2),
    ResultDividendPerShareFiscalYearEnd           DECIMAL(10, 2),
    ResultDividendPerShareAnnual                  DECIMAL(10, 2),
    DistributionsPerUnit_REIT                    DECIMAL(10, 2),
    ResultTotalDividendPaidAnnual                 DECIMAL(20, 2),
    ResultPayoutRatioAnnual                       DECIMAL(5, 2),

    ForecastDividendPerShare1stQuarter            DECIMAL(10, 2),
    ForecastDividendPerShare2ndQuarter            DECIMAL(10, 2),
    ForecastDividendPerShare3rdQuarter            DECIMAL(10, 2),
    ForecastDividendPerShareFiscalYearEnd         DECIMAL(10, 2),
    ForecastDividendPerShareAnnual                DECIMAL(10, 2),
    ForecastDistributionsPerUnit_REIT            DECIMAL(10, 2),
    ForecastTotalDividendPaidAnnual               DECIMAL(20, 2),
    ForecastPayoutRatioAnnual                     DECIMAL(5, 2),

    NextYearForecastDividendPerShare1stQuarter    DECIMAL(10, 2),
    NextYearForecastDividendPerShare2ndQuarter    DECIMAL(10, 2),
    NextYearForecastDividendPerShare3rdQuarter    DECIMAL(10, 2),
    NextYearForecastDividendPerShareFiscalYearEnd DECIMAL(10, 2),
    NextYearForecastDividendPerShareAnnual        DECIMAL(10, 2),
    NextYearForecastDistributionsPerUnit_REIT    DECIMAL(10, 2),
    NextYearForecastPayoutRatioAnnual             DECIMAL(5, 2),

    ForecastNetSales2ndQuarter                    DECIMAL(20, 2),
    ForecastOperatingProfit2ndQuarter             DECIMAL(20, 2),
    ForecastOrdinaryProfit2ndQuarter              DECIMAL(20, 2),
    ForecastProfit2ndQuarter                      DECIMAL(20, 2),
    ForecastEarningsPerShare2ndQuarter            DECIMAL(10, 2),
    NextYearForecastNetSales2ndQuarter            DECIMAL(20, 2),
    NextYearForecastOperatingProfit2ndQuarter     DECIMAL(20, 2),
    NextYearForecastOrdinaryProfit2ndQuarter      DECIMAL(20, 2),
    NextYearForecastProfit2ndQuarter              DECIMAL(20, 2),
    NextYearForecastEarningsPerShare2ndQuarter    DECIMAL(10, 2),

    ForecastNetSales                              DECIMAL(20, 2),
    ForecastOperatingProfit                       DECIMAL(20, 2),
    ForecastOrdinaryProfit                        DECIMAL(20, 2),
    ForecastProfit                                DECIMAL(20, 2),
    ForecastEarningsPerShare                      DECIMAL(10, 2),
    NextYearForecastNetSales                      DECIMAL(20, 2),
    NextYearForecastOperatingProfit               DECIMAL(20, 2),
    NextYearForecastOrdinaryProfit                DECIMAL(20, 2),
    NextYearForecastProfit                        DECIMAL(20, 2),
    NextYearForecastEarningsPerShare              DECIMAL(10, 2),

    MaterialChangesInSubsidiaries                  TEXT,
    SignificantChangesInTheScopeOfConsolidation    TEXT,
    ChangesBasedOnRevisionsOfAccountingStandard    TEXT,
    ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard TEXT,
    ChangesInAccountingEstimates                   TEXT,
    RetrospectiveRestatement                      TEXT,

    NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock DECIMAL(20, 0),
    NumberOfTreasuryStockAtTheEndOfFiscalYear     DECIMAL(20, 0),
    AverageNumberOfShares                         DECIMAL(20, 0),

    NonConsolidatedNetSales                       DECIMAL(20, 2),
    NonConsolidatedOperatingProfit                 DECIMAL(20, 2),
    NonConsolidatedOrdinaryProfit                  DECIMAL(20, 2),
    NonConsolidatedProfit                         DECIMAL(20, 2),
    NonConsolidatedEarningsPerShare                DECIMAL(10, 2),
    NonConsolidatedTotalAssets                    DECIMAL(20, 2),
    NonConsolidatedEquity                         DECIMAL(20, 2),
    NonConsolidatedEquityToAssetRatio             DECIMAL(5, 2),
    NonConsolidatedBookValuePerShare               DECIMAL(10, 2),

    ForecastNonConsolidatedNetSales2ndQuarter     DECIMAL(20, 2),
    ForecastNonConsolidatedOperatingProfit2ndQuarter DECIMAL(20, 2),
    ForecastNonConsolidatedOrdinaryProfit2ndQuarter DECIMAL(20, 2),
    ForecastNonConsolidatedProfit2ndQuarter       DECIMAL(20, 2),
    ForecastNonConsolidatedEarningsPerShare2ndQuarter DECIMAL(10, 2),
    NextYearForecastNonConsolidatedNetSales2ndQuarter DECIMAL(20, 2),
    NextYearForecastNonConsolidatedOperatingProfit2ndQuarter DECIMAL(20, 2),
    NextYearForecastNonConsolidatedOrdinaryProfit2ndQuarter DECIMAL(20, 2),
    NextYearForecastNonConsolidatedProfit2ndQuarter DECIMAL(20, 2),
    NextYearForecastNonConsolidatedEarningsPerShare2ndQuarter DECIMAL(10, 2),

    ForecastNonConsolidatedNetSales               DECIMAL(20, 2),
    ForecastNonConsolidatedOperatingProfit         DECIMAL(20, 2),
    ForecastNonConsolidatedOrdinaryProfit          DECIMAL(20, 2),
    ForecastNonConsolidatedProfit                 DECIMAL(20, 2),
    ForecastNonConsolidatedEarningsPerShare        DECIMAL(10, 2),
    NextYearForecastNonConsolidatedNetSales       DECIMAL(20, 2),
    NextYearForecastNonConsolidatedOperatingProfit DECIMAL(20, 2),
    NextYearForecastNonConsolidatedOrdinaryProfit  DECIMAL(20, 2),
    NextYearForecastNonConsolidatedProfit         DECIMAL(20, 2),
    NextYearForecastNonConsolidatedEarningsPerShare DECIMAL(10, 2)
);

CREATE INDEX IF NOT EXISTS idx_statements_code ON statements(code);
CREATE INDEX IF NOT EXISTS idx_statements_disclosure_no ON statements(DisclosureNumber);
CREATE INDEX IF NOT EXISTS idx_statements_disclosed_date ON statements(DisclosedDate);

-- fundamental_signals ----------------------------------------------
CREATE TABLE IF NOT EXISTS fundamental_signals (
    code                VARCHAR(10) NOT NULL,
    DisclosedAt         TIMESTAMP NOT NULL,
    TypeOfCurrentPeriod VARCHAR(50),

    eps_yoy_fy          DECIMAL(10, 4),
    eps_yoy_q           DECIMAL(10, 4),
    op_margin_delta     DECIMAL(10, 4),
    feps_revision       DECIMAL(10, 4),
    cf_quality          DECIMAL(10, 4),
    eta_delta           DECIMAL(10, 4),
    leverage            DECIMAL(10, 4),
    turnaround          SMALLINT,  -- 0/1
    treasury_delta      DECIMAL(10, 4),

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, DisclosedAt)
);
CREATE INDEX IF NOT EXISTS idx_fsignals_code ON fundamental_signals(code);
CREATE INDEX IF NOT EXISTS idx_fsignals_created ON fundamental_signals(created_at);

-- technical_indicators ----------------------------------------------
CREATE TABLE IF NOT EXISTS technical_indicators (
    code VARCHAR(10) NOT NULL,
    signal_date DATE NOT NULL,
    signal_ma SMALLINT,
    signal_rsi SMALLINT,
    signal_adx SMALLINT,
    signal_bb SMALLINT,
    signal_macd SMALLINT,
    signal_ma_short SMALLINT,
    signal_rsi_short SMALLINT,
    signal_bb_short SMALLINT,
    signal_macd_short SMALLINT,
    signals_count SMALLINT,
    signals_short_count SMALLINT,
    signals_overheating SMALLINT,
    signals_oversold SMALLINT,
    signals_short_first SMALLINT,
    signals_first SMALLINT,

    PRIMARY KEY (code, signal_date)
);
CREATE INDEX IF NOT EXISTS idx_tindicators_code ON technical_indicators(code);
CREATE INDEX IF NOT EXISTS idx_tindicators_date ON technical_indicators(signal_date);
CREATE INDEX IF NOT EXISTS idx_tech_date_count ON technical_indicators(signal_date, signals_count);
CREATE INDEX IF NOT EXISTS idx_tech_date_short_count ON technical_indicators(signal_date, signals_short_count);

-- users --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'trader',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- sessions -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS sessions (
    id VARCHAR(255) PRIMARY KEY,
    user_id INTEGER NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    remember_me SMALLINT DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);

-- daytrade_futures -------------------------------------------------------
CREATE TABLE IF NOT EXISTS daytrade_futures (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    trade_date DATE NOT NULL,
    trade_number VARCHAR(50) NOT NULL,
    trade_datetime TIMESTAMP NOT NULL,
    market VARCHAR(50),
    symbol VARCHAR(50) NOT NULL,
    trade_type VARCHAR(50) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    quantity INTEGER NOT NULL,
    commission DECIMAL(10, 2) DEFAULT 0,
    tax DECIMAL(10, 2) DEFAULT 0,
    settlement_amount DECIMAL(20, 2) NOT NULL,
    delivery_amount DECIMAL(20, 2),
    delivery_date DATE,
    open_date DATE,
    open_price DECIMAL(10, 2),
    open_commission DECIMAL(10, 2),
    open_tax DECIMAL(10, 2),
    profit_loss DECIMAL(20, 2) NOT NULL,
    sq_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_daytrade_futures_user_id ON daytrade_futures(user_id);
CREATE INDEX IF NOT EXISTS idx_daytrade_futures_trade_date ON daytrade_futures(trade_date);
CREATE INDEX IF NOT EXISTS idx_daytrade_futures_symbol ON daytrade_futures(symbol);
CREATE INDEX IF NOT EXISTS idx_daytrade_futures_user_date ON daytrade_futures(user_id, trade_date);

-- daytrade_stocks -------------------------------------------------------
CREATE TABLE IF NOT EXISTS daytrade_stocks (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    code VARCHAR(10) NOT NULL,
    name VARCHAR(255) NOT NULL,
    market VARCHAR(50),
    trade_type VARCHAR(100) NOT NULL,
    term VARCHAR(50),
    custody_type VARCHAR(50),
    trade_date DATE NOT NULL,
    delivery_date DATE,
    quantity INTEGER NOT NULL,
    average_price DECIMAL(10, 2) NOT NULL,
    commission_tax DECIMAL(10, 2),
    capital_gains_tax DECIMAL(10, 2),
    settlement_amount DECIMAL(20, 2),
    day_trade_amount DECIMAL(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_user_id ON daytrade_stocks(user_id);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_trade_date ON daytrade_stocks(trade_date);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_code ON daytrade_stocks(code);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_user_date ON daytrade_stocks(user_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_user_code_date ON daytrade_stocks(user_id, code, trade_date);

-- holdings -------------------------------------------------------
CREATE TABLE IF NOT EXISTS holdings (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    code VARCHAR(10) NOT NULL,
    account_name VARCHAR(100) NOT NULL DEFAULT 'default',
    account_type VARCHAR(20) NOT NULL DEFAULT '特定',
    stock_type VARCHAR(20) DEFAULT '現物',
    trade_position VARCHAR(20),
    margin_term VARCHAR(50),
    quantity INTEGER NOT NULL,
    average_price DECIMAL(10, 2) NOT NULL,
    current_price DECIMAL(10, 2),
    market_value DECIMAL(20, 2),
    profit_loss DECIMAL(20, 2),
    profit_loss_ratio DECIMAL(10, 4),
    expected_per DECIMAL(10, 2),
    actual_pbr DECIMAL(10, 2),
    dividend_yield DECIMAL(10, 4),
    expected_eps DECIMAL(10, 2),
    actual_bps DECIMAL(10, 2),
    expected_dividend DECIMAL(10, 2),
    lending_type VARCHAR(50),
    acquisition_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(user_id, code, account_name, account_type, stock_type, trade_position)
);
CREATE INDEX IF NOT EXISTS idx_holdings_user_id ON holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_holdings_code ON holdings(code);
CREATE INDEX IF NOT EXISTS idx_holdings_account ON holdings(account_name);

-- PostgreSQL固有の最適化設定
-- 自動VACUUM設定（大量データ処理対応）
ALTER TABLE prices SET (autovacuum_vacuum_scale_factor = 0.1);
ALTER TABLE statements SET (autovacuum_vacuum_scale_factor = 0.1);
ALTER TABLE technical_indicators SET (autovacuum_vacuum_scale_factor = 0.1);
"""


def init_schema(connection_params=None):
    """PostgreSQLスキーマを初期化

    Args:
        connection_params: データベース接続パラメータ（省略時は環境変数から取得）
    """
    if connection_params is None:
        connection_params = get_postgres_config()

    # PostgreSQLアダプターを使用
    os.environ["DATABASE_TYPE"] = "postgres"

    try:
        with get_database_adapter(connection_params=connection_params) as db:
            logger.info("PostgreSQLデータベースに接続しました")

            # スキーマを作成
            db.create_tables(DDL)
            logger.info("スキーマの作成が完了しました")

            # テーブルの確認
            tables = [
                "prices",
                "listed_info",
                "statements",
                "fundamental_signals",
                "technical_indicators",
                "users",
                "sessions",
                "daytrade_futures",
                "daytrade_stocks",
                "holdings",
            ]

            for table in tables:
                if db.table_exists(table):
                    logger.info(f"✓ テーブル {table} が存在します")
                else:
                    logger.warning(f"✗ テーブル {table} が見つかりません")

    except Exception as e:
        logger.error(f"スキーマ初期化中にエラーが発生しました: {e}")
        raise


if __name__ == "__main__":
    init_schema()

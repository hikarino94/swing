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
from src.config import get_db_path

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

-- daytrade_futures -------------------------------------------------------
-- 先物デイトレード記録（取引日ベースで決済損益を管理）
CREATE TABLE IF NOT EXISTS daytrade_futures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    trade_date TEXT NOT NULL,  -- 取引日 YYYY-MM-DD
    trade_number TEXT NOT NULL,  -- 約定番号
    trade_datetime TEXT NOT NULL,  -- 約定日時
    market TEXT,  -- 市場
    symbol TEXT NOT NULL,  -- 銘柄
    trade_type TEXT NOT NULL,  -- 取引（決済買/決済売）
    price REAL NOT NULL,  -- 約定価格
    quantity INTEGER NOT NULL,  -- 約定数量
    commission REAL DEFAULT 0,  -- 手数料
    tax REAL DEFAULT 0,  -- 消費税
    settlement_amount REAL NOT NULL,  -- 約定金額
    delivery_amount REAL,  -- 受渡金額
    delivery_date TEXT,  -- 受渡日
    open_date TEXT,  -- 新規建日
    open_price REAL,  -- 新規建単価
    open_commission REAL,  -- 新規建手数料
    open_tax REAL,  -- 新規建消費税
    profit_loss REAL NOT NULL,  -- 決済損益
    sq_date TEXT,  -- SQ日
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_daytrade_futures_user_id ON daytrade_futures(user_id);
CREATE INDEX IF NOT EXISTS idx_daytrade_futures_trade_date ON daytrade_futures(trade_date);
CREATE INDEX IF NOT EXISTS idx_daytrade_futures_symbol ON daytrade_futures(symbol);
CREATE INDEX IF NOT EXISTS idx_daytrade_futures_user_date ON daytrade_futures(user_id, trade_date);

-- daytrade_stocks -------------------------------------------------------
-- 株式デイトレード記録（約定日ベースで受渡金額・決済損益を管理）
CREATE TABLE IF NOT EXISTS daytrade_stocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    code TEXT NOT NULL,  -- 銘柄コード
    name TEXT NOT NULL,  -- 銘柄名
    market TEXT,  -- 市場
    trade_type TEXT NOT NULL,  -- 取引区分（信用新規買/信用返済売等）
    term TEXT,  -- 期限（６ヵ月/日計り/無期限）
    custody_type TEXT,  -- 預り区分（特定/一般）
    trade_date TEXT NOT NULL,  -- 約定日 YYYY-MM-DD
    delivery_date TEXT,  -- 受渡日
    quantity INTEGER NOT NULL,  -- 株数
    average_price REAL NOT NULL,  -- 平均約定単価
    commission_tax REAL,  -- 手数料・諸経費等
    capital_gains_tax REAL,  -- 課税額・譲渡益税
    settlement_amount REAL,  -- 受渡金額・決済損益
    day_trade_amount REAL,  -- 受渡金額(日計り分)
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_user_id ON daytrade_stocks(user_id);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_trade_date ON daytrade_stocks(trade_date);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_code ON daytrade_stocks(code);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_user_date ON daytrade_stocks(user_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_daytrade_stocks_user_code_date ON daytrade_stocks(user_id, code, trade_date);

-- holdings -------------------------------------------------------
-- 保有銘柄管理（現物・信用取引対応）
CREATE TABLE IF NOT EXISTS holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    account_name TEXT NOT NULL DEFAULT 'default',
    account_type TEXT NOT NULL DEFAULT '特定',  -- 特定/一般/NISA/旧NISA
    stock_type TEXT DEFAULT '現物',  -- 現物/信用
    trade_position TEXT,  -- 買建/売建（信用の場合）
    margin_term TEXT,  -- 6ヵ月/無期限（信用の場合）
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
    deleted_at TEXT DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(user_id, code, account_name, account_type, stock_type, trade_position)
);
CREATE INDEX IF NOT EXISTS idx_holdings_user_id ON holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_holdings_code ON holdings(code);
CREATE INDEX IF NOT EXISTS idx_holdings_account ON holdings(account_name);


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

        # 既存テーブルへのカラム追加（マイグレーション）
        cursor = conn.cursor()

        # account_typeカラムの追加（存在しない場合のみ）
        try:
            cursor.execute("SELECT account_type FROM holdings LIMIT 1")
        except sqlite3.OperationalError:
            # カラムが存在しない場合は追加
            cursor.execute(
                "ALTER TABLE holdings ADD COLUMN account_type TEXT NOT NULL DEFAULT '特定'"
            )
            conn.commit()
            logger.info("holdingsテーブルにaccount_typeカラムを追加しました")

        # UNIQUE制約の確認と更新
        cursor.execute("PRAGMA index_list(holdings)")
        indexes = cursor.fetchall()

        # 既存のUNIQUE制約を探す
        old_unique_found = False
        for idx in indexes:
            if idx[2] == 1:  # UNIQUE制約
                cursor.execute(f"PRAGMA index_info({idx[1]})")
                cols = cursor.fetchall()
                if len(cols) == 3:  # 古い3カラムのUNIQUE制約
                    old_unique_found = True
                    break

        if old_unique_found:
            # 古いUNIQUE制約がある場合、テーブルを再作成
            logger.info("UNIQUE制約を更新するためにholdingsテーブルを再作成します")

            # 既存データをバックアップ
            cursor.execute("SELECT * FROM holdings")
            backup_data = cursor.fetchall()

            # テーブルを削除して再作成
            cursor.execute("DROP TABLE holdings")
            cursor.execute(
                """
                CREATE TABLE holdings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    account_name TEXT NOT NULL DEFAULT 'default',
                    account_type TEXT NOT NULL DEFAULT '特定',
                    stock_type TEXT DEFAULT '現物',  -- 現物/信用
                    trade_position TEXT,  -- 買建/売建（信用の場合）
                    margin_term TEXT,  -- 6ヵ月/無期限（信用の場合）
                    quantity INTEGER NOT NULL,
                    average_price REAL NOT NULL,
                    current_price REAL,  -- 現在値
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
                    acquisition_date TEXT,  -- 取得日
                    updated_at TEXT DEFAULT (datetime('now')),
                    deleted_at TEXT DEFAULT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE(user_id, code, account_name, account_type, stock_type, trade_position)
                )
            """
            )

            # インデックスを再作成
            cursor.execute("CREATE INDEX idx_holdings_user_id ON holdings(user_id)")
            cursor.execute("CREATE INDEX idx_holdings_code ON holdings(code)")
            cursor.execute(
                "CREATE INDEX idx_holdings_account ON holdings(account_name)"
            )

            # データを復元（古いデータの場合はデフォルト値を設定）
            for row in backup_data:
                if len(row) == 18:  # 最も古い形式（account_typeなし）
                    # id, user_id, code, account_name, quantity, ...
                    cursor.execute(
                        """
                        INSERT INTO holdings (id, user_id, code, account_name, account_type,
                                            stock_type, trade_position, margin_term, quantity,
                                            average_price, current_price, market_value, profit_loss,
                                            profit_loss_ratio, expected_per, actual_pbr,
                                            dividend_yield, expected_eps, actual_bps,
                                            expected_dividend, lending_type, acquisition_date, updated_at)
                        VALUES (?, ?, ?, ?, '特定', '現物', NULL, NULL, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
                    """,
                        (
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6],
                            row[7],
                            row[8],
                            row[9],
                            row[10],
                            row[11],
                            row[12],
                            row[13],
                            row[14],
                            row[15],
                            row[16],
                            row[17],
                        ),
                    )
                elif len(row) == 19:  # 古い形式（account_typeあり、新カラムなし）
                    cursor.execute(
                        """
                        INSERT INTO holdings (id, user_id, code, account_name, account_type,
                                            stock_type, trade_position, margin_term, quantity,
                                            average_price, current_price, market_value, profit_loss,
                                            profit_loss_ratio, expected_per, actual_pbr,
                                            dividend_yield, expected_eps, actual_bps,
                                            expected_dividend, lending_type, acquisition_date, updated_at, deleted_at)
                        VALUES (?, ?, ?, ?, ?, '現物', NULL, NULL, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                    """,
                        (
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6],
                            row[7],
                            row[8],
                            row[9],
                            row[10],
                            row[11],
                            row[12],
                            row[13],
                            row[14],
                            row[15],
                            row[16],
                            row[17],
                            row[18],
                        ),
                    )
                else:  # 最新形式（全カラムあり）
                    cursor.execute(
                        """
                        INSERT INTO holdings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        row,
                    )

            conn.commit()
            logger.info("holdingsテーブルの再作成とデータ復元が完了しました")

        # 保有銘柄管理テーブルの新しいカラム追加（信用取引対応）
        try:
            cursor.execute("SELECT stock_type FROM holdings LIMIT 1")
        except sqlite3.OperationalError:
            # カラムが存在しない場合は追加
            cursor.execute(
                "ALTER TABLE holdings ADD COLUMN stock_type TEXT DEFAULT '現物'"
            )  # 現物/信用
            cursor.execute(
                "ALTER TABLE holdings ADD COLUMN trade_position TEXT"
            )  # 買建/売建（信用の場合）
            cursor.execute(
                "ALTER TABLE holdings ADD COLUMN margin_term TEXT"
            )  # 6ヵ月/無期限（信用の場合）
            cursor.execute(
                "ALTER TABLE holdings ADD COLUMN current_price REAL"
            )  # 現在値
            cursor.execute(
                "ALTER TABLE holdings ADD COLUMN acquisition_date TEXT"
            )  # 取得日
            conn.commit()
            logger.info("holdingsテーブルに信用取引関連カラムを追加しました")


def main() -> None:  # pragma: no cover
    init_schema(get_db_path())
    logger.info("Schema created or verified at %s", get_db_path())


if __name__ == "__main__":
    # • 必要なテーブルとインデックスを作成
    main()

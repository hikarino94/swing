"""
リポジトリテスト用の共通フィクスチャ
"""

from pathlib import Path

import pytest


@pytest.fixture
def init_db_tables(temp_db: Path):
    """テスト用データベースのテーブルを初期化"""
    import sqlite3

    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()

    # WALモードを有効化
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")

    # pricesテーブル
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            Date TEXT,
            Code TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Volume INTEGER,
            TurnoverValue REAL,
            AdjustmentFactor REAL,
            AdjustmentOpen REAL,
            AdjustmentHigh REAL,
            AdjustmentLow REAL,
            AdjustmentClose REAL,
            AdjustmentVolume INTEGER,
            PRIMARY KEY (Date, Code)
        )
    """
    )

    # listed_infoテーブル
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS listed_info (
            Date TEXT,
            Code TEXT PRIMARY KEY,
            CompanyName TEXT,
            CompanyNameEnglish TEXT,
            Sector17Code TEXT,
            Sector17CodeName TEXT,
            Sector33Code TEXT,
            Sector33CodeName TEXT,
            ScaleCategory TEXT,
            MarketCode TEXT,
            MarketCodeName TEXT,
            MarginCode TEXT,
            MarginCodeName TEXT,
            delete_flag INTEGER DEFAULT 0
        )
    """
    )

    # statementsテーブル
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS statements (
            DisclosedDate TEXT,
            DisclosedTime TEXT,
            LocalCode TEXT,
            DisclosureNumber TEXT PRIMARY KEY,
            TypeOfDocument TEXT,
            TypeOfCurrentPeriod TEXT,
            CurrentPeriodStartDate TEXT,
            CurrentPeriodEndDate TEXT,
            CurrentFiscalYearStartDate TEXT,
            CurrentFiscalYearEndDate TEXT,
            NextFiscalYearStartDate TEXT,
            NextFiscalYearEndDate TEXT,
            NetSales REAL,
            OperatingProfit REAL,
            OrdinaryProfit REAL,
            Profit REAL,
            EarningsPerShare REAL,
            DilutedEarningsPerShare REAL,
            TotalAssets REAL,
            Equity REAL,
            EquityToAssetRatio REAL,
            BookValuePerShare REAL,
            CashFlowsFromOperatingActivities REAL,
            CashFlowsFromInvestingActivities REAL,
            CashFlowsFromFinancingActivities REAL,
            CashAndEquivalents REAL,
            ResultDividendPerShareAnnual REAL,
            ResultPayoutRatio REAL,
            ForecastDividendPerShareAnnual REAL,
            ForecastPayoutRatio REAL,
            NextYearForecastDividendPerShareAnnual REAL,
            NextYearForecastPayoutRatio REAL,
            ForecastNetSales REAL,
            ForecastOperatingProfit REAL,
            ForecastOrdinaryProfit REAL,
            ForecastProfit REAL,
            ForecastEarningsPerShare REAL,
            NextYearForecastNetSales REAL,
            NextYearForecastOperatingProfit REAL,
            NextYearForecastOrdinaryProfit REAL,
            NextYearForecastProfit REAL,
            NextYearForecastEarningsPerShare REAL,
            MaterialChangesInSubsidiaries INTEGER,
            SignificantChangesInTheScopeOfConsolidation TEXT,
            ChangesInAccountingEstimates INTEGER,
            RetrospectiveRestatement INTEGER,
            NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock INTEGER,
            NumberOfTreasuryStockAtTheEndOfFiscalYear INTEGER,
            AverageNumberOfShares INTEGER,
            NonConsolidatedNetSales REAL,
            NonConsolidatedOperatingProfit REAL,
            NonConsolidatedOrdinaryProfit REAL,
            NonConsolidatedProfit REAL,
            NonConsolidatedEarningsPerShare REAL,
            NonConsolidatedTotalAssets REAL,
            NonConsolidatedEquity REAL,
            NonConsolidatedEquityToAssetRatio REAL,
            NonConsolidatedBookValuePerShare REAL,
            ForecastNonConsolidatedNetSales REAL,
            ForecastNonConsolidatedOperatingProfit REAL,
            ForecastNonConsolidatedOrdinaryProfit REAL,
            ForecastNonConsolidatedProfit REAL,
            ForecastNonConsolidatedEarningsPerShare REAL,
            NextYearForecastNonConsolidatedNetSales REAL,
            NextYearForecastNonConsolidatedOperatingProfit REAL,
            NextYearForecastNonConsolidatedOrdinaryProfit REAL,
            NextYearForecastNonConsolidatedProfit REAL,
            NextYearForecastNonConsolidatedEarningsPerShare REAL
        )
    """
    )

    # fundamental_signalsテーブル
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_signals (
            signal_date TEXT,
            code TEXT,
            company_name TEXT,
            signal_type TEXT,
            reason TEXT,
            details TEXT,
            PRIMARY KEY (signal_date, code, signal_type)
        )
    """
    )

    # technical_indicatorsテーブル
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS technical_indicators (
            date TEXT,
            code TEXT,
            sma_5 REAL,
            sma_20 REAL,
            sma_60 REAL,
            ema_5 REAL,
            ema_20 REAL,
            ema_60 REAL,
            macd REAL,
            macd_signal REAL,
            macd_hist REAL,
            rsi REAL,
            bb_upper REAL,
            bb_middle REAL,
            bb_lower REAL,
            bb_width REAL,
            bb_percent REAL,
            volume_ratio REAL,
            volume_sma_20 REAL,
            golden_cross INTEGER,
            dead_cross INTEGER,
            macd_golden_cross INTEGER,
            macd_dead_cross INTEGER,
            bb_squeeze INTEGER,
            bb_expansion INTEGER,
            PRIMARY KEY (date, code)
        )
    """
    )

    conn.commit()
    conn.close()

    return temp_db

"""
pytest共通設定とフィクスチャ定義
"""

import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """一時ディレクトリを作成するフィクスチャ"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def temp_db(temp_dir: Path) -> Path:
    """テスト用の一時データベースファイルを作成するフィクスチャ"""
    db_path = temp_dir / "test_stock.db"
    return db_path


@pytest.fixture
def mock_config_dir(temp_dir: Path, monkeypatch) -> Path:
    """テスト用の設定ディレクトリを作成するフィクスチャ"""
    config_dir = temp_dir / "config"
    config_dir.mkdir(exist_ok=True)

    # 環境変数を設定
    monkeypatch.setenv("SWING_CONFIG_DIR", str(config_dir))

    return config_dir


@pytest.fixture
def sample_account_config(mock_config_dir: Path) -> dict:
    """サンプルのアカウント設定を作成するフィクスチャ"""
    import json

    account_data = {"mail": "test@example.com", "password": "test_password"}

    with open(mock_config_dir / "account.json", "w") as f:
        json.dump(account_data, f)

    return account_data


@pytest.fixture
def sample_idtoken_config(mock_config_dir: Path) -> dict:
    """サンプルのIDトークン設定を作成するフィクスチャ"""
    import json

    token_data = {"idToken": "test_id_token_12345"}

    with open(mock_config_dir / "idtoken.json", "w") as f:
        json.dump(token_data, f)

    return token_data


@pytest.fixture
def sample_stock_data() -> pd.DataFrame:
    """サンプルの株価データを作成するフィクスチャ"""
    data = {
        "Date": pd.date_range("2024-01-01", periods=5),
        "Code": ["1234"] * 5,
        "Open": [1000, 1010, 1020, 1015, 1025],
        "High": [1020, 1025, 1030, 1025, 1035],
        "Low": [990, 1005, 1015, 1010, 1020],
        "Close": [1010, 1020, 1025, 1020, 1030],
        "Volume": [100000, 120000, 110000, 90000, 105000],
        "TurnoverValue": [101000000, 122400000, 112750000, 91800000, 108150000],
        "AdjustmentFactor": [1.0] * 5,
        "AdjustmentOpen": [1000, 1010, 1020, 1015, 1025],
        "AdjustmentHigh": [1020, 1025, 1030, 1025, 1035],
        "AdjustmentLow": [990, 1005, 1015, 1010, 1020],
        "AdjustmentClose": [1010, 1020, 1025, 1020, 1030],
        "AdjustmentVolume": [100000, 120000, 110000, 90000, 105000],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_statements_data() -> pd.DataFrame:
    """サンプルの財務諸表データを作成するフィクスチャ"""
    data = {
        "DisclosedDate": ["2024-01-15"],
        "DisclosedTime": ["15:00"],
        "LocalCode": ["1234"],
        "DisclosureNumber": ["20240115150000"],
        "TypeOfDocument": ["1Qc"],
        "TypeOfCurrentPeriod": ["1Q"],
        "CurrentPeriodStartDate": ["2023-10-01"],
        "CurrentPeriodEndDate": ["2023-12-31"],
        "CurrentFiscalYearStartDate": ["2023-10-01"],
        "CurrentFiscalYearEndDate": ["2024-09-30"],
        "NextFiscalYearStartDate": ["2024-10-01"],
        "NextFiscalYearEndDate": ["2025-09-30"],
        "NetSales": [1000000000],
        "OperatingProfit": [150000000],
        "OrdinaryProfit": [140000000],
        "Profit": [100000000],
        "EarningsPerShare": [50.5],
        "DilutedEarningsPerShare": [50.0],
        "TotalAssets": [5000000000],
        "Equity": [2000000000],
        "EquityToAssetRatio": [0.40],
        "BookValuePerShare": [1000.0],
        "CashFlowsFromOperatingActivities": [200000000],
        "CashFlowsFromInvestingActivities": [-50000000],
        "CashFlowsFromFinancingActivities": [-100000000],
        "CashAndEquivalents": [500000000],
        "ResultDividendPerShareAnnual": [20.0],
        "ResultPayoutRatio": [0.4],
        "ForecastDividendPerShareAnnual": [25.0],
        "ForecastPayoutRatio": [0.35],
        "NextYearForecastDividendPerShareAnnual": [30.0],
        "NextYearForecastPayoutRatio": [0.35],
        "ForecastNetSales": [4500000000],
        "ForecastOperatingProfit": [700000000],
        "ForecastOrdinaryProfit": [680000000],
        "ForecastProfit": [480000000],
        "ForecastEarningsPerShare": [240.0],
        "NextYearForecastNetSales": [5000000000],
        "NextYearForecastOperatingProfit": [800000000],
        "NextYearForecastOrdinaryProfit": [780000000],
        "NextYearForecastProfit": [550000000],
        "NextYearForecastEarningsPerShare": [275.0],
        "MaterialChangesInSubsidiaries": [False],
        "SignificantChangesInTheScopeOfConsolidation": [None],
        "ChangesInAccountingEstimates": [False],
        "RetrospectiveRestatement": [False],
        "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock": [
            2100000
        ],
        "NumberOfTreasuryStockAtTheEndOfFiscalYear": [100000],
        "AverageNumberOfShares": [2000000],
        "NonConsolidatedNetSales": [None],
        "NonConsolidatedOperatingProfit": [None],
        "NonConsolidatedOrdinaryProfit": [None],
        "NonConsolidatedProfit": [None],
        "NonConsolidatedEarningsPerShare": [None],
        "NonConsolidatedTotalAssets": [None],
        "NonConsolidatedEquity": [None],
        "NonConsolidatedEquityToAssetRatio": [None],
        "NonConsolidatedBookValuePerShare": [None],
        "ForecastNonConsolidatedNetSales": [None],
        "ForecastNonConsolidatedOperatingProfit": [None],
        "ForecastNonConsolidatedOrdinaryProfit": [None],
        "ForecastNonConsolidatedProfit": [None],
        "ForecastNonConsolidatedEarningsPerShare": [None],
        "NextYearForecastNonConsolidatedNetSales": [None],
        "NextYearForecastNonConsolidatedOperatingProfit": [None],
        "NextYearForecastNonConsolidatedOrdinaryProfit": [None],
        "NextYearForecastNonConsolidatedProfit": [None],
        "NextYearForecastNonConsolidatedEarningsPerShare": [None],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_listed_info() -> pd.DataFrame:
    """サンプルの上場企業情報データを作成するフィクスチャ"""
    data = {
        "Date": ["2024-01-01"],
        "Code": ["1234"],
        "CompanyName": ["テスト株式会社"],
        "CompanyNameEnglish": ["Test Corporation"],
        "Sector17Code": ["1"],
        "Sector17CodeName": ["食品"],
        "Sector33Code": ["0050"],
        "Sector33CodeName": ["水産・農林業"],
        "ScaleCategory": ["TOPIX Small 2"],
        "MarketCode": ["0111"],
        "MarketCodeName": ["プライム"],
        "MarginCode": ["1"],
        "MarginCodeName": ["制度信用"],
        "delete_flag": [False],
    }
    return pd.DataFrame(data)


@pytest.fixture
def mock_requests_get():
    """requests.getをモックするフィクスチャ"""
    with patch("requests.get") as mock_get:
        # デフォルトのモックレスポンス
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.text = '{"data": "test"}'
        mock_get.return_value = mock_response

        yield mock_get


@pytest.fixture
def mock_requests_post():
    """requests.postをモックするフィクスチャ"""
    with patch("requests.post") as mock_post:
        # デフォルトのモックレスポンス
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"idToken": "test_token"}
        mock_response.text = '{"idToken": "test_token"}'
        mock_post.return_value = mock_response

        yield mock_post


@pytest.fixture
def mock_sqlite_connection(temp_db: Path):
    """SQLite接続をモックするフィクスチャ"""
    import sqlite3

    # テスト用データベースを初期化
    conn = sqlite3.connect(temp_db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # WALモードを有効化
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")

    # 基本的なテーブルを作成
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

    conn.commit()

    with patch("sqlite3.connect") as mock_connect:
        mock_connect.return_value = conn
        yield conn

    conn.close()


@pytest.fixture
def disable_logging():
    """テスト中のロギングを無効化するフィクスチャ"""
    import logging

    # 全てのロガーのレベルをCRITICALに設定
    logging.disable(logging.CRITICAL)

    yield

    # テスト後にロギングを再度有効化
    logging.disable(logging.NOTSET)


# モックヘルパー関数
def create_mock_response(
    status_code: int = 200, json_data: dict = None, text: str = None
):
    """HTTPレスポンスのモックを作成するヘルパー関数"""
    mock_response = MagicMock()
    mock_response.status_code = status_code

    if json_data is not None:
        mock_response.json.return_value = json_data
        if text is None:
            import json

            text = json.dumps(json_data)

    if text is not None:
        mock_response.text = text

    return mock_response


def create_mock_db_cursor(results: list = None):
    """データベースカーソルのモックを作成するヘルパー関数"""
    mock_cursor = MagicMock()

    if results is not None:
        mock_cursor.fetchall.return_value = results
        mock_cursor.fetchone.return_value = results[0] if results else None

    return mock_cursor

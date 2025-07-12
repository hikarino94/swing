"""
財務諸表データのSQLiteリポジトリ実装
"""

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from ..interfaces import StatementsRepository
from .base import SqliteBaseRepository


class SqliteStatementsRepository(SqliteBaseRepository, StatementsRepository):
    """財務諸表データのSQLiteリポジトリ実装"""

    def find_by_code_and_period(
        self, code: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """指定銘柄・期間の財務諸表を取得"""
        query = """
            SELECT * FROM statements
            WHERE LocalCode = ?
            AND DisclosedDate >= ?
            AND DisclosedDate <= ?
            ORDER BY DisclosedDate DESC
        """
        cursor = self.execute(
            query, (code, start_date.isoformat(), end_date.isoformat())
        )

        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(data, columns=columns)
        df["DisclosedDate"] = pd.to_datetime(df["DisclosedDate"])
        return df

    def find_latest_by_code(self, code: str) -> dict[str, Any] | None:
        """指定銘柄の最新財務諸表を取得"""
        query = """
            SELECT * FROM statements
            WHERE LocalCode = ?
            ORDER BY DisclosedDate DESC, DisclosedTime DESC
            LIMIT 1
        """
        cursor = self.execute(query, (code,))
        row = cursor.fetchone()

        if row:
            return dict(row)
        return None

    def find_by_disclosure_date(self, disclosure_date: date) -> pd.DataFrame:
        """指定開示日の財務諸表を取得"""
        query = """
            SELECT * FROM statements
            WHERE DisclosedDate = ?
            ORDER BY LocalCode, DisclosedTime
        """
        cursor = self.execute(query, (disclosure_date.isoformat(),))

        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(data, columns=columns)
        df["DisclosedDate"] = pd.to_datetime(df["DisclosedDate"])
        return df

    def save_batch(self, data: pd.DataFrame) -> int:
        """財務諸表データを一括保存"""
        if data.empty:
            return 0

        # カラム名のリストを作成
        columns = [
            "DisclosedDate",
            "DisclosedTime",
            "LocalCode",
            "DisclosureNumber",
            "TypeOfDocument",
            "TypeOfCurrentPeriod",
            "CurrentPeriodStartDate",
            "CurrentPeriodEndDate",
            "CurrentFiscalYearStartDate",
            "CurrentFiscalYearEndDate",
            "NextFiscalYearStartDate",
            "NextFiscalYearEndDate",
            "NetSales",
            "OperatingProfit",
            "OrdinaryProfit",
            "Profit",
            "EarningsPerShare",
            "DilutedEarningsPerShare",
            "TotalAssets",
            "Equity",
            "EquityToAssetRatio",
            "BookValuePerShare",
            "CashFlowsFromOperatingActivities",
            "CashFlowsFromInvestingActivities",
            "CashFlowsFromFinancingActivities",
            "CashAndEquivalents",
            "ResultDividendPerShareAnnual",
            "ResultPayoutRatio",
            "ForecastDividendPerShareAnnual",
            "ForecastPayoutRatio",
            "NextYearForecastDividendPerShareAnnual",
            "NextYearForecastPayoutRatio",
            "ForecastNetSales",
            "ForecastOperatingProfit",
            "ForecastOrdinaryProfit",
            "ForecastProfit",
            "ForecastEarningsPerShare",
            "NextYearForecastNetSales",
            "NextYearForecastOperatingProfit",
            "NextYearForecastOrdinaryProfit",
            "NextYearForecastProfit",
            "NextYearForecastEarningsPerShare",
            "MaterialChangesInSubsidiaries",
            "SignificantChangesInTheScopeOfConsolidation",
            "ChangesInAccountingEstimates",
            "RetrospectiveRestatement",
            "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
            "NumberOfTreasuryStockAtTheEndOfFiscalYear",
            "AverageNumberOfShares",
            "NonConsolidatedNetSales",
            "NonConsolidatedOperatingProfit",
            "NonConsolidatedOrdinaryProfit",
            "NonConsolidatedProfit",
            "NonConsolidatedEarningsPerShare",
            "NonConsolidatedTotalAssets",
            "NonConsolidatedEquity",
            "NonConsolidatedEquityToAssetRatio",
            "NonConsolidatedBookValuePerShare",
            "ForecastNonConsolidatedNetSales",
            "ForecastNonConsolidatedOperatingProfit",
            "ForecastNonConsolidatedOrdinaryProfit",
            "ForecastNonConsolidatedProfit",
            "ForecastNonConsolidatedEarningsPerShare",
            "NextYearForecastNonConsolidatedNetSales",
            "NextYearForecastNonConsolidatedOperatingProfit",
            "NextYearForecastNonConsolidatedOrdinaryProfit",
            "NextYearForecastNonConsolidatedProfit",
            "NextYearForecastNonConsolidatedEarningsPerShare",
        ]

        # DataFrameをレコードのリストに変換
        records = []
        for _, row in data.iterrows():
            record = tuple(row.get(col) for col in columns)
            records.append(record)

        placeholders = ",".join(["?" for _ in columns])
        query = f"""
            INSERT OR REPLACE INTO statements ({','.join(columns)})
            VALUES ({placeholders})
        """

        cursor = self.executemany(query, records)
        return cursor.rowcount or 0

    def get_latest_disclosure_date(self) -> date | None:
        """最新の開示日を取得"""
        query = """
            SELECT MAX(DisclosedDate) as latest_date FROM statements
        """
        cursor = self.execute(query)
        row = cursor.fetchone()

        if row and row["latest_date"]:
            return datetime.fromisoformat(row["latest_date"]).date()
        return None

    def find_quarterly_statements(
        self, codes: list[str], lookback_days: int
    ) -> pd.DataFrame:
        """四半期決算データを取得"""
        if not codes:
            return pd.DataFrame()

        # 対象期間の計算
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)

        placeholders = ",".join(["?" for _ in codes])
        query = f"""
            SELECT * FROM statements
            WHERE LocalCode IN ({placeholders})
            AND DisclosedDate >= ?
            AND DisclosedDate <= ?
            AND TypeOfDocument IN ('1Qc', '2Qc', '3Qc', 'YTDc', 'FYc')
            ORDER BY LocalCode, DisclosedDate DESC
        """

        params = tuple(codes) + (start_date.isoformat(), end_date.isoformat())
        cursor = self.execute(query, params)

        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()

        if not data:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(data, columns=columns)
        df["DisclosedDate"] = pd.to_datetime(df["DisclosedDate"])
        return df

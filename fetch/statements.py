#!/usr/bin/env python
"""
statements.py – Fetch /statements (J-Quants) and upsert into SQLite `statements`, supporting pagination using pagination_key

Usage
-----
    python statements.py <db_path> 1   # listed_info にあるコード単位で一括取得（過去分も含む）
    python statements.py <db_path> 2   # 当日日付の開示分を取得（日次取得）

環境
----
- Python 3.9+
- `pandas`, `requests`

機能
----
- モード "1": listed_info テーブルから delete_flag=0 の銘柄コードを取得し、各コードごとに /statements API を呼び出して全過去開示情報を取得 (pagination_keyによるページネーションを考慮) → statements テーブルに Upsert
- モード "2": 当日日付をキーに /statements?date=<YYYYMMDD> を呼び出し、本日の開示情報を取得 (pagination_keyを考慮) → statements テーブルに Upsert

テーブル定義（schema）は db_schema.py に記載の CREATE TABLE 文に準拠しています。
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import json
import datetime as dt
from pathlib import Path
from typing import List
from concurrent.futures import ThreadPoolExecutor
import time
from requests import Session

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Config & logging
# ---------------------------------------------------------------------------
API_ENDPOINT = "https://api.jquants.com/v1/fins/statements"
LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("statements")
DB_PATH = (Path(__file__).resolve().parents[1] / "db/stock.db").as_posix()

# ---------------------------------------------------------------------------
# SQLite側の statements テーブルに合わせたカラム一覧
# ---------------------------------------------------------------------------
SCHEMA_COLUMNS: List[str] = [
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
    "ResultDividendPerShare1stQuarter",
    "ResultDividendPerShare2ndQuarter",
    "ResultDividendPerShare3rdQuarter",
    "ResultDividendPerShareFiscalYearEnd",
    "ResultDividendPerShareAnnual",
    "DistributionsPerUnit_REIT",
    "ResultTotalDividendPaidAnnual",
    "ResultPayoutRatioAnnual",
    "ForecastDividendPerShare1stQuarter",
    "ForecastDividendPerShare2ndQuarter",
    "ForecastDividendPerShare3rdQuarter",
    "ForecastDividendPerShareFiscalYearEnd",
    "ForecastDividendPerShareAnnual",
    "ForecastDistributionsPerUnit_REIT",
    "ForecastTotalDividendPaidAnnual",
    "ForecastPayoutRatioAnnual",
    "NextYearForecastDividendPerShare1stQuarter",
    "NextYearForecastDividendPerShare2ndQuarter",
    "NextYearForecastDividendPerShare3rdQuarter",
    "NextYearForecastDividendPerShareFiscalYearEnd",
    "NextYearForecastDividendPerShareAnnual",
    "NextYearForecastDistributionsPerUnit_REIT",
    "NextYearForecastPayoutRatioAnnual",
    "ForecastNetSales2ndQuarter",
    "ForecastOperatingProfit2ndQuarter",
    "ForecastOrdinaryProfit2ndQuarter",
    "ForecastProfit2ndQuarter",
    "ForecastEarningsPerShare2ndQuarter",
    "NextYearForecastNetSales2ndQuarter",
    "NextYearForecastOperatingProfit2ndQuarter",
    "NextYearForecastOrdinaryProfit2ndQuarter",
    "NextYearForecastProfit2ndQuarter",
    "NextYearForecastEarningsPerShare2ndQuarter",
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
    "ChangesBasedOnRevisionsOfAccountingStandard",
    "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
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
    "ForecastNonConsolidatedNetSales2ndQuarter",
    "ForecastNonConsolidatedOperatingProfit2ndQuarter",
    "ForecastNonConsolidatedOrdinaryProfit2ndQuarter",
    "ForecastNonConsolidatedProfit2ndQuarter",
    "ForecastNonConsolidatedEarningsPerShare2ndQuarter",
    "NextYearForecastNonConsolidatedNetSales2ndQuarter",
    "NextYearForecastNonConsolidatedOperatingProfit2ndQuarter",
    "NextYearForecastNonConsolidatedOrdinaryProfit2ndQuarter",
    "NextYearForecastNonConsolidatedProfit2ndQuarter",
    "NextYearForecastNonConsolidatedEarningsPerShare2ndQuarter",
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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_token() -> str:
    with open("../idtoken.json", "r", encoding="utf-8") as f:
        tok = json.load(f).get("idToken")
    if not tok:
        raise RuntimeError("idToken not found in idtoken.json")
    return tok


def _fetch_statements_by_code(session: Session, idtoken: str, code: str) -> List[dict]:
    """GET /statements?code=<code> with pagination and return all statement dicts."""
    headers = {"Authorization": f"Bearer {idtoken}"}
    params = {"code": code}
    all_statements: List[dict] = []
    page = 1
    while True:
        resp = session.get(API_ENDPOINT, headers=headers, params=params, timeout=60)
        if resp.status_code != 200:
            logger.warning("コード %s のAPIエラー: %s", code, resp.text)
            break
        data = resp.json()
        stmts = data.get("statements", [])
        if not stmts:
            break
        all_statements.extend(stmts)
        pagination_key = data.get("pagination_key")
        if pagination_key:
            params["pagination_key"] = pagination_key
            page += 1
        else:
            break
    return all_statements


def _fetch_statements_by_date(session: Session, idtoken: str, date_str: str) -> List[dict]:
    """GET /statements?date=<YYYYMMDD> with pagination and return all statement dicts."""
    headers = {"Authorization": f"Bearer {idtoken}"}
    params = {"date": date_str}
    all_statements: List[dict] = []
    page = 1
    while True:
        resp = session.get(API_ENDPOINT, headers=headers, params=params, timeout=60)
        if resp.status_code != 200:
            logger.warning("日付 %s のAPIエラー: %s", date_str, resp.text)
            break
        data = resp.json()
        stmts = data.get("statements", [])
        if not stmts:
            break
        all_statements.extend(stmts)
        pagination_key = data.get("pagination_key")
        if pagination_key:
            params["pagination_key"] = pagination_key
            page += 1
        else:
            break
    return all_statements


def _fetch_multiple_codes(idtoken: str, codes: List[str], workers: int = 5) -> List[dict]:
    """Fetch statements for many codes concurrently."""
    results: List[dict] = []
    logger.info("%d 件のコードのデータ取得を開始します", len(codes))

    def _task(code: str) -> List[dict]:
        logger.info("%s の取得を開始", code)
        with requests.Session() as sess:
            stmts = _fetch_statements_by_code(sess, idtoken, code)
        logger.info("%s の取得完了: %d 件", code, len(stmts))
        return stmts

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for i, stmts in enumerate(ex.map(_task, codes), 1):
            if stmts:
                results.extend(stmts)
            logger.info("進捗 %d/%d", i, len(codes))
    return results


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    # (既存の _normalize 実装)
    for col in SCHEMA_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[SCHEMA_COLUMNS]


def _upsert(conn: sqlite3.Connection, records: List[dict]) -> None:
    # (既存の _upsert 実装)
    if not records:
        return
    df = pd.DataFrame(records)
    df = _normalize(df)
    df.to_sql("_tmp_statements", conn, if_exists="replace", index=False)
    conn.executescript(
        """
        INSERT OR REPLACE INTO statements
        SELECT * FROM _tmp_statements;
        DROP TABLE _tmp_statements;
        """
    )
    logger.info("statements テーブルに %d 行 upsert しました", len(df))



def main(mode: str) -> None:
    idtoken = _load_token()
    start = time.perf_counter()
    logger.info("モード%sで処理を開始します", mode)
    with sqlite3.connect(DB_PATH) as conn:
        if mode == "1":
            cur = conn.execute("SELECT code FROM listed_info WHERE delete_flag = 0")
            codes = [row[0] for row in cur.fetchall()]
            logger.info("有効な銘柄コードを %d 件取得しました", len(codes))
            stmts = _fetch_multiple_codes(idtoken, codes)
            if stmts:
                _upsert(conn, stmts)
            logger.info("一括取得完了: 合計 %d 件", len(stmts))
        elif mode == "2":
            today = dt.date.today().strftime("%Y%m%d")
            with requests.Session() as sess:
                stmts = _fetch_statements_by_date(sess, idtoken, today)
            if stmts:
                _upsert(conn, stmts)
            logger.info("日付 %s の取得完了: %d 件", today, len(stmts))
        else:
            logger.error("無効なモードです: %s。'1' または '2' を指定してください", mode)
    elapsed = time.perf_counter() - start
    logger.info("処理時間: %.2f 秒", elapsed)


if __name__ == "__main__":
    # • モードを指定して決算データを取得
    # • SQLite DB の statements テーブルへ upsert
    parser = argparse.ArgumentParser(description="Fetch statements into SQLite")
    parser.add_argument(
        "mode", choices=["1", "2"], help="1: bulk by code, 2: daily by date"
    )
    args = parser.parse_args()
    main(args.mode)

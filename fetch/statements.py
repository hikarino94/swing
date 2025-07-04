#!/usr/bin/env python
"""
statements.py – Fetch /statements (J-Quants) and upsert into SQLite `statements`, supporting pagination using pagination_key

Usage
-----
    python statements.py 1                   # listed_info にあるコード単位で一括取得（過去分も含む）
    python statements.py 2                   # 当日日付の開示分を取得（日次取得）
    python statements.py 2 --start 2024-01-01 --end 2024-01-31
                                          # 期間指定で取得

環境
----
- Python 3.9+
- `pandas`, `requests`

機能
----
- モード "1": listed_info テーブルから delete_flag=0 の銘柄コードを取得し、各コードごとに /statements API を呼び出して全過去開示情報を取得 (pagination_key によるページネーションを考慮) → statements テーブルに Upsert
- モード "2": /statements?date=<YYYY-MM-DD> を呼び出し、指定日の開示情報を取得 (pagination_key を考慮)。--start/--end を指定すると期間分ループして取得 → statements テーブルに Upsert

テーブル定義（schema）は db_schema.py に記載の CREATE TABLE 文に準拠しています。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests
from requests import Session

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.config import DB_PATH, config  # noqa: E402

# ---------------------------------------------------------------------------
# Config & logging
# ---------------------------------------------------------------------------
API_ENDPOINT = config.get_api_endpoint("statements")
LOG_FMT = config.log_format
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("statements")

# ---------------------------------------------------------------------------
# SQLite側の statements テーブルに合わせたカラム一覧
# ---------------------------------------------------------------------------
SCHEMA_COLUMNS: list[str] = [
    "DisclosedDate",
    "DisclosedTime",
    "code",
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
    path = config.get_file_path("idtoken")
    with path.open("r", encoding="utf-8") as f:
        data: dict[str, str] = json.load(f)
        tok = data.get("idToken")
    if not tok:
        raise RuntimeError("idToken not found in idtoken.json")
    return tok


def _fetch_statements_by_code(session: Session, idtoken: str, code: str) -> list[dict]:
    """GET /statements?code=<code> with pagination and return all statement dicts."""
    headers = {"Authorization": f"Bearer {idtoken}"}
    params = {"code": code}
    all_statements: list[dict] = []
    page = 1
    while True:
        resp = session.get(API_ENDPOINT, headers=headers, params=params, timeout=60)
        if resp.status_code != 200:
            logger.warning("コード %s のAPIエラー: %s", code, resp.text)
            resp.raise_for_status()
        data = resp.json()
        if "message" in data:
            logger.info("API message: %s", data["message"])
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


def _fetch_statements_by_date(
    session: Session, idtoken: str, date_str: str
) -> list[dict]:
    """GET /statements?date=<YYYY-MM-DD> and return all rows."""
    headers = {"Authorization": f"Bearer {idtoken}"}
    params = {"date": date_str}
    all_statements: list[dict] = []
    page = 1
    while True:
        resp = session.get(API_ENDPOINT, headers=headers, params=params, timeout=60)
        if resp.status_code != 200:
            logger.warning("日付 %s のAPIエラー: %s", date_str, resp.text)
            resp.raise_for_status()
        data = resp.json()
        if "message" in data:
            logger.info("API message: %s", data["message"])
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


def _daterange(s: dt.date, e: dt.date) -> list[dt.date]:
    """Return list of dates from ``s`` to ``e`` inclusive."""
    d, out = s, []
    while d <= e:
        out.append(d)
        d += dt.timedelta(days=1)
    return out


def _fetch_statements_by_period(
    session: Session, idtoken: str, start: str, end: str
) -> list[dict]:
    """Fetch statements for each day in the range ``start``–``end``."""
    s = dt.datetime.strptime(start, "%Y-%m-%d").date()
    e = dt.datetime.strptime(end, "%Y-%m-%d").date()
    if s > e:
        s, e = e, s
    records: list[dict] = []
    for d in _daterange(s, e):
        rec = _fetch_statements_by_date(session, idtoken, d.strftime("%Y-%m-%d"))
        if rec:
            records.extend(rec)
    return records


def _fetch_multiple_codes(
    idtoken: str, codes: list[str], workers: int = 5
) -> list[dict]:
    """Fetch statements for many codes concurrently."""
    results: list[dict] = []
    logger.info("%d 件のコードのデータ取得を開始します", len(codes))

    def _task(code: str) -> list[dict]:
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
    """DataFrameを正規化し、スキーマに合わせて列を調整する。

    断片化警告を避けるため、不足している列を一括で追加します。
    APIレスポンスのLocalCodeをcodeに変換します。
    """
    # LocalCodeをcodeに変換（APIレスポンスとの互換性のため）
    if "LocalCode" in df.columns and "code" not in df.columns:
        df = df.rename(columns={"LocalCode": "code"})

    # 不足している列を特定
    missing_cols = [col for col in SCHEMA_COLUMNS if col not in df.columns]

    # 不足している列がある場合は一括で追加
    if missing_cols:
        # 新しいDataFrameを作成して不足している列を追加
        missing_data = dict.fromkeys(missing_cols, pd.NA)
        missing_df = pd.DataFrame(missing_data, index=df.index)
        df = pd.concat([df, missing_df], axis=1)

    return df[SCHEMA_COLUMNS]


def _upsert(conn: sqlite3.Connection, records: list[dict]) -> None:
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


def main(mode: str, start_date: str | None, end_date: str | None) -> None:
    idtoken = _load_token()
    start = time.perf_counter()
    logger.info("モード%sで処理を開始します", mode)
    conn = sqlite3.connect(DB_PATH)
    try:
        if mode == "1":
            cur = conn.execute("SELECT code FROM listed_info WHERE delete_flag = 0")
            codes = [row[0] for row in cur.fetchall()]
            logger.info("有効な銘柄コードを %d 件取得しました", len(codes))
            stmts = _fetch_multiple_codes(idtoken, codes)
            if stmts:
                _upsert(conn, stmts)
            logger.info("一括取得完了: 合計 %d 件", len(stmts))
        elif mode == "2":
            if start_date or end_date:
                if start_date and not end_date:
                    s = start_date
                    e = dt.date.today().strftime("%Y-%m-%d")
                else:
                    s = start_date or end_date or dt.date.today().strftime("%Y-%m-%d")
                    e = end_date or start_date or s
                with requests.Session() as sess:
                    stmts = _fetch_statements_by_period(sess, idtoken, s, e)
                if stmts:
                    _upsert(conn, stmts)
                logger.info("期間 %s 〜 %s の取得完了: %d 件", s, e, len(stmts))
            else:
                today = dt.date.today().strftime("%Y-%m-%d")
                with requests.Session() as sess:
                    stmts = _fetch_statements_by_date(sess, idtoken, today)
                if stmts:
                    _upsert(conn, stmts)
                logger.info("日付 %s の取得完了: %d 件", today, len(stmts))
        else:
            logger.error(
                "無効なモードです: %s。'1' または '2' を指定してください", mode
            )
    except requests.HTTPError as exc:
        conn.commit()
        logger.error("API error: %s", exc)
        raise
    else:
        conn.commit()
    finally:
        conn.close()
    elapsed = time.perf_counter() - start
    logger.info("処理時間: %.2f 秒", elapsed)


if __name__ == "__main__":
    # • モードを指定して決算データを取得
    # • SQLite DB の statements テーブルへ upsert
    parser = argparse.ArgumentParser(description="財務諸表を取得して SQLite に保存")
    parser.add_argument(
        "mode",
        choices=["1", "2"],
        help="1: 銘柄ごとに一括取得、2: 日付または期間で取得",
    )
    parser.add_argument("--start", help="開始日 YYYY-MM-DD")
    parser.add_argument("--end", help="終了日 YYYY-MM-DD")
    args = parser.parse_args()
    main(args.mode, args.start, args.end)

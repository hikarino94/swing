#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fully‑paged downloader for **J‑Quants `/prices/daily_quotes`** that respects the
rate‑limit & pagination notes in the official "Attention" page
(https://jpx.gitbook.io/j‑quants‑ja/api‑reference/attention).

Highlights
==========
* **Pagination** – request param must be **`pagination_key`** (2024‑02 update).
  Older alias `page_key` in responses is still accepted.
* **Rate limit** – API allows **≤3 requests / sec**; we add `time.sleep(0.35)`
  between calls to stay well under.
* **Robust break** – stop if received 0 rows even when a key is returned
  (avoid empty‑page loop noted in docs).
* **Retry** – simple back‑off for 429 / 5xx.

CLI
---
```
python daily_quotes.py                # today only
python daily_quotes.py --start 20240101 --end 20240331
```
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from requests import Session, Response

API_URL = "https://api.jquants.com/v1/prices/daily_quotes"
RATE_SLEEP = 0.35  # ~3 req/sec safety
LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("daily_quotes")
DB_PATH = (Path(__file__).resolve().parents[1] / "db/stock.db").as_posix()

# SQLite prices テーブルのカラム順序を定義
_PRICE_COLS = [
    "code",
    "date",
    "open",
    "high",
    "low",
    "close",
    "upper_limit",
    "lower_limit",
    "volume",
    "turnover_value",
    "adj_factor",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_volume",
]
# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _load_token() -> str:
    """Read the JWT token stored in ``idtoken.json``."""
    path = Path(__file__).resolve().parents[1] / "idtoken.json"
    with path.open("r", encoding="utf-8") as f:
        tok = json.load(f).get("idToken")
    if not tok:
        raise RuntimeError("idToken not found in idtoken.json")
    return tok


def _daterange(s: dt.date, e: dt.date) -> List[dt.date]:
    """Return all weekdays between ``s`` and ``e`` (inclusive)."""
    d, out = s, []
    while d <= e:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


# ---------------------------------------------------------------------------
# API with pagination
# ---------------------------------------------------------------------------


def _call(session: Session, params: dict, token: str, retries: int = 3) -> dict:
    """Send one API request with simple retry and rate limiting."""
    headers = {"Authorization": f"Bearer {token}"}
    for i in range(retries):
        r: Response = session.get(API_URL, headers=headers, params=params, timeout=60)
        if r.status_code < 400:
            time.sleep(RATE_SLEEP)
            return r.json()
        wait = 2**i
        logger.warning("HTTP %s → %ss 後に再試行", r.status_code, wait)
        time.sleep(wait)
    r.raise_for_status()


def _fetch_all(session: Session, base_params: dict, token: str) -> pd.DataFrame:
    """Retrieve all pages for the given API parameters."""
    frames: List[pd.DataFrame] = []
    params = base_params.copy()
    seen: set[str] = set()
    while True:
        js = _call(session, params, token)
        rows = js.get("daily_quotes", [])
        if not rows:
            logger.debug("データなし → ループ終了")
            break
        frames.append(pd.DataFrame(rows))
        key = js.get("pagination_key") or js.get("page_key")
        if not key or key in seen:
            break
        seen.add(key)
        params = base_params.copy()
        params["pagination_key"] = key  # per Attention doc
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _by_date(sess: Session, tok: str, d: dt.date) -> pd.DataFrame:
    """指定日の株価を取得する."""
    # API は YYYY-MM-DD 形式を受け付ける
    return _fetch_all(sess, {"date": d.strftime("%Y-%m-%d")}, tok)


def _by_code(sess: Session, tok: str, code: str) -> pd.DataFrame:
    """Fetch all quotes for the specified stock code."""
    return _fetch_all(sess, {"code": code}, tok)


# ---------------------------------------------------------------------------
# dataframe utils
# ---------------------------------------------------------------------------


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize API columns and types for the database."""
    rename = {
        "Code": "code",
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "UpperLimit": "upper_limit",
        "LowerLimit": "lower_limit",
        "Volume": "volume",
        "TurnoverValue": "turnover_value",
        "AdjustmentFactor": "adj_factor",
        "AdjustmentOpen": "adj_open",
        "AdjustmentHigh": "adj_high",
        "AdjustmentLow": "adj_low",
        "AdjustmentClose": "adj_close",
        "AdjustmentVolume": "adj_volume",
    }
    df = df.rename(columns=rename)
    num = [
        "open",
        "high",
        "low",
        "close",
        "upper_limit",
        "lower_limit",
        "volume",
        "turnover_value",
        "adj_factor",
        "adj_open",
        "adj_high",
        "adj_low",
        "adj_close",
        "adj_volume",
    ]
    for c in num:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Store dates as YYYY-MM-DD in the DB
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df[[c for c in _PRICE_COLS if c in df.columns]]


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


def _upsert(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Insert or update rows into ``prices`` using executemany."""

    if df.empty:
        return

    cols = [c for c in _PRICE_COLS if c in df.columns]
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT OR REPLACE INTO prices ({', '.join(cols)}) VALUES ({placeholders})"

    records = df[cols].itertuples(index=False, name=None)
    conn.executemany(sql, records)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def fetch_and_load(start: Optional[str], end: Optional[str]) -> None:
    """Fetch quotes from the API and load them into SQLite."""
    tok = _load_token()
    sess = requests.Session()
    with sqlite3.connect(DB_PATH) as conn:
        if start or end:
            s = (
                dt.datetime.strptime(start, "%Y-%m-%d").date()
                if start
                else dt.date.today()
            )
            e = dt.datetime.strptime(end, "%Y-%m-%d").date() if end else dt.date.today()
            for d in _daterange(s, e):
                df = _by_date(sess, tok, d)
                if df.empty:
                    logger.info("%s: データなし（休場）", d)
                    continue
                logger.info("%s のデータ取得", d)
                _upsert(conn, _norm(df))
        else:
            today = dt.date.today()
            logger.info("本日 %s", today)
            df_today = _norm(_by_date(sess, tok, today))
            _upsert(conn, df_today)
            splits = df_today.loc[
                df_today["adj_factor"].fillna(1.0) != 1.0, "code"
            ].unique()
            for c in splits:
                logger.info("株式分割検出 %s → 全履歴取得", c)
                _upsert(conn, _norm(_by_code(sess, tok, c)))
    logger.info("完了")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    """Command‑line entry point."""
    ap = argparse.ArgumentParser(description="J‑Quants の日足データを SQLite に保存")
    ap.add_argument("--start", help="開始日 YYYY-MM-DD")
    ap.add_argument("--end", help="終了日 YYYY-MM-DD")
    a = ap.parse_args()
    fetch_and_load(a.start, a.end)


if __name__ == "__main__":
    # • 開始日と終了日を受け取り日足データを取得
    # • prices テーブルへ保存
    _cli()

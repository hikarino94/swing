#!/usr/bin/env python
"""
listed_info.py – Fetch /listed/info (J-Quants) and upsert into SQLite `listed_info`

v4  ✨  Fix column names based on API spec
---------------------------------
- API 仕様に合わせ、`Sector17CodeName` → `sector17_name`, `Sector33CodeName` → `sector33_name`,
  `MarketCodeName` → `market_name` を使用するよう修正。
- 他の列についても JSON キー名を再確認。
- 全ての処理の最後に、`listed_info.date` を確認し、本日日付でない場合 `delete_flag` に 1 を設定。

Usage
-----
    python listed_info.py

環境
----
- Python 3.9+
- `pandas`, `requests`
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import json
import datetime as dt
from pathlib import Path


import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Config & logging
# ---------------------------------------------------------------------------
API_ENDPOINT = "https://api.jquants.com/v1/listed/info"
LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("listed_info")
DB_PATH = (Path(__file__).resolve().parents[1] / "db/stock.db").as_posix()
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_token() -> str:
    path = Path(__file__).resolve().parents[1] / "idtoken.json"
    with path.open("r", encoding="utf-8") as f:
        tok = json.load(f).get("idToken")
    if not tok:
        raise RuntimeError("idToken not found in idtoken.json")
    return tok


def _fetch_listed_info(idtoken: str) -> pd.DataFrame:
    """GET /listed/info and return DataFrame."""
    headers = {"Authorization": f"Bearer {idtoken}"}
    resp = requests.get(API_ENDPOINT, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"API error {resp.status_code}: {resp.text}")

    data = resp.json().get("info", [])
    if not data:
        raise ValueError("/listed/info response contained no 'info' key")

    df = pd.DataFrame(data)
    logger.debug("API列: %s", df.columns.tolist())
    return df


# ---------------------------------------------------------------------------
# DB loader
# ---------------------------------------------------------------------------


def _to_db(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """Extract, rename, NULL-fill → INSERT OR REPLACE into listed_info, then set delete_flag."""
    if df.empty:
        logger.warning("APIから行が返されず、挿入するものがありません。")
        return

    # === 明示的に取り出す列を指定して新しい DataFrame を作成 ===
    mapped = pd.DataFrame(
        {
            "code": df.get("Code", pd.NA),
            "date": df.get("Date", pd.NA),
            "company_name": df.get("CompanyName", pd.NA),
            "company_name_en": df.get("CompanyNameEnglish", pd.NA),
            "sector17_code": df.get("Sector17Code", pd.NA),
            # API のキーは Sector17CodeName
            "sector17_name": df.get("Sector17CodeName", pd.NA),
            "sector33_code": df.get("Sector33Code", pd.NA),
            # API のキーは Sector33CodeName
            "sector33_name": df.get("Sector33CodeName", pd.NA),
            "scale_category": df.get("ScaleCategory", pd.NA),
            "market_code": df.get("MarketCode", pd.NA),
            # API のキーは MarketCodeName
            "market_name": df.get("MarketCodeName", pd.NA),
            "margin_code": df.get("MarginCode", pd.NA),
            "margin_name": df.get("MarginCodeName", pd.NA),
        }
    )

    # 日付を YYYY-MM-DD に揃える
    mapped["date"] = pd.to_datetime(mapped["date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )

    # Upsert via temp table (delete_flag を除く) --------------------------------
    mapped.to_sql("_tmp_listed", conn, if_exists="replace", index=False)
    conn.executescript(
        """
        INSERT OR REPLACE INTO listed_info
            (code, date, company_name, company_name_en,
             sector17_code, sector17_name, sector33_code, sector33_name,
             scale_category, market_code, market_name, margin_code, margin_name)
        SELECT
            code, date, company_name, company_name_en,
            sector17_code, sector17_name, sector33_code, sector33_name,
            scale_category, market_code, market_name, margin_code, margin_name
        FROM _tmp_listed;
        DROP TABLE _tmp_listed;
        """
    )
    logger.info("listed_info に %d 行 upsert しました", len(mapped))

    # 全行の delete_flag を更新（本日日付以外を 1、本日を 0 に設定）
    today_str = dt.date.today().strftime("%Y-%m-%d")
    conn.execute(
        "UPDATE listed_info SET delete_flag = CASE WHEN date = ? THEN 0 ELSE 1 END;",
        (today_str,),
    )
    logger.info("日付に基づき delete_flag を更新しました（本日: %s）", today_str)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def update_listed_info() -> None:
    db_path = Path(DB_PATH).expanduser().resolve()
    idtoken = _load_token()

    logger.info("上場銘柄情報を取得中 …")
    df = _fetch_listed_info(idtoken)

    with sqlite3.connect(db_path) as conn:
        _to_db(df, conn)

    logger.info("listed_info の更新完了 ✔︎")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:  # pragma: no cover
    argparse.ArgumentParser(description="listed_info を取得して SQLite に保存")
    update_listed_info()


if __name__ == "__main__":  # pragma: no cover
    # • J-Quants の上場銘柄一覧を取得
    # • SQLite DB の listed_info を更新
    _cli()

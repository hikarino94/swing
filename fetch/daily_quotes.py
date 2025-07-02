#!/usr/bin/env python
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
python daily_quotes.py --start 2024-01-01 --end 2024-03-31
```
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from requests import Response, Session

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import DB_PATH, config  # noqa: E402

API_URL = config.get_api_endpoint("daily_quotes")
RATE_SLEEP = config.api_rate_limit_sleep
LOG_FMT = config.log_format
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("daily_quotes")

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


class RateLimiter:
    """APIレート制限を管理するクラス（3リクエスト/秒）"""

    def __init__(self, max_per_second: int = 3):
        self.max_per_second = max_per_second
        self.lock = threading.Lock()
        self.last_request_times: list[float] = []

    def wait_if_needed(self) -> None:
        """必要に応じて待機してレート制限を守る"""
        with self.lock:
            now = time.time()
            # 1秒以内のリクエストタイムスタンプを保持
            self.last_request_times = [
                t for t in self.last_request_times if now - t < 1.0
            ]

            if len(self.last_request_times) >= self.max_per_second:
                # レート制限に達している場合は待機
                sleep_time = 1.0 - (now - self.last_request_times[0]) + 0.01
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    # 古いタイムスタンプを削除
                    self.last_request_times = [
                        t for t in self.last_request_times if now - t < 1.0
                    ]

            self.last_request_times.append(time.time())


def _load_token() -> str:
    """Read the JWT token stored in ``idtoken.json``."""
    path = config.get_file_path("idtoken")
    with path.open("r", encoding="utf-8") as f:
        data: dict[str, str] = json.load(f)
        tok = data.get("idToken")
    if not tok:
        raise RuntimeError("idToken not found in idtoken.json")
    return tok


def _daterange(s: dt.date, e: dt.date) -> list[dt.date]:
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
            js: dict = r.json()
            if "message" in js:
                logger.info("API message: %s", js["message"])
            time.sleep(RATE_SLEEP)
            return js
        wait = 2**i
        logger.warning("HTTP %s → %ss 後に再試行", r.status_code, wait)
        time.sleep(wait)
    r.raise_for_status()
    raise RuntimeError("Unexpected end of function")  # 型チェッカー用


def _fetch_all(session: Session, base_params: dict, token: str) -> pd.DataFrame:
    """Retrieve all pages for the given API parameters."""
    frames: list[pd.DataFrame] = []
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


def _fetch_date_with_limiter(
    args: tuple[dt.date, str, RateLimiter],
) -> tuple[dt.date, pd.DataFrame | None, str | None]:
    """レート制限付きで指定日の株価を取得（並列実行用）"""
    date, token, rate_limiter = args
    error_msg = None

    try:
        with requests.Session() as sess:
            rate_limiter.wait_if_needed()
            df = _by_date(sess, token, date)
            return date, df, None
    except requests.HTTPError as exc:
        error_msg = str(exc)
        return date, None, error_msg
    except Exception as exc:
        error_msg = f"Unexpected error: {exc}"
        return date, None, error_msg


def fetch_dates_parallel(
    dates: list[dt.date], token: str, max_workers: int = 3
) -> tuple[list[pd.DataFrame], list[tuple[dt.date, str]]]:
    """複数日付の株価を並列で取得

    Returns:
        tuple: (成功したDataFrameのリスト, 失敗した(日付, エラーメッセージ)のリスト)
    """
    rate_limiter = RateLimiter(max_per_second=3)
    successful_dfs = []
    failed_dates = []

    # 並列実行用の引数を準備
    args_list = [(d, token, rate_limiter) for d in dates]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 全てのタスクを投入
        futures = {
            executor.submit(_fetch_date_with_limiter, args): args[0]
            for args in args_list
        }

        # 完了順に結果を処理
        for future in as_completed(futures):
            date = futures[future]
            try:
                result_date, df, error_msg = future.result()

                if error_msg:
                    logger.error("%s のデータ取得エラー: %s", result_date, error_msg)
                    failed_dates.append((result_date, error_msg))
                elif df is not None and not df.empty:
                    logger.info("%s のデータ取得完了", result_date)
                    successful_dfs.append(df)
                else:
                    logger.info("%s: データなし（休場）", result_date)

            except Exception as exc:
                logger.error("%s の処理中に予期しないエラー: %s", date, exc)
                failed_dates.append((date, str(exc)))

    return successful_dfs, failed_dates


# ---------------------------------------------------------------------------
# dataframe utils
# ---------------------------------------------------------------------------


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize API columns and types for the database."""
    if df.empty:
        return df

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

    # メモリ効率を考慮したデータ型の最適化
    # カテゴリ型を使用してメモリ削減
    if "code" in df.columns:
        df["code"] = df["code"].astype("category")

    # 価格データをfloat32に最適化（精度は十分）
    float32_cols = [
        "open",
        "high",
        "low",
        "close",
        "upper_limit",
        "lower_limit",
        "adj_open",
        "adj_high",
        "adj_low",
        "adj_close",
        "adj_factor",
    ]
    for c in float32_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")

    # ボリュームデータの整数型最適化
    int_cols = ["volume", "turnover_value", "adj_volume"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce", downcast="integer")

    # Store dates as YYYY-MM-DD in the DB
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df[[c for c in _PRICE_COLS if c in df.columns]]


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


def _get_optimized_connection() -> sqlite3.Connection:
    """最適化されたSQLite接続を取得する。

    パフォーマンス向上のためのPRAGMA設定を適用します。
    """
    conn = sqlite3.connect(DB_PATH)

    # パフォーマンス最適化設定
    conn.execute("PRAGMA cache_size = -64000")  # 64MBのキャッシュ
    conn.execute("PRAGMA temp_store = MEMORY")  # 一時データをメモリに保存
    conn.execute("PRAGMA mmap_size = 268435456")  # 256MBのメモリマップI/O

    return conn


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


def fetch_and_load(start: str | None, end: str | None) -> None:
    """Fetch quotes from the API and load them into SQLite."""
    tok = _load_token()
    conn = _get_optimized_connection()  # 最適化された接続を使用

    try:
        if start or end:
            s = (
                dt.datetime.strptime(start, "%Y-%m-%d").date()
                if start
                else dt.date.today()
            )
            e = dt.datetime.strptime(end, "%Y-%m-%d").date() if end else dt.date.today()

            all_dates = _daterange(s, e)
            logger.info("%d日分のデータを並列で取得開始", len(all_dates))

            # 日付のバッチ処理（100日ずつ）
            for i in range(0, len(all_dates), 100):
                batch_dates = all_dates[i : i + 100]

                # 並列でデータ取得
                successful_dfs, failed_list = fetch_dates_parallel(batch_dates, tok)

                # 成功したデータをデータベースに保存
                if successful_dfs:
                    conn.execute("BEGIN")
                    # 各DataFrameを正規化してから結合
                    normalized_dfs = [_norm(df) for df in successful_dfs]
                    combined_df = pd.concat(normalized_dfs, ignore_index=True)
                    _upsert(conn, combined_df)
                    conn.commit()
                    logger.info("%d日分のデータをコミットしました", len(successful_dfs))

                # 失敗した日付を記録
                if failed_list:
                    for date, error in failed_list:
                        logger.warning("%s: %s", date, error)

        else:
            today = dt.date.today()
            logger.info("本日 %s", today)

            try:
                with requests.Session() as sess:
                    conn.execute("BEGIN")
                    df_today = _norm(_by_date(sess, tok, today))
                    _upsert(conn, df_today)

                    # 空のDataFrameの場合は株式分割チェックをスキップ
                    if not df_today.empty:
                        splits = df_today.loc[
                            df_today["adj_factor"].fillna(1.0) != 1.0,
                            "code",
                        ].unique()
                        for c in splits:
                            logger.info("株式分割検出 %s → 全履歴取得", c)
                            try:
                                _upsert(conn, _norm(_by_code(sess, tok, c)))
                            except requests.HTTPError as exc:
                                logger.error("株式分割データ取得エラー %s: %s", c, exc)
                                # 個別エラーは無視して処理を継続

                    conn.commit()
            except requests.HTTPError as exc:
                logger.error("本日のデータ取得エラー: %s", exc)
                # エラーが発生しても正常終了

    except Exception as exc:
        # 予期しないエラーの場合のみロールバック
        try:
            conn.rollback()
        except Exception:
            pass
        logger.error("予期しないエラー: %s", exc)
        raise
    finally:
        conn.close()
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

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""backtest_ml.py

Machine-learning back-test using the screen_ml features.

For each date in the given range this script ranks stocks by the
predicted probability of rising in the next 30 trading days and
simulates buying the top N symbols.
"""

from __future__ import annotations

import argparse
import sqlite3
import datetime as dt
import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

from screening.screen_ml import (
    PRICE_FEATURES,
    NUMERIC_STMT_COLS,
    FUTURE_WINDOW,
    _make_price_features,
    _merge_features,
    _fetch_stmt,
    _add_label,
    _train_model,
)

DB_PATH = (Path(__file__).resolve().parents[1] / "db/stock.db").as_posix()

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("backtest_ml")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _result_paths(prefix: str) -> Tuple[str, str]:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.xlsx", f"{prefix}_{ts}.json"


def _fetch_price_range(con: sqlite3.Connection, start: str, end: str) -> pd.DataFrame:
    q = (
        "SELECT code, date, adj_close, adj_volume FROM prices "
        "WHERE date BETWEEN ? AND ?"
    )
    return pd.read_sql(q, con, params=(start, end), parse_dates=["date"])


def _prepare_dataset(
    con: sqlite3.Connection, start: str, end: str
) -> pd.DataFrame:
    price = _fetch_price_range(
        con,
        (dt.datetime.strptime(start, "%Y-%m-%d") - dt.timedelta(days=365)).strftime("%Y-%m-%d"),
        (dt.datetime.strptime(end, "%Y-%m-%d") + dt.timedelta(days=FUTURE_WINDOW)).strftime("%Y-%m-%d"),
    )
    price_feat = _make_price_features(price)
    stmt = _fetch_stmt(con)
    merged = _merge_features(price_feat, stmt)
    merged = _add_label(merged)
    merged = merged.sort_values(["code", "date"]).copy()
    merged["future_date"] = merged.groupby("code")["date"].shift(-FUTURE_WINDOW)
    req_cols = PRICE_FEATURES + NUMERIC_STMT_COLS
    return merged.dropna(subset=req_cols)


# ---------------------------------------------------------------------------
# Back-test core
# ---------------------------------------------------------------------------


def run_backtest(
    con: sqlite3.Connection,
    start: str,
    end: str | None,
    top: int = 10,
    capital: int = 1_000_000,
    lookback: int = 1095,
) -> pd.DataFrame:
    end = end or start
    logger.info("Preparing dataset…")
    df = _prepare_dataset(con, start, end)

    train_end = (dt.datetime.strptime(start, "%Y-%m-%d") - dt.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    train_df = df[df["date"] <= train_end]
    if train_df.empty:
        raise ValueError("Not enough history to train the model")

    logger.info("Training model on %d rows", len(train_df))
    model = _train_model(train_df)

    trades = []
    start_dt = dt.datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = dt.datetime.strptime(end, "%Y-%m-%d").date()
    for i in range((end_dt - start_dt).days + 1):
        as_of = start_dt + dt.timedelta(days=i)
        daily = df[df["date"] == pd.Timestamp(as_of)]
        daily = daily.dropna(subset=PRICE_FEATURES + NUMERIC_STMT_COLS)
        if daily.empty:
            continue
        X = daily[PRICE_FEATURES + NUMERIC_STMT_COLS].astype(float)
        daily = daily.copy()
        daily["prob"] = model.predict_proba(X)[:, 1]
        picks = daily.sort_values("prob", ascending=False).head(top)
        for _, row in picks.iterrows():
            entry_price = row["adj_close"]
            exit_price = row["future_close"]
            exit_date = row["future_date"]
            if pd.isna(exit_price) or pd.isna(exit_date):
                continue
            shares = int(capital // entry_price)
            if shares <= 0:
                continue
            pnl_yen = (exit_price - entry_price) * shares
            pnl_pct = (exit_price - entry_price) / entry_price * 100
            trades.append(
                {
                    "code": row["code"],
                    "entry_date": as_of,
                    "exit_date": pd.to_datetime(exit_date).date(),
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "shares": shares,
                    "pnl_yen": round(pnl_yen, 0),
                    "pnl_pct": round(pnl_pct, 2),
                    "prob": row["prob"],
                }
            )
    return pd.DataFrame(trades)


def summarize(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    total_profit = trades["pnl_yen"].sum()
    win_rate = (trades["pnl_yen"] > 0).mean()
    mean_ret_pct = trades["pnl_pct"].mean()
    sharpe = trades["pnl_pct"].mean() / trades["pnl_pct"].std(ddof=0)
    return pd.DataFrame(
        {
            "metric": ["trades", "total_profit", "win_rate", "avg_ret_pct", "sharpe"],
            "value": [len(trades), total_profit, win_rate, mean_ret_pct, sharpe],
        }
    )


def _ascii_bar_chart(values: list[float], width: int = 40) -> str:
    if not values:
        return ""
    max_v = max(abs(v) for v in values) or 1
    lines = []
    for i, v in enumerate(values, 1):
        bar = "#" * int(abs(v) / max_v * width)
        sign = "" if v >= 0 else "-"
        lines.append(f"{i:>3} {sign}{bar} ({v:+.0f})")
    return "\n".join(lines)


def show_results(trades: pd.DataFrame, summary: pd.DataFrame) -> None:
    print("=== Summary ===")
    print(summary.to_string(index=False))
    if not trades.empty:
        print("\n=== Profit per Trade ===")
        chart = _ascii_bar_chart(trades["pnl_yen"].tolist())
        print(chart)


def to_excel(trades: pd.DataFrame, summary: pd.DataFrame, path: str) -> None:
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        trades.to_excel(writer, sheet_name="trades", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)
        sheet = writer.sheets["trades"]
        for i, col in enumerate(trades.columns):
            width = max(10, int(trades[col].astype(str).str.len().max() * 1.1))
            sheet.set_column(i, i, width)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ML back-test")
    parser.add_argument("--db", default=DB_PATH, help="SQLite DB path")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument("--top", type=int, default=10, help="Top N picks per day")
    parser.add_argument("--capital", type=int, default=1_000_000, help="Capital per trade")
    parser.add_argument("--lookback", type=int, default=1095, help="Lookback days for training")
    default_xlsx, default_json = _result_paths("ml")
    parser.add_argument("--outfile", default=default_xlsx, help="Excel output")
    parser.add_argument("--json", default=default_json, help="JSON output")
    parser.add_argument("--show", action="store_true", help="Show summary on stdout")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    trades = run_backtest(
        conn,
        args.start,
        args.end,
        top=args.top,
        capital=args.capital,
        lookback=args.lookback,
    )
    summary = summarize(trades)
    to_excel(trades, summary, args.outfile)
    trades.to_json(args.json, orient="records", force_ascii=False)
    logger.info("Excel exported → %s", args.outfile)
    logger.info("JSON exported → %s", args.json)
    if args.show:
        show_results(trades, summary)

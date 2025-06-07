#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""backtest_signals.py – Capital‑sized swing back‑tester + Excel output
=======================================================================
* 1 取引あたり指定資金 (default 1,000,000 JPY) で最大株数を購入
* Entry : DisclosedAt + entry_offset 営業日の adj_close
* Exit  : entry_date + hold_days 営業日の adj_close
* Excel : trades sheet + summary sheet + 損益棒グラフ

Usage
-----
$ python backtest_signals.py \
       --db ../db/stock.db \
       --hold 40 --entry-offset 1 \
       --capital 1000000 --xlsx trades.xlsx -v
"""

from __future__ import annotations
import argparse, logging, math, sqlite3, sys
from datetime import datetime
from pathlib import Path

import pandas as pd

TD_FMT = "%Y-%m-%d"
DEFAULT_CAPITAL = 1_000_000  # JPY

# ── DB helpers ───────────────────────────────────────────────────────

def read_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    q = (
        "SELECT code   AS LocalCode,"
        "       date   AS trade_date,"
        "       adj_close"
        "  FROM prices"
    )
    df = pd.read_sql(q, conn, parse_dates=["trade_date"])
    return df.set_index(["LocalCode", "trade_date"]).sort_index()


def read_signals(conn: sqlite3.Connection, start: str | None, end: str | None) -> pd.DataFrame:
    q = "SELECT LocalCode, DisclosedAt FROM fundamental_signals"
    if start or end:
        q += " WHERE 1=1"
        if start:
            q += f" AND DisclosedAt >= '{start} 00:00:00'"
        if end:
            q += f" AND DisclosedAt <= '{end} 23:59:59'"
    df = pd.read_sql(q, conn, parse_dates=["DisclosedAt"])
    return df

# ── Trading‑days utility ────────────────────────────────────────────

def add_n_trading_days(s: pd.Series, n: int, calendar: pd.DatetimeIndex) -> pd.Series:
    idx = calendar.searchsorted(s) + n
    idx[idx >= len(calendar)] = len(calendar) - 1
    return calendar[idx]

# ── Backtest core ───────────────────────────────────────────────────

def run_backtest(prices: pd.DataFrame, signals: pd.DataFrame, *,
                 hold: int, offset: int, capital: int) -> pd.DataFrame:
    calendar = prices.index.get_level_values(1).unique().sort_values()

    signals = signals.copy()
    signals["entry_date"] = add_n_trading_days(signals["DisclosedAt"], offset, calendar)
    signals["exit_date"]  = add_n_trading_days(signals["entry_date"],  hold,   calendar)

    # マルチ‑インデックスで価格取得
    entry_idx = signals.set_index(["LocalCode", "entry_date"]).index
    exit_idx  = signals.set_index(["LocalCode", "exit_date"]).index

    entry_px = prices.reindex(entry_idx)["adj_close"].values
    exit_px  = prices.reindex(exit_idx)["adj_close"].values

    shares   = (capital // entry_px).astype(int)
    invest   = shares * entry_px
    proceed  = shares * exit_px
    profit   = proceed - invest

    trades = pd.DataFrame({
        "code":        signals["LocalCode"],
        "DisclosedAt": signals["DisclosedAt"].dt.date,
        "entry_date":  signals["entry_date"].dt.date,
        "exit_date":   signals["exit_date"].dt.date,
        "entry_px":    entry_px,
        "exit_px":     exit_px,
        "shares":      shares,
        "invest":      invest,
        "proceed":     proceed,
        "profit_jpy":  profit,
        "ret_pct":     profit / invest,
        "days":        hold,
    })
    return trades


def summarize(trades: pd.DataFrame) -> pd.DataFrame:
    total_profit = trades["profit_jpy"].sum()
    win_rate     = (trades["profit_jpy"] > 0).mean()
    mean_ret_pct = trades["ret_pct"].mean()
    sharpe       = trades["ret_pct"].mean() / trades["ret_pct"].std(ddof=0)

    summary = pd.DataFrame({
        "metric": ["trades", "total_profit", "win_rate", "avg_ret_pct", "sharpe"],
        "value":  [len(trades), total_profit, win_rate, mean_ret_pct, sharpe],
    })
    return summary

# ── Excel output ────────────────────────────────────────────────────

def to_excel(trades: pd.DataFrame, summary: pd.DataFrame, path: str):
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        trades.to_excel(writer, sheet_name="trades", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)

        workbook  = writer.book
        sheet     = writer.sheets["trades"]

        # 自動列幅調整
        for i, col in enumerate(trades.columns):
            width = max(10, int(trades[col].astype(str).str.len().max() * 1.1))
            sheet.set_column(i, i, width)

        # Profit bar chart
        chart = workbook.add_chart({"type": "column"})
        n = len(trades)
        chart.add_series({
            "name":       "profit_jpy",
            "categories": ["trades", 1, 0, n, 0],  # code 列
            "values":     ["trades", 1, trades.columns.get_loc("profit_jpy"), n, trades.columns.get_loc("profit_jpy")],
        })
        chart.set_title({"name": "Profit per Trade (JPY)"})
        chart.set_y_axis({"num_format": "#,##0"})
        sheet.insert_chart("L2", chart)

# ── CLI ─────────────────────────────────────────────────────────────

def parse_args(argv=None):
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--db", default="../db/stock.db", help="SQLite DB file")
    p.add_argument("--hold", type=int, default=40, help="Holding period (trading days)")
    p.add_argument("--entry-offset", type=int, default=1, help="Entry day offset")
    p.add_argument("--capital", type=int, default=DEFAULT_CAPITAL, help="Capital per trade (JPY)")
    p.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    p.add_argument("--end",   type=str, default=None, help="End date YYYY-MM-DD")
    p.add_argument("--xlsx",  type=str, default="trades.xlsx", help="Excel output path")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="[%(levelname)s] %(message)s")

    with sqlite3.connect(args.db) as conn:
        prices  = read_prices(conn)
        signals = read_signals(conn, args.start, args.end)

    logging.info("signals : %d rows", len(signals))
    logging.info("prices  : %d rows", len(prices))

    if signals.empty:
        logging.warning("No signals to back‑test.")
        sys.exit()

    trades  = run_backtest(prices, signals, hold=args.hold, offset=args.entry_offset, capital=args.capital)
    summary = summarize(trades)

    logging.info("Saving Excel → %s", args.xlsx)
    to_excel(trades, summary, args.xlsx)

    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()

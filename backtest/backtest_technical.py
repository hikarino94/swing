#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
backtest_technical.py

Standalone back-test utility for the technical swing-trade framework using adjusted prices.

Scenario
--------
• Entry: 指定期間内の各日について technical_indicators テーブルで signals_count ≥ 3 の銘柄を購入。
• Position size: 100 万円 (デフォルト) に近い整数株。
• Exit: 保有日数が 60 日（≒2 ヶ月）経過、または調整後終値が −5 % に達した最初の日。
• Output: 取引履歴 & サマリを Excel に保存。

Usage example
-------------
```bash
python backtest_technical.py --start 2024-03-01 --end 2024-03-14
```

Optional parameters:
  --capital     投入資金 (JPY, default=1,000,000)
  --hold-days   保有日数 (default=60)
  --stop-loss   損切り閾値 (fraction, default=0.05)
  --min-price   エントリー株価の下限 (JPY, default=300)
"""

from __future__ import annotations

import argparse
import sqlite3
import pandas as pd
import datetime as dt
import logging
from pathlib import Path
from typing import Tuple

CAPITAL_DEFAULT = 1_000_000
HOLD_DAYS_DEFAULT = 60
STOP_LOSS_PCT_DEFAULT = 0.05
MIN_PRICE_DEFAULT = 300
DB_PATH = (Path(__file__).resolve().parents[1] / "db/stock.db").as_posix()

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("backtest_technical")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _result_paths(prefix: str) -> Tuple[str, str]:
    """Return Excel and JSON file paths with a timestamp."""

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.xlsx", f"{prefix}_{ts}.json"


# ---------------------------------------------------------------------------
# Back-test core
# ---------------------------------------------------------------------------


def run_backtest(
    conn,
    as_of: str,
    capital: int = CAPITAL_DEFAULT,
    hold_days: int = HOLD_DAYS_DEFAULT,
    stop_loss_pct: float = STOP_LOSS_PCT_DEFAULT,
    min_price: float = MIN_PRICE_DEFAULT,
) -> pd.DataFrame:
    # Entry signals
    sig_df = pd.read_sql(
        "SELECT code FROM technical_indicators "
        "WHERE signal_date=? "
        "AND signals_count>=3 "
        "AND signals_first=1 "
        "AND signals_overheating=0",
        conn,
        params=(as_of,),
    )
    if sig_df.empty:
        logger.info("No signals on %s", as_of)
        return pd.DataFrame()

    # Convert as_of to date for comparisons
    entry_dt = dt.datetime.strptime(as_of, "%Y-%m-%d").date()
    exit_cut_dt = entry_dt + dt.timedelta(days=hold_days)

    trades = []
    total = len(sig_df)
    logger.info("Start: %d symbols on %s", total, as_of)

    for idx, code in enumerate(sig_df["code"], start=1):
        logger.info("[%d/%d] %s...", idx, total, code)
        try:
            # Load adjusted close prices from as_of onward
            prices = pd.read_sql(
                "SELECT date, adj_close AS close FROM prices "
                "WHERE code=? AND date>=? ORDER BY date",
                conn,
                params=(code, as_of),
                parse_dates=["date"],
            )
            prices = prices.dropna(subset=["date", "close"])
            if prices.empty:
                logger.info("skip (no data)")
                continue

            # Entry price is at as_of date
            # Filter exact date row
            first_row = prices[prices["date"].dt.date == entry_dt]
            if first_row.empty:
                logger.info("skip (no entry date price)")
                continue
            entry_price = first_row.iloc[0]["close"]
            if pd.isna(entry_price) or entry_price <= 0:
                logger.info("skip (invalid entry)")
                continue
            if entry_price < min_price:
                logger.info("skip (price < %s)", min_price)
                continue

            shares = int(capital // entry_price)
            if shares <= 0:
                logger.info("skip (insufficient capital)")
                continue

            stop_price = entry_price * (1 - stop_loss_pct)
            exit_date = None
            exit_price = None

            # Iterate dates after entry_dt
            future = prices[prices["date"].dt.date > entry_dt]
            for _, row in future.iterrows():
                d = row["date"].date()
                p = row["close"]
                if pd.isna(p):
                    continue
                if p <= stop_price or d >= exit_cut_dt:
                    exit_date, exit_price = d, p
                    break

            # If no exit found, close at last available
            if exit_date is None and not future.empty:
                last = future.iloc[-1]
                exit_date, exit_price = last["date"].date(), last["close"]
            if exit_date is None:
                logger.info("skip (no exit data)")
                continue

            pnl_pct = (exit_price - entry_price) / entry_price * 100
            pnl_yen = (exit_price - entry_price) * shares
            holding_days = (exit_date - entry_dt).days

            # Get company name if available
            try:
                r = conn.execute(
                    "SELECT company_name FROM listed_info WHERE code=?", (code,)
                ).fetchone()
                name = r[0] if r else ""
            except sqlite3.OperationalError:
                name = ""

            trades.append(
                {
                    "code": code,
                    "name": name,
                    "entry_date": entry_dt,
                    "exit_date": exit_date,
                    "holding_days": holding_days,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "shares": shares,
                    "pnl_pct": round(pnl_pct, 2),
                    "pnl_yen": round(pnl_yen, 0),
                }
            )
            logger.info("done (P&L=%s)", round(pnl_yen, 0))
        except Exception as e:
            logger.exception("Skip %s: %s", code, e)
            continue

    if not trades:
        logger.info("No trades executed.")
        return pd.DataFrame()

    df = pd.DataFrame(trades)
    total_pnl = df["pnl_yen"].sum()
    logger.info("=== Trades ===\n%s", df)
    logger.info("Total P&L: %s", total_pnl)

    return df


def summarize(trades: pd.DataFrame) -> pd.DataFrame:
    """Generate summary statistics for technical backtest results."""

    total_profit = trades["pnl_yen"].sum()
    win_rate = (trades["pnl_yen"] > 0).mean()
    mean_ret_pct = trades["pnl_pct"].mean()
    sharpe = trades["pnl_pct"].mean() / trades["pnl_pct"].std(ddof=0)

    summary = pd.DataFrame(
        {
            "metric": ["trades", "total_profit", "win_rate", "avg_ret_pct", "sharpe"],
            "value": [len(trades), total_profit, win_rate, mean_ret_pct, sharpe],
        }
    )
    return summary


def _ascii_bar_chart(values: list[float], width: int = 40) -> str:
    """Return simple ASCII bar chart for a sequence of values."""
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
    """Display trades and summary on stdout."""
    print("=== Summary ===")
    print(summary.to_string(index=False))
    if not trades.empty:
        print("\n=== Profit per Trade ===")
        chart = _ascii_bar_chart(trades["pnl_yen"].tolist())
        print(chart)


def to_excel(trades: pd.DataFrame, summary: pd.DataFrame, path: str) -> None:
    """Save trades and summary to an Excel file."""

    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        trades.to_excel(writer, sheet_name="trades", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)

        sheet = writer.sheets["trades"]

        for i, col in enumerate(trades.columns):
            width = max(10, int(trades[col].astype(str).str.len().max() * 1.1))
            sheet.set_column(i, i, width)


def run_backtest_range(
    conn,
    start: str,
    end: str | None,
    capital: int = CAPITAL_DEFAULT,
    hold_days: int = HOLD_DAYS_DEFAULT,
    stop_loss_pct: float = STOP_LOSS_PCT_DEFAULT,
    min_price: float = MIN_PRICE_DEFAULT,
    outfile: str | None = None,
    jsonfile: str | None = None,
    show: bool = False,
) -> None:
    """Run backtest for each entry date between start and end."""

    start_dt = dt.datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = dt.datetime.strptime(end, "%Y-%m-%d").date() if end else start_dt

    all_trades = []
    for i in range((end_dt - start_dt).days + 1):
        as_of = (start_dt + dt.timedelta(days=i)).strftime("%Y-%m-%d")
        logger.info("===== Entry date: %s =====", as_of)
        df = run_backtest(
            conn,
            as_of,
            capital=capital,
            hold_days=hold_days,
            stop_loss_pct=stop_loss_pct,
            min_price=min_price,
        )
        if not df.empty:
            all_trades.append(df)

    if not all_trades:
        logger.info("No trades in the specified period.")
        return

    result = pd.concat(all_trades, ignore_index=True)
    total_pnl = result["pnl_yen"].sum()
    logger.info("=== All Trades ===\n%s", result)
    logger.info("Total P&L: %s", total_pnl)

    summary = summarize(result)
    logger.info("=== Summary ===\n%s", summary)

    if outfile:
        to_excel(result, summary, outfile)
        logger.info("Excel exported → %s", outfile)

    if jsonfile:
        result.to_json(jsonfile, orient="records", force_ascii=False)
        logger.info("JSON exported → %s", jsonfile)

    if show:
        show_results(result, summary)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # • コマンドライン引数を解析して各種設定を取得
    # • 指定 DB に接続
    # • run_backtest() を呼び出し結果を Excel へ保存
    parser = argparse.ArgumentParser(description="スイングトレードのバックテストツール")
    parser.add_argument("--db", default=DB_PATH, help="SQLite DB のパス")
    default_xlsx, default_json = _result_paths("technical")
    parser.add_argument("--start", required=True, help="エントリー開始日 YYYY-MM-DD")
    parser.add_argument("--end", help="エントリー終了日 YYYY-MM-DD")
    parser.add_argument(
        "--outfile",
        default=default_xlsx,
        help="Excel 出力ファイル",
    )
    parser.add_argument(
        "--json",
        default=default_json,
        help="結果を保存するJSONファイル",
    )
    parser.add_argument(
        "--capital",
        type=int,
        default=CAPITAL_DEFAULT,
        help="1 トレードあたりの資金 (JPY)",
    )
    parser.add_argument(
        "--hold-days", type=int, default=HOLD_DAYS_DEFAULT, help="保有日数"
    )
    parser.add_argument(
        "--stop-loss", type=float, default=STOP_LOSS_PCT_DEFAULT, help="損切り率"
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=MIN_PRICE_DEFAULT,
        help="エントリー株価の下限 (JPY)",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="結果を標準出力に表示",
    )
    args = parser.parse_args()
    conn = sqlite3.connect(args.db)
    run_backtest_range(
        conn,
        args.start,
        args.end,
        capital=args.capital,
        hold_days=args.hold_days,
        stop_loss_pct=args.stop_loss,
        min_price=args.min_price,
        outfile=args.outfile,
        jsonfile=args.json,
        show=args.show,
    )

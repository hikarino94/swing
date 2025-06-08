#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
backtest_swing.py

Standalone back-test utility for the technical swing-trade framework using adjusted prices.

Scenario
--------
• Entry: 指定日 (as_of) に technical_indicators テーブルで signals_count ≥ 3 の銘柄を購入。
• Position size: 100 万円 (デフォルト) に近い整数株。
• Exit: 保有日数が 60 日（≒2 ヶ月）経過、または調整後終値が −5 % に達した最初の日。
• Output: 取引履歴 & サマリを Excel に保存。

Usage example
-------------
```bash
python backtest_swing.py --as-of 20240314
```

Optional parameters:
  --capital     投入資金 (JPY, default=1,000,000)
  --hold-days   保有日数 (default=60)
  --stop-loss   損切り閾値 (fraction, default=0.05)
"""
import argparse
import sqlite3
import pandas as pd
import datetime as dt
import sys
from pathlib import Path

CAPITAL_DEFAULT = 1_000_000
HOLD_DAYS_DEFAULT = 60
STOP_LOSS_PCT_DEFAULT = 0.05
DB_PATH = (Path(__file__).resolve().parents[1] / "db/stock.db").as_posix()

# ---------------------------------------------------------------------------
# Back-test core
# ---------------------------------------------------------------------------


def run_backtest(
    conn,
    as_of: str,
    outfile: str,
    capital: int = CAPITAL_DEFAULT,
    hold_days: int = HOLD_DAYS_DEFAULT,
    stop_loss_pct: float = STOP_LOSS_PCT_DEFAULT,
):
    # Entry signals
    sig_df = pd.read_sql(
        "SELECT code FROM technical_indicators WHERE signal_date=? AND signals_count>=3 AND signals_overheating !=1",
        conn,
        params=(as_of,),
    )
    if sig_df.empty:
        print(f"[Backtest] No signals on {as_of}")
        return

    # Convert as_of to date and integer YYYYMMDD for SQL filter
    entry_dt = dt.datetime.strptime(as_of, "%Y%m%d").date()
    as_of_int = int(entry_dt.strftime("%Y%m%d"))
    exit_cut_dt = entry_dt + dt.timedelta(days=hold_days)

    trades = []
    total = len(sig_df)
    print(f"[Backtest] Start: {total} symbols on {as_of}")

    for idx, code in enumerate(sig_df["code"], start=1):
        print(f"[{idx}/{total}] {code}...", end=" ", flush=True)
        try:
            # Load adjusted close prices from as_of onward
            prices = pd.read_sql(
                "SELECT date, adj_close AS close FROM prices "
                "WHERE code=? AND date>=? ORDER BY date",
                conn,
                params=(code, as_of_int),
                parse_dates=["date"],
            )
            prices = prices.dropna(subset=["date", "close"])
            if prices.empty:
                print("skip (no data)")
                continue

            # Entry price is at as_of date
            # Filter exact date row
            first_row = prices[prices["date"].dt.date == entry_dt]
            if first_row.empty:
                print("skip (no entry date price)")
                continue
            entry_price = first_row.iloc[0]["close"]
            if pd.isna(entry_price) or entry_price <= 0:
                print("skip (invalid entry)")
                continue

            shares = int(capital // entry_price)
            if shares <= 0:
                print("skip (insufficient capital)")
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
                print("skip (no exit data)")
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
            print(f"done (P&L={round(pnl_yen,0)})")
        except Exception as e:
            print(f"[Backtest] Skip {code}: {e}", file=sys.stderr)
            continue

    if not trades:
        print("[Backtest] No trades executed.")
        return

    df = pd.DataFrame(trades)
    total_pnl = df["pnl_yen"].sum()
    print("\n=== Trades ===")
    print(df)
    print(f"\nTotal P&L: {total_pnl}")

    if outfile:
        with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="trades", index=False)
            pd.DataFrame([{"total_pnl_yen": total_pnl}]).to_excel(
                writer, sheet_name="summary", index=False
            )
        print(f"Excel exported → {outfile}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # • コマンドライン引数を解析して各種設定を取得
    # • 指定 DB に接続
    # • run_backtest() を呼び出し結果を Excel へ保存
    parser = argparse.ArgumentParser(description="Swing-trade back-test tool")
    parser.add_argument("--db", default=DB_PATH, help="SQLite DB path")
    parser.add_argument("--as-of", required=True, help="Entry date (YYYYMMDD)")
    parser.add_argument(
        "--outfile", default="backtest_results.xlsx", help="Excel output path"
    )
    parser.add_argument(
        "--capital", type=int, default=CAPITAL_DEFAULT, help="Capital per trade"
    )
    parser.add_argument(
        "--hold-days", type=int, default=HOLD_DAYS_DEFAULT, help="Holding period days"
    )
    parser.add_argument(
        "--stop-loss", type=float, default=STOP_LOSS_PCT_DEFAULT, help="Stop-loss pct"
    )
    args = parser.parse_args()
    conn = sqlite3.connect(args.db)
    run_backtest(
        conn, args.as_of, args.outfile, args.capital, args.hold_days, args.stop_loss
    )

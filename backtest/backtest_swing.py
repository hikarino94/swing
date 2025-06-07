#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
backtest_swing.py

Standalone back‑test utility for the technical swing‑trade framework.

Scenario
--------
• *Entry*: 指定日 (as‑of) に `technical_indicators` テーブルで `signals_count ≥ 3` の銘柄を購入。
• *Position size*: 100 万円 (デフォルト) に近い整数株。
• *Exit*: ① 保有日数が 60 日 (≒2 ヶ月) 経過 ② もしくは終値が −5 % に達した最初の日のいずれか早い方。
• *Output*: 取引履歴 & サマリを Excel に保存 (`pandas.ExcelWriter + xlsxwriter`)。

Usage example
-------------
```bash
python backtest_swing.py --db ./db/stock.db --as-of 2024-03-14 --outfile backtest_result.xlsx
```

Optional parameters:
  --capital     1 取引あたり投入資金 (default = 1,000,000 JPY)
  --hold-days   利益確定までの保有日数 (default = 60)
  --stop-loss   損切り閾値 [%]       (default = 0.05)
"""
import argparse
import sqlite3
import pandas as pd
import datetime as dt
import sys

CAPITAL_DEFAULT = 1_000_000
HOLD_DAYS_DEFAULT = 60  # 約 2 ヶ月
STOP_LOSS_PCT_DEFAULT = 0.05  # 5 %
DB_PATH ="./stock.db"
# ---------------------------------------------------------------------------
# Back‑test core
# ---------------------------------------------------------------------------

def run_backtest(conn, as_of: str, outfile: str,
                 capital: int = CAPITAL_DEFAULT,
                 hold_days: int = HOLD_DAYS_DEFAULT,
                 stop_loss_pct: float = STOP_LOSS_PCT_DEFAULT):
    # エントリー対象抽出
    sig = pd.read_sql("SELECT code FROM technical_indicators WHERE signal_date=? AND signals_count>=3",
                      conn, params=(as_of,))
    if sig.empty:
        print("[Backtest] シグナル該当銘柄なし → 処理終了")
        return

    entry_dt = dt.datetime.strptime(as_of, "%Y-%m-%d").date()
    exit_cut_dt = entry_dt + dt.timedelta(days=hold_days)

    results = []

    for code in sig["code"]:
        try:
            prices = pd.read_sql("SELECT date, close FROM prices WHERE code=? AND date>=? ORDER BY date",
                                 conn, params=(code, as_of), parse_dates=["date"])
            if prices.empty:
                continue

            entry_price = prices.iloc[0]["close"]
            if not entry_price:
                continue

            shares = int(capital // entry_price)
            if shares == 0:
                continue

            stop_px = round(entry_price * (1 - stop_loss_pct), 3)
            exit_date, exit_price = None, None

            for _, row in prices.iterrows():
                d = row["date"].date()
                px = row["close"]
                if d == entry_dt:
                    continue  # skip entry row
                if px <= stop_px or d >= exit_cut_dt:
                    exit_date, exit_price = d, px
                    break

            # データ末尾でクローズ
            if exit_date is None:
                exit_date = prices.iloc[-1]["date"].date()
                exit_price = prices.iloc[-1]["close"]

            pnl_pct = (exit_price - entry_price) / entry_price
            pnl_yen = (exit_price - entry_price) * shares
            holding_days = (exit_date - entry_dt).days

            name_row = conn.execute("SELECT company_name FROM listed_info WHERE code=?", (code,)).fetchone()
            name = name_row[0] if name_row else ""

            results.append({
                "code": code,
                "name": name,
                "entry_date": entry_dt,
                "exit_date": exit_date,
                "holding_days": holding_days,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "shares": shares,
                "pnl_pct": round(pnl_pct * 100, 2),
                "pnl_yen": round(pnl_yen, 0),
            })
        except Exception as e:
            print(f"[Backtest] Skip {code}: {e}", file=sys.stderr)
            continue

    if not results:
        print("[Backtest] 取引対象なし")
        return

    df = pd.DataFrame(results)
    summary = df[["pnl_yen"]].sum().rename({"pnl_yen": "total_pnl_yen"})

    print(df)
    print("\n=== SUMMARY ===")
    print(summary)

    # Excel 出力
    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="trades", index=False)
        summary.to_frame().T.to_excel(writer, sheet_name="summary", index=False)
    print("Excel exported →", outfile)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Swing‑trade back‑test tool")
    parser.add_argument("--db", default=DB_PATH, help="SQLite DB path")
    parser.add_argument("--as-of", required=True, help="Entry date (YYYY‑MM‑DD)")
    parser.add_argument("--outfile", default="backtest_results.xlsx", help="Excel output path")
    parser.add_argument("--capital", type=int, default=CAPITAL_DEFAULT, help="Capital per trade (JPY)")
    parser.add_argument("--hold-days", type=int, default=HOLD_DAYS_DEFAULT, help="Holding period in days")
    parser.add_argument("--stop-loss", type=float, default=STOP_LOSS_PCT_DEFAULT, help="Stop‑loss pct (0‑1)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    run_backtest(conn, args.as_of, args.outfile, args.capital, args.hold_days, args.stop_loss)
#todo 株価は調整済を使うように修正する
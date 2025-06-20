#!/usr/bin/env python
"""List screening signals stored in the SQLite database."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
import datetime as dt

import pandas as pd

DB_PATH = Path(__file__).resolve().parent / "stock.db"
TABLES = {
    "fund": ("fundamental_signals", "DisclosedAt"),
    "tech": ("technical_indicators", "signal_date"),
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Show recent screening signals from the DB"
    )
    parser.add_argument("kind", choices=TABLES.keys(), help="fund or tech")
    parser.add_argument("--db", default=DB_PATH, help="SQLite DB path")
    parser.add_argument("--start", help="開始日 YYYY-MM-DD")
    parser.add_argument("--end", help="終了日 YYYY-MM-DD")
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="開始/終了日の指定がない場合に表示する件数",
    )
    args = parser.parse_args()

    # ── default date range ───────────────────────────────────────────────
    if not args.start and not args.end:
        today = dt.date.today().isoformat()
        args.start = today
        args.end = today

    table, date_col = TABLES[args.kind]

    filters: list[str] = []
    params: list[str | int] = []

    if args.kind == "tech":
        filters += [
            "signals_count>=3",
            "signals_first=1",
            "signals_overheating=0",
        ]

    if args.start:
        filters.append(f"{date_col} >= ?")
        params.append(args.start)
    if args.end:
        filters.append(f"{date_col} <= ?")
        params.append(args.end)

    with sqlite3.connect(args.db) as conn:
        if filters:
            where = " WHERE " + " AND ".join(filters)
            sql = f"SELECT * FROM {table}{where} ORDER BY {date_col}"
            df = pd.read_sql(sql, conn, params=params)
        else:
            sql = f"SELECT * FROM {table} ORDER BY {date_col} DESC LIMIT ?"
            df = pd.read_sql(sql, conn, params=(args.limit,))

    if df.empty:
        print("(no rows)")
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()

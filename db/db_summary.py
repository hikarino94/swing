#!/usr/bin/env python
"""Show a quick summary of the SQLite database."""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "stock.db"

TABLES = {
    "prices": "date",
    "listed_info": "date",
    "statements": "DisclosedDate",
    "fundamental_signals": "created_at",
    "technical_indicators": "signal_date",
}


def main() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        for table, date_col in TABLES.items():
            cur = conn.execute(
                f"SELECT COUNT(*), MIN({date_col}), MAX({date_col}) FROM {table}"
            )
            count, min_d, max_d = cur.fetchone()
            print(f"{table:20s}: rows={count:7d}  range=[{min_d} .. {max_d}]")


if __name__ == "__main__":
    main()

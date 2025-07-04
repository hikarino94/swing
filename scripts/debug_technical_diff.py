# \!/usr/bin/env python
"""差異の詳細を調査"""
import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import DB_PATH

# 問題のある銘柄の一つを詳細に調査
code = "17980"
date = "2025-07-03"

conn = sqlite3.connect(DB_PATH)

# 価格データを取得
price_query = """
    SELECT date, adj_open, adj_high, adj_low, adj_close
    FROM prices
    WHERE code = ? AND date >= date(?, '-80 days') AND date <= ?
    ORDER BY date
"""

df_price = pd.read_sql(price_query, conn, params=[code, date, date])
print(f"銘柄 {code} の価格データ: {len(df_price)} 件")
print(df_price.tail(10))

# テクニカル指標を取得
tech_query = """
    SELECT * FROM technical_indicators
    WHERE code = ? AND signal_date = ?
"""

df_tech = pd.read_sql(tech_query, conn, params=[code, date])
print("\nテクニカル指標:")
print(df_tech)

conn.close()

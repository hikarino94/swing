#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
technical_swing_tools.py

Swing-trade signal extraction tool based on technical indicators.

Commands:
  indicators   Calculate & upsert daily signal flags into `technical_indicators`
  screen       Preview today’s signals (optional)

Usage examples:
  python technical_swing_tools.py indicators --db ./db/stock.db --as-of 2025-06-07
  python technical_swing_tools.py screen     --db ./db/stock.db --as-of 2025-06-07
"""
import argparse
import sqlite3
import pandas as pd
import sys

DB_PATH ="./stock.db"

# --- Compute flags ----------------------------------------------------------


def compute_indicators(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date']).sort_values('date').set_index('date')
    for col in ['open', 'high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df[['open','high','low','close']] = df[['open','high','low','close']].ffill().bfill()
    if len(df) < 50:
        return pd.DataFrame()
    sma10 = df['close'].rolling(10).mean()
    sma20 = df['close'].rolling(20).mean()
    sma50 = df['close'].rolling(50).mean()
    slope10 = sma10.diff()
    slope20 = sma20.diff()
    slope50 = sma50.diff()
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    rsi14 = 100 - (100 / (1 + rs))
    plus_dm = df['high'].diff().clip(lower=0)
    minus_dm = df['low'].diff().clip(upper=0).abs()
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low'] - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    plus_di = 100 * plus_dm.rolling(14).sum() / atr
    minus_di = 100 * minus_dm.rolling(14).sum() / atr
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di) * 100
    adx14 = dx.rolling(14).mean()
    ma20 = sma20
    std20 = df['close'].rolling(20).std()
    bb_up1 = ma20 + std20
    bb_low1 = ma20 - std20
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    flags = pd.DataFrame({
        'signal_ma': ((sma10 > sma20) & (sma20 > sma50) & (slope10 > 0) & (slope20 > 0) & (slope50 > 0)).astype(int),
        'signal_rsi': (rsi14 >= 50).astype(int),
        'signal_adx': (adx14 >= 20).astype(int),
        'signal_bb': ((df['close'] >= bb_up1) | (df['close'] <= bb_low1)).astype(int),
        'signal_macd': (macd > macd_signal).astype(int)
    }, index=df.index)
    flags['signals_count'] = flags.sum(axis=1)
    flags = flags.reset_index().rename(columns={'date': 'signal_date'})
    return flags

# --- Run indicators ---------------------------------------------------------
def run_indicators(conn, as_of=None):
    if not as_of:
        as_of = conn.execute("SELECT MAX(date) FROM prices").fetchone()[0]
    codes = [row[0] for row in conn.execute("SELECT DISTINCT code FROM prices").fetchall()]
    total = len(codes)
    print(f"開始: {total} 銘柄を処理します (as_of={as_of})")
    for idx, code in enumerate(codes, start=1):
        print(f"[{idx}/{total}] 銘柄 {code} のシグナル算出中...", flush=True)
        try:
            df = pd.read_sql(
                "SELECT date, open, high, low, close FROM prices "
                "WHERE code=? AND date<=? ORDER BY date", conn,
                params=(code, as_of)
            )
            flags = compute_indicators(df)
            if flags.empty:
                print(f"  → スキップ (データ不足)")
                continue
            today = pd.to_datetime(as_of)
            row = flags[flags['signal_date'] == today]
            if row.empty:
                print(f"  → 当日分なし")
                continue
            rec = row.iloc[0].to_dict()
            rec['signal_date'] = rec['signal_date'].strftime('%Y-%m-%d')
            rec['code'] = code
            conn.execute(
                "INSERT OR REPLACE INTO technical_indicators "
                "(code,signal_date,signal_ma,signal_rsi,signal_adx,signal_bb,signal_macd,signals_count) "
                "VALUES (:code,:signal_date,:signal_ma,:signal_rsi,:signal_adx,:signal_bb,:signal_macd,:signals_count)",
                rec
            )
            conn.commit()
            print(f"  → 完了 (signals_count={rec['signals_count']})")
        except Exception as e:
            print(f"Skipping {code}: {e}", file=sys.stderr)
    print("全処理完了")

# --- Screen signals --------------------------------------------------------
def screen_signals(conn, as_of=None):
    if not as_of:
        as_of = conn.execute("SELECT MAX(signal_date) FROM technical_indicators").fetchone()[0]
    df = pd.read_sql(
        "SELECT * FROM technical_indicators WHERE signal_date=? AND signals_count>=3", conn,
        params=(as_of,)
    )
    print(df)

# --- Main -------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Swing-trade technical signal tool")
    parser.add_argument('command', choices=['indicators','screen'])
    parser.add_argument('--db', default=DB_PATH, help="SQLite DB path")
    parser.add_argument('--as-of', help="Date (YYYY-MM-DD) to compute or screen")
    args = parser.parse_args()
    conn = sqlite3.connect(args.db)
    if args.command == 'indicators':
        run_indicators(conn, args.as_of)
    else:
        screen_signals(conn, args.as_of)
#todo 株価は調整済を使うように修正する
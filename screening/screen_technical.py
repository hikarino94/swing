#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
screen_technical.py

Swing-trade signal extraction tool based on technical indicators.

Commands:
  indicators   Calculate & upsert daily signal flags into `technical_indicators`
  screen       Preview today’s signals (optional)

Usage examples:
  python screen_technical.py indicators --db ./db/stock.db --as-of 2025-06-07
  python screen_technical.py screen     --db ./db/stock.db --as-of 2025-06-07
"""
import argparse
import sqlite3
import pandas as pd
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Threshold constants shared across screening modules
from thresholds import (
    ADX_THRESHOLD,
    FIRST_LOOKBACK_DAYS,
    OVERHEAT_FACTOR,
    RSI_THRESHOLD,
    SIGNAL_COUNT_MIN,
    log_thresholds,
)

DB_PATH = (Path(__file__).resolve().parents[1] / "db/stock.db").as_posix()

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("screen_technical")
log_thresholds(logger)

# Price history to load for indicator calculation
# Holidays can create gaps, so keep roughly 80 days of data
PRICE_LOOKBACK_DAYS = 80

# --- Compute flags ----------------------------------------------------------


def compute_indicators(df):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").set_index("date")
    for col in ["adj_open", "adj_high", "adj_low", "adj_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df[["adj_open", "adj_high", "adj_low", "adj_close"]] = (
        df[["adj_open", "adj_high", "adj_low", "adj_close"]].ffill().bfill()
    )
    if len(df) < 50:
        return pd.DataFrame()
    # --- Moving averages ---
    sma10 = df["adj_close"].rolling(10).mean()
    sma20 = df["adj_close"].rolling(20).mean()
    sma50 = df["adj_close"].rolling(50).mean()

    # price slope of each MA
    slope10 = sma10.diff()
    slope20 = sma20.diff()
    slope50 = sma50.diff()

    # --- RSI(14) ---
    delta = df["adj_close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    rsi14 = 100 - (100 / (1 + rs))

    # --- ADX(14) ---
    plus_dm = df["adj_high"].diff().clip(lower=0)
    minus_dm = df["adj_low"].diff().clip(upper=0).abs()
    tr = pd.concat(
        [
            df["adj_high"] - df["adj_low"],
            (df["adj_high"] - df["adj_close"].shift()).abs(),
            (df["adj_low"] - df["adj_close"].shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(14).mean()
    plus_di = 100 * plus_dm.rolling(14).sum() / atr
    minus_di = 100 * minus_dm.rolling(14).sum() / atr
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di) * 100
    adx14 = dx.rolling(14).mean()

    # --- Bollinger Bands (20-day, 1σ) ---
    ma20 = sma20
    std20 = df["adj_close"].rolling(20).std()
    bb_up1 = ma20 + std20
    bb_low1 = ma20 - std20

    # --- MACD ---
    ema12 = df["adj_close"].ewm(span=12, adjust=False).mean()
    ema26 = df["adj_close"].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()

    # --- Overheating check ---
    overheat = (df["adj_close"] > sma10 * OVERHEAT_FACTOR).astype(
        int
    )  # 10% above 10MA is considered overheated

    flags = pd.DataFrame(
        {
            "signal_ma": (
                (sma10 > sma20)
                & (sma20 > sma50)
                & (slope10 > 0)
                & (slope20 > 0)
                & (slope50 > 0)
            ).astype(int),
            "signal_rsi": (rsi14 >= RSI_THRESHOLD).astype(int),
            "signal_adx": (adx14 >= ADX_THRESHOLD).astype(int),
            "signal_bb": (
                (df["adj_close"] >= bb_up1) | (df["adj_close"] <= bb_low1)
            ).astype(int),
            "signal_macd": (macd > macd_signal).astype(int),
            # signals_overheating: flag when close is >10% above its 10MA
            "signals_overheating": overheat,
        },
        index=df.index,
    )
    flags["signals_count"] = flags[
        ["signal_ma", "signal_rsi", "signal_adx", "signal_bb", "signal_macd"]
    ].sum(axis=1)
    flags = flags.reset_index().rename(columns={"date": "signal_date"})
    return flags


# --- Run indicators ---------------------------------------------------------
def run_indicators(conn, as_of=None):
    if not as_of:
        as_of = datetime.today().strftime("%Y-%m-%d")
    cnt = conn.execute("SELECT COUNT(*) FROM prices WHERE date=?", (as_of,)).fetchone()[
        0
    ]
    if cnt == 0:
        logger.info("%s の価格データがないためスキップ", as_of)
        return
    codes = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT A.code FROM prices A join listed_info B on A.code = B.code where B.market_code!= '0109';"
        ).fetchall()
    ]
    total = len(codes)
    logger.info("開始: %d 銘柄を処理します (as_of=%s)", total, as_of)
    records = []
    start = (
        datetime.strptime(as_of, "%Y-%m-%d") - timedelta(days=PRICE_LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")
    for idx, code in enumerate(codes, start=1):
        # print(f"[{idx}/{total}] 銘柄 {code} のシグナル算出中...", flush=True)
        try:
            df = pd.read_sql(
                "SELECT date, adj_open, adj_high, adj_low, adj_close FROM prices "
                "WHERE code=? AND date>=? AND date<=? ORDER BY date",
                conn,
                params=(code, start, as_of),
            )
            flags = compute_indicators(df)
            if flags.empty:
                logger.info("%s  → スキップ (データ不足)", code)
                continue
            today = pd.to_datetime(as_of)
            row = flags[flags["signal_date"] == today]
            if row.empty:
                logger.info("%s  → 当日分なし", code)
                continue
            rec = row.iloc[0].to_dict()
            rec["signal_date"] = rec["signal_date"].strftime("%Y-%m-%d")
            rec["code"] = code
            # --- signals_first の計算 ---
            # 過去FIRST_LOOKBACK_DAYS日間に signals_count>=SIGNAL_COUNT_MIN の日が
            # ひとつもなければ初回フラグを立てる
            if rec["signals_count"] >= SIGNAL_COUNT_MIN:
                start_30 = (today - timedelta(days=FIRST_LOOKBACK_DAYS)).strftime(
                    "%Y-%m-%d"
                )
                cnt = conn.execute(
                    "SELECT COUNT(*) FROM technical_indicators "
                    "WHERE code=? AND signal_date>=? AND signal_date<? AND signals_count>=?",
                    (code, start_30, rec["signal_date"], SIGNAL_COUNT_MIN),
                ).fetchone()[0]
                rec["signals_first"] = 1 if cnt == 0 else 0
            else:
                rec["signals_first"] = 0
            records.append(rec)
            logger.info(
                "  → 完了 (signal_date=%s,signals_count=%s, overheating=%s)",
                rec["signal_date"],
                rec["signals_count"],
                rec["signals_overheating"],
            )
        except Exception as e:
            logger.exception("Skipping %s: %s", code, e)
    if records:
        sql = """INSERT OR REPLACE INTO technical_indicators
            (code, signal_date, signal_ma, signal_rsi, signal_adx, signal_bb, signal_macd,
            signals_count, signals_overheating, signals_first)
            VALUES (:code, :signal_date, :signal_ma, :signal_rsi, :signal_adx, :signal_bb,
            :signal_macd, :signals_count, :signals_overheating, :signals_first)"""
        conn.executemany(sql, records)
        conn.commit()
    logger.info("全処理完了")


# --- Screen signals --------------------------------------------------------
def screen_signals(conn, as_of=None):
    if not as_of:
        as_of = conn.execute(
            "SELECT MAX(signal_date) FROM technical_indicators"
        ).fetchone()[0]
    df = pd.read_sql(
        "SELECT * FROM technical_indicators WHERE signal_date=? AND signals_count>=?",
        conn,
        params=(as_of, SIGNAL_COUNT_MIN),
    )
    logger.info("\n%s", df)


# --- Main -------------------------------------------------------------------
if __name__ == "__main__":
    # • 引数を解析してコマンドを判定
    # • SQLite DB に接続
    # • indicators: run_indicators() / screen: screen_signals()
    parser = argparse.ArgumentParser(
        description="スイングトレード向けテクニカルシグナルツール"
    )
    parser.add_argument("command", choices=["indicators", "screen"])
    parser.add_argument("--db", default=DB_PATH, help="SQLite DB のパス")
    parser.add_argument("--as-of", help="計算またはスクリーニング対象日 YYYY-MM-DD")
    parser.add_argument(
        "--lookback",
        type=int,
        default=50,
        help="--as-of から遡る日数",
    )
    args = parser.parse_args()
    conn = sqlite3.connect(args.db)
    if args.command == "indicators":
        if args.as_of:
            # 引数 --as-of に YYYY-MM-DD 形式の日付が指定されていたら、
            # 指定された期間ぶん遡って処理する
            end_date = datetime.strptime(args.as_of, "%Y-%m-%d").date()
            back_days = max(args.lookback, 0)
            start_date = end_date - timedelta(days=back_days)
            for i in range(back_days + 1):
                target = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
                logger.info("===== 実行日: %s =====", target)
                run_indicators(conn, target)
        else:
            # 日付指定なしなら従来通り最新日だけ処理
            run_indicators(conn, None)
    else:
        screen_signals(conn, args.as_of)

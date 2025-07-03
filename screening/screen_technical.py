#!/usr/bin/env python
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
import logging
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from screening.thresholds import (  # noqa: E402
    ADX_THRESHOLD,
    FIRST_LOOKBACK_DAYS,
    OVERHEAT_FACTOR,
    OVERSOLD_FACTOR,
    RSI_THRESHOLD,
    SHORT_SIGNAL_COUNT_MIN,
    SIGNAL_COUNT_MIN,
    log_thresholds,
)
from src.config import DB_PATH  # noqa: E402

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("screen_technical")
log_thresholds(logger)

# 並列処理の設定
MAX_WORKERS = 4  # NumPyのGILを考慮してワーカー数を抑える
BATCH_SIZE = 100  # バッチ挿入のサイズ

# Price history to load for indicator calculation
# Holidays can create gaps, so keep roughly 80 days of data
PRICE_LOOKBACK_DAYS = 80

# --- Compute flags ----------------------------------------------------------


def compute_indicators(df):
    """単一銘柄のテクニカル指標を計算"""
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
    sma5 = df["adj_close"].rolling(5).mean()
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

    # --- MACD ---
    ema12 = df["adj_close"].ewm(span=12, adjust=False).mean()
    ema26 = df["adj_close"].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()

    # --- Bollinger lower band (20-day, 1σ) for short ---
    bb_low1 = ma20 - std20

    # --- Overheating & oversold checks ---
    overheat = (df["adj_close"] > sma10 * OVERHEAT_FACTOR).astype(int)
    oversold = (df["adj_close"] < sma5 * OVERSOLD_FACTOR).astype(int)

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
            "signal_bb": (df["adj_close"] >= bb_up1).astype(int),
            "signal_macd": (macd > macd_signal).astype(int),
            "signal_ma_short": (
                (sma50 > sma20)
                & (sma20 > sma10)
                & (slope10 < 0)
                & (slope20 < 0)
                & (slope50 < 0)
            ).astype(int),
            "signal_rsi_short": (rsi14 <= RSI_THRESHOLD).astype(int),
            "signal_bb_short": (df["adj_close"] <= bb_low1).astype(int),
            "signal_macd_short": (macd < macd_signal).astype(int),
            # signals_overheating: flag when close is >10% above its 10MA
            "signals_overheating": overheat,
            # signals_oversold: flag when close is <5% below its 5MA
            "signals_oversold": oversold,
        },
        index=df.index,
    )
    WEIGHTS = {
        "signal_ma": 2,  # trend confirmation
        "signal_bb": 2,  # momentum confirmation
        "signal_rsi": 1,
        "signal_adx": 1,
        "signal_macd": 1,
    }
    flags["signals_count"] = (
        flags[list(WEIGHTS)].mul(pd.Series(WEIGHTS)).sum(axis=1).astype(int)
    )
    SHORT_WEIGHTS = {
        "signal_ma_short": 2,
        "signal_bb_short": 2,
        "signal_rsi_short": 1,
        "signal_macd_short": 1,
        "signal_adx": 1,
    }
    flags["signals_short_count"] = (
        flags[list(SHORT_WEIGHTS)].mul(pd.Series(SHORT_WEIGHTS)).sum(axis=1).astype(int)
    )
    flags = flags.reset_index().rename(columns={"date": "signal_date"})
    return flags


def compute_indicators_parallel(code_groups, max_workers=MAX_WORKERS):
    """複数銘柄のテクニカル指標を並列計算"""
    results = []

    def process_code(code, group):
        try:
            result = compute_indicators(group)
            if not result.empty:
                result["code"] = code
                return result
        except Exception as e:
            logger.warning(f"銘柄 {code} の処理中にエラー: {e}")
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for code, group in code_groups:
            future = executor.submit(process_code, code, group)
            futures[future] = code

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                results.append(result)

            # 進捗状況のログ出力（100銘柄ごと）
            if len(results) % 100 == 0 and len(results) > 0:
                logger.info(f"  処理中: {len(results)}/{len(code_groups)} 銘柄完了")

    return results


# --- Run indicators ---------------------------------------------------------
def run_indicators(conn, as_of=None, use_parallel=True, max_workers=MAX_WORKERS):
    """
    テクニカル指標を計算してデータベースに保存

    Args:
        conn: データベース接続
        as_of: 計算対象日（YYYY-MM-DD）
        use_parallel: 並列処理を使用するかどうか
        max_workers: 並列処理のワーカー数
    """
    if not as_of:
        as_of = datetime.today().strftime("%Y-%m-%d")
    cnt = conn.execute("SELECT COUNT(*) FROM prices WHERE date=?", (as_of,)).fetchone()[
        0
    ]
    if cnt == 0:
        logger.info("%s の価格データがないためスキップ", as_of)
        return
    start = (
        datetime.strptime(as_of, "%Y-%m-%d") - timedelta(days=PRICE_LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")

    # --- Load price data for all target codes in a single query ---
    logger.info("価格データを読み込み中...")
    # 必要なカラムのみを選択し、データ型を最適化
    df_price = pd.read_sql(
        """
        SELECT P.code, P.date, P.adj_open, P.adj_high, P.adj_low, P.adj_close
        FROM prices P
        JOIN listed_info L ON P.code = L.code
        WHERE L.market_code != '0109' AND P.date>=? AND P.date<=?
        ORDER BY P.code, P.date
        """,
        conn,
        params=(start, as_of),
        dtype={
            "adj_open": np.float32,
            "adj_high": np.float32,
            "adj_low": np.float32,
            "adj_close": np.float32,
        },
    )

    if df_price.empty:
        logger.info("対象銘柄なし")
        return

    codes = df_price["code"].unique()
    total = len(codes)
    logger.info("開始: %d 銘柄を処理します (as_of=%s)", total, as_of)
    records = []

    # 各銘柄ごとにインジケーターを計算
    if use_parallel:
        # 並列処理
        code_groups = list(df_price.groupby("code"))
        results = compute_indicators_parallel(code_groups, max_workers)
        processed_count = len(results)
    else:
        # 逐次処理（従来の方法）
        results = []
        processed_count = 0
        for code, group in df_price.groupby("code"):
            result = compute_indicators(group)
            if not result.empty:
                result["code"] = code
                results.append(result)
                processed_count += 1

            # 進捗状況のログ出力（100銘柄ごと）
            if len(results) % 100 == 0 and len(results) > 0:
                logger.info("  処理中: %d/%d 銘柄完了", len(results), total)

    if not results:
        logger.info("全ての銘柄で計算結果が空でした (処理銘柄数: %d)", total)
        return

    logger.info("インジケーター計算完了: %d/%d 銘柄で結果取得", processed_count, total)

    all_flags = pd.concat(results, ignore_index=True)

    today = pd.to_datetime(as_of)
    # デバッグ用：データフレームの構造を確認
    if all_flags.empty:
        logger.info("計算結果が空です")
        return

    logger.debug("all_flags columns: %s", list(all_flags.columns))

    # signal_dateカラムが存在することを確認
    if "signal_date" not in all_flags.columns:
        logger.error(
            "signal_dateカラムが見つかりません。利用可能なカラム: %s",
            list(all_flags.columns),
        )
        return

    today_flags = all_flags[all_flags["signal_date"] == today]
    if today_flags.empty:
        logger.info("当日シグナルなし")
        return

    start_30 = (today - timedelta(days=FIRST_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    hist = pd.read_sql(
        "SELECT DISTINCT code FROM technical_indicators "
        "WHERE signal_date>=? AND signal_date<? AND signals_count>=?",
        conn,
        params=(start_30, as_of, SIGNAL_COUNT_MIN),
    )
    hist_codes = set(hist["code"]) if not hist.empty else set()

    hist_short = pd.read_sql(
        "SELECT DISTINCT code FROM technical_indicators "
        "WHERE signal_date>=? AND signal_date<? AND signals_short_count>=?",
        conn,
        params=(start_30, as_of, SHORT_SIGNAL_COUNT_MIN),
    )
    hist_short_codes = set(hist_short["code"]) if not hist_short.empty else set()

    today_flags = today_flags.copy()
    # Filter out oversold symbols and those with recent short signals
    today_flags = today_flags[
        (today_flags["signals_oversold"] == 0)
        & (~today_flags["code"].isin(hist_short_codes))
    ]
    today_flags["signals_first"] = 0
    today_flags["signals_short_first"] = 0
    mask = today_flags["signals_count"] >= SIGNAL_COUNT_MIN
    today_flags.loc[mask, "signals_first"] = (
        ~today_flags.loc[mask, "code"].isin(hist_codes)
    ).astype(int)
    mask_short = today_flags["signals_short_count"] >= SHORT_SIGNAL_COUNT_MIN
    today_flags.loc[mask_short, "signals_short_first"] = (
        ~today_flags.loc[mask_short, "code"].isin(hist_short_codes)
    ).astype(int)

    today_flags["signal_date"] = today_flags["signal_date"].dt.strftime("%Y-%m-%d")
    records = today_flags.to_dict("records")

    # ログ出力を最小限に抑える（サマリーのみ）
    if records:
        signal_counts = [
            r["signals_count"]
            for r in records
            if r["signals_count"] >= SIGNAL_COUNT_MIN
        ]
        short_counts = [
            r["signals_short_count"]
            for r in records
            if r["signals_short_count"] >= SHORT_SIGNAL_COUNT_MIN
        ]
        logger.info(
            "シグナルサマリー: ロング=%d銘柄, ショート=%d銘柄, 全体=%d銘柄",
            len(signal_counts),
            len(short_counts),
            len(records),
        )

        # バッチ挿入でパフォーマンスを向上
        sql = """INSERT OR REPLACE INTO technical_indicators
            (code, signal_date, signal_ma, signal_rsi,
            signal_adx, signal_bb, signal_macd,
            signal_ma_short, signal_rsi_short,
            signal_bb_short, signal_macd_short,
            signals_count, signals_short_count,
            signals_overheating, signals_oversold, signals_short_first, signals_first)
            VALUES (:code, :signal_date, :signal_ma, :signal_rsi,
            :signal_adx, :signal_bb,
            :signal_macd,
            :signal_ma_short, :signal_rsi_short,
            :signal_bb_short, :signal_macd_short,
            :signals_count, :signals_short_count,
            :signals_overheating, :signals_oversold, :signals_short_first, :signals_first)"""

        # バッチサイズで挿入
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            conn.executemany(sql, batch)
        conn.commit()
    logger.info("全処理完了")


# --- Screen signals --------------------------------------------------------
def screen_signals(conn, as_of=None):
    if not as_of:
        as_of = conn.execute(
            "SELECT MAX(signal_date) FROM technical_indicators"
        ).fetchone()[0]
    df = pd.read_sql(
        "SELECT * FROM technical_indicators "
        "WHERE signal_date=? AND (signals_count>=? OR signals_short_count>=?)",
        conn,
        params=(as_of, SIGNAL_COUNT_MIN, SIGNAL_COUNT_MIN),
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
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="並列処理を無効化（デバッグ用）",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"並列処理のワーカー数（デフォルト: {MAX_WORKERS}）",
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
                run_indicators(
                    conn,
                    target,
                    use_parallel=not args.no_parallel,
                    max_workers=args.workers,
                )
        else:
            # 日付指定なしなら従来通り最新日だけ処理
            run_indicators(
                conn, None, use_parallel=not args.no_parallel, max_workers=args.workers
            )
    else:
        screen_signals(conn, args.as_of)

#!/usr/bin/env python
"""
screen_technical_fast.py

高速化されたテクニカルスクリーニング実装
- データベースクエリの最適化
- ベクトル化された計算
- メモリ効率の改善
"""
import argparse
import logging
import sqlite3
import sys
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
logger = logging.getLogger("screen_technical_fast")
log_thresholds(logger)

# Price history to load for indicator calculation
PRICE_LOOKBACK_DAYS = 80
BATCH_SIZE = 500  # バッチ挿入のサイズ


def compute_indicators_vectorized(df_all):
    """ベクトル化されたテクニカル指標計算（全銘柄を一度に処理）"""
    # データ型の最適化
    df_all["date"] = pd.to_datetime(df_all["date"])

    # 銘柄ごとにグループ化して計算
    results = []

    for code, df in df_all.groupby("code", sort=False):
        if len(df) < 50:
            continue

        df = df.sort_values("date").set_index("date")

        # 価格データの前処理（ベクトル化）
        price_cols = ["adj_open", "adj_high", "adj_low", "adj_close"]
        df[price_cols] = df[price_cols].astype(np.float32)

        # --- Moving averages (ベクトル化) ---
        close = df["adj_close"].values
        sma5 = pd.Series(close, index=df.index).rolling(5, min_periods=5).mean()
        sma10 = pd.Series(close, index=df.index).rolling(10, min_periods=10).mean()
        sma20 = pd.Series(close, index=df.index).rolling(20, min_periods=20).mean()
        sma50 = pd.Series(close, index=df.index).rolling(50, min_periods=50).mean()

        # Slopes
        slope10 = sma10.diff()
        slope20 = sma20.diff()
        slope50 = sma50.diff()

        # --- RSI(14) (ベクトル化) ---
        delta = pd.Series(close, index=df.index).diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(14, min_periods=14).mean()
        avg_loss = loss.rolling(14, min_periods=14).mean()
        rs = avg_gain / avg_loss
        rsi14 = 100 - (100 / (1 + rs))

        # --- ADX(14) (ベクトル化) ---
        high = df["adj_high"].values
        low = df["adj_low"].values

        plus_dm = pd.Series(high, index=df.index).diff().clip(lower=0)
        minus_dm = pd.Series(low, index=df.index).diff().clip(upper=0).abs()

        tr1 = high - low
        tr2 = np.abs(high - np.roll(close, 1))
        tr3 = np.abs(low - np.roll(close, 1))
        tr = pd.Series(np.maximum(tr1, np.maximum(tr2, tr3)), index=df.index)

        atr = tr.rolling(14, min_periods=14).mean()
        plus_di = 100 * plus_dm.rolling(14, min_periods=14).sum() / atr
        minus_di = 100 * minus_dm.rolling(14, min_periods=14).sum() / atr
        dx = (plus_di - minus_di).abs() / (plus_di + minus_di) * 100
        adx14 = dx.rolling(14, min_periods=14).mean()

        # --- Bollinger Bands ---
        std20 = pd.Series(close, index=df.index).rolling(20, min_periods=20).std()
        bb_up1 = sma20 + std20
        bb_low1 = sma20 - std20

        # --- MACD ---
        ema12 = pd.Series(close, index=df.index).ewm(span=12, adjust=False).mean()
        ema26 = pd.Series(close, index=df.index).ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        macd_signal = macd.ewm(span=9, adjust=False).mean()

        # --- Signals (ベクトル化) ---
        flags = pd.DataFrame(index=df.index)
        flags["signal_ma"] = (
            (sma10 > sma20)
            & (sma20 > sma50)
            & (slope10 > 0)
            & (slope20 > 0)
            & (slope50 > 0)
        ).astype(np.int8)
        flags["signal_rsi"] = (rsi14 >= RSI_THRESHOLD).astype(np.int8)
        flags["signal_adx"] = (adx14 >= ADX_THRESHOLD).astype(np.int8)
        flags["signal_bb"] = (close >= bb_up1).astype(np.int8)
        flags["signal_macd"] = (macd > macd_signal).astype(np.int8)

        flags["signal_ma_short"] = (
            (sma50 > sma20)
            & (sma20 > sma10)
            & (slope10 < 0)
            & (slope20 < 0)
            & (slope50 < 0)
        ).astype(np.int8)
        flags["signal_rsi_short"] = (rsi14 <= RSI_THRESHOLD).astype(np.int8)
        flags["signal_bb_short"] = (close <= bb_low1).astype(np.int8)
        flags["signal_macd_short"] = (macd < macd_signal).astype(np.int8)

        flags["signals_overheating"] = (close > sma10 * OVERHEAT_FACTOR).astype(np.int8)
        flags["signals_oversold"] = (close < sma5 * OVERSOLD_FACTOR).astype(np.int8)

        # 重み付けカウント（ベクトル化）
        weights = np.array([2, 1, 1, 2, 1])  # ma, rsi, adx, bb, macd
        long_signals = flags[
            ["signal_ma", "signal_rsi", "signal_adx", "signal_bb", "signal_macd"]
        ].values
        flags["signals_count"] = (long_signals @ weights).astype(np.int8)

        short_weights = np.array(
            [2, 1, 1, 2, 1]
        )  # ma_short, rsi_short, adx, bb_short, macd_short
        short_signals = flags[
            [
                "signal_ma_short",
                "signal_rsi_short",
                "signal_adx",
                "signal_bb_short",
                "signal_macd_short",
            ]
        ].values
        flags["signals_short_count"] = (short_signals @ short_weights).astype(np.int8)

        flags["code"] = code
        flags = flags.reset_index().rename(columns={"date": "signal_date"})

        # NaNを含む行を削除
        flags = flags.dropna()

        if not flags.empty:
            results.append(flags)

    return results


def run_indicators_fast(conn, as_of=None):
    """高速化されたテクニカル指標計算"""
    if not as_of:
        as_of = datetime.today().strftime("%Y-%m-%d")

    # 価格データの存在確認
    cnt = conn.execute("SELECT COUNT(*) FROM prices WHERE date=?", (as_of,)).fetchone()[
        0
    ]
    if cnt == 0:
        logger.info("%s の価格データがないためスキップ", as_of)
        return

    start = (
        datetime.strptime(as_of, "%Y-%m-%d") - timedelta(days=PRICE_LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")

    # 最適化されたクエリ（必要なカラムのみ、インデックスを活用）
    logger.info("価格データを読み込み中...")
    query = """
        SELECT P.code, P.date, P.adj_open, P.adj_high, P.adj_low, P.adj_close
        FROM prices P
        INNER JOIN listed_info L ON P.code = L.code
        WHERE L.market_code != '0109'
          AND P.date >= ?
          AND P.date <= ?
        ORDER BY P.code, P.date
    """

    # データ型を事前に指定して読み込み
    df_price = pd.read_sql(query, conn, params=(start, as_of))

    if df_price.empty:
        logger.info("対象銘柄なし")
        return

    total = df_price["code"].nunique()
    logger.info("開始: %d 銘柄を処理します (as_of=%s)", total, as_of)

    # ベクトル化された計算
    results = compute_indicators_vectorized(df_price)

    if not results:
        logger.info("全ての銘柄で計算結果が空でした")
        return

    processed_count = len(results)
    logger.info("インジケーター計算完了: %d/%d 銘柄で結果取得", processed_count, total)

    # 結果の結合
    all_flags = pd.concat(results, ignore_index=True)

    # 当日のフラグのみ抽出
    today = pd.to_datetime(as_of)
    today_flags = all_flags[all_flags["signal_date"] == today].copy()

    if today_flags.empty:
        logger.info("当日シグナルなし")
        return

    # 履歴データの確認（最適化されたクエリ）
    start_30 = (today - timedelta(days=FIRST_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    hist_query = """
        SELECT DISTINCT code
        FROM technical_indicators
        WHERE signal_date >= ?
          AND signal_date < ?
          AND signals_count >= ?
    """
    hist = pd.read_sql(hist_query, conn, params=(start_30, as_of, SIGNAL_COUNT_MIN))
    hist_codes = set(hist["code"]) if not hist.empty else set()

    hist_short_query = """
        SELECT DISTINCT code
        FROM technical_indicators
        WHERE signal_date >= ?
          AND signal_date < ?
          AND signals_short_count >= ?
    """
    hist_short = pd.read_sql(
        hist_short_query, conn, params=(start_30, as_of, SHORT_SIGNAL_COUNT_MIN)
    )
    hist_short_codes = set(hist_short["code"]) if not hist_short.empty else set()

    # フィルタリングと初回シグナルの判定
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

    # 日付フォーマット
    today_flags["signal_date"] = today_flags["signal_date"].dt.strftime("%Y-%m-%d")
    records = today_flags.to_dict("records")

    # サマリーログ
    if records:
        signal_counts = sum(
            1 for r in records if r["signals_count"] >= SIGNAL_COUNT_MIN
        )
        short_counts = sum(
            1 for r in records if r["signals_short_count"] >= SHORT_SIGNAL_COUNT_MIN
        )
        logger.info(
            "シグナルサマリー: ロング=%d銘柄, ショート=%d銘柄, 全体=%d銘柄",
            signal_counts,
            short_counts,
            len(records),
        )

        # 高速バッチ挿入
        sql = """INSERT OR REPLACE INTO technical_indicators
            (code, signal_date, signal_ma, signal_rsi, signal_adx, signal_bb, signal_macd,
             signal_ma_short, signal_rsi_short, signal_bb_short, signal_macd_short,
             signals_count, signals_short_count, signals_overheating, signals_oversold,
             signals_short_first, signals_first)
            VALUES (:code, :signal_date, :signal_ma, :signal_rsi, :signal_adx, :signal_bb, :signal_macd,
                    :signal_ma_short, :signal_rsi_short, :signal_bb_short, :signal_macd_short,
                    :signals_count, :signals_short_count, :signals_overheating, :signals_oversold,
                    :signals_short_first, :signals_first)"""

        # トランザクションで高速化
        conn.execute("BEGIN TRANSACTION")
        try:
            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i : i + BATCH_SIZE]
                conn.executemany(sql, batch)
            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            raise e

    logger.info("全処理完了")


def screen_signals(conn, as_of=None):
    """シグナルのスクリーニング表示"""
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="高速テクニカルシグナルツール")
    parser.add_argument("command", choices=["indicators", "screen"])
    parser.add_argument("--db", default=DB_PATH, help="SQLite DB のパス")
    parser.add_argument("--as-of", help="計算またはスクリーニング対象日 YYYY-MM-DD")
    parser.add_argument("--lookback", type=int, default=50, help="--as-of から遡る日数")

    args = parser.parse_args()
    conn = sqlite3.connect(args.db)

    if args.command == "indicators":
        if args.as_of:
            end_date = datetime.strptime(args.as_of, "%Y-%m-%d").date()
            back_days = max(args.lookback, 0)
            start_date = end_date - timedelta(days=back_days)
            for i in range(back_days + 1):
                target = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
                logger.info("===== 実行日: %s =====", target)
                run_indicators_fast(conn, target)
        else:
            run_indicators_fast(conn, None)
    else:
        screen_signals(conn, args.as_of)

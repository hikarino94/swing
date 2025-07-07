#!/usr/bin/env python
"""
screen_technical_fast.py - 高速化されたテクニカルスクリーニング

主な高速化手法:
1. 銘柄単位での並列処理（日付単位ではなく）
2. データ型の最適化（float64→float32）
3. バッチ処理の最適化
4. より効率的なデータベースアクセス
"""
import argparse
import logging
import sqlite3
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from screening.thresholds import (
    ADX_THRESHOLD,
    FIRST_LOOKBACK_DAYS,
    OVERHEAT_FACTOR,
    OVERSOLD_FACTOR,
    RSI_THRESHOLD,
    SHORT_SIGNAL_COUNT_MIN,
    SIGNAL_COUNT_MIN,
    log_thresholds,
)
from src.config import get_db_path

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("screen_technical_fast")
log_thresholds(logger)

# 並列処理の設定
MAX_WORKERS = 8  # CPUコア数に応じて調整
BATCH_SIZE = 1000  # バッチ挿入のサイズ
CHUNK_SIZE = 50  # 各ワーカーが処理する銘柄数

# Price history to load for indicator calculation
PRICE_LOOKBACK_DAYS = 80


def compute_indicators_for_code(args):
    """単一銘柄の全日付分のインジケーターを計算"""
    code, price_data, date_list = args

    # 価格データをDataFrameに変換
    df = pd.DataFrame(price_data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")

    # データ型を最適化（NaNを適切に処理）
    for col in ["adj_open", "adj_high", "adj_low", "adj_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(np.float32)

    # 欠損値を前方・後方補完
    df[["adj_open", "adj_high", "adj_low", "adj_close"]] = (
        df[["adj_open", "adj_high", "adj_low", "adj_close"]].ffill().bfill()
    )

    # 必要なデータが不足している場合はスキップ
    if len(df) < 50:
        return []

    # インジケーターを一度に計算
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

    # --- Bollinger Bands ---
    ma20 = sma20
    std20 = df["adj_close"].rolling(20).std()
    bb_up1 = ma20 + std20
    bb_low1 = ma20 - std20

    # --- MACD ---
    ema12 = df["adj_close"].ewm(span=12, adjust=False).mean()
    ema26 = df["adj_close"].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()

    # --- Overheating & oversold checks ---
    overheat = (df["adj_close"] > sma10 * OVERHEAT_FACTOR).astype(int)
    oversold = (df["adj_close"] < sma5 * OVERSOLD_FACTOR).astype(int)

    # シグナルフラグの計算
    flags = pd.DataFrame(index=df.index)
    flags["signal_ma"] = (
        (sma10 > sma20)
        & (sma20 > sma50)
        & (slope10 > 0)
        & (slope20 > 0)
        & (slope50 > 0)
    ).astype(int)
    flags["signal_rsi"] = (rsi14 >= RSI_THRESHOLD).astype(int)
    flags["signal_adx"] = (adx14 >= ADX_THRESHOLD).astype(int)
    flags["signal_bb"] = (df["adj_close"] >= bb_up1).astype(int)
    flags["signal_macd"] = (macd > macd_signal).astype(int)
    flags["signal_ma_short"] = (
        (sma50 > sma20)
        & (sma20 > sma10)
        & (slope10 < 0)
        & (slope20 < 0)
        & (slope50 < 0)
    ).astype(int)
    flags["signal_rsi_short"] = (rsi14 <= RSI_THRESHOLD).astype(int)
    flags["signal_bb_short"] = (df["adj_close"] <= bb_low1).astype(int)
    flags["signal_macd_short"] = (macd < macd_signal).astype(int)
    flags["signals_overheating"] = overheat
    flags["signals_oversold"] = oversold

    # 重み付けスコアの計算
    WEIGHTS = {
        "signal_ma": 2,
        "signal_bb": 2,
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

    # 指定された日付のみフィルタリング
    results = []
    for target_date in date_list:
        if target_date in flags.index:
            row = flags.loc[target_date]
            results.append(
                {
                    "code": code,
                    "signal_date": target_date.strftime("%Y-%m-%d"),
                    "signal_ma": int(row["signal_ma"]),
                    "signal_rsi": int(row["signal_rsi"]),
                    "signal_adx": int(row["signal_adx"]),
                    "signal_bb": int(row["signal_bb"]),
                    "signal_macd": int(row["signal_macd"]),
                    "signal_ma_short": int(row["signal_ma_short"]),
                    "signal_rsi_short": int(row["signal_rsi_short"]),
                    "signal_bb_short": int(row["signal_bb_short"]),
                    "signal_macd_short": int(row["signal_macd_short"]),
                    "signals_count": int(row["signals_count"]),
                    "signals_short_count": int(row["signals_short_count"]),
                    "signals_overheating": int(row["signals_overheating"]),
                    "signals_oversold": int(row["signals_oversold"]),
                }
            )

    return results


def process_chunk(chunk_data):
    """チャンクごとに銘柄を処理"""
    results = []
    for args in chunk_data:
        try:
            code_results = compute_indicators_for_code(args)
            results.extend(code_results)
        except Exception as e:
            logger.warning(f"銘柄 {args[0]} の処理中にエラー: {e}")
    return results


def run_indicators_fast(conn, date_list, use_parallel=True, max_workers=MAX_WORKERS):
    """
    高速化されたテクニカル指標計算

    Args:
        conn: データベース接続
        date_list: 計算対象日のリスト
        use_parallel: 並列処理を使用するかどうか
        max_workers: 並列処理のワーカー数
    """
    if not date_list:
        logger.info("計算対象日がありません")
        return

    # 日付範囲を計算
    min_date = min(date_list)
    max_date = max(date_list)
    start_date = (min_date - timedelta(days=PRICE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    end_date = max_date.strftime("%Y-%m-%d")

    logger.info(f"価格データを読み込み中... ({start_date} から {end_date})")

    # 全銘柄の価格データを一度に読み込む
    query = """
        SELECT P.code, P.date, P.adj_open, P.adj_high, P.adj_low, P.adj_close
        FROM prices P
        JOIN listed_info L ON P.code = L.code
        WHERE L.market_code != '0109' AND P.date >= ? AND P.date <= ?
        ORDER BY P.code, P.date
    """

    df_all = pd.read_sql(query, conn, params=(start_date, end_date))

    if df_all.empty:
        logger.info("対象銘柄なし")
        return

    # 銘柄ごとにグループ化
    grouped = df_all.groupby("code")
    codes = list(grouped.groups.keys())
    total_codes = len(codes)
    logger.info(f"開始: {total_codes} 銘柄を処理します")

    # 各銘柄のデータを準備
    task_data = []
    for code in codes:
        price_data = grouped.get_group(code).to_dict("records")
        task_data.append((code, price_data, date_list))

    # 処理実行
    all_results = []

    if use_parallel and total_codes > 100:
        # 並列処理
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # タスクをチャンクに分割
            chunks = [
                task_data[i : i + CHUNK_SIZE]
                for i in range(0, len(task_data), CHUNK_SIZE)
            ]

            # 各チャンクを並列処理
            futures = {
                executor.submit(process_chunk, chunk): i
                for i, chunk in enumerate(chunks)
            }

            completed = 0
            for future in as_completed(futures):
                try:
                    chunk_results = future.result()
                    all_results.extend(chunk_results)
                    completed += 1

                    # 進捗表示
                    processed_codes = completed * CHUNK_SIZE
                    if processed_codes % 500 == 0:
                        logger.info(
                            f"  処理中: {min(processed_codes, total_codes)}/{total_codes} 銘柄完了"
                        )

                except Exception as e:
                    logger.error(f"チャンク処理中にエラー: {e}")
    else:
        # 逐次処理
        for i, args in enumerate(task_data):
            try:
                code_results = compute_indicators_for_code(args)
                all_results.extend(code_results)

                if (i + 1) % 100 == 0:
                    logger.info(f"  処理中: {i + 1}/{total_codes} 銘柄完了")

            except Exception as e:
                logger.warning(f"銘柄 {args[0]} の処理中にエラー: {e}")

    logger.info(f"インジケーター計算完了: {len(all_results)} レコード生成")

    if not all_results:
        logger.info("計算結果が空です")
        return

    # シグナルの初回判定を行う
    df_results = pd.DataFrame(all_results)

    # 各日付ごとに処理
    for target_date in date_list:
        date_str = target_date.strftime("%Y-%m-%d")
        date_results = df_results[df_results["signal_date"] == date_str].copy()

        if date_results.empty:
            continue

        # 過去30日間のシグナル履歴を取得
        start_30 = (target_date - timedelta(days=FIRST_LOOKBACK_DAYS)).strftime(
            "%Y-%m-%d"
        )

        # ロングシグナル履歴
        hist_long = pd.read_sql(
            """SELECT DISTINCT code FROM technical_indicators
               WHERE signal_date >= ? AND signal_date < ? AND signals_count >= ?""",
            conn,
            params=(start_30, date_str, SIGNAL_COUNT_MIN),
        )
        hist_long_codes = set(hist_long["code"]) if not hist_long.empty else set()

        # ショートシグナル履歴
        hist_short = pd.read_sql(
            """SELECT DISTINCT code FROM technical_indicators
               WHERE signal_date >= ? AND signal_date < ? AND signals_short_count >= ?""",
            conn,
            params=(start_30, date_str, SHORT_SIGNAL_COUNT_MIN),
        )
        hist_short_codes = set(hist_short["code"]) if not hist_short.empty else set()

        # 初回フラグを設定
        date_results["signals_first"] = 0
        date_results["signals_short_first"] = 0

        # oversoldでない、かつ最近ショートシグナルがない銘柄のみ対象
        valid_mask = (date_results["signals_oversold"] == 0) & (
            ~date_results["code"].isin(hist_short_codes)
        )
        date_results = date_results[valid_mask].copy()

        # ロング初回判定
        long_mask = date_results["signals_count"] >= SIGNAL_COUNT_MIN
        date_results.loc[long_mask, "signals_first"] = (
            ~date_results.loc[long_mask, "code"].isin(hist_long_codes)
        ).astype(int)

        # ショート初回判定
        short_mask = date_results["signals_short_count"] >= SHORT_SIGNAL_COUNT_MIN
        date_results.loc[short_mask, "signals_short_first"] = (
            ~date_results.loc[short_mask, "code"].isin(hist_short_codes)
        ).astype(int)

        # データベースに保存
        if not date_results.empty:
            records = date_results.to_dict("records")

            # ログ出力
            signal_counts = len(
                date_results[date_results["signals_count"] >= SIGNAL_COUNT_MIN]
            )
            short_counts = len(
                date_results[
                    date_results["signals_short_count"] >= SHORT_SIGNAL_COUNT_MIN
                ]
            )
            logger.info(
                f"{date_str}: ロング={signal_counts}銘柄, ショート={short_counts}銘柄, 全体={len(date_results)}銘柄"
            )

            # バッチ挿入
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

            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i : i + BATCH_SIZE]
                conn.executemany(sql, batch)

            conn.commit()

    logger.info("全処理完了")


def screen_signals(conn, as_of=None, lookback=None):
    """シグナルのスクリーニング（従来と同じ）"""
    if lookback is None:
        lookback = FIRST_LOOKBACK_DAYS

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
    parser = argparse.ArgumentParser(
        description="高速化されたスイングトレード向けテクニカルシグナルツール"
    )
    parser.add_argument("command", choices=["indicators", "screen"])
    parser.add_argument("--db", default=get_db_path(), help="SQLite DB のパス")
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
            # 指定された期間の日付リストを作成
            end_date = datetime.strptime(args.as_of, "%Y-%m-%d").date()
            back_days = max(args.lookback, 0)
            start_date = end_date - timedelta(days=back_days)

            date_list = []
            current = start_date
            while current <= end_date:
                date_list.append(pd.Timestamp(current))
                current += timedelta(days=1)

            logger.info(
                f"処理対象: {len(date_list)} 日分 ({start_date} から {end_date})"
            )

            # 高速版を実行
            run_indicators_fast(
                conn,
                date_list,
                use_parallel=not args.no_parallel,
                max_workers=args.workers,
            )
        else:
            # 日付指定なしなら最新日だけ処理
            today = pd.Timestamp(datetime.today().date())
            run_indicators_fast(
                conn,
                [today],
                use_parallel=not args.no_parallel,
                max_workers=args.workers,
            )
    else:
        screen_signals(conn, args.as_of, args.lookback)

    conn.close()

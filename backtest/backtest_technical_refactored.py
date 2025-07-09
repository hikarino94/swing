#!/usr/bin/env python
"""
backtest_technical.py (リファクタリング版)

テクニカル指標に基づくバックテストの実行

このモジュールは戦略を分離し、より保守性の高い構造に改善されています。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# プロジェクトのパスを追加
sys.path.append(str(Path(__file__).resolve().parents[1]))

from screening.thresholds import log_thresholds  # noqa: E402
from src.config import get_db_path  # noqa: E402
from src.utils.file_utils import get_timestamped_output_path  # noqa: E402

from .strategies import TechnicalLongStrategy, TechnicalShortStrategy  # noqa: E402
from .technical_runner import TechnicalBacktestRunner  # noqa: E402

# デフォルト値
CAPITAL_DEFAULT = 1_000_000
HOLD_DAYS_DEFAULT = 60
STOP_LOSS_PCT_DEFAULT = 0.05
MIN_PRICE_DEFAULT = 300

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=LOG_FMT, level=logging.INFO)
logger = logging.getLogger("backtest_technical")
log_thresholds(logger)


def summarize(trades_df: pd.DataFrame) -> pd.DataFrame:
    """トレード結果のサマリーを生成"""
    if trades_df.empty:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "trades": len(trades_df),
                "profit_mean": trades_df["profit_pct"].mean(),
                "profit_std": trades_df["profit_pct"].std(),
                "profit_sum_jpy": trades_df["profit_jpy"].sum(),
                "win_rate": (trades_df["profit_pct"] > 0).mean(),
                "avg_hold_days": trades_df["hold_days"].mean(),
            }
        ]
    )


def show_results(trades_df: pd.DataFrame, summary_df: pd.DataFrame) -> None:
    """結果を標準出力に表示"""
    print("\n=== Trades ===")
    print(trades_df.to_string(index=False))
    print("\n=== Summary ===")
    print(summary_df.to_string(index=False))


def to_excel(
    trades_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    long_summary: pd.DataFrame,
    short_summary: pd.DataFrame,
    excel_path: Path,
) -> None:
    """結果をExcelファイルに保存"""
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        if not trades_df.empty:
            trades_df.to_excel(writer, sheet_name="Trades", index=False)
        if not summary_df.empty:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
        if not long_summary.empty:
            long_summary.to_excel(writer, sheet_name="Long Summary", index=False)
        if not short_summary.empty:
            short_summary.to_excel(writer, sheet_name="Short Summary", index=False)
    logger.info(f"Results saved to {excel_path}")


def save_json(trades_df: pd.DataFrame, json_path: Path) -> None:
    """結果をJSONファイルに保存"""
    result_data = {
        "metadata": {
            "backtest_type": "technical",
            "timestamp": dt.datetime.now().isoformat(),
        },
        "trades": trades_df.to_dict("records") if not trades_df.empty else [],
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result_data, f, indent=2, ensure_ascii=False, default=str)

    logger.info(f"Results saved to {json_path}")


def run_backtest_range(
    start_date: str,
    end_date: str,
    capital: int = CAPITAL_DEFAULT,
    hold_days: int = HOLD_DAYS_DEFAULT,
    stop_loss_pct: float = STOP_LOSS_PCT_DEFAULT,
    min_price: float = MIN_PRICE_DEFAULT,
    show: bool = False,
) -> None:
    """期間指定でバックテストを実行"""
    conn = sqlite3.connect(get_db_path())

    try:
        # ロング戦略
        long_strategy = TechnicalLongStrategy(
            capital=capital,
            hold_days=hold_days,
            stop_loss_pct=stop_loss_pct,
            min_price=min_price,
        )
        long_runner = TechnicalBacktestRunner(conn, long_strategy)
        long_trades = long_runner.run_period(start_date, end_date)

        # ショート戦略
        short_strategy = TechnicalShortStrategy(
            capital=capital,
            hold_days=hold_days,
            stop_loss_pct=stop_loss_pct,
            min_price=min_price,
        )
        short_runner = TechnicalBacktestRunner(conn, short_strategy)
        short_trades = short_runner.run_period(start_date, end_date)

        # 結果を結合
        all_trades = pd.concat([long_trades, short_trades], ignore_index=True)

        if all_trades.empty:
            logger.warning("No trades executed.")
            return

        # サマリー計算
        summary_all = summarize(all_trades)
        summary_long = (
            summarize(long_trades) if not long_trades.empty else pd.DataFrame()
        )
        summary_short = (
            summarize(short_trades) if not short_trades.empty else pd.DataFrame()
        )

        # 結果を保存
        excel_path = get_timestamped_output_path("backtest", "technical_swing", ".xlsx")
        json_path = get_timestamped_output_path("backtest", "technical_swing", ".json")

        to_excel(all_trades, summary_all, summary_long, summary_short, excel_path)
        save_json(all_trades, json_path)

        if show:
            show_results(all_trades, summary_all)

        # 統計を表示
        logger.info(f"Total trades: {len(all_trades)}")
        logger.info(f"Long trades: {len(long_trades)}")
        logger.info(f"Short trades: {len(short_trades)}")
        logger.info(f"Total P/L: {all_trades['profit_jpy'].sum():,.0f} JPY")

    finally:
        conn.close()


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(
        description="Backtest technical indicators strategy"
    )
    parser.add_argument(
        "--start",
        type=str,
        default=(dt.date.today() - dt.timedelta(days=30)).isoformat(),
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=dt.date.today().isoformat(),
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--capital",
        type=int,
        default=CAPITAL_DEFAULT,
        help=f"Capital per position (JPY, default={CAPITAL_DEFAULT:,})",
    )
    parser.add_argument(
        "--hold-days",
        type=int,
        default=HOLD_DAYS_DEFAULT,
        help=f"Holding period (days, default={HOLD_DAYS_DEFAULT})",
    )
    parser.add_argument(
        "--stop-loss",
        type=float,
        default=STOP_LOSS_PCT_DEFAULT,
        help=f"Stop loss percentage (default={STOP_LOSS_PCT_DEFAULT})",
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=MIN_PRICE_DEFAULT,
        help=f"Minimum entry price (JPY, default={MIN_PRICE_DEFAULT})",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Show results in terminal",
    )
    return parser.parse_args()


def main():
    """メイン関数"""
    args = parse_args()

    logger.info(f"Backtest period: {args.start} to {args.end}")
    logger.info(f"Capital per position: {args.capital:,} JPY")
    logger.info(f"Hold days: {args.hold_days}")
    logger.info(f"Stop loss: {args.stop_loss * 100}%")
    logger.info(f"Min price: {args.min_price} JPY")

    run_backtest_range(
        start_date=args.start,
        end_date=args.end,
        capital=args.capital,
        hold_days=args.hold_days,
        stop_loss_pct=args.stop_loss,
        min_price=args.min_price,
        show=args.show,
    )


if __name__ == "__main__":
    main()

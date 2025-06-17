#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""backtest_statements.py – Capital‑sized swing back‑tester + Excel output
=======================================================================
* 1 取引あたり指定資金 (default 1,000,000 JPY) で最大株数を購入
* Entry : DisclosedAt + entry_offset 営業日の adj_close
* Exit  : entry_date + hold_days 営業日の adj_close
* Excel : trades sheet + summary sheet + 損益棒グラフ

Usage
-----
$ python backtest_statements.py \
       --db ../db/stock.db \
       --hold 40 --entry-offset 1 \
       --capital 1000000 --xlsx trades.xlsx -v
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

TD_FMT = "%Y-%m-%d"
DEFAULT_CAPITAL = 1_000_000  # JPY
DB_PATH = (Path(__file__).resolve().parents[1] / "db/stock.db").as_posix()

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logger = logging.getLogger("backtest_statements")

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def read_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    """価格テーブルを読み込む。

    入力パラメータ: SQLite の接続オブジェクト。
    戻り値: 銘柄コードと取引日をインデックスとした DataFrame。
    処理内容: prices テーブルを取得しマルチインデックスで整形して返す。
    """

    q = (
        "SELECT code   AS LocalCode,"
        "       date   AS trade_date,"
        "       adj_close"
        "  FROM prices"
    )
    df = pd.read_sql(q, conn, parse_dates=["trade_date"])
    return df.set_index(["LocalCode", "trade_date"]).sort_index()


def read_signals(
    conn: sqlite3.Connection, start: str | None, end: str | None
) -> pd.DataFrame:
    """シグナルを日付範囲で取得する。

    入力パラメータ: SQLite 接続と開始・終了日の文字列。
    戻り値: DisclosedAt を持つ DataFrame。
    処理内容: fundamental_signals テーブルから期間で絞り込んで読み込む。
    """

    q = "SELECT LocalCode, DisclosedAt FROM fundamental_signals"
    if start or end:
        q += " WHERE 1=1"
        if start:
            q += f" AND DisclosedAt >= '{start} 00:00:00'"
        if end:
            q += f" AND DisclosedAt <= '{end} 23:59:59'"
    df = pd.read_sql(q, conn, parse_dates=["DisclosedAt"])
    return df


# ---------------------------------------------------------------------------
# Trading-days utility
# ---------------------------------------------------------------------------


def add_n_trading_days(s: pd.Series, n: int, calendar: pd.DatetimeIndex) -> pd.Series:
    """営業日ベースで日付をずらす。

    入力パラメータ: 日付シリーズ、加算日数、取引日カレンダー。
    戻り値: カレンダー上で n 日後の日付を並べた Series。
    処理内容: searchsorted を使い範囲外は最終日に丸めて返す。
    """

    idx = calendar.searchsorted(s) + n
    idx[idx >= len(calendar)] = len(calendar) - 1
    return calendar[idx]


# ---------------------------------------------------------------------------
# Backtest core
# ---------------------------------------------------------------------------


def run_backtest(
    prices: pd.DataFrame, signals: pd.DataFrame, *, hold: int, offset: int, capital: int
) -> pd.DataFrame:
    """シグナルに基づくバックテストを実施する。

    入力パラメータ: 価格データ、シグナル、保有日数、エントリーオフセット、資金量。
    戻り値: 各トレードの結果をまとめた DataFrame。
    処理内容: エントリー日とイグジット日を計算し損益などを算出する。
    """

    calendar = prices.index.get_level_values(1).unique().sort_values()

    signals = signals.copy()
    signals["entry_date"] = add_n_trading_days(signals["DisclosedAt"], offset, calendar)
    signals["exit_date"] = add_n_trading_days(signals["entry_date"], hold, calendar)

    # マルチ‑インデックスで価格取得
    entry_idx = signals.set_index(["LocalCode", "entry_date"]).index
    exit_idx = signals.set_index(["LocalCode", "exit_date"]).index

    entry_px = prices.reindex(entry_idx)["adj_close"].values
    exit_px = prices.reindex(exit_idx)["adj_close"].values

    shares = (capital // entry_px).astype(int)
    invest = shares * entry_px
    proceed = shares * exit_px
    profit = proceed - invest

    trades = pd.DataFrame(
        {
            "code": signals["LocalCode"],
            "DisclosedAt": signals["DisclosedAt"].dt.date,
            "entry_date": signals["entry_date"].dt.date,
            "exit_date": signals["exit_date"].dt.date,
            "entry_px": entry_px,
            "exit_px": exit_px,
            "shares": shares,
            "invest": invest,
            "proceed": proceed,
            "profit_jpy": profit,
            "ret_pct": profit / invest,
            "days": hold,
        }
    )
    return trades


def summarize(trades: pd.DataFrame) -> pd.DataFrame:
    """バックテスト結果のサマリーを作成する。

    入力パラメータ: トレード結果の DataFrame。
    戻り値: 指標をまとめた DataFrame。
    処理内容: 総損益や勝率などを計算して表形式にまとめる。
    """

    total_profit = trades["profit_jpy"].sum()
    win_rate = (trades["profit_jpy"] > 0).mean()
    mean_ret_pct = trades["ret_pct"].mean()
    sharpe = trades["ret_pct"].mean() / trades["ret_pct"].std(ddof=0)

    summary = pd.DataFrame(
        {
            "metric": ["trades", "total_profit", "win_rate", "avg_ret_pct", "sharpe"],
            "value": [len(trades), total_profit, win_rate, mean_ret_pct, sharpe],
        }
    )
    return summary


def _ascii_bar_chart(values: list[float], width: int = 40) -> str:
    """Return simple ASCII bar chart for a sequence of values."""
    if not values:
        return ""
    max_v = max(abs(v) for v in values) or 1
    lines = []
    for i, v in enumerate(values, 1):
        bar = "#" * int(abs(v) / max_v * width)
        sign = "" if v >= 0 else "-"
        lines.append(f"{i:>3} {sign}{bar} ({v:.0f})")
    return "\n".join(lines)


def show_results(trades: pd.DataFrame, summary: pd.DataFrame) -> None:
    """Display trades and summary on stdout."""
    print("=== Summary ===")
    print(summary.to_string(index=False))
    if not trades.empty:
        print("\n=== Profit per Trade ===")
        chart = _ascii_bar_chart(trades["profit_jpy"].tolist())
        print(chart)


def show_results_window(trades: pd.DataFrame, summary: pd.DataFrame) -> None:
    """Display results in a new matplotlib window."""
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:  # pylint: disable=broad-except
        print(f"matplotlib is required: {exc}")
        show_results(trades, summary)
        return

    fig, axes = plt.subplots(2, 1, figsize=(8, 6))
    axes[0].axis("off")
    axes[0].table(
        cellText=summary.values,
        colLabels=summary.columns,
        loc="center",
    )
    profits = trades["profit_jpy"].tolist() if not trades.empty else []
    axes[1].bar(
        range(1, len(profits) + 1),
        profits,
        color=["green" if p >= 0 else "red" for p in profits],
    )
    axes[1].set_title("Profit per Trade (JPY)")
    axes[1].set_xlabel("Trade #")
    axes[1].set_ylabel("Profit (JPY)")
    plt.tight_layout()
    plt.show()


# ---------------------------------------------------------------------------
# Excel output
# ---------------------------------------------------------------------------


def to_excel(trades: pd.DataFrame, summary: pd.DataFrame, path: str):
    """バックテスト結果を Excel ファイルに出力する。

    入力パラメータ: トレード表、サマリー表、保存先パス。
    戻り値: なし。
    処理内容: trades と summary をシートに書き込み、グラフを追加する。
    """

    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        trades.to_excel(writer, sheet_name="trades", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)

        workbook = writer.book
        sheet = writer.sheets["trades"]

        # 自動列幅調整
        for i, col in enumerate(trades.columns):
            width = max(10, int(trades[col].astype(str).str.len().max() * 1.1))
            sheet.set_column(i, i, width)

        # Profit bar chart
        chart = workbook.add_chart({"type": "column"})
        n = len(trades)
        chart.add_series(
            {
                "name": "profit_jpy",
                "categories": ["trades", 1, 0, n, 0],  # code 列
                "values": [
                    "trades",
                    1,
                    trades.columns.get_loc("profit_jpy"),
                    n,
                    trades.columns.get_loc("profit_jpy"),
                ],
            }
        )
        chart.set_title({"name": "Profit per Trade (JPY)"})
        chart.set_y_axis({"num_format": "#,##0"})
        sheet.insert_chart("L2", chart)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None):
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--db", default=DB_PATH, help="SQLite DB ファイル")
    p.add_argument("--hold", type=int, default=40, help="保有期間（日数）")
    p.add_argument(
        "--entry-offset", type=int, default=1, help="エントリー日のオフセット"
    )
    p.add_argument(
        "--capital",
        type=int,
        default=DEFAULT_CAPITAL,
        help="1 トレードあたりの資金 (JPY)",
    )
    p.add_argument("--start", type=str, default=None, help="開始日 YYYY-MM-DD")
    p.add_argument("--end", type=str, default=None, help="終了日 YYYY-MM-DD")
    p.add_argument("--xlsx", type=str, default="trades.xlsx", help="Excel 出力ファイル")
    p.add_argument("--json", type=str, help="結果を保存するJSONファイル")
    p.add_argument(
        "--ascii",
        action="store_true",
        help="結果を標準出力にテキスト表示",
    )
    p.add_argument(
        "--no-show",
        action="store_true",
        help="結果表示を抑制",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="詳細ログを表示")
    return p.parse_args(argv)


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format=LOG_FMT,
    )

    with sqlite3.connect(args.db) as conn:
        prices = read_prices(conn)
        signals = read_signals(conn, args.start, args.end)

    logger.info("signals : %d rows", len(signals))
    logger.info("prices  : %d rows", len(prices))

    if signals.empty:
        logger.warning("No signals to back‑test.")
        sys.exit()

    trades = run_backtest(
        prices, signals, hold=args.hold, offset=args.entry_offset, capital=args.capital
    )
    summary = summarize(trades)

    logger.info("Saving Excel → %s", args.xlsx)
    to_excel(trades, summary, args.xlsx)

    if args.json:
        trades.to_json(args.json, orient="records", force_ascii=False)
        logger.info("JSON exported -> %s", args.json)

    logger.info("\n%s", summary.to_string(index=False))
    if not args.no_show:
        if args.ascii:
            show_results(trades, summary)
        else:
            show_results_window(trades, summary)


if __name__ == "__main__":
    # • 引数を解析して価格とシグナルを取得
    # • バックテストを実施し Excel に保存
    main()

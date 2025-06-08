#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""screen_statements.py – boolean‑fix & robust screening 2025‑06‑07
====================================================================
*   正規化したブール列（"true"/"false"/"1"/"0"/空/NaN → bool）で
    ノイズ除外が機能するよう修正。
*   Stage counts を DEBUG 出力して詰まり箇所を可視化。
*   pandas FutureWarning（pct_change デフォルト変更）を回避。
*   デフォルト `lookback_days` を 3 年に拡大（FY YoY 計算向け）。

Usage:
    python screen_statements.py --lookback 3000 --recent 1500 -v
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Final

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    db_path: Path = Path(__file__).resolve().parents[1] / "db/stock.db"
    lookback_days: int = 365 * 3      # 3 年分ロード
    recent_days: int = 7              # 開示から何日以内を対象にするか
    window_q: int = 4                 # 四半期 MA

# ブール列名（statements テーブル側では TEXT 型）
BOOL_COLS: Final = [
    "MaterialChangesInSubsidiaries",
    "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
    "ChangesInAccountingEstimates",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cast_bool(series: pd.Series) -> pd.Series:
    """"true"/"false"/"1"/"0"/NaN/空文字 → bool へ正規化"""
    return (
        series.astype(str)
        .str.lower()
        .map({"true": True, "1": True, "false": False, "0": False, "nan": False, "": False})
        .fillna(False)
        .astype(bool)
    )

# ---------------------------------------------------------------------------
# Data Access
# ---------------------------------------------------------------------------

def fetch_statements(conn: sqlite3.Connection, cfg: Config) -> pd.DataFrame:
    """Load recent statements rows from DB and return as DataFrame."""
    sql = f"""
        SELECT LocalCode, DisclosedDate, DisclosedTime, TypeOfCurrentPeriod,
               NetSales, OperatingProfit, Profit, EarningsPerShare,
               ForecastEarningsPerShare,
               CashFlowsFromOperatingActivities, EquityToAssetRatio,
               NumberOfTreasuryStockAtTheEndOfFiscalYear,
               MaterialChangesInSubsidiaries,
               ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard,
               ChangesInAccountingEstimates
          FROM statements
         WHERE date(DisclosedDate) >= date('now','-{cfg.lookback_days} day');
    """
    df = pd.read_sql(sql, conn)

    # Cast numerics
    non_numeric_cols: Final = [
        "LocalCode",
        "DisclosedDate",
        "DisclosedTime",
        "TypeOfCurrentPeriod",
        *BOOL_COLS,
    ]
    num_cols = df.columns.difference(non_numeric_cols)
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    # Normalise boolean text columns → bool
    for col in BOOL_COLS:
        df[col] = _cast_bool(df[col])

    # Combine date & time
    df["DisclosedAt"] = pd.to_datetime(
        df["DisclosedDate"].fillna("1970-01-01") + " " + df["DisclosedTime"].fillna("00:00:00")
    )

    df.sort_values(["LocalCode", "DisclosedAt"], inplace=True)
    return df

# ---------------------------------------------------------------------------
# Feature Engineering
# ---------------------------------------------------------------------------

def compute_features(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Add QoQ / YoY / quality metrics per LocalCode."""
    quarter_map = {"1Q": 1, "2Q": 2, "3Q": 3, "4Q": 4}

    def _add(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()

        # Basic growth
        g["sales_qoq"] = g["NetSales"].pct_change(fill_method=None)
        g["op_qoq"] = g["OperatingProfit"].pct_change(fill_method=None)

        # Margin trends
        g["op_margin"] = g["OperatingProfit"] / g["NetSales"]
        g["op_margin_ma4"] = g["op_margin"].rolling(cfg.window_q).mean()
        g["op_margin_delta"] = g["op_margin"] - g["op_margin_ma4"]

        # Leverage (operating)
        g["leverage"] = g["op_qoq"] / g["sales_qoq"]

        # Forecast EPS revision
        g["feps_revision"] = g["ForecastEarningsPerShare"].pct_change(fill_method=None)

        # Turnaround flag
        g["turnaround"] = (g["Profit"].shift(1) < 0) & (g["Profit"] > 0)

        # Cash‑flow quality & equity ratio delta
        g["cf_quality"] = g["CashFlowsFromOperatingActivities"] / g["OperatingProfit"]
        g["eta_delta"] = g["EquityToAssetRatio"].diff()

        # Treasury stock delta
        g["treasury_delta"] = g["NumberOfTreasuryStockAtTheEndOfFiscalYear"].diff()

        # FY YoY
        fy_mask = g["TypeOfCurrentPeriod"] == "FY"
        g.loc[fy_mask, "eps_yoy_fy"] = g.loc[fy_mask, "EarningsPerShare"].pct_change(fill_method=None)

        # Quarter YoY
        g["q_num"] = g["TypeOfCurrentPeriod"].map(quarter_map)
        g["eps_yoy_q"] = g.groupby("q_num")["EarningsPerShare"].pct_change(fill_method=None)
        g.drop(columns="q_num", inplace=True)
        return g

    return df.groupby("LocalCode", group_keys=False).apply(_add)

# ---------------------------------------------------------------------------
# Screening
# ---------------------------------------------------------------------------

def screen_signals(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Apply sequential filters and log stage counts."""
    recent_cut = pd.Timestamp(date.today() - timedelta(days=cfg.recent_days))

    stage = {}
    m = df["DisclosedAt"] >= recent_cut
    stage["recent"] = m.sum()

    eps_yoy = df["eps_yoy_fy"].fillna(df["eps_yoy_q"]).fillna(0)
    m &= eps_yoy > 0.30
    stage["eps"] = m.sum()

    m &= df["cf_quality"].fillna(0) > 0.8
    stage["cf"] = m.sum()

    m &= df["eta_delta"].fillna(0) > 0
    stage["eta"] = m.sum()

    m &= df["treasury_delta"].fillna(0) <= 0
    stage["treasury"] = m.sum()

    for col in BOOL_COLS:
        m &= ~df[col]
    stage["noise"] = m.sum()

    logging.debug("Stage counts: %s", stage)
    return df.loc[m].copy()

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_signals(sig_df: pd.DataFrame, conn: sqlite3.Connection) -> int:
    if sig_df.empty:
        return 0

    sig = sig_df[
        [
            "LocalCode",
            "DisclosedAt",
            "TypeOfCurrentPeriod",
            "eps_yoy_fy",
            "eps_yoy_q",
            "op_margin_delta",
            "feps_revision",
            "cf_quality",
            "eta_delta",
            "leverage",
            "turnaround",
            "treasury_delta",
        ]
    ].copy()

    sig["DisclosedAt"] = sig["DisclosedAt"].dt.strftime("%Y-%m-%d %H:%M:%S")
    sig["turnaround"] = sig["turnaround"].astype(int)
    sig["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cols = list(sig.columns)
    sql = f"INSERT OR IGNORE INTO fundamental_signals ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
    conn.executemany(sql, sig.to_records(index=False))
    conn.commit()
    return len(sig)

# ---------------------------------------------------------------------------
# CLI / Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Screen statements for fundamental signals.")
    p.add_argument("--db", type=Path, default=Config.db_path, help="Path to SQLite DB file")
    p.add_argument("--lookback", type=int, default=Config.lookback_days, help="Lookback window (days)")
    p.add_argument("--recent", type=int, default=Config.recent_days, help="Recent disclosure window (days)")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="[%(levelname)s] %(message)s",
    )

    cfg = Config(db_path=args.db, lookback_days=args.lookback, recent_days=args.recent)
    logging.info("DB          : %s", cfg.db_path)
    logging.info("Lookback    : %s days", cfg.lookback_days)
    logging.info("Recent win  : %s days", cfg.recent_days)

    with sqlite3.connect(cfg.db_path) as conn:
        df = fetch_statements(conn, cfg)
        logging.debug("Fetched %s rows from statements", len(df))

        df_feat = compute_features(df, cfg)
        logging.debug("Features computed: %s rows", len(df_feat))

        sig = screen_signals(df_feat, cfg)
        logging.info("%s rows passed screening", len(sig))

        inserted = save_signals(sig, conn)
        logging.info("%s signal(s) inserted into fundamental_signals", inserted)


if __name__ == "__main__":
    # • 引数を解析しログを設定
    # • 決算データ取得 → 特徴量計算 → スクリーニング
    # • シグナルを DB へ保存
    main()

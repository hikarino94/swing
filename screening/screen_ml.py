#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
screen_ml.py — Machine-learning swing-trade screener
---------------------------------------------------
* 学習: 財務指標 + 株価特徴量 → 30 営業日後に +5% 以上上昇する確率を推定
* 予測: 直近データで上位銘柄を抽出し CLI / GUI から利用

Usage::

    # モデル学習（直近 3 年）
    python screen_ml.py train  --db ./db/stock.db --lookback 1095

    # 予測して上位 30 銘柄を出力（必要なら --retrain）
    python screen_ml.py screen --db ./db/stock.db --top 30
"""
from __future__ import annotations

import argparse
import logging
import pickle
import sqlite3
from pathlib import Path

import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# -----------------------------------------------------------------------------
# pandas future-proof settings & logger
# -----------------------------------------------------------------------------
pd.set_option("future.no_silent_downcasting", True)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
PRICE_TABLE = "prices"
STMT_TABLE = "statements"
LOOKBACK_DAYS = 1095  # デフォルト過去 3 年
FUTURE_WINDOW = 30  # 30 営業日後を予測
THRESH_PCT = 0.05  # +5% 以上なら陽線ラベル
MODEL_FNAME = "ml_screen_model.pkl"

NUMERIC_STMT_COLS = [
    "NetSales",
    "OperatingProfit",
    "OrdinaryProfit",
    "Profit",
    "TotalAssets",
    "Equity",
    "EquityToAssetRatio",
    "BookValuePerShare",
    "CashFlowsFromOperatingActivities",
    "CashFlowsFromInvestingActivities",
    "CashFlowsFromFinancingActivities",
]

PRICE_FEATURES = [
    "ret_5",
    "ret_10",
    "ret_20",
    "volatility_20",
    "turnover_norm",
]

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------


def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path.as_posix())
    con.row_factory = sqlite3.Row
    return con


def _fetch_price(con: sqlite3.Connection, lookback: int) -> pd.DataFrame:
    query = f"""
        SELECT code, date, adj_close, adj_volume
        FROM {PRICE_TABLE}
        WHERE date >= date('now', '-{lookback} day')
    """
    return pd.read_sql(query, con, parse_dates=["date"])


def _fetch_stmt(con: sqlite3.Connection) -> pd.DataFrame:
    cols = ", ".join(["LocalCode"] + NUMERIC_STMT_COLS + ["DisclosedDate"])
    query = f"SELECT {cols} FROM {STMT_TABLE} WHERE DisclosedDate IS NOT NULL"
    df = pd.read_sql(query, con, parse_dates=["DisclosedDate"])
    return df.rename(columns={"LocalCode": "code"})


# -----------------------------------------------------------------------------
# Feature engineering
# -----------------------------------------------------------------------------


def _make_price_features(df_price: pd.DataFrame) -> pd.DataFrame:
    """Add momentum / volatility / turnover features."""
    df_price = df_price.sort_values(["code", "date"]).copy()
    frames = []
    for code, g in df_price.groupby("code"):
        g = g.set_index("date").copy()
        # pct_change with fill_method=None (pandas ≥2.2 推奨)
        g["ret_5"] = g["adj_close"].pct_change(5, fill_method=None)
        g["ret_10"] = g["adj_close"].pct_change(10, fill_method=None)
        g["ret_20"] = g["adj_close"].pct_change(20, fill_method=None)
        g["volatility_20"] = (
            g["adj_close"].pct_change(fill_method=None).rolling(20).std()
        )
        g["turnover_norm"] = (
            g["adj_volume"] / g["adj_volume"].rolling(20).mean()
        ).fillna(0)
        g["code"] = code
        frames.append(g.reset_index())
    return pd.concat(frames, ignore_index=True)


def _merge_features(price_feat: pd.DataFrame, stmt: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill latest statement per code & asof-merge to price."""
    # ----- forward fill statements per code -----
    stmt_filled = (
        stmt.sort_values(["code", "DisclosedDate"])
        .groupby("code", group_keys=False)
        .apply(lambda g: g.ffill())  # 日次で穴埋め
        .reset_index(drop=True)
    )

    # ----- asof merge (nearest past disclosure) -----
    merged = pd.merge_asof(
        price_feat.sort_values("date"),
        stmt_filled.sort_values("DisclosedDate"),
        left_on="date",
        right_on="DisclosedDate",
        by="code",
        direction="backward",
    )

    # ----- convert object cols → numeric, 欠損を 0 で補完 -----
    obj_cols = merged.select_dtypes(include="object").columns.difference(["code"])
    merged[obj_cols] = merged[obj_cols].apply(
        lambda c: pd.to_numeric(c, errors="coerce")
    )
    merged = merged.fillna(0)
    return merged


def _add_label(df: pd.DataFrame) -> pd.DataFrame:
    """Add binary label whether price rises ≥THRESH_PCT within FUTURE_WINDOW."""
    dfs = []
    for code, g in df.groupby("code"):
        g = g.sort_values("date").copy()
        g["future_close"] = g["adj_close"].shift(-FUTURE_WINDOW)
        g["future_ret"] = (g["future_close"] - g["adj_close"]) / g["adj_close"]
        g["label"] = (g["future_ret"] >= THRESH_PCT).astype(int)
        dfs.append(g)
    return pd.concat(dfs, ignore_index=True)


def _build_dataset(con: sqlite3.Connection, lookback: int) -> pd.DataFrame:
    price = _fetch_price(con, lookback)
    price_feat = _make_price_features(price)
    stmt = _fetch_stmt(con)
    merged = _merge_features(price_feat, stmt)
    merged = _add_label(merged)
    # 学習用データとして必要カラムが欠損していない行のみ残す
    req_cols = PRICE_FEATURES + NUMERIC_STMT_COLS + ["label"]
    return merged.dropna(subset=req_cols)


# -----------------------------------------------------------------------------
# Model training / inference
# -----------------------------------------------------------------------------


def _train_model(df: pd.DataFrame):
    X = df[PRICE_FEATURES + NUMERIC_STMT_COLS].astype(float)
    y = df["label"].astype(int)
    pipe = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("gb", GradientBoostingClassifier()),
        ]
    )
    pipe.fit(X, y)
    auc = roc_auc_score(y, pipe.predict_proba(X)[:, 1])
    logger.info("Training done — in-sample AUC: %.3f", auc)
    return pipe


# -----------------------------------------------------------------------------
# CLI entry
# -----------------------------------------------------------------------------


def cli():
    p = argparse.ArgumentParser(description="ML-based swing-trade screener")
    p.add_argument("cmd", choices=["train", "screen"], help="Command")
    p.add_argument("--db", default="./db/stock.db", help="SQLite DB path")
    p.add_argument(
        "--lookback", type=int, default=LOOKBACK_DAYS, help="History days for training"
    )
    p.add_argument("--top", type=int, default=30, help="Rows to output when screening")
    p.add_argument("--retrain", action="store_true", help="Force retrain before screen")
    args = p.parse_args()

    db_path = Path(args.db)
    model_path = db_path.parent / MODEL_FNAME

    con = _connect(db_path)

    # ───────────────────────── TRAIN ──────────────────────────
    if args.cmd == "train":
        df = _build_dataset(con, args.lookback)
        model = _train_model(df)
        with open(model_path, "wb") as fh:
            pickle.dump(model, fh)
        logger.info("Model saved to %s", model_path)
        return

    # ───────────────────────── SCREEN ─────────────────────────
    if args.retrain or not model_path.exists():
        logger.info("Retraining because --retrain or model not found…")
        df = _build_dataset(con, args.lookback)
        model = _train_model(df)
        with open(model_path, "wb") as fh:
            pickle.dump(model, fh)
    else:
        with open(model_path, "rb") as fh:
            model = pickle.load(fh)
        logger.info("Loaded model from %s", model_path)

    # 最新日の特徴量だけ抽出
    price = _fetch_price(con, args.lookback)
    latest_dt = price["date"].max()
    price_window = price[price["date"] >= latest_dt]
    price_feat = _make_price_features(price_window)
    stmt = _fetch_stmt(con)
    merged = _merge_features(price_feat, stmt)

    feat_df = merged.dropna(subset=PRICE_FEATURES + NUMERIC_STMT_COLS)
    if feat_df.empty:
        logger.warning("No feature rows for latest date — aborting")
        return

    X_pred = feat_df[PRICE_FEATURES + NUMERIC_STMT_COLS].astype(float)
    feat_df["prob_up30d"] = model.predict_proba(X_pred)[:, 1]

    logger.info("Predictions for %s — top %d", latest_dt.date(), args.top)
    out = (
        feat_df.sort_values("prob_up30d", ascending=False)
        .head(args.top)
        .loc[:, ["code", "prob_up30d"]]
    )
    print(out.to_string(index=False, float_format=lambda x: f"{x:.3f}"))


if __name__ == "__main__":
    cli()

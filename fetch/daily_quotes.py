#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fully‑paged downloader for **J‑Quants `/prices/daily_quotes`** that respects the
rate‑limit & pagination notes in the official "Attention" page
(https://jpx.gitbook.io/j‑quants‑ja/api‑reference/attention).

Highlights
==========
* **Pagination** – request param must be **`pagination_key`** (2024‑02 update).
  Older alias `page_key` in responses is still accepted.
* **Rate limit** – API allows **≤3 requests / sec**; we add `time.sleep(0.35)`
  between calls to stay well under.
* **Robust break** – stop if received 0 rows even when a key is returned
  (avoid empty‑page loop noted in docs).
* **Retry** – simple back‑off for 429 / 5xx.

CLI
---
```
python daily_quotes.py                # today only
python daily_quotes.py --start 2024-01-01 --end 2024-03-31
```
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional
from datetime import date, datetime, timedelta
import pandas as pd

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.config import get_config_manager
from utils.db_utils import get_db_manager, DatabaseManager
from utils.jquants_client import get_jquants_client, JQuantsClient
from utils.logging_config import get_logger
from utils.cli_utils import create_parser, add_date_arguments, setup_logging_from_args, validate_date_range
from utils.exceptions import APIError, DatabaseError

logger = get_logger(__name__)

# SQLite prices テーブルのカラム順序を定義
_PRICE_COLS = [
    "code",
    "date",
    "open",
    "high",
    "low",
    "close",
    "upper_limit",
    "lower_limit",
    "volume",
    "turnover_value",
    "adj_factor",
    "adj_open",
    "adj_high",
    "adj_low",
    "adj_close",
    "adj_volume",
]


def _daterange(start_date: date, end_date: date) -> List[date]:
    """Return all weekdays between start_date and end_date (inclusive).
    
    Args:
        start_date: 開始日
        end_date: 終了日
        
    Returns:
        営業日のリスト
    """
    current, dates = start_date, []
    while current <= end_date:
        if current.weekday() < 5:  # 土日を除外
            dates.append(current)
        current += timedelta(days=1)
    return dates


def _fetch_all_pages(client: JQuantsClient, target_date: Optional[date] = None, code: Optional[str] = None) -> pd.DataFrame:
    """ページネーションを考慮して全データを取得
    
    Args:
        client: JQuantsClientインスタンス
        target_date: 対象日付
        code: 銘柄コード
        
    Returns:
        取得したデータのDataFrame
    """
    all_data = []
    
    if target_date:
        # 指定日のデータを取得
        data = client.get_daily_quotes(date_from=target_date, date_to=target_date)
        all_data.extend(data)
    elif code:
        # 指定銘柄の全データを取得
        data = client.get_daily_quotes(code=code)
        all_data.extend(data)
    else:
        raise ValueError("target_date または code を指定してください")
    
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """APIレスポンスのDataFrameを正規化
    
    Args:
        df: APIから取得したDataFrame
        
    Returns:
        正規化されたDataFrame
    """
    if df.empty:
        return df
    
    # カラム名の変換マップ
    rename_map = {
        "Code": "code",
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "UpperLimit": "upper_limit",
        "LowerLimit": "lower_limit",
        "Volume": "volume",
        "TurnoverValue": "turnover_value",
        "AdjustmentFactor": "adj_factor",
        "AdjustmentOpen": "adj_open",
        "AdjustmentHigh": "adj_high",
        "AdjustmentLow": "adj_low",
        "AdjustmentClose": "adj_close",
        "AdjustmentVolume": "adj_volume",
    }
    
    # カラム名を変換
    df = df.rename(columns=rename_map)
    
    # 数値型に変換するカラム
    numeric_columns = [
        "open", "high", "low", "close",
        "upper_limit", "lower_limit",
        "volume", "turnover_value", "adj_factor",
        "adj_open", "adj_high", "adj_low", "adj_close", "adj_volume"
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # 日付をYYYY-MM-DD形式に統一
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    # 必要なカラムのみを選択
    return df[[col for col in _PRICE_COLS if col in df.columns]]


def _save_to_database(db_manager: DatabaseManager, df: pd.DataFrame) -> int:
    """DataFrameをデータベースに保存
    
    Args:
        db_manager: DatabaseManagerインスタンス
        df: 保存するDataFrame
        
    Returns:
        保存した行数
    """
    if df.empty:
        return 0
    
    # データベースに存在するカラムのみを選択
    cols = [col for col in _PRICE_COLS if col in df.columns]
    
    # INSERT OR REPLACE文を生成
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT OR REPLACE INTO prices ({', '.join(cols)}) VALUES ({placeholders})"
    
    # データをタプルのリストに変換
    records = df[cols].values.tolist()
    
    # バッチ処理で保存
    rows_affected = db_manager.execute_many(sql, records)
    logger.info(f"{rows_affected} 行のデータを保存しました")
    
    return rows_affected


class DailyQuotesFetcher:
    """日次株価データ取得クラス"""
    
    def __init__(self, client: Optional[JQuantsClient] = None, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            client: JQuantsClientインスタンス
            db_manager: DatabaseManagerインスタンス
        """
        self.client = client or get_jquants_client()
        self.db_manager = db_manager or get_db_manager()
    
    def fetch_by_date_range(self, start_date: date, end_date: date) -> None:
        """指定期間のデータを取得して保存
        
        Args:
            start_date: 開始日
            end_date: 終了日
        """
        logger.info(f"{start_date} から {end_date} までのデータを取得します")
        
        for target_date in _daterange(start_date, end_date):
            try:
                # データ取得
                df = _fetch_all_pages(self.client, target_date=target_date)
                
                if df.empty:
                    logger.info(f"{target_date}: データなし（休場）")
                    continue
                
                logger.info(f"{target_date}: {len(df)} 件のデータを取得")
                
                # 正規化して保存
                normalized_df = _normalize_dataframe(df)
                _save_to_database(self.db_manager, normalized_df)
                
            except APIError as e:
                logger.error(f"{target_date} のデータ取得中にエラー: {e}")
                continue
    
    def fetch_today_and_splits(self) -> None:
        """本日のデータを取得し、株式分割があれば全履歴を再取得"""
        today = date.today()
        logger.info(f"本日 {today} のデータを取得します")
        
        try:
            # 本日のデータを取得
            df_today = _fetch_all_pages(self.client, target_date=today)
            
            if df_today.empty:
                logger.info("本日のデータはありません（休場）")
                return
            
            # 正規化して保存
            normalized_df = _normalize_dataframe(df_today)
            _save_to_database(self.db_manager, normalized_df)
            
            # 株式分割のチェック
            if "adj_factor" in normalized_df.columns:
                split_codes = normalized_df.loc[
                    normalized_df["adj_factor"].fillna(1.0) != 1.0,
                    "code"
                ].unique()
                
                for code in split_codes:
                    logger.info(f"株式分割検出: {code} → 全履歴を再取得")
                    df_all = _fetch_all_pages(self.client, code=code)
                    if not df_all.empty:
                        normalized_all = _normalize_dataframe(df_all)
                        _save_to_database(self.db_manager, normalized_all)
            
        except APIError as e:
            logger.error(f"データ取得中にエラー: {e}")
            raise


def main() -> None:
    """メイン処理"""
    # パーサーの作成
    parser = create_parser("J-Quants の日次株価データを取得してデータベースに保存")
    add_date_arguments(parser)
    
    args = parser.parse_args()
    
    # ロギングの設定
    setup_logging_from_args(args)
    
    try:
        # フェッチャーのインスタンス作成
        if args.db:
            db_manager = DatabaseManager(args.db)
        else:
            db_manager = get_db_manager()
        
        fetcher = DailyQuotesFetcher(db_manager=db_manager)
        
        # 日付範囲の処理
        if args.start or args.end:
            start_date, end_date = validate_date_range(args.start, args.end)
            fetcher.fetch_by_date_range(start_date, end_date)
        else:
            # 引数なしの場合は本日のデータと株式分割の処理
            fetcher.fetch_today_and_splits()
        
        logger.info("処理が完了しました")
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        raise


if __name__ == "__main__":
    main()
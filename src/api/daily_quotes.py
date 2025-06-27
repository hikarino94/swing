#!/usr/bin/env python
"""
高速化版 J-Quants 日次株価データ取得モジュール

改善点:
1. 並列処理による同時データ取得（ThreadPoolExecutor使用）
2. バッチ処理によるDB保存の最適化
3. pandas vectorized操作による数値変換の高速化
4. メモリ効率的なデータ処理

CLI:
```
python daily_quotes.py                # 本日のみ
python daily_quotes.py --start 2024-01-01 --end 2024-03-31
python daily_quotes.py --workers 5    # 並列度を指定
```
"""

from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.utils.cli_utils import add_date_arguments, create_parser, setup_logging_from_args, validate_date_range
from src.utils.db_utils import DatabaseManager, get_db_manager
from src.utils.exceptions import APIError
from src.utils.jquants_client import JQuantsClient, get_jquants_client
from src.utils.logging_config import get_logger

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


def _daterange(start_date: date, end_date: date) -> list[date]:
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


def _fetch_all_pages(
    client: JQuantsClient, target_date: Optional[date] = None, code: Optional[str] = None
) -> pd.DataFrame:
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
        data = client.get_daily_quotes(target_date=target_date)
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


def _fetch_single_date(client: JQuantsClient, target_date: date) -> dict[str, Any]:
    """単一日付のデータを取得（並列処理用）"""
    try:
        data = client.get_daily_quotes(target_date=target_date)
        return {"date": target_date, "data": data, "error": None}
    except APIError as e:
        logger.error(f"{target_date} のデータ取得中にエラー: {e}")
        return {"date": target_date, "data": [], "error": str(e)}


def _save_batch_to_database(db_manager: DatabaseManager, dfs: list[pd.DataFrame]) -> int:
    """複数のDataFrameをバッチでデータベースに保存"""
    if not dfs:
        return 0

    # 全DataFrameを結合
    combined_df = pd.concat(dfs, ignore_index=True)

    if combined_df.empty:
        return 0

    # データベースに存在するカラムのみを選択
    cols = [col for col in _PRICE_COLS if col in combined_df.columns]

    # INSERT OR REPLACE文を生成
    placeholders = ", ".join("?" for _ in cols)
    sql = f"INSERT OR REPLACE INTO prices ({', '.join(cols)}) VALUES ({placeholders})"

    # データをタプルのリストに変換
    records = combined_df[cols].values.tolist()

    # バッチ処理で保存（より大きなバッチサイズ）
    rows_affected = db_manager.execute_many(sql, records, batch_size=5000)
    logger.info(f"バッチ処理で {rows_affected} 行のデータを保存しました")

    return rows_affected


class DailyQuotesFetcher:
    """高速化版の日次株価データ取得クラス"""

    def __init__(
        self,
        client: Optional[JQuantsClient] = None,
        db_manager: Optional[DatabaseManager] = None,
        max_workers: int = 3,  # API rate limit (3 req/sec) を考慮
    ):
        """
        Args:
            client: JQuantsClientインスタンス
            db_manager: DatabaseManagerインスタンス
            max_workers: 並列処理のワーカー数（デフォルト3）
        """
        self.client = client or get_jquants_client()
        self.db_manager = db_manager or get_db_manager()
        self.max_workers = max_workers

    def fetch_by_date_range(self, start_date: date, end_date: date) -> None:
        """指定期間のデータを並列処理で取得して保存"""
        logger.info(f"{start_date} から {end_date} までのデータを並列処理で取得します")

        dates = _daterange(start_date, end_date)
        total_dates = len(dates)

        if total_dates == 0:
            logger.info("処理対象の営業日がありません")
            return

        # バッチサイズの計算（メモリ効率を考慮）
        batch_size = min(10, total_dates)  # 最大10日分ずつ処理
        processed_count = 0
        total_rows = 0

        # 日付をバッチに分割
        for i in range(0, total_dates, batch_size):
            batch_dates = dates[i : i + batch_size]
            batch_dfs = []

            # 並列処理でデータ取得
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # タスクを投入
                future_to_date = {executor.submit(_fetch_single_date, self.client, date): date for date in batch_dates}

                # 結果を収集
                for future in as_completed(future_to_date):
                    result = future.result()
                    target_date = result["date"]

                    if result["error"]:
                        logger.error(f"{target_date}: エラー - {result['error']}")
                        continue

                    if not result["data"]:
                        logger.info(f"{target_date}: データなし（休場）")
                        continue

                    # データを正規化
                    df = pd.DataFrame(result["data"])
                    normalized_df = _normalize_dataframe(df)

                    if not normalized_df.empty:
                        batch_dfs.append(normalized_df)
                        logger.info(f"{target_date}: {len(normalized_df)} 件のデータを取得")

                    processed_count += 1

                    # 進捗表示
                    if processed_count % 10 == 0:
                        logger.info(f"進捗: {processed_count}/{total_dates} 日処理完了")

            # バッチをデータベースに保存
            if batch_dfs:
                rows = _save_batch_to_database(self.db_manager, batch_dfs)
                total_rows += rows

            # API rate limitを考慮した待機（バッチ間）
            if i + batch_size < total_dates:
                time.sleep(0.5)

        logger.info(f"処理完了: 合計 {total_rows} 行のデータを保存しました")

    def fetch_today_and_splits(self) -> None:
        """本日のデータを取得し、株式分割があれば全履歴を並列で再取得"""
        today = date.today()
        logger.info(f"本日 {today} のデータを取得します")

        try:
            # 本日のデータを取得
            result = _fetch_single_date(self.client, today)

            if result["error"]:
                logger.error(f"データ取得エラー: {result['error']}")
                return

            if not result["data"]:
                logger.info("本日のデータはありません（休場）")
                return

            # 正規化して保存
            df_today = pd.DataFrame(result["data"])
            normalized_df = _normalize_dataframe(df_today)
            _save_batch_to_database(self.db_manager, [normalized_df])

            # 株式分割のチェック（vectorized operation）
            if "adj_factor" in normalized_df.columns:
                # adj_factorが1.0でない銘柄を効率的に抽出
                mask = normalized_df["adj_factor"].fillna(1.0) != 1.0
                split_codes = normalized_df.loc[mask, "code"].unique()

                if len(split_codes) > 0:
                    logger.info(f"{len(split_codes)} 件の株式分割を検出")

                    # 並列処理で全履歴を取得
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = {
                            executor.submit(self.client.get_daily_quotes, code=code): code for code in split_codes
                        }

                        split_dfs = []
                        for future in as_completed(futures):
                            code = futures[future]
                            try:
                                data = future.result()
                                if data:
                                    df = pd.DataFrame(data)
                                    normalized = _normalize_dataframe(df)
                                    split_dfs.append(normalized)
                                    logger.info(f"株式分割銘柄 {code}: {len(normalized)} 件の履歴を取得")
                            except Exception as e:
                                logger.error(f"銘柄 {code} の取得エラー: {e}")

                        # バッチ保存
                        if split_dfs:
                            _save_batch_to_database(self.db_manager, split_dfs)

        except APIError as e:
            logger.error(f"データ取得中にエラー: {e}")
            raise


def main() -> None:
    """メイン処理"""
    # パーサーの作成
    parser = create_parser("高速版 J-Quants 日次株価データ取得")
    add_date_arguments(parser)
    parser.add_argument("--workers", type=int, default=3, help="並列処理のワーカー数（デフォルト: 3）")

    args = parser.parse_args()

    # ロギングの設定
    setup_logging_from_args(args)

    try:
        # フェッチャーのインスタンス作成
        if args.db:
            db_manager = DatabaseManager(args.db)
        else:
            db_manager = get_db_manager()

        fetcher = DailyQuotesFetcher(db_manager=db_manager, max_workers=args.workers)

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

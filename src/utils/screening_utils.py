"""スクリーニング共通ユーティリティ"""
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .db_utils import DatabaseManager, get_db_manager
from .exceptions import DatabaseError, DataError

logger = logging.getLogger(__name__)


def save_screening_results(
    results_df: pd.DataFrame,
    table_name: str,
    db_manager: Optional[DatabaseManager] = None,
    replace_existing: bool = True,
) -> int:
    """スクリーニング結果を指定されたテーブルに保存

    Args:
        results_df: 保存する結果のDataFrame
        table_name: 保存先テーブル名
        db_manager: DatabaseManagerインスタンス
        replace_existing: 既存データを置き換えるか

    Returns:
        保存した件数

    Raises:
        DatabaseError: データベースエラー
    """
    if results_df.empty:
        logger.info("保存するデータがありません")
        return 0

    if db_manager is None:
        db_manager = get_db_manager()

    try:
        # created_atカラムを追加
        if "created_at" not in results_df.columns:
            results_df = results_df.copy()
            results_df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with db_manager.transaction() as conn:
            # 既存データの削除（必要に応じて）
            if replace_existing:
                # 今日のデータを削除
                today = date.today().strftime("%Y-%m-%d")
                delete_sql = f"DELETE FROM {table_name} WHERE DATE(created_at) = ?"
                conn.execute(delete_sql, (today,))
                logger.debug(f"既存データを削除しました: {table_name}")

            # データ挿入
            cols = list(results_df.columns)
            placeholders = ", ".join("?" for _ in cols)

            insert_type = "INSERT OR REPLACE" if replace_existing else "INSERT OR IGNORE"
            sql = f"{insert_type} INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"

            records = results_df.to_records(index=False).tolist()
            conn.executemany(sql, records)

            inserted_count = len(results_df)
            logger.info(f"{table_name}に{inserted_count}件のデータを保存しました")
            return inserted_count

    except Exception as e:
        logger.error(f"スクリーニング結果の保存中にエラー: {e}")
        raise DatabaseError(f"スクリーニング結果の保存に失敗しました: {e}")


def get_latest_prices(
    codes: Optional[List[str]] = None,
    as_of_date: Optional[date] = None,
    lookback_days: int = 30,
    db_manager: Optional[DatabaseManager] = None,
) -> pd.DataFrame:
    """最新の株価データを取得

    Args:
        codes: 取得する銘柄コードのリスト（Noneの場合は全銘柄）
        as_of_date: 基準日（Noneの場合は今日）
        lookback_days: 何日前までのデータを取得するか
        db_manager: DatabaseManagerインスタンス

    Returns:
        株価データのDataFrame

    Raises:
        DatabaseError: データベースエラー
    """
    if db_manager is None:
        db_manager = get_db_manager()

    if as_of_date is None:
        as_of_date = date.today()

    start_date = (as_of_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date = as_of_date.strftime("%Y-%m-%d")

    sql = """
        SELECT code, date, open, high, low, close, volume,
               adj_open, adj_high, adj_low, adj_close, adj_volume
        FROM prices
        WHERE date BETWEEN ? AND ?
    """
    params = [start_date, end_date]

    if codes:
        placeholders = ", ".join("?" for _ in codes)
        sql += f" AND code IN ({placeholders})"
        params.extend(codes)

    sql += " ORDER BY code, date"

    try:
        with db_manager.get_connection() as conn:
            df = pd.read_sql(sql, conn, params=params)

            if not df.empty:
                # 日付型に変換
                df["date"] = pd.to_datetime(df["date"])
                # 数値型に変換
                numeric_cols = [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "adj_open",
                    "adj_high",
                    "adj_low",
                    "adj_close",
                    "adj_volume",
                ]
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

            logger.debug(f"株価データ取得: {len(df)} レコード")
            return df

    except Exception as e:
        logger.error(f"株価データ取得中にエラー: {e}")
        raise DatabaseError(f"株価データの取得に失敗しました: {e}")


def calculate_technical_indicators(df: pd.DataFrame, window_short: int = 5, window_long: int = 25) -> pd.DataFrame:
    """テクニカル指標を計算

    Args:
        df: 株価データのDataFrame（code, date, close等を含む）
        window_short: 短期移動平均の期間
        window_long: 長期移動平均の期間

    Returns:
        テクニカル指標が追加されたDataFrame
    """
    if df.empty:
        return df

    def _calc_indicators(group: pd.DataFrame) -> pd.DataFrame:
        """各銘柄ごとにテクニカル指標を計算"""
        g = group.copy().sort_values("date")

        # 移動平均
        g[f"ma_{window_short}"] = g["close"].rolling(window_short).mean()
        g[f"ma_{window_long}"] = g["close"].rolling(window_long).mean()

        # RSI
        delta = g["close"].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / avg_loss
        g["rsi"] = 100 - (100 / (1 + rs))

        # ボリンジャーバンド
        g["bb_middle"] = g["close"].rolling(20).mean()
        bb_std = g["close"].rolling(20).std()
        g["bb_upper"] = g["bb_middle"] + (bb_std * 2)
        g["bb_lower"] = g["bb_middle"] - (bb_std * 2)
        g["bb_position"] = (g["close"] - g["bb_lower"]) / (g["bb_upper"] - g["bb_lower"])

        # 出来高移動平均
        g["volume_ma"] = g["volume"].rolling(20).mean()
        g["volume_ratio"] = g["volume"] / g["volume_ma"]

        # 価格変動率
        g["price_change_1d"] = g["close"].pct_change(1)
        g["price_change_5d"] = g["close"].pct_change(5)
        g["price_change_20d"] = g["close"].pct_change(20)

        return g

    try:
        result = df.groupby("code", group_keys=False).apply(_calc_indicators)
        logger.debug(f"テクニカル指標計算完了: {len(result)} レコード")
        return result
    except Exception as e:
        logger.error(f"テクニカル指標計算中にエラー: {e}")
        raise DataError(f"テクニカル指標計算に失敗しました: {e}")


def get_market_filter(
    exclude_markets: Optional[List[str]] = None, db_manager: Optional[DatabaseManager] = None
) -> List[str]:
    """市場フィルターを適用して有効な銘柄コードを取得

    Args:
        exclude_markets: 除外する市場コードのリスト
        db_manager: DatabaseManagerインスタンス

    Returns:
        有効な銘柄コードのリスト
    """
    if db_manager is None:
        db_manager = get_db_manager()

    if exclude_markets is None:
        exclude_markets = ["0109"]  # デフォルトでREITを除外

    sql = "SELECT DISTINCT code FROM listed_info WHERE 1=1"
    params = []

    if exclude_markets:
        placeholders = ", ".join("?" for _ in exclude_markets)
        sql += f" AND market_code NOT IN ({placeholders})"
        params.extend(exclude_markets)

    try:
        with db_manager.get_connection() as conn:
            result = conn.execute(sql, params).fetchall()
            codes = [row[0] for row in result]
            logger.debug(f"有効な銘柄数: {len(codes)}")
            return codes
    except Exception as e:
        logger.error(f"市場フィルター取得中にエラー: {e}")
        raise DatabaseError(f"市場フィルターの取得に失敗しました: {e}")


def calculate_returns(df: pd.DataFrame, price_col: str = "close", periods: Optional[List[int]] = None) -> pd.DataFrame:
    """リターン計算

    Args:
        df: 株価データのDataFrame
        price_col: 価格カラム名
        periods: 計算する期間のリスト

    Returns:
        リターンが追加されたDataFrame
    """
    if df.empty:
        return df

    if periods is None:
        periods = [1, 5, 10, 20, 60]

    def _calc_returns(group: pd.DataFrame) -> pd.DataFrame:
        """各銘柄ごとにリターンを計算"""
        g = group.copy().sort_values("date")

        for period in periods:
            g[f"return_{period}d"] = g[price_col].pct_change(period)

        return g

    try:
        result = df.groupby("code", group_keys=False).apply(_calc_returns)
        logger.debug(f"リターン計算完了: {len(result)} レコード")
        return result
    except Exception as e:
        logger.error(f"リターン計算中にエラー: {e}")
        raise DataError(f"リターン計算に失敗しました: {e}")


def filter_by_liquidity(
    df: pd.DataFrame, min_volume: float = 1000, min_turnover: float = 1000000, days: int = 20
) -> pd.DataFrame:
    """流動性によるフィルタリング

    Args:
        df: 株価データのDataFrame
        min_volume: 最小出来高
        min_turnover: 最小売買代金
        days: 計算期間

    Returns:
        フィルタリング後のDataFrame
    """
    if df.empty:
        return df

    def _check_liquidity(group: pd.DataFrame) -> pd.DataFrame:
        """各銘柄の流動性をチェック"""
        g = group.copy().sort_values("date")

        # 平均出来高と売買代金を計算
        g["avg_volume"] = g["volume"].rolling(days).mean()
        g["turnover"] = g["close"] * g["volume"]
        g["avg_turnover"] = g["turnover"].rolling(days).mean()

        # 流動性フィルター
        g["liquidity_ok"] = (g["avg_volume"] >= min_volume) & (g["avg_turnover"] >= min_turnover)

        return g

    try:
        result = df.groupby("code", group_keys=False).apply(_check_liquidity)

        # 流動性条件を満たすデータのみを返す
        filtered = result[result["liquidity_ok"]].copy()

        logger.debug(f"流動性フィルタリング: {len(filtered)} / {len(result)} レコード")
        return filtered

    except Exception as e:
        logger.error(f"流動性フィルタリング中にエラー: {e}")
        raise DataError(f"流動性フィルタリングに失敗しました: {e}")


class ScreeningResultExporter:
    """スクリーニング結果エクスポートクラス"""

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Args:
            output_dir: 出力ディレクトリ
        """
        self.output_dir = output_dir or Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_to_excel(self, data: Dict[str, pd.DataFrame], filename: str, add_timestamp: bool = True) -> Path:
        """ExcelファイルにエクスポートExcel

        Args:
            data: シート名とDataFrameの辞書
            filename: ファイル名
            add_timestamp: タイムスタンプを追加するか

        Returns:
            保存したファイルのパス
        """
        if add_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name, ext = filename.rsplit(".", 1) if "." in filename else (filename, "xlsx")
            filename = f"{name}_{timestamp}.{ext}"

        filepath = self.output_dir / filename

        try:
            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                for sheet_name, df in data.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            logger.info(f"Excel ファイルを保存しました: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Excel エクスポート中にエラー: {e}")
            raise DataError(f"Excel エクスポートに失敗しました: {e}")

    def export_to_csv(self, df: pd.DataFrame, filename: str, add_timestamp: bool = True) -> Path:
        """CSVファイルにエクスポート

        Args:
            df: エクスポートするDataFrame
            filename: ファイル名
            add_timestamp: タイムスタンプを追加するか

        Returns:
            保存したファイルのパス
        """
        if add_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name, ext = filename.rsplit(".", 1) if "." in filename else (filename, "csv")
            filename = f"{name}_{timestamp}.{ext}"

        filepath = self.output_dir / filename

        try:
            df.to_csv(filepath, index=False, encoding="utf-8-sig")
            logger.info(f"CSV ファイルを保存しました: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"CSV エクスポート中にエラー: {e}")
            raise DataError(f"CSV エクスポートに失敗しました: {e}")

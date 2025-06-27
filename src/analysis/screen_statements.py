#!/usr/bin/env python
"""screen_statements.py – boolean‑fix & robust screening 2025‑06‑07
====================================================================
*   正規化したブール列（"true"/"false"/"1"/"0"/空/NaN → bool）で
    ノイズ除外が機能するよう修正。
*   Stage counts を DEBUG 出力して詰まり箇所を可視化。
*   pandas FutureWarning（pct_change デフォルト変更）を回避。
*   デフォルト `lookback_days` を 3 年に拡大（FY YoY 計算向け）。
*   `--as-of` で基準日を指定可能に（省略時は当日）。

Usage:
    python screen_statements.py --lookback 3000 --recent 1500 \
        --as-of 2025-06-07 -v
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Optional

import pandas as pd

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Threshold constants shared across screening modules
from src.analysis.thresholds import CF_QUALITY_MIN, EPS_YOY_MIN, ETA_DELTA_MIN, TREASURY_DELTA_MAX, log_thresholds
from src.utils.cli_utils import create_parser, setup_logging_from_args
from src.utils.db_utils import DatabaseManager, get_db_manager
from src.utils.exceptions import DatabaseError, DataError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ブール列名（statements テーブル側では TEXT 型）
BOOL_COLS: Final = [
    "MaterialChangesInSubsidiaries",
    "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
    "ChangesInAccountingEstimates",
]


@dataclass(frozen=True)
class ScreeningConfig:
    """スクリーニング設定"""

    lookback_days: int = 365 * 3  # 3 年分ロード
    recent_days: int = 7  # 開示から何日以内を対象にするか
    as_of: date = field(default_factory=date.today)  # 処理基準日
    window_q: int = 4  # 四半期 MA


def _cast_bool(series: pd.Series) -> pd.Series:
    """ "true"/"false"/"1"/"0"/NaN/空文字 → bool へ正規化

    Args:
        series: 変換対象のSeries

    Returns:
        bool型に変換されたSeries
    """
    return (
        series.astype(str)
        .str.lower()
        .map(
            {
                "true": True,
                "1": True,
                "false": False,
                "0": False,
                "nan": False,
                "": False,
            }
        )
        .fillna(False)
        .astype(bool)
    )


class StatementsFetcher:
    """財務諸表データ取得クラス"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.db_manager = db_manager or get_db_manager()

    def fetch_statements(self, config: ScreeningConfig) -> pd.DataFrame:
        """Load recent statements rows from DB and return as DataFrame.

        Args:
            config: スクリーニング設定

        Returns:
            財務諸表データのDataFrame

        Raises:
            DatabaseError: データベースエラー
        """
        start_date = (config.as_of - timedelta(days=config.lookback_days)).strftime("%Y-%m-%d")

        sql = """
            SELECT A.LocalCode,
                A.DisclosedDate,
                A.DisclosedTime,
                A.TypeOfCurrentPeriod,
                A.NetSales,
                A.OperatingProfit,
                A.Profit,
                A.EarningsPerShare,
                A.ForecastEarningsPerShare,
                A.CashFlowsFromOperatingActivities,
                A.EquityToAssetRatio,
                A.NumberOfTreasuryStockAtTheEndOfFiscalYear,
                A.MaterialChangesInSubsidiaries,
                A.ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard,
                A.ChangesInAccountingEstimates
            FROM statements A
            join listed_info B
            on A.LocalCode = B.code
            where  B.market_code != "0109"
            and A.DisclosedDate >= ?;
        """

        try:
            with self.db_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn, params=(start_date,))

                if df.empty:
                    logger.warning("取得されたデータがありません")
                    return df

                logger.debug(f"取得したレコード数: {len(df)}")

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
                    if col in df.columns:
                        df[col] = _cast_bool(df[col])

                # Combine date & time
                df["DisclosedAt"] = pd.to_datetime(
                    df["DisclosedDate"].fillna("1970-01-01") + " " + df["DisclosedTime"].fillna("00:00:00")
                )

                df.sort_values(["LocalCode", "DisclosedAt"], inplace=True)
                return df

        except Exception as e:
            logger.error(f"財務諸表データの取得中にエラー: {e}")
            raise DatabaseError(f"財務諸表データの取得に失敗しました: {e}")


class FeaturesCalculator:
    """特徴量計算クラス"""

    def __init__(self, config: ScreeningConfig):
        """
        Args:
            config: スクリーニング設定
        """
        self.config = config

    def compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add QoQ / YoY / quality metrics per LocalCode.

        Args:
            df: 財務諸表データのDataFrame

        Returns:
            特徴量が追加されたDataFrame
        """
        if df.empty:
            return df

        quarter_map = {"1Q": 1, "2Q": 2, "3Q": 3, "4Q": 4}

        def _add_features(g: pd.DataFrame) -> pd.DataFrame:
            """各銘柄ごとに特徴量を計算"""
            g = g.copy()

            # Basic growth
            g["sales_qoq"] = g["NetSales"].pct_change(fill_method=None)
            g["op_qoq"] = g["OperatingProfit"].pct_change(fill_method=None)

            # Margin trends
            g["op_margin"] = g["OperatingProfit"] / g["NetSales"]
            g["op_margin_ma4"] = g["op_margin"].rolling(self.config.window_q).mean()
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

        try:
            result = df.groupby("LocalCode", group_keys=False).apply(_add_features)
            logger.debug(f"特徴量計算完了: {len(result)} レコード")
            return result
        except Exception as e:
            logger.error(f"特徴量計算中にエラー: {e}")
            raise DataError(f"特徴量計算に失敗しました: {e}")


class FundamentalScreener:
    """ファンダメンタルスクリーニングクラス"""

    def __init__(self, config: ScreeningConfig):
        """
        Args:
            config: スクリーニング設定
        """
        self.config = config

    def screen_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply sequential filters and log stage counts.

        Args:
            df: 特徴量付きDataFrame

        Returns:
            フィルタリング後のDataFrame
        """
        if df.empty:
            return df

        recent_cut = pd.Timestamp(self.config.as_of - timedelta(days=self.config.recent_days))

        # スクリーニング段階の記録
        stage_counts = {}

        # 1. 最近の開示データ
        mask = df["DisclosedAt"] >= recent_cut
        stage_counts["recent"] = mask.sum()
        logger.debug(f"最近の開示: {stage_counts['recent']} 件")

        # 2. EPS成長率
        eps_yoy = df["eps_yoy_fy"].fillna(df["eps_yoy_q"]).fillna(0)
        mask &= eps_yoy > EPS_YOY_MIN
        stage_counts["eps"] = mask.sum()
        logger.debug(f"EPS成長率フィルタ後: {stage_counts['eps']} 件")

        # 3. キャッシュフロー品質
        mask &= df["cf_quality"].fillna(0) > CF_QUALITY_MIN
        stage_counts["cf"] = mask.sum()
        logger.debug(f"キャッシュフロー品質フィルタ後: {stage_counts['cf']} 件")

        # 4. 自己資本比率の改善
        mask &= df["eta_delta"].fillna(0) > ETA_DELTA_MIN
        stage_counts["eta"] = mask.sum()
        logger.debug(f"自己資本比率フィルタ後: {stage_counts['eta']} 件")

        # 5. 自己株式の変動
        mask &= df["treasury_delta"].fillna(0) <= TREASURY_DELTA_MAX
        stage_counts["treasury"] = mask.sum()
        logger.debug(f"自己株式フィルタ後: {stage_counts['treasury']} 件")

        # 6. ノイズ除外（特別要因）
        for col in BOOL_COLS:
            if col in df.columns:
                mask &= ~df[col]
        stage_counts["noise"] = mask.sum()
        logger.debug(f"ノイズ除外後: {stage_counts['noise']} 件")

        logger.info(f"スクリーニング段階別件数: {stage_counts}")

        return df.loc[mask].copy()


class SignalsSaver:
    """シグナル保存クラス"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.db_manager = db_manager or get_db_manager()

    def save_signals(self, sig_df: pd.DataFrame) -> int:
        """シグナルをデータベースに保存

        Args:
            sig_df: 保存するシグナルのDataFrame

        Returns:
            保存した件数

        Raises:
            DatabaseError: データベースエラー
        """
        if sig_df.empty:
            logger.info("保存するシグナルがありません")
            return 0

        # 保存するカラムを選択
        signal_columns = [
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

        # 存在するカラムのみを選択
        available_cols = [col for col in signal_columns if col in sig_df.columns]
        sig = sig_df[available_cols].copy()

        # データ型の調整
        sig["DisclosedAt"] = sig["DisclosedAt"].dt.strftime("%Y-%m-%d %H:%M:%S")
        if "turnaround" in sig.columns:
            sig["turnaround"] = sig["turnaround"].astype(int)
        sig["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            with self.db_manager.transaction() as conn:
                cols = list(sig.columns)
                placeholders = ", ".join("?" for _ in cols)
                sql = f"INSERT OR IGNORE INTO fundamental_signals ({', '.join(cols)}) VALUES ({placeholders})"

                records = sig.to_records(index=False).tolist()
                conn.executemany(sql, records)

                inserted_count = len(sig)
                logger.info(f"{inserted_count} 件のシグナルを保存しました")
                return inserted_count

        except Exception as e:
            logger.error(f"シグナル保存中にエラー: {e}")
            raise DatabaseError(f"シグナルの保存に失敗しました: {e}")


class FundamentalScreeningService:
    """ファンダメンタルスクリーニングサービス"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.fetcher = StatementsFetcher(db_manager)
        self.saver = SignalsSaver(db_manager)

    def run_screening(
        self, lookback_days: int = 365 * 3, recent_days: int = 7, as_of: Optional[date] = None
    ) -> dict[str, Any]:
        """スクリーニングを実行

        Args:
            lookback_days: 過去の参照期間（日数）
            recent_days: 開示日の閾値（日数）
            as_of: 処理基準日

        Returns:
            実行結果の辞書
        """
        if as_of is None:
            as_of = date.today()

        config = ScreeningConfig(lookback_days=lookback_days, recent_days=recent_days, as_of=as_of)

        logger.info(f"ファンダメンタルスクリーニング開始: {config.as_of}")
        logger.info(f"参照期間: {config.lookback_days} 日")
        logger.info(f"開示期間: {config.recent_days} 日")

        try:
            # データ取得
            df = self.fetcher.fetch_statements(config)
            if df.empty:
                return {"status": "success", "signals_count": 0, "message": "対象データなし"}

            # 特徴量計算
            calculator = FeaturesCalculator(config)
            df_feat = calculator.compute_features(df)

            # スクリーニング
            screener = FundamentalScreener(config)
            signals_df = screener.screen_signals(df_feat)

            # 保存
            inserted_count = self.saver.save_signals(signals_df)

            result = {
                "status": "success",
                "signals_count": inserted_count,
                "total_records": len(df),
                "processed_records": len(df_feat),
                "config": {
                    "lookback_days": config.lookback_days,
                    "recent_days": config.recent_days,
                    "as_of": config.as_of.isoformat(),
                },
            }

            logger.info(f"スクリーニング完了: {inserted_count} 件のシグナルを抽出")
            return result

        except Exception as e:
            logger.error(f"スクリーニング実行中にエラー: {e}")
            return {"status": "error", "error": str(e)}


def create_screening_parser():
    """スクリーニング用のArgumentParserを作成"""
    parser = create_parser("財務諸表をスクリーニングしてシグナルを抽出")

    parser.add_argument(
        "--lookback",
        type=int,
        default=365 * 3,
        help="過去の参照期間（日数、デフォルト: 1095）",
    )
    parser.add_argument(
        "--recent",
        type=int,
        default=7,
        help="開示日の閾値（日数、デフォルト: 7）",
    )
    parser.add_argument(
        "--as-of",
        help="処理基準日 YYYY-MM-DD (省略時は当日)",
    )

    return parser


def main() -> None:
    """メイン処理"""
    parser = create_screening_parser()
    args = parser.parse_args()

    # ロギングの設定
    setup_logging_from_args(args)
    log_thresholds()

    # 基準日の設定
    as_of = None
    if args.as_of:
        try:
            as_of = date.fromisoformat(args.as_of)
        except ValueError:
            logger.error(f"無効な日付形式: {args.as_of}")
            return

    try:
        # データベースマネージャーの設定
        if args.db:
            db_manager = DatabaseManager(args.db)
        else:
            db_manager = get_db_manager()

        # スクリーニングサービスの実行
        service = FundamentalScreeningService(db_manager)
        result = service.run_screening(lookback_days=args.lookback, recent_days=args.recent, as_of=as_of)

        if result["status"] == "success":
            logger.info(f"処理完了: {result['signals_count']} 件のシグナルを生成")
        else:
            logger.error(f"処理失敗: {result.get('error', '不明なエラー')}")

    except Exception as e:
        logger.error(f"予期しないエラー: {e}")
        raise


if __name__ == "__main__":
    main()

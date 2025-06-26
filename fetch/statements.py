#!/usr/bin/env python
"""
statements.py – Fetch /statements (J-Quants) and upsert into SQLite `statements`

v5 ✨ Refactored with common utilities
---------------------------------
- Migrated to use common utilities (ConfigManager, DatabaseManager, JQuantsClient)
- Improved error handling and logging
- Added type hints and documentation
- Service-oriented architecture
- Enhanced pagination handling

Usage
-----
    python statements.py 1                   # listed_info にあるコード単位で一括取得（過去分も含む）
    python statements.py 2                   # 当日日付の開示分を取得（日次取得）
    python statements.py 2 --start 2024-01-01 --end 2024-01-31
                                          # 期間指定で取得

環境
----
- Python 3.9+
- `pandas`, `requests`

機能
----
- モード "1": listed_info テーブルから delete_flag=0 の銘柄コードを取得し、各コードごとに /statements API を呼び出して全過去開示情報を取得 (pagination_key によるページネーションを考慮) → statements テーブルに Upsert
- モード "2": /statements?date=<YYYY-MM-DD> を呼び出し、指定日の開示情報を取得 (pagination_key を考慮)。--start/--end を指定すると期間分ループして取得 → statements テーブルに Upsert
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time

import pandas as pd

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.config import get_config_manager
from utils.db_utils import get_db_manager, DatabaseManager
from utils.jquants_client import get_jquants_client, JQuantsClient
from utils.logging_config import get_logger
from utils.cli_utils import create_parser, setup_logging_from_args
from utils.exceptions import APIError, DatabaseError, DataError
from utils.common import parse_date_string

logger = get_logger(__name__)

# SQLite側の statements テーブルに合わせたカラム一覧
SCHEMA_COLUMNS: List[str] = [
    "DisclosedDate", "DisclosedTime", "LocalCode", "DisclosureNumber",
    "TypeOfDocument", "TypeOfCurrentPeriod", "CurrentPeriodStartDate",
    "CurrentPeriodEndDate", "CurrentFiscalYearStartDate", "CurrentFiscalYearEndDate",
    "NextFiscalYearStartDate", "NextFiscalYearEndDate", "NetSales",
    "OperatingProfit", "OrdinaryProfit", "Profit", "EarningsPerShare",
    "DilutedEarningsPerShare", "TotalAssets", "Equity", "EquityToAssetRatio",
    "BookValuePerShare", "CashFlowsFromOperatingActivities",
    "CashFlowsFromInvestingActivities", "CashFlowsFromFinancingActivities",
    "CashAndEquivalents", "ResultDividendPerShare1stQuarter",
    "ResultDividendPerShare2ndQuarter", "ResultDividendPerShare3rdQuarter",
    "ResultDividendPerShareFiscalYearEnd", "ResultDividendPerShareAnnual",
    "DistributionsPerUnit_REIT", "ResultTotalDividendPaidAnnual",
    "ResultPayoutRatioAnnual", "ForecastDividendPerShare1stQuarter",
    "ForecastDividendPerShare2ndQuarter", "ForecastDividendPerShare3rdQuarter",
    "ForecastDividendPerShareFiscalYearEnd", "ForecastDividendPerShareAnnual",
    "ForecastDistributionsPerUnit_REIT", "ForecastTotalDividendPaidAnnual",
    "ForecastPayoutRatioAnnual", "NextYearForecastDividendPerShare1stQuarter",
    "NextYearForecastDividendPerShare2ndQuarter", "NextYearForecastDividendPerShare3rdQuarter",
    "NextYearForecastDividendPerShareFiscalYearEnd", "NextYearForecastDividendPerShareAnnual",
    "NextYearForecastDistributionsPerUnit_REIT", "NextYearForecastPayoutRatioAnnual",
    "ForecastNetSales2ndQuarter", "ForecastOperatingProfit2ndQuarter",
    "ForecastOrdinaryProfit2ndQuarter", "ForecastProfit2ndQuarter",
    "ForecastEarningsPerShare2ndQuarter", "NextYearForecastNetSales2ndQuarter",
    "NextYearForecastOperatingProfit2ndQuarter", "NextYearForecastOrdinaryProfit2ndQuarter",
    "NextYearForecastProfit2ndQuarter", "NextYearForecastEarningsPerShare2ndQuarter",
    "ForecastNetSales", "ForecastOperatingProfit", "ForecastOrdinaryProfit",
    "ForecastProfit", "ForecastEarningsPerShare", "NextYearForecastNetSales",
    "NextYearForecastOperatingProfit", "NextYearForecastOrdinaryProfit",
    "NextYearForecastProfit", "NextYearForecastEarningsPerShare",
    "MaterialChangesInSubsidiaries", "SignificantChangesInTheScopeOfConsolidation",
    "ChangesBasedOnRevisionsOfAccountingStandard", "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
    "ChangesInAccountingEstimates", "RetrospectiveRestatement",
    "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
    "NumberOfTreasuryStockAtTheEndOfFiscalYear", "AverageNumberOfShares",
    "NonConsolidatedNetSales", "NonConsolidatedOperatingProfit",
    "NonConsolidatedOrdinaryProfit", "NonConsolidatedProfit",
    "NonConsolidatedEarningsPerShare", "NonConsolidatedTotalAssets",
    "NonConsolidatedEquity", "NonConsolidatedEquityToAssetRatio",
    "NonConsolidatedBookValuePerShare", "ForecastNonConsolidatedNetSales2ndQuarter",
    "ForecastNonConsolidatedOperatingProfit2ndQuarter", "ForecastNonConsolidatedOrdinaryProfit2ndQuarter",
    "ForecastNonConsolidatedProfit2ndQuarter", "ForecastNonConsolidatedEarningsPerShare2ndQuarter",
    "NextYearForecastNonConsolidatedNetSales2ndQuarter", "NextYearForecastNonConsolidatedOperatingProfit2ndQuarter",
    "NextYearForecastNonConsolidatedOrdinaryProfit2ndQuarter", "NextYearForecastNonConsolidatedProfit2ndQuarter",
    "NextYearForecastNonConsolidatedEarningsPerShare2ndQuarter", "ForecastNonConsolidatedNetSales",
    "ForecastNonConsolidatedOperatingProfit", "ForecastNonConsolidatedOrdinaryProfit",
    "ForecastNonConsolidatedProfit", "ForecastNonConsolidatedEarningsPerShare",
    "NextYearForecastNonConsolidatedNetSales", "NextYearForecastNonConsolidatedOperatingProfit",
    "NextYearForecastNonConsolidatedOrdinaryProfit", "NextYearForecastNonConsolidatedProfit",
    "NextYearForecastNonConsolidatedEarningsPerShare",
]


class StatementsFetcher:
    """財務諸表データ取得クラス"""
    
    def __init__(self, client: Optional[JQuantsClient] = None):
        """
        Args:
            client: JQuantsClientインスタンス
        """
        self.client = client or get_jquants_client()
    
    def fetch_by_code(self, code: str) -> List[Dict[str, Any]]:
        """銘柄コード別に財務諸表データを取得
        
        Args:
            code: 銘柄コード
            
        Returns:
            財務諸表データのリスト
            
        Raises:
            APIError: API呼び出しエラー
        """
        logger.info(f"銘柄 {code} の財務諸表データを取得中...")
        
        try:
            statements = self.client.get_statements(code=code)
            logger.info(f"銘柄 {code}: {len(statements)} 件取得")
            return statements
            
        except Exception as e:
            logger.error(f"銘柄 {code} の取得中にエラー: {e}")
            raise APIError(f"銘柄 {code} の財務諸表取得に失敗: {e}")
    
    def fetch_by_date(self, target_date: Union[str, date]) -> List[Dict[str, Any]]:
        """日付別に財務諸表データを取得
        
        Args:
            target_date: 対象日付
            
        Returns:
            財務諸表データのリスト
            
        Raises:
            APIError: API呼び出しエラー
        """
        if isinstance(target_date, str):
            date_str = target_date
        else:
            date_str = target_date.strftime("%Y-%m-%d")
        
        logger.info(f"日付 {date_str} の財務諸表データを取得中...")
        
        try:
            statements = self.client.get_statements(date=date_str)
            logger.info(f"日付 {date_str}: {len(statements)} 件取得")
            return statements
            
        except Exception as e:
            logger.error(f"日付 {date_str} の取得中にエラー: {e}")
            raise APIError(f"日付 {date_str} の財務諸表取得に失敗: {e}")
    
    def fetch_by_date_range(self, start_date: Union[str, date], end_date: Union[str, date]) -> List[Dict[str, Any]]:
        """期間指定で財務諸表データを取得
        
        Args:
            start_date: 開始日
            end_date: 終了日
            
        Returns:
            財務諸表データのリスト
        """
        if isinstance(start_date, str):
            start_date = parse_date_string(start_date)
        if isinstance(end_date, str):
            end_date = parse_date_string(end_date)
        
        logger.info(f"期間 {start_date} 〜 {end_date} の財務諸表データを取得中...")
        
        all_statements = []
        current_date = start_date
        
        while current_date <= end_date:
            try:
                statements = self.fetch_by_date(current_date)
                if statements:
                    all_statements.extend(statements)
                    
            except APIError as e:
                logger.warning(f"日付 {current_date} の取得をスキップ: {e}")
                
            current_date += timedelta(days=1)
        
        logger.info(f"期間取得完了: 合計 {len(all_statements)} 件")
        return all_statements
    
    def fetch_multiple_codes(self, codes: List[str], max_workers: int = 5) -> List[Dict[str, Any]]:
        """複数銘柄の財務諸表データを並行取得
        
        Args:
            codes: 銘柄コードのリスト
            max_workers: 並行実行数
            
        Returns:
            財務諸表データのリスト
        """
        logger.info(f"{len(codes)} 銘柄の並行取得を開始 (max_workers={max_workers})")
        
        all_statements = []
        
        def fetch_task(code: str) -> List[Dict[str, Any]]:
            try:
                return self.fetch_by_code(code)
            except APIError:
                return []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i, statements in enumerate(executor.map(fetch_task, codes), 1):
                if statements:
                    all_statements.extend(statements)
                logger.info(f"進捗 {i}/{len(codes)}")
        
        logger.info(f"並行取得完了: 合計 {len(all_statements)} 件")
        return all_statements


class StatementsProcessor:
    """財務諸表データ処理クラス"""
    
    @staticmethod
    def normalize_statements(statements: List[Dict[str, Any]]) -> pd.DataFrame:
        """財務諸表データをスキーマに合わせて正規化
        
        Args:
            statements: 財務諸表データのリスト
            
        Returns:
            正規化されたDataFrame
        """
        if not statements:
            logger.warning("空の財務諸表データが渡されました")
            return pd.DataFrame(columns=SCHEMA_COLUMNS)
        
        # DataFrameに変換
        df = pd.DataFrame(statements)
        
        # スキーマに合わせて列を整理
        for col in SCHEMA_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        
        # スキーマ順に列を並び替え
        normalized_df = df[SCHEMA_COLUMNS].copy()
        
        logger.debug(f"正規化完了: {len(normalized_df)} 行, {len(SCHEMA_COLUMNS)} 列")
        return normalized_df


class CodesProvider:
    """銘柄コード提供クラス"""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.db_manager = db_manager or get_db_manager()
    
    def get_active_codes(self) -> List[str]:
        """有効な銘柄コードを取得
        
        Returns:
            有効な銘柄コードのリスト
            
        Raises:
            DatabaseError: データベースエラー
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute("SELECT code FROM listed_info WHERE delete_flag = 0")
                codes = [row["code"] for row in cursor.fetchall()]
                
            logger.info(f"有効な銘柄コードを {len(codes)} 件取得")
            return codes
            
        except Exception as e:
            logger.error(f"銘柄コード取得中にエラー: {e}")
            raise DatabaseError(f"銘柄コードの取得に失敗: {e}")


class StatementsSaver:
    """財務諸表データ保存クラス"""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.db_manager = db_manager or get_db_manager()
    
    def save_to_database(self, df: pd.DataFrame) -> int:
        """財務諸表データをデータベースに保存
        
        Args:
            df: 保存する正規化済みDataFrame
            
        Returns:
            保存した行数
            
        Raises:
            DatabaseError: データベースエラー
        """
        if df.empty:
            logger.warning("保存するデータがありません")
            return 0
        
        try:
            with self.db_manager.transaction() as conn:
                # 一時テーブルを使用してupsert
                df.to_sql("_tmp_statements", conn, if_exists="replace", index=False)
                
                # INSERT OR REPLACEでupsert実行
                conn.execute("""
                    INSERT OR REPLACE INTO statements
                    SELECT * FROM _tmp_statements
                """)
                
                # 一時テーブルを削除
                conn.execute("DROP TABLE _tmp_statements")
                
                row_count = len(df)
                logger.info(f"財務諸表データを保存: {row_count} 行")
                return row_count
                
        except Exception as e:
            logger.error(f"データベース保存中にエラー: {e}")
            raise DatabaseError(f"財務諸表データの保存に失敗: {e}")


class StatementsService:
    """財務諸表データ更新サービス"""
    
    def __init__(
        self,
        client: Optional[JQuantsClient] = None,
        db_manager: Optional[DatabaseManager] = None
    ):
        """
        Args:
            client: JQuantsClientインスタンス
            db_manager: DatabaseManagerインスタンス
        """
        self.fetcher = StatementsFetcher(client)
        self.processor = StatementsProcessor()
        self.codes_provider = CodesProvider(db_manager)
        self.saver = StatementsSaver(db_manager)
    
    def update_by_codes(self, max_workers: int = 5) -> Dict[str, Any]:
        """銘柄コード別で財務諸表データを更新
        
        Args:
            max_workers: 並行実行数
            
        Returns:
            実行結果の辞書
        """
        logger.info("銘柄コード別財務諸表データ更新処理を開始")
        start_time = time.perf_counter()
        
        try:
            # 有効な銘柄コードを取得
            codes = self.codes_provider.get_active_codes()
            
            if not codes:
                return {
                    "status": "success",
                    "message": "有効な銘柄コードが見つかりません",
                    "records_updated": 0
                }
            
            # データ取得
            statements = self.fetcher.fetch_multiple_codes(codes, max_workers)
            
            if not statements:
                return {
                    "status": "success",
                    "message": "取得データが空です",
                    "records_updated": 0
                }
            
            # データ処理
            normalized_df = self.processor.normalize_statements(statements)
            
            # データ保存
            records_updated = self.saver.save_to_database(normalized_df)
            
            elapsed_time = time.perf_counter() - start_time
            
            result = {
                "status": "success",
                "message": "銘柄コード別財務諸表データの更新が完了しました",
                "records_updated": records_updated,
                "codes_processed": len(codes),
                "processing_time": round(elapsed_time, 2)
            }
            
            logger.info(f"更新完了: {records_updated} 件 ({elapsed_time:.2f}秒)")
            return result
            
        except (APIError, DatabaseError, DataError) as e:
            logger.error(f"銘柄コード別更新中にエラー: {e}")
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__
            }
        except Exception as e:
            logger.error(f"予期しないエラー: {e}")
            return {
                "status": "error",
                "error": f"予期しないエラーが発生しました: {e}",
                "error_type": "UnexpectedError"
            }
    
    def update_by_date(self, target_date: Optional[Union[str, date]] = None) -> Dict[str, Any]:
        """日付別で財務諸表データを更新
        
        Args:
            target_date: 対象日付（Noneの場合は今日）
            
        Returns:
            実行結果の辞書
        """
        if target_date is None:
            target_date = date.today()
        elif isinstance(target_date, str):
            target_date = parse_date_string(target_date)
        
        logger.info(f"日付別財務諸表データ更新処理を開始: {target_date}")
        start_time = time.perf_counter()
        
        try:
            # データ取得
            statements = self.fetcher.fetch_by_date(target_date)
            
            if not statements:
                return {
                    "status": "success",
                    "message": f"日付 {target_date} のデータが見つかりません",
                    "records_updated": 0
                }
            
            # データ処理
            normalized_df = self.processor.normalize_statements(statements)
            
            # データ保存
            records_updated = self.saver.save_to_database(normalized_df)
            
            elapsed_time = time.perf_counter() - start_time
            
            result = {
                "status": "success",
                "message": f"日付 {target_date} の財務諸表データ更新が完了しました",
                "records_updated": records_updated,
                "target_date": target_date.isoformat(),
                "processing_time": round(elapsed_time, 2)
            }
            
            logger.info(f"更新完了: {records_updated} 件 ({elapsed_time:.2f}秒)")
            return result
            
        except (APIError, DatabaseError, DataError) as e:
            logger.error(f"日付別更新中にエラー: {e}")
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__
            }
        except Exception as e:
            logger.error(f"予期しないエラー: {e}")
            return {
                "status": "error",
                "error": f"予期しないエラーが発生しました: {e}",
                "error_type": "UnexpectedError"
            }
    
    def update_by_date_range(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date]
    ) -> Dict[str, Any]:
        """期間指定で財務諸表データを更新
        
        Args:
            start_date: 開始日
            end_date: 終了日
            
        Returns:
            実行結果の辞書
        """
        if isinstance(start_date, str):
            start_date = parse_date_string(start_date)
        if isinstance(end_date, str):
            end_date = parse_date_string(end_date)
        
        logger.info(f"期間指定財務諸表データ更新処理を開始: {start_date} 〜 {end_date}")
        start_time = time.perf_counter()
        
        try:
            # データ取得
            statements = self.fetcher.fetch_by_date_range(start_date, end_date)
            
            if not statements:
                return {
                    "status": "success",
                    "message": f"期間 {start_date} 〜 {end_date} のデータが見つかりません",
                    "records_updated": 0
                }
            
            # データ処理
            normalized_df = self.processor.normalize_statements(statements)
            
            # データ保存
            records_updated = self.saver.save_to_database(normalized_df)
            
            elapsed_time = time.perf_counter() - start_time
            
            result = {
                "status": "success",
                "message": f"期間 {start_date} 〜 {end_date} の財務諸表データ更新が完了しました",
                "records_updated": records_updated,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "processing_time": round(elapsed_time, 2)
            }
            
            logger.info(f"更新完了: {records_updated} 件 ({elapsed_time:.2f}秒)")
            return result
            
        except (APIError, DatabaseError, DataError) as e:
            logger.error(f"期間指定更新中にエラー: {e}")
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__
            }
        except Exception as e:
            logger.error(f"予期しないエラー: {e}")
            return {
                "status": "error",
                "error": f"予期しないエラーが発生しました: {e}",
                "error_type": "UnexpectedError"
            }


def create_statements_parser():
    """財務諸表取得用ArgumentParserを作成"""
    parser = create_parser("J-Quants APIから財務諸表データを取得してデータベースを更新")
    parser.add_argument(
        "mode",
        choices=["1", "2"],
        help="1: 銘柄ごとに一括取得、2: 日付または期間で取得"
    )
    parser.add_argument("--start", help="開始日 YYYY-MM-DD (モード2で使用)")
    parser.add_argument("--end", help="終了日 YYYY-MM-DD (モード2で使用)")
    parser.add_argument("--workers", type=int, default=5, help="並行実行数 (モード1で使用)")
    return parser


def main() -> None:
    """メイン処理"""
    parser = create_statements_parser()
    args = parser.parse_args()
    
    # ロギングの設定
    setup_logging_from_args(args)
    
    try:
        # データベースマネージャーの設定
        if args.db:
            db_manager = DatabaseManager(args.db)
        else:
            db_manager = get_db_manager()
        
        # サービスの実行
        service = StatementsService(db_manager=db_manager)
        
        if args.mode == "1":
            # 銘柄コード別取得
            result = service.update_by_codes(max_workers=args.workers)
        elif args.mode == "2":
            if args.start or args.end:
                # 期間指定取得
                start_date = args.start or date.today().strftime("%Y-%m-%d")
                end_date = args.end or start_date
                result = service.update_by_date_range(start_date, end_date)
            else:
                # 当日取得
                result = service.update_by_date()
        
        if result["status"] == "success":
            logger.info(f"処理完了: {result['message']}")
            if result.get("records_updated", 0) > 0:
                logger.info(f"更新レコード数: {result['records_updated']}")
            if "processing_time" in result:
                logger.info(f"処理時間: {result['processing_time']} 秒")
        else:
            logger.error(f"処理失敗: {result.get('error', '不明なエラー')}")
            exit(1)
            
    except Exception as e:
        logger.error(f"予期しないエラー: {e}")
        raise


if __name__ == "__main__":
    main()
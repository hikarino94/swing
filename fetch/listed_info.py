#!/usr/bin/env python
"""
listed_info.py – Fetch /listed/info (J-Quants) and upsert into SQLite `listed_info`

v5 ✨ Refactored with common utilities
---------------------------------
- Migrated to use common utilities (ConfigManager, DatabaseManager, JQuantsClient)
- Improved error handling and logging
- Added type hints and documentation
- Service-oriented architecture

Usage
-----
    python listed_info.py

環境
----
- Python 3.9+
- `pandas`, `requests`
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import date, datetime
import pandas as pd

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.config import get_config_manager
from utils.db_utils import get_db_manager, DatabaseManager
from utils.jquants_client import get_jquants_client, JQuantsClient
from utils.logging_config import get_logger
from utils.cli_utils import create_parser, setup_logging_from_args
from utils.exceptions import APIError, DatabaseError, DataError

logger = get_logger(__name__)


class ListedInfoFetcher:
    """上場銘柄情報取得クラス"""
    
    def __init__(self, client: Optional[JQuantsClient] = None):
        """
        Args:
            client: JQuantsClientインスタンス
        """
        self.client = client or get_jquants_client()
    
    def fetch_listed_info(self) -> pd.DataFrame:
        """J-Quants APIから上場銘柄情報を取得
        
        Returns:
            上場銘柄情報のDataFrame
            
        Raises:
            APIError: API呼び出しエラー
        """
        logger.info("上場銘柄情報を取得中...")
        
        try:
            data = self.client.get_listed_info()
            
            if not data:
                raise APIError("上場銘柄情報が取得できませんでした")
            
            df = pd.DataFrame(data)
            logger.info(f"取得した銘柄数: {len(df)}")
            logger.debug(f"APIレスポンスカラム: {df.columns.tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"上場銘柄情報取得中にエラー: {e}")
            raise APIError(f"上場銘柄情報の取得に失敗しました: {e}")


class ListedInfoProcessor:
    """上場銘柄情報処理クラス"""
    
    @staticmethod
    def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """APIレスポンスのDataFrameを正規化
        
        Args:
            df: APIから取得したDataFrame
            
        Returns:
            正規化されたDataFrame
        """
        if df.empty:
            logger.warning("空のDataFrameが渡されました")
            return df
        
        # カラムマッピング定義
        column_mapping = {
            "code": "Code",
            "date": "Date", 
            "company_name": "CompanyName",
            "company_name_en": "CompanyNameEnglish",
            "sector17_code": "Sector17Code",
            "sector17_name": "Sector17CodeName",
            "sector33_code": "Sector33Code", 
            "sector33_name": "Sector33CodeName",
            "scale_category": "ScaleCategory",
            "market_code": "MarketCode",
            "market_name": "MarketCodeName",
            "margin_code": "MarginCode",
            "margin_name": "MarginCodeName"
        }
        
        # 新しいDataFrameを作成
        mapped_data = {}
        for db_col, api_col in column_mapping.items():
            mapped_data[db_col] = df.get(api_col, pd.NA)
        
        normalized_df = pd.DataFrame(mapped_data)
        
        # 日付を標準形式に変換
        if "date" in normalized_df.columns:
            normalized_df["date"] = pd.to_datetime(
                normalized_df["date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        
        logger.debug(f"正規化後のデータ: {len(normalized_df)} 行")
        return normalized_df


class ListedInfoSaver:
    """上場銘柄情報保存クラス"""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.db_manager = db_manager or get_db_manager()
    
    def save_to_database(self, df: pd.DataFrame) -> int:
        """上場銘柄情報をデータベースに保存
        
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
                df.to_sql("_tmp_listed", conn, if_exists="replace", index=False)
                
                # INSERT OR REPLACEでupsert実行
                upsert_sql = """
                    INSERT OR REPLACE INTO listed_info
                        (code, date, company_name, company_name_en,
                         sector17_code, sector17_name, sector33_code, sector33_name,
                         scale_category, market_code, market_name, margin_code, margin_name)
                    SELECT
                        code, date, company_name, company_name_en,
                        sector17_code, sector17_name, sector33_code, sector33_name,
                        scale_category, market_code, market_name, margin_code, margin_name
                    FROM _tmp_listed
                """
                conn.execute(upsert_sql)
                
                # 一時テーブルを削除
                conn.execute("DROP TABLE _tmp_listed")
                
                # delete_flagを更新（本日のデータは0、それ以外は1）
                today_str = date.today().strftime("%Y-%m-%d")
                update_sql = """
                    UPDATE listed_info 
                    SET delete_flag = CASE WHEN date = ? THEN 0 ELSE 1 END
                """
                conn.execute(update_sql, (today_str,))
                
                row_count = len(df)
                logger.info(f"上場銘柄情報を保存: {row_count} 行")
                logger.info(f"delete_flag更新: 本日({today_str})以外は無効フラグを設定")
                
                return row_count
                
        except Exception as e:
            logger.error(f"データベース保存中にエラー: {e}")
            raise DatabaseError(f"上場銘柄情報の保存に失敗しました: {e}")


class ListedInfoService:
    """上場銘柄情報更新サービス"""
    
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
        self.fetcher = ListedInfoFetcher(client)
        self.processor = ListedInfoProcessor()
        self.saver = ListedInfoSaver(db_manager)
    
    def update_listed_info(self) -> Dict[str, Any]:
        """上場銘柄情報を取得してデータベースを更新
        
        Returns:
            実行結果の辞書
        """
        logger.info("上場銘柄情報更新処理を開始")
        
        try:
            # データ取得
            df = self.fetcher.fetch_listed_info()
            
            if df.empty:
                return {
                    "status": "success",
                    "message": "取得データが空です",
                    "records_updated": 0
                }
            
            # データ処理
            normalized_df = self.processor.normalize_dataframe(df)
            
            # データ保存
            records_updated = self.saver.save_to_database(normalized_df)
            
            result = {
                "status": "success",
                "message": "上場銘柄情報の更新が完了しました",
                "records_updated": records_updated,
                "update_date": date.today().isoformat()
            }
            
            logger.info(f"更新完了: {records_updated} 銘柄")
            return result
            
        except (APIError, DatabaseError, DataError) as e:
            logger.error(f"上場銘柄情報更新中にエラー: {e}")
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


def create_listed_info_parser():
    """上場銘柄情報取得用ArgumentParserを作成"""
    return create_parser("J-Quants APIから上場銘柄情報を取得してデータベースを更新")


def main() -> None:
    """メイン処理"""
    parser = create_listed_info_parser()
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
        service = ListedInfoService(db_manager=db_manager)
        result = service.update_listed_info()
        
        if result["status"] == "success":
            logger.info(f"処理完了: {result['message']}")
            if result.get("records_updated", 0) > 0:
                logger.info(f"更新レコード数: {result['records_updated']}")
        else:
            logger.error(f"処理失敗: {result.get('error', '不明なエラー')}")
            exit(1)
            
    except Exception as e:
        logger.error(f"予期しないエラー: {e}")
        raise


if __name__ == "__main__":
    main()
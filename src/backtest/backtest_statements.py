#!/usr/bin/env python
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

import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Threshold constants
from src.analysis.thresholds import log_thresholds
from src.utils.backtest_utils import (
    BacktestConfig,
    BacktestEngine,
    BacktestResult,
    BacktestResultExporter,
    PriceDataProvider,
    SignalProvider,
)
from src.utils.cli_utils import add_date_arguments, create_parser, setup_logging_from_args
from src.utils.db_utils import DatabaseManager, get_db_manager
from src.utils.exceptions import DatabaseError, DataError
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_CAPITAL = 1_000_000  # JPY
MIN_PRICE_DEFAULT = 300.0


class FundamentalBacktestEngine(BacktestEngine):
    """ファンダメンタルシグナル用バックテストエンジン"""

    def run_fundamental_backtest(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        hold_days: int = 40,
        entry_offset: int = 1,
        capital: float = DEFAULT_CAPITAL,
        min_price: float = MIN_PRICE_DEFAULT,
    ) -> BacktestResult:
        """ファンダメンタルシグナルでバックテストを実行

        Args:
            start_date: 開始日
            end_date: 終了日
            hold_days: 保有日数
            entry_offset: エントリーオフセット
            capital: 初期資金
            min_price: 最低株価

        Returns:
            バックテスト結果
        """
        if start_date is None:
            start_date = date.today() - timedelta(days=365)
        if end_date is None:
            end_date = date.today()

        config = BacktestConfig(
            start_date=start_date,
            end_date=end_date,
            initial_capital=capital,
            hold_days=hold_days,
            entry_offset=entry_offset,
            min_price=min_price,
            max_positions=1,  # ファンダメンタルは1銘柄ずつ
        )

        logger.info(f"ファンダメンタルバックテスト開始: {start_date} - {end_date}")

        # ファンダメンタルシグナルを取得
        signals_df = self.signal_provider.get_fundamental_signals(start_date, end_date)

        if signals_df.empty:
            logger.warning("対象期間にシグナルがありません")
            return BacktestResult(config=config)

        logger.info(f"取得したシグナル数: {len(signals_df)}")

        # バックテスト実行
        return self.run_backtest(signals_df, config)


class TradingDaysCalculator:
    """営業日計算ユーティリティ"""

    def __init__(self, price_provider: PriceDataProvider):
        """
        Args:
            price_provider: 価格データ提供者
        """
        self.price_provider = price_provider
        self._trading_calendar = None

    def get_trading_calendar(self, start_date: date, end_date: date) -> pd.DatetimeIndex:
        """営業日カレンダーを取得

        Args:
            start_date: 開始日
            end_date: 終了日

        Returns:
            営業日のインデックス
        """
        if self._trading_calendar is None:
            # 価格データから営業日を抽出
            extended_start = start_date - timedelta(days=100)
            extended_end = end_date + timedelta(days=100)

            # 適当な銘柄コードで価格データを取得して営業日を抽出
            df = self.price_provider.get_price_data(["1301"], extended_start, extended_end)  # 極洋（流動性の高い銘柄）

            if not df.empty:
                self._trading_calendar = pd.to_datetime(df["date"].unique()).sort_values()
            else:
                # フォールバック: 平日カレンダー
                self._trading_calendar = pd.bdate_range(extended_start, extended_end)

        return self._trading_calendar

    def add_trading_days(self, dates: pd.Series, n_days: int, start_date: date, end_date: date) -> pd.Series:
        """営業日ベースで日付をずらす

        Args:
            dates: 基準日のSeries
            n_days: 加算する営業日数
            start_date: カレンダーの開始日
            end_date: カレンダーの終了日

        Returns:
            n_days後の営業日のSeries
        """
        calendar = self.get_trading_calendar(start_date, end_date)

        # 各日付に対してn日後の営業日を計算
        result_dates = []
        for date_val in dates:
            # カレンダー内での位置を取得
            try:
                idx = calendar.get_loc(pd.Timestamp(date_val))
                new_idx = min(idx + n_days, len(calendar) - 1)
                result_dates.append(calendar[new_idx])
            except KeyError:
                # 該当日が営業日でない場合は最も近い営業日を使用
                nearest_idx = calendar.searchsorted(pd.Timestamp(date_val))
                if nearest_idx >= len(calendar):
                    nearest_idx = len(calendar) - 1
                new_idx = min(nearest_idx + n_days, len(calendar) - 1)
                result_dates.append(calendar[new_idx])

        return pd.Series(result_dates, index=dates.index)


class LegacyCompatibleBacktester:
    """従来のバックテストロジックとの互換性を保つクラス"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.db_manager = db_manager or get_db_manager()
        self.price_provider = PriceDataProvider(db_manager)
        self.signal_provider = SignalProvider(db_manager)
        self.trading_calc = TradingDaysCalculator(self.price_provider)

    def read_prices(self) -> pd.DataFrame:
        """価格テーブルを読み込む（従来形式）

        Returns:
            マルチインデックス形式の価格DataFrame
        """
        sql = """
            SELECT code AS LocalCode,
                   date AS trade_date,
                   adj_close
            FROM prices
        """

        try:
            with self.db_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn, parse_dates=["trade_date"])
                return df.set_index(["LocalCode", "trade_date"]).sort_index()
        except Exception as e:
            logger.error(f"価格データ読み込み中にエラー: {e}")
            raise DatabaseError(f"価格データの読み込みに失敗しました: {e}")

    def read_signals(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> pd.DataFrame:
        """シグナルを日付範囲で取得する（従来形式）

        Args:
            start_date: 開始日
            end_date: 終了日

        Returns:
            シグナルのDataFrame
        """
        sql = "SELECT LocalCode, DisclosedAt FROM fundamental_signals"
        params = []

        if start_date or end_date:
            conditions = []
            if start_date:
                conditions.append("DisclosedAt >= ?")
                params.append(f"{start_date.strftime('%Y-%m-%d')} 00:00:00")
            if end_date:
                conditions.append("DisclosedAt <= ?")
                params.append(f"{end_date.strftime('%Y-%m-%d')} 23:59:59")

            if conditions:
                sql += " WHERE " + " AND ".join(conditions)

        try:
            with self.db_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn, params=params, parse_dates=["DisclosedAt"])
                logger.debug(f"取得したシグナル数: {len(df)}")
                return df
        except Exception as e:
            logger.error(f"シグナル読み込み中にエラー: {e}")
            raise DatabaseError(f"シグナルの読み込みに失敗しました: {e}")

    def run_backtest(
        self,
        prices: pd.DataFrame,
        signals: pd.DataFrame,
        hold_days: int,
        entry_offset: int,
        capital: float,
        min_price: float = MIN_PRICE_DEFAULT,
    ) -> pd.DataFrame:
        """従来のバックテストロジックを実行

        Args:
            prices: 価格DataFrame（マルチインデックス）
            signals: シグナルDataFrame
            hold_days: 保有日数
            entry_offset: エントリーオフセット
            capital: 投資資金
            min_price: 最低株価

        Returns:
            取引結果のDataFrame
        """
        if signals.empty:
            return pd.DataFrame()

        # 営業日カレンダーを取得
        calendar = prices.index.get_level_values(1).unique().sort_values()

        # エントリー日とエグジット日を計算
        signals = signals.copy()
        signals["entry_date"] = self.trading_calc.add_trading_days(
            signals["DisclosedAt"], entry_offset, calendar.min().date(), calendar.max().date()
        )
        signals["exit_date"] = self.trading_calc.add_trading_days(
            signals["entry_date"], hold_days, calendar.min().date(), calendar.max().date()
        )

        # マルチインデックスで価格取得
        entry_idx = signals.set_index(["LocalCode", "entry_date"]).index
        exit_idx = signals.set_index(["LocalCode", "exit_date"]).index

        try:
            entry_prices = prices.reindex(entry_idx)["adj_close"].values
            exit_prices = prices.reindex(exit_idx)["adj_close"].values

            # 有効な価格データのみフィルタリング
            valid_mask = (~pd.isna(entry_prices)) & (~pd.isna(exit_prices)) & (entry_prices >= min_price)

            if not valid_mask.any():
                logger.warning("有効な取引データがありません")
                return pd.DataFrame()

            entry_prices = entry_prices[valid_mask]
            exit_prices = exit_prices[valid_mask]
            signals_filtered = signals[valid_mask].reset_index(drop=True)

            # 取引計算
            shares = (capital // entry_prices).astype(int)
            invest = shares * entry_prices
            proceed = shares * exit_prices
            profit = proceed - invest

            trades = pd.DataFrame(
                {
                    "code": signals_filtered["LocalCode"],
                    "DisclosedAt": signals_filtered["DisclosedAt"].dt.date,
                    "entry_date": signals_filtered["entry_date"].dt.date,
                    "exit_date": signals_filtered["exit_date"].dt.date,
                    "entry_px": entry_prices,
                    "exit_px": exit_prices,
                    "shares": shares,
                    "invest": invest,
                    "proceed": proceed,
                    "profit_jpy": profit,
                    "ret_pct": profit / invest,
                    "days": hold_days,
                }
            )

            logger.info(f"バックテスト完了: {len(trades)} 取引")
            return trades

        except Exception as e:
            logger.error(f"バックテスト実行中にエラー: {e}")
            raise DataError(f"バックテストの実行に失敗しました: {e}")

    def summarize(self, trades: pd.DataFrame) -> pd.DataFrame:
        """バックテスト結果のサマリーを作成

        Args:
            trades: 取引結果DataFrame

        Returns:
            サマリーDataFrame
        """
        if trades.empty:
            return pd.DataFrame(
                {"metric": ["trades", "total_profit", "win_rate", "avg_ret_pct", "sharpe"], "value": [0, 0, 0, 0, 0]}
            )

        total_profit = trades["profit_jpy"].sum()
        win_rate = (trades["profit_jpy"] > 0).mean()
        mean_ret_pct = trades["ret_pct"].mean()

        # シャープレシオ計算（ゼロ除算対策）
        ret_std = trades["ret_pct"].std(ddof=0)
        sharpe = mean_ret_pct / ret_std if ret_std > 0 else 0

        summary = pd.DataFrame(
            {
                "metric": ["trades", "total_profit", "win_rate", "avg_ret_pct", "sharpe"],
                "value": [len(trades), total_profit, win_rate, mean_ret_pct, sharpe],
            }
        )

        return summary


class FundamentalBacktestService:
    """ファンダメンタルバックテストサービス"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.backtester = LegacyCompatibleBacktester(db_manager)
        self.exporter = BacktestResultExporter()

    def run_backtest(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        hold_days: int = 40,
        entry_offset: int = 1,
        capital: float = DEFAULT_CAPITAL,
        min_price: float = MIN_PRICE_DEFAULT,
        excel_file: Optional[str] = None,
        json_file: Optional[str] = None,
        show_results: bool = False,
    ) -> dict[str, Any]:
        """ファンダメンタルバックテストを実行

        Args:
            start_date: 開始日
            end_date: 終了日
            hold_days: 保有日数
            entry_offset: エントリーオフセット
            capital: 投資資金
            min_price: 最低株価
            excel_file: Excelファイル名
            json_file: JSONファイル名
            show_results: 結果表示フラグ

        Returns:
            実行結果の辞書
        """
        logger.info("ファンダメンタルバックテスト開始")
        logger.info(f"期間: {start_date} - {end_date}")
        logger.info(f"設定: 保有{hold_days}日, オフセット{entry_offset}日, 資金{capital:,.0f}円")

        try:
            # データ読み込み
            prices = self.backtester.read_prices()
            signals = self.backtester.read_signals(start_date, end_date)

            logger.info(f"価格データ: {len(prices)} レコード")
            logger.info(f"シグナル: {len(signals)} 件")

            if signals.empty:
                return {"status": "success", "message": "対象期間にシグナルがありません", "trades_count": 0}

            # バックテスト実行
            trades = self.backtester.run_backtest(prices, signals, hold_days, entry_offset, capital, min_price)
            summary = self.backtester.summarize(trades)

            # ファイル出力
            output_files = {}

            if excel_file or json_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                if excel_file:
                    if not excel_file.endswith(".xlsx"):
                        excel_file = f"{excel_file}_{timestamp}.xlsx"
                    self._save_to_excel(trades, summary, excel_file)
                    output_files["excel"] = excel_file

                if json_file:
                    if not json_file.endswith(".json"):
                        json_file = f"{json_file}_{timestamp}.json"
                    trades.to_json(json_file, orient="records", force_ascii=False)
                    output_files["json"] = json_file
                    logger.info(f"JSON出力: {json_file}")

            # 結果表示
            if show_results:
                self._show_results(trades, summary)

            result = {
                "status": "success",
                "trades_count": len(trades),
                "total_profit": summary.loc[summary["metric"] == "total_profit", "value"].iloc[0]
                if not summary.empty
                else 0,
                "win_rate": summary.loc[summary["metric"] == "win_rate", "value"].iloc[0] if not summary.empty else 0,
                "output_files": output_files,
                "summary": summary.to_dict("records") if not summary.empty else [],
            }

            logger.info(f"バックテスト完了: {len(trades)} 取引")
            return result

        except Exception as e:
            logger.error(f"バックテスト実行中にエラー: {e}")
            return {"status": "error", "error": str(e)}

    def _save_to_excel(self, trades: pd.DataFrame, summary: pd.DataFrame, filepath: str):
        """Excel形式で保存"""
        try:
            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                trades.to_excel(writer, sheet_name="trades", index=False)
                summary.to_excel(writer, sheet_name="summary", index=False)

            logger.info(f"Excel出力: {filepath}")
        except Exception as e:
            logger.error(f"Excel出力中にエラー: {e}")
            raise DataError(f"Excel出力に失敗しました: {e}")

    def _show_results(self, trades: pd.DataFrame, summary: pd.DataFrame):
        """結果を標準出力に表示"""
        print("\n=== Summary ===")
        if not summary.empty:
            for _, row in summary.iterrows():
                metric, value = row["metric"], row["value"]
                if metric == "total_profit":
                    print(f"{metric:>15}: {value:>12,.0f} JPY")
                elif metric in ["win_rate", "avg_ret_pct"]:
                    print(f"{metric:>15}: {value:>12.2%}")
                elif metric == "sharpe":
                    print(f"{metric:>15}: {value:>12.3f}")
                else:
                    print(f"{metric:>15}: {value:>12}")

        if not trades.empty and len(trades) <= 20:
            print(f"\n=== Top Trades (showing {len(trades)}) ===")
            display_cols = ["code", "entry_date", "exit_date", "profit_jpy", "ret_pct"]
            available_cols = [col for col in display_cols if col in trades.columns]
            print(trades[available_cols].to_string(index=False))


def create_backtest_parser():
    """バックテスト用ArgumentParserを作成"""
    parser = create_parser("ファンダメンタルシグナルのバックテストを実行")

    add_date_arguments(parser, start_help="シグナル開始日 (YYYY-MM-DD)", end_help="シグナル終了日 (YYYY-MM-DD)")

    parser.add_argument("--hold", type=int, default=40, help="保有期間（日数、デフォルト: 40）")
    parser.add_argument("--entry-offset", type=int, default=1, help="エントリー日のオフセット（デフォルト: 1）")
    parser.add_argument(
        "--capital", type=float, default=DEFAULT_CAPITAL, help=f"1取引あたりの資金 (JPY、デフォルト: {DEFAULT_CAPITAL:,})"
    )
    parser.add_argument(
        "--min-price", type=float, default=MIN_PRICE_DEFAULT, help=f"エントリー株価の下限 (JPY、デフォルト: {MIN_PRICE_DEFAULT})"
    )

    parser.add_argument("--xlsx", type=str, help="Excel出力ファイル名")
    parser.add_argument("--json", type=str, help="JSON出力ファイル名")
    parser.add_argument("--show", action="store_true", help="結果を標準出力に表示")

    return parser


def main() -> None:
    """メイン処理"""
    parser = create_backtest_parser()
    args = parser.parse_args()

    # ロギングの設定
    setup_logging_from_args(args)
    log_thresholds(logger)

    try:
        # データベースマネージャーの設定
        if args.db:
            db_manager = DatabaseManager(args.db)
        else:
            db_manager = get_db_manager()

        # サービスの実行
        service = FundamentalBacktestService(db_manager)
        result = service.run_backtest(
            start_date=args.start,
            end_date=args.end,
            hold_days=args.hold,
            entry_offset=args.entry_offset,
            capital=args.capital,
            min_price=args.min_price,
            excel_file=args.xlsx,
            json_file=args.json,
            show_results=args.show,
        )

        if result["status"] == "success":
            logger.info(f"処理完了: {result['trades_count']} 取引")
            if result.get("output_files"):
                for file_type, path in result["output_files"].items():
                    logger.info(f"{file_type.upper()}ファイル: {path}")
        else:
            logger.error(f"処理失敗: {result.get('error', '不明なエラー')}")

    except Exception as e:
        logger.error(f"予期しないエラー: {e}")
        raise


if __name__ == "__main__":
    main()

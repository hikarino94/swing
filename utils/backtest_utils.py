"""バックテスト共通ユーティリティ"""
from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple, Union
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from dataclasses import dataclass, field
import json

from .db_utils import DatabaseManager, get_db_manager
from .exceptions import DataError, DatabaseError
from .common import generate_timestamped_filename, save_dataframe

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """バックテスト設定"""
    start_date: date
    end_date: date
    initial_capital: float = 10_000_000.0  # 初期資金
    hold_days: int = 20  # 保有日数
    entry_offset: int = 1  # シグナル日からエントリーまでのオフセット
    min_price: float = 300.0  # 最低株価
    stop_loss_pct: Optional[float] = None  # ストップロス率
    take_profit_pct: Optional[float] = None  # 利益確定率
    max_positions: int = 10  # 最大同時保有ポジション数
    commission_rate: float = 0.001  # 手数料率


@dataclass
class Trade:
    """取引記録"""
    code: str
    signal_date: date
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    shares: int
    profit_loss: float
    return_pct: float
    hold_days: int
    exit_reason: str = "planned"  # planned, stop_loss, take_profit, forced


@dataclass
class BacktestResult:
    """バックテスト結果"""
    config: BacktestConfig
    trades: List[Trade] = field(default_factory=list)
    total_return: float = 0.0
    total_profit: float = 0.0
    win_rate: float = 0.0
    avg_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    start_date: date = field(default_factory=date.today)
    end_date: date = field(default_factory=date.today)


class PriceDataProvider:
    """株価データ提供クラス"""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.db_manager = db_manager or get_db_manager()
        self._price_cache = {}
    
    def get_price_data(
        self,
        codes: List[str],
        start_date: date,
        end_date: date,
        use_adjusted: bool = True
    ) -> pd.DataFrame:
        """株価データを取得
        
        Args:
            codes: 銘柄コードのリスト
            start_date: 開始日
            end_date: 終了日
            use_adjusted: 調整済み価格を使用するか
            
        Returns:
            株価データのDataFrame
        """
        cache_key = (tuple(sorted(codes)), start_date, end_date, use_adjusted)
        if cache_key in self._price_cache:
            return self._price_cache[cache_key].copy()
        
        price_cols = ["adj_open", "adj_high", "adj_low", "adj_close"] if use_adjusted else ["open", "high", "low", "close"]
        col_aliases = ["open", "high", "low", "close"]
        
        select_cols = ", ".join([f"{col} as {alias}" for col, alias in zip(price_cols, col_aliases)])
        
        sql = f"""
            SELECT code, date, {select_cols}, volume
            FROM prices
            WHERE date BETWEEN ? AND ?
            AND code IN ({', '.join('?' for _ in codes)})
            ORDER BY code, date
        """
        
        params = [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")] + codes
        
        try:
            with self.db_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn, params=params)
                
                if not df.empty:
                    df["date"] = pd.to_datetime(df["date"])
                    for col in ["open", "high", "low", "close", "volume"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                
                self._price_cache[cache_key] = df.copy()
                logger.debug(f"株価データ取得: {len(df)} レコード, {len(codes)} 銘柄")
                return df
                
        except Exception as e:
            logger.error(f"株価データ取得中にエラー: {e}")
            raise DatabaseError(f"株価データの取得に失敗しました: {e}")
    
    def get_price_at_date(
        self,
        code: str,
        target_date: date,
        price_type: str = "open",
        search_days: int = 5
    ) -> Optional[float]:
        """指定日の株価を取得（前後の営業日も検索）
        
        Args:
            code: 銘柄コード
            target_date: 対象日
            price_type: 価格タイプ（open, high, low, close）
            search_days: 検索日数
            
        Returns:
            株価（見つからない場合はNone）
        """
        start_search = target_date - timedelta(days=search_days)
        end_search = target_date + timedelta(days=search_days)
        
        df = self.get_price_data([code], start_search, end_search)
        if df.empty:
            return None
        
        # 指定日に最も近い日のデータを取得
        df["date_diff"] = abs((df["date"].dt.date - target_date).dt.days)
        closest_row = df.loc[df["date_diff"].idxmin()]
        
        return closest_row[price_type] if not pd.isna(closest_row[price_type]) else None


class SignalProvider:
    """シグナル提供クラス"""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.db_manager = db_manager or get_db_manager()
    
    def get_fundamental_signals(
        self,
        start_date: date,
        end_date: date,
        additional_filters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """ファンダメンタルシグナルを取得
        
        Args:
            start_date: 開始日
            end_date: 終了日
            additional_filters: 追加フィルター条件
            
        Returns:
            シグナルのDataFrame
        """
        sql = """
            SELECT LocalCode as code, DATE(DisclosedAt) as signal_date,
                   eps_yoy_fy, eps_yoy_q, cf_quality, eta_delta,
                   leverage, turnaround, op_margin_delta
            FROM fundamental_signals
            WHERE DATE(DisclosedAt) BETWEEN ? AND ?
        """
        
        params = [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")]
        
        # 追加フィルター条件を適用
        if additional_filters:
            for column, condition in additional_filters.items():
                if isinstance(condition, (list, tuple)) and len(condition) == 2:
                    operator, value = condition
                    sql += f" AND {column} {operator} ?"
                    params.append(value)
        
        try:
            with self.db_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn, params=params)
                
                if not df.empty:
                    df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.date
                
                logger.debug(f"ファンダメンタルシグナル取得: {len(df)} 件")
                return df
                
        except Exception as e:
            logger.error(f"ファンダメンタルシグナル取得中にエラー: {e}")
            raise DatabaseError(f"ファンダメンタルシグナルの取得に失敗しました: {e}")
    
    def get_technical_signals(
        self,
        start_date: date,
        end_date: date,
        signal_types: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """テクニカルシグナルを取得
        
        Args:
            start_date: 開始日
            end_date: 終了日
            signal_types: シグナルタイプのリスト
            
        Returns:
            シグナルのDataFrame
        """
        sql = """
            SELECT code, signal_date, signal_type, strength,
                   rsi, bb_position, price_position, adx
            FROM technical_indicators
            WHERE signal_date BETWEEN ? AND ?
        """
        
        params = [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")]
        
        if signal_types:
            placeholders = ", ".join("?" for _ in signal_types)
            sql += f" AND signal_type IN ({placeholders})"
            params.extend(signal_types)
        
        try:
            with self.db_manager.get_connection() as conn:
                df = pd.read_sql(sql, conn, params=params)
                
                if not df.empty:
                    df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.date
                
                logger.debug(f"テクニカルシグナル取得: {len(df)} 件")
                return df
                
        except Exception as e:
            logger.error(f"テクニカルシグナル取得中にエラー: {e}")
            raise DatabaseError(f"テクニカルシグナルの取得に失敗しました: {e}")


class BacktestEngine:
    """バックテストエンジン"""
    
    def __init__(
        self,
        price_provider: Optional[PriceDataProvider] = None,
        signal_provider: Optional[SignalProvider] = None
    ):
        """
        Args:
            price_provider: 株価データ提供者
            signal_provider: シグナル提供者
        """
        self.price_provider = price_provider or PriceDataProvider()
        self.signal_provider = signal_provider or SignalProvider()
    
    def run_backtest(
        self,
        signals_df: pd.DataFrame,
        config: BacktestConfig
    ) -> BacktestResult:
        """バックテストを実行
        
        Args:
            signals_df: シグナルのDataFrame
            config: バックテスト設定
            
        Returns:
            バックテスト結果
        """
        logger.info(f"バックテスト開始: {config.start_date} - {config.end_date}")
        
        # 必要な銘柄の株価データを取得
        all_codes = signals_df["code"].unique().tolist()
        price_df = self.price_provider.get_price_data(
            all_codes,
            config.start_date - timedelta(days=10),  # 余裕をもって取得
            config.end_date + timedelta(days=config.hold_days + 10)
        )
        
        if price_df.empty:
            logger.warning("価格データが見つかりません")
            return BacktestResult(config=config)
        
        # 株価データをピボット形式に変換（高速化のため）
        price_pivot = {}
        for price_type in ["open", "high", "low", "close"]:
            pivot = price_df.pivot(index="date", columns="code", values=price_type)
            price_pivot[price_type] = pivot
        
        trades = []
        current_positions = {}  # code -> (entry_date, entry_price, shares, signal_date)
        capital = config.initial_capital
        
        # 日付順にシグナルを処理
        signals_sorted = signals_df.sort_values("signal_date")
        
        for _, signal in signals_sorted.iterrows():
            signal_date = signal["signal_date"]
            code = signal["code"]
            
            # シグナル日以降でバックテスト期間内のみ処理
            if signal_date < config.start_date or signal_date > config.end_date:
                continue
            
            # エントリー日を計算
            entry_date = signal_date + timedelta(days=config.entry_offset)
            
            # エントリー価格を取得
            entry_price = self._get_price_from_pivot(
                price_pivot["open"], code, entry_date, search_days=5
            )
            
            if entry_price is None or entry_price < config.min_price:
                continue
            
            # ポジション数制限チェック
            if len(current_positions) >= config.max_positions:
                continue
            
            # 既に同じ銘柄を保有している場合はスキップ
            if code in current_positions:
                continue
            
            # 投資金額を計算（等金額投資）
            position_size = capital / config.max_positions
            shares = int(position_size / entry_price)
            
            if shares <= 0:
                continue
            
            # ポジションを開く
            current_positions[code] = {
                "entry_date": entry_date,
                "entry_price": entry_price,
                "shares": shares,
                "signal_date": signal_date
            }
            
            logger.debug(f"ポジション開始: {code} @ {entry_price} x {shares}株")
        
        # 保有期間終了やストップロス条件でポジションを閉じる
        self._close_positions(
            current_positions, price_pivot, config, trades
        )
        
        # バックテスト結果を計算
        result = self._calculate_results(trades, config)
        
        logger.info(f"バックテスト完了: {len(trades)} 取引, 総利益: {result.total_profit:,.0f}円")
        return result
    
    def _get_price_from_pivot(
        self,
        price_pivot: pd.DataFrame,
        code: str,
        target_date: date,
        search_days: int = 5
    ) -> Optional[float]:
        """ピボットテーブルから価格を取得"""
        if code not in price_pivot.columns:
            return None
        
        # 指定日から前後search_days日以内で検索
        target_datetime = pd.Timestamp(target_date)
        start_search = target_datetime - pd.Timedelta(days=search_days)
        end_search = target_datetime + pd.Timedelta(days=search_days)
        
        # 対象期間のデータを取得
        mask = (price_pivot.index >= start_search) & (price_pivot.index <= end_search)
        subset = price_pivot.loc[mask, code].dropna()
        
        if subset.empty:
            return None
        
        # 指定日に最も近い日のデータを取得
        date_diffs = abs(subset.index - target_datetime)
        closest_idx = date_diffs.idxmin()
        
        return subset.loc[closest_idx]
    
    def _close_positions(
        self,
        positions: Dict[str, Dict],
        price_pivot: Dict[str, pd.DataFrame],
        config: BacktestConfig,
        trades: List[Trade]
    ):
        """ポジションを閉じる"""
        for code, position in positions.items():
            entry_date = position["entry_date"]
            entry_price = position["entry_price"]
            shares = position["shares"]
            signal_date = position["signal_date"]
            
            # 予定終了日
            planned_exit_date = entry_date + timedelta(days=config.hold_days)
            
            # ストップロス・利益確定チェック（実装簡略化）
            exit_date = planned_exit_date
            exit_reason = "planned"
            
            # 終了価格を取得
            exit_price = self._get_price_from_pivot(
                price_pivot["open"], code, exit_date, search_days=5
            )
            
            if exit_price is None:
                continue
            
            # 取引記録を作成
            profit_loss = (exit_price - entry_price) * shares
            return_pct = (exit_price - entry_price) / entry_price
            actual_hold_days = (exit_date - entry_date).days
            
            trade = Trade(
                code=code,
                signal_date=signal_date,
                entry_date=entry_date,
                exit_date=exit_date,
                entry_price=entry_price,
                exit_price=exit_price,
                shares=shares,
                profit_loss=profit_loss,
                return_pct=return_pct,
                hold_days=actual_hold_days,
                exit_reason=exit_reason
            )
            
            trades.append(trade)
    
    def _calculate_results(
        self,
        trades: List[Trade],
        config: BacktestConfig
    ) -> BacktestResult:
        """バックテスト結果を計算"""
        if not trades:
            return BacktestResult(
                config=config,
                start_date=config.start_date,
                end_date=config.end_date
            )
        
        # 基本統計
        total_profit = sum(trade.profit_loss for trade in trades)
        total_return = total_profit / config.initial_capital
        
        returns = [trade.return_pct for trade in trades]
        win_trades = [r for r in returns if r > 0]
        win_rate = len(win_trades) / len(returns) if returns else 0
        avg_return = np.mean(returns) if returns else 0
        
        # シャープレシオ（簡易計算）
        if returns and np.std(returns) > 0:
            sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252 / config.hold_days)
        else:
            sharpe_ratio = 0
        
        # 最大ドローダウン（簡易計算）
        cumulative_returns = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative_returns)
        drawdowns = cumulative_returns - running_max
        max_drawdown = np.min(drawdowns) if len(drawdowns) > 0 else 0
        
        return BacktestResult(
            config=config,
            trades=trades,
            total_return=total_return,
            total_profit=total_profit,
            win_rate=win_rate,
            avg_return=avg_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            start_date=config.start_date,
            end_date=config.end_date
        )


class BacktestResultExporter:
    """バックテスト結果エクスポートクラス"""
    
    def __init__(self, output_dir: Optional[Path] = None):
        """
        Args:
            output_dir: 出力ディレクトリ
        """
        self.output_dir = output_dir or Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def export_to_excel(
        self,
        result: BacktestResult,
        filename: Optional[str] = None
    ) -> Path:
        """Excelファイルにエクスポート
        
        Args:
            result: バックテスト結果
            filename: ファイル名（Noneの場合は自動生成）
            
        Returns:
            保存したファイルのパス
        """
        if filename is None:
            filename = f"backtest_result_{result.start_date}_{result.end_date}.xlsx"
        
        filepath = generate_timestamped_filename(
            filename.replace(".xlsx", ""), ".xlsx", self.output_dir
        )
        
        # サマリーデータを作成
        summary_data = {
            "項目": [
                "開始日", "終了日", "初期資金", "総利益", "総リターン",
                "取引回数", "勝率", "平均リターン", "シャープレシオ", "最大ドローダウン"
            ],
            "値": [
                result.start_date.strftime("%Y-%m-%d"),
                result.end_date.strftime("%Y-%m-%d"),
                f"{result.config.initial_capital:,.0f}円",
                f"{result.total_profit:,.0f}円",
                f"{result.total_return:.2%}",
                len(result.trades),
                f"{result.win_rate:.2%}",
                f"{result.avg_return:.2%}",
                f"{result.sharpe_ratio:.3f}",
                f"{result.max_drawdown:.2%}"
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        
        # 取引詳細データを作成
        if result.trades:
            trades_data = []
            for trade in result.trades:
                trades_data.append({
                    "銘柄コード": trade.code,
                    "シグナル日": trade.signal_date.strftime("%Y-%m-%d"),
                    "エントリー日": trade.entry_date.strftime("%Y-%m-%d"),
                    "エグジット日": trade.exit_date.strftime("%Y-%m-%d"),
                    "エントリー価格": trade.entry_price,
                    "エグジット価格": trade.exit_price,
                    "株数": trade.shares,
                    "損益": trade.profit_loss,
                    "リターン": trade.return_pct,
                    "保有日数": trade.hold_days,
                    "終了理由": trade.exit_reason
                })
            trades_df = pd.DataFrame(trades_data)
        else:
            trades_df = pd.DataFrame()
        
        # Excelファイルに保存
        data_dict = {
            "サマリー": summary_df,
            "取引詳細": trades_df
        }
        
        try:
            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                for sheet_name, df in data_dict.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logger.info(f"バックテスト結果をExcelファイルに保存: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Excel エクスポート中にエラー: {e}")
            raise DataError(f"Excel エクスポートに失敗しました: {e}")
    
    def export_to_json(
        self,
        result: BacktestResult,
        filename: Optional[str] = None
    ) -> Path:
        """JSONファイルにエクスポート
        
        Args:
            result: バックテスト結果
            filename: ファイル名（Noneの場合は自動生成）
            
        Returns:
            保存したファイルのパス
        """
        if filename is None:
            filename = f"backtest_result_{result.start_date}_{result.end_date}.json"
        
        filepath = generate_timestamped_filename(
            filename.replace(".json", ""), ".json", self.output_dir
        )
        
        # JSON用データを作成
        data = {
            "summary": {
                "start_date": result.start_date.isoformat(),
                "end_date": result.end_date.isoformat(),
                "initial_capital": result.config.initial_capital,
                "total_profit": result.total_profit,
                "total_return": result.total_return,
                "trade_count": len(result.trades),
                "win_rate": result.win_rate,
                "avg_return": result.avg_return,
                "sharpe_ratio": result.sharpe_ratio,
                "max_drawdown": result.max_drawdown
            },
            "config": {
                "hold_days": result.config.hold_days,
                "entry_offset": result.config.entry_offset,
                "min_price": result.config.min_price,
                "max_positions": result.config.max_positions,
                "stop_loss_pct": result.config.stop_loss_pct,
                "take_profit_pct": result.config.take_profit_pct
            },
            "trades": []
        }
        
        for trade in result.trades:
            trade_dict = {
                "code": trade.code,
                "signal_date": trade.signal_date.isoformat(),
                "entry_date": trade.entry_date.isoformat(),
                "exit_date": trade.exit_date.isoformat(),
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "shares": trade.shares,
                "profit_loss": trade.profit_loss,
                "return_pct": trade.return_pct,
                "hold_days": trade.hold_days,
                "exit_reason": trade.exit_reason
            }
            data["trades"].append(trade_dict)
        
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"バックテスト結果をJSONファイルに保存: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"JSON エクスポート中にエラー: {e}")
            raise DataError(f"JSON エクスポートに失敗しました: {e}")
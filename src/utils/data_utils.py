"""データ処理の共通ユーティリティ"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class DataProcessor:
    """共通データ処理ユーティリティ"""

    @staticmethod
    def normalize_types(
        df: pd.DataFrame,
        numeric_cols: list[str] | None = None,
        date_cols: list[str] | None = None,
        bool_cols: list[str] | None = None,
    ) -> pd.DataFrame:
        """データ型の正規化

        Args:
            df: 処理対象のDataFrame
            numeric_cols: 数値型に変換するカラム名リスト
            date_cols: 日付型に変換するカラム名リスト
            bool_cols: ブール型に変換するカラム名リスト

        Returns:
            型変換後のDataFrame
        """
        df = df.copy()

        # 数値型変換
        if numeric_cols:
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        # 日付型変換
        if date_cols:
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

        # ブール型変換
        if bool_cols:
            for col in bool_cols:
                if col in df.columns:
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.lower()
                        .map(
                            {
                                "true": True,
                                "1": True,
                                "yes": True,
                                "false": False,
                                "0": False,
                                "no": False,
                                "nan": False,
                                "": False,
                            }
                        )
                        .fillna(False)
                    )

        return df

    @staticmethod
    def add_trading_days(
        dates: pd.Series | pd.DatetimeIndex,
        n_days: int,
        calendar: pd.DatetimeIndex,
    ) -> pd.Series:
        """営業日ベースで日付を加算

        Args:
            dates: 基準日付
            n_days: 加算する営業日数
            calendar: 営業日カレンダー

        Returns:
            加算後の日付
        """
        if isinstance(dates, pd.DatetimeIndex):
            dates = pd.Series(dates)

        result = pd.Series(index=dates.index, dtype="datetime64[ns]")

        for idx, date in dates.items():
            if pd.isna(date):
                result[idx] = pd.NaT
                continue

            # 基準日以降の営業日を取得
            future_days = calendar[calendar >= date]
            if len(future_days) > n_days:
                result[idx] = future_days[n_days]
            else:
                # 営業日が不足する場合は最後の日付
                result[idx] = future_days[-1] if len(future_days) > 0 else pd.NaT

        return result

    @staticmethod
    def calculate_returns(
        df: pd.DataFrame,
        price_col: str = "close",
        periods: list[int] | None = None,
    ) -> pd.DataFrame:
        """リターン（収益率）を計算

        Args:
            df: 価格データを含むDataFrame
            price_col: 価格カラム名
            periods: 計算期間のリスト（デフォルト: [1, 5, 20, 60]）

        Returns:
            リターンカラムが追加されたDataFrame
        """
        if periods is None:
            periods = [1, 5, 20, 60]

        df = df.copy()

        for period in periods:
            col_name = f"return_{period}d"
            df[col_name] = df[price_col].pct_change(period)

        return df

    @staticmethod
    def filter_market_codes(
        df: pd.DataFrame,
        market_code_col: str = "MarketCode",
        include_codes: list[str] | None = None,
    ) -> pd.DataFrame:
        """市場コードでフィルタリング

        Args:
            df: フィルタリング対象のDataFrame
            market_code_col: 市場コードカラム名
            include_codes: 含める市場コードのリスト（デフォルト: プライム・スタンダード）

        Returns:
            フィルタリング後のDataFrame
        """
        if include_codes is None:
            include_codes = ["0111", "0112"]  # プライム, スタンダード

        if market_code_col not in df.columns:
            logger.warning(f"Column '{market_code_col}' not found in DataFrame")
            return df

        return df[df[market_code_col].isin(include_codes)]

    @staticmethod
    def calculate_performance_metrics(
        returns: pd.Series, risk_free_rate: float = 0.0
    ) -> dict[str, float]:
        """パフォーマンス指標を計算

        Args:
            returns: リターンの時系列データ
            risk_free_rate: 無リスク金利（年率）

        Returns:
            各種パフォーマンス指標の辞書
        """
        # 有効なリターンのみを使用
        valid_returns = returns.dropna()

        if len(valid_returns) == 0:
            return {
                "mean_return": np.nan,
                "std_return": np.nan,
                "sharpe_ratio": np.nan,
                "max_drawdown": np.nan,
                "win_rate": np.nan,
            }

        # 基本統計量
        mean_return = valid_returns.mean()
        std_return = valid_returns.std()

        # シャープレシオ（年率換算）
        if std_return > 0:
            sharpe_ratio = (
                (mean_return - risk_free_rate / 252) / std_return * np.sqrt(252)
            )
        else:
            sharpe_ratio = np.nan

        # 最大ドローダウン
        cumulative_returns: pd.Series = (1 + valid_returns).cumprod()
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min()

        # 勝率
        win_rate = (valid_returns > 0).mean()

        return {
            "mean_return": mean_return,
            "std_return": std_return,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "win_rate": win_rate,
        }

    @staticmethod
    def create_date_range(
        start_date: str | None = None,
        end_date: str | None = None,
        lookback_days: int | None = None,
    ) -> tuple[str, str]:
        """日付範囲を作成

        Args:
            start_date: 開始日（YYYY-MM-DD形式）
            end_date: 終了日（YYYY-MM-DD形式）
            lookback_days: 終了日から遡る日数

        Returns:
            (開始日, 終了日)のタプル
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        if start_date is None:
            if lookback_days is None:
                lookback_days = 365  # デフォルト1年

            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=lookback_days)
            start_date = start_dt.strftime("%Y-%m-%d")

        return start_date, end_date

    @staticmethod
    def calculate_basic_metrics(returns: pd.Series) -> dict[str, float]:
        """基本的なメトリクスを計算

        Args:
            returns: リターンの時系列データ

        Returns:
            各種メトリクスの辞書
        """
        if returns.empty:
            return {
                "total_return": 0.0,
                "mean_return": 0.0,
                "std_return": 0.0,
                "sharpe_ratio": 0.0,
                "max_drawdown": 0.0,
                "win_rate": 0.0,
            }

        # 累積リターン
        cumulative_returns: pd.Series = (1 + returns).cumprod()
        total_return = (
            cumulative_returns.iloc[-1] - 1 if len(cumulative_returns) > 0 else 0.0
        )

        # 平均リターンと標準偏差
        mean_return = returns.mean()
        std_return = returns.std()

        # シャープレシオ（年率換算）
        if std_return > 0:
            sharpe_ratio = mean_return / std_return * np.sqrt(252)
        else:
            sharpe_ratio = 0.0

        # 最大ドローダウン
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min()

        # 勝率
        win_rate = (returns > 0).mean() if len(returns) > 0 else 0.0

        return {
            "total_return": float(total_return),
            "mean_return": float(mean_return),
            "std_return": float(std_return),
            "sharpe_ratio": float(sharpe_ratio),
            "max_drawdown": float(max_drawdown),
            "win_rate": float(win_rate),
        }

    @staticmethod
    def safe_divide(
        numerator: pd.Series | np.ndarray | float,
        denominator: pd.Series | np.ndarray | float,
        fill_value: float = 0.0,
    ) -> pd.Series | np.ndarray | float:
        """ゼロ除算を回避した除算

        Args:
            numerator: 分子
            denominator: 分母
            fill_value: ゼロ除算時の代替値

        Returns:
            除算結果
        """
        if isinstance(numerator, pd.Series) and isinstance(denominator, pd.Series):
            return (
                numerator.div(denominator)
                .fillna(fill_value)
                .replace([np.inf, -np.inf], fill_value)
            )
        elif isinstance(numerator, np.ndarray) or isinstance(denominator, np.ndarray):
            with np.errstate(divide="ignore", invalid="ignore"):
                result = np.divide(numerator, denominator)
                result[~np.isfinite(result)] = fill_value
                return result
        else:
            # スカラー値の場合
            if denominator == 0:
                return fill_value
            return numerator / denominator

"""共通ユーティリティ関数"""
import subprocess
import shlex
from pathlib import Path
from datetime import datetime, date
from typing import Optional, Tuple, List, Dict, Any
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def run_command(
    cmd: str,
    timeout: Optional[int] = None,
    check: bool = True
) -> Tuple[str, str, int]:
    """コマンド実行の共通ロジック
    
    Args:
        cmd: 実行するコマンド
        timeout: タイムアウト秒数
        check: 終了コードをチェックするか
        
    Returns:
        (stdout, stderr, returncode)のタプル
        
    Raises:
        subprocess.CalledProcessError: checkがTrueで終了コードが0でない場合
        subprocess.TimeoutExpired: タイムアウトした場合
    """
    logger.debug(f"Executing command: {cmd}")
    
    try:
        proc = subprocess.run(
            shlex.split(cmd),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=check
        )
        
        logger.debug(f"Command completed with return code: {proc.returncode}")
        return proc.stdout, proc.stderr, proc.returncode
        
    except subprocess.TimeoutExpired:
        logger.error(f"Command timed out after {timeout} seconds")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with return code {e.returncode}")
        logger.error(f"stderr: {e.stderr}")
        raise


def generate_timestamped_filename(
    base_name: str,
    extension: str,
    directory: Optional[Path] = None,
    timestamp_format: str = "%Y%m%d_%H%M%S"
) -> Path:
    """タイムスタンプ付きファイル名を生成
    
    Args:
        base_name: ベースとなるファイル名
        extension: 拡張子（.を含む）
        directory: 保存ディレクトリ
        timestamp_format: タイムスタンプフォーマット
        
    Returns:
        生成されたファイルパス
    """
    # マイクロ秒を追加して一意性を保証
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    microseconds = f"{now.microsecond:06d}"
    filename = f"{base_name}_{timestamp}_{microseconds}{extension}"
    
    if directory:
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        return directory / filename
    
    return Path(filename)


def save_dataframe(
    df: pd.DataFrame,
    filepath: Path,
    format: str = "xlsx",
    **kwargs
) -> None:
    """DataFrameを指定形式で保存
    
    Args:
        df: 保存するDataFrame
        filepath: 保存先パス
        format: 保存形式 (xlsx, csv, json)
        **kwargs: 各保存メソッドに渡す追加引数
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    if format == "xlsx":
        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, **kwargs)
        logger.info(f"Saved to Excel: {filepath}")
    
    elif format == "csv":
        df.to_csv(filepath, index=False, **kwargs)
        logger.info(f"Saved to CSV: {filepath}")
    
    elif format == "json":
        df.to_json(filepath, orient="records", force_ascii=False, **kwargs)
        logger.info(f"Saved to JSON: {filepath}")
    
    else:
        raise ValueError(f"Unsupported format: {format}")


def parse_date_range(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    default_days: int = 30
) -> Tuple[date, date]:
    """日付範囲を解析
    
    Args:
        date_from: 開始日文字列 (YYYY-MM-DD or YYYYMMDD)
        date_to: 終了日文字列 (YYYY-MM-DD or YYYYMMDD)
        default_days: デフォルトの期間
        
    Returns:
        (開始日, 終了日)のタプル
    """
    # 終了日のデフォルトは今日
    if date_to is None:
        end_date = date.today()
    else:
        end_date = parse_date_string(date_to)
    
    # 開始日のデフォルトは終了日からdefault_days日前
    if date_from is None:
        start_date = end_date - pd.Timedelta(days=default_days)
    else:
        start_date = parse_date_string(date_from)
    
    if start_date > end_date:
        raise ValueError(f"開始日({start_date})が終了日({end_date})より後です")
    
    return start_date, end_date


def parse_date_string(date_str: str) -> date:
    """日付文字列をdateオブジェクトに変換
    
    Args:
        date_str: 日付文字列 (YYYY-MM-DD or YYYYMMDD)
        
    Returns:
        dateオブジェクト
        
    Raises:
        ValueError: 日付形式が不正な場合
    """
    # 複数の形式を試す
    formats = ["%Y-%m-%d", "%Y%m%d"]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    raise ValueError(f"日付形式が不正です: {date_str}")


def format_number(value: float, decimal_places: int = 2) -> str:
    """数値を見やすい形式にフォーマット
    
    Args:
        value: フォーマットする数値
        decimal_places: 小数点以下の桁数
        
    Returns:
        フォーマット済み文字列
    """
    if abs(value) >= 1e9:
        return f"{value/1e9:.{decimal_places}f}B"
    elif abs(value) >= 1e6:
        return f"{value/1e6:.{decimal_places}f}M"
    elif abs(value) >= 1e3:
        return f"{value/1e3:.{decimal_places}f}K"
    else:
        return f"{value:.{decimal_places}f}"


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """リストを指定サイズのチャンクに分割
    
    Args:
        lst: 分割するリスト
        chunk_size: チャンクサイズ
        
    Returns:
        チャンクのリスト
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """安全な除算（ゼロ除算対策）
    
    Args:
        numerator: 分子
        denominator: 分母
        default: ゼロ除算時のデフォルト値
        
    Returns:
        除算結果
    """
    if denominator == 0:
        return default
    return numerator / denominator


def get_business_days(start_date: date, end_date: date) -> int:
    """営業日数を計算
    
    Args:
        start_date: 開始日
        end_date: 終了日
        
    Returns:
        営業日数
    """
    return pd.bdate_range(start_date, end_date).size
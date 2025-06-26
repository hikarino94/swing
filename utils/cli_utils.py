"""コマンドライン引数処理のユーティリティ"""
import argparse
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Callable, Any
import logging

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> date:
    """日付文字列をdateオブジェクトに変換
    
    Args:
        date_str: YYYY-MM-DD形式の日付文字列
        
    Returns:
        dateオブジェクト
        
    Raises:
        argparse.ArgumentTypeError: 日付形式が不正な場合
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"日付は YYYY-MM-DD 形式で指定してください: {date_str}"
        )


def add_date_arguments(
    parser: argparse.ArgumentParser,
    start_help: str = "開始日 (YYYY-MM-DD)",
    end_help: str = "終了日 (YYYY-MM-DD)"
) -> None:
    """共通の日付引数を追加
    
    Args:
        parser: ArgumentParserインスタンス
        start_help: --startのヘルプメッセージ
        end_help: --endのヘルプメッセージ
    """
    parser.add_argument(
        "--start",
        type=parse_date,
        help=start_help
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        help=end_help
    )


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """共通引数を追加
    
    Args:
        parser: ArgumentParserインスタンス
    """
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="詳細ログを表示"
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="SQLite DB ファイルパス（デフォルト: db/stock.db）"
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="ログファイルパス"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="ログレベル（デフォルト: INFO）"
    )


def add_output_arguments(
    parser: argparse.ArgumentParser,
    default_format: str = "json"
) -> None:
    """出力関連の引数を追加
    
    Args:
        parser: ArgumentParserインスタンス
        default_format: デフォルトの出力形式
    """
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="出力ファイルパス"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["json", "xlsx", "csv"],
        default=default_format,
        help=f"出力形式（デフォルト: {default_format}）"
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="結果を標準出力に表示"
    )


def add_backtest_arguments(parser: argparse.ArgumentParser) -> None:
    """バックテスト関連の引数を追加
    
    Args:
        parser: ArgumentParserインスタンス
    """
    parser.add_argument(
        "--hold-days",
        type=int,
        default=20,
        help="保有日数（デフォルト: 20）"
    )
    parser.add_argument(
        "--capital",
        type=int,
        default=10000000,
        help="初期資金（デフォルト: 10,000,000円）"
    )
    parser.add_argument(
        "--stop-loss",
        type=float,
        help="ストップロス率（0.05 = 5%%）"
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=1,
        help="シグナル日から購入日までのオフセット（デフォルト: 1）"
    )


def validate_date_range(
    start: Optional[date],
    end: Optional[date],
    default_days: int = 30
) -> tuple[date, date]:
    """日付範囲を検証し、デフォルト値を設定
    
    Args:
        start: 開始日
        end: 終了日
        default_days: デフォルトの期間（日数）
        
    Returns:
        (開始日, 終了日)のタプル
        
    Raises:
        ValueError: 日付範囲が不正な場合
    """
    if end is None:
        end = date.today()
    
    if start is None:
        start = end - timedelta(days=default_days)
    
    if start > end:
        raise ValueError(f"開始日({start})が終了日({end})より後になっています")
    
    logger.debug(f"Date range: {start} to {end}")
    return start, end


def create_parser(
    description: str,
    add_subcommands: Optional[Callable[[Any], None]] = None
) -> argparse.ArgumentParser:
    """共通設定済みのArgumentParserを作成
    
    Args:
        description: プログラムの説明
        add_subcommands: サブコマンドを追加する関数
        
    Returns:
        設定済みのArgumentParser
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 共通引数を追加
    add_common_arguments(parser)
    
    # サブコマンドがある場合は追加
    if add_subcommands:
        subparsers = parser.add_subparsers(
            dest="command",
            help="実行するコマンド"
        )
        add_subcommands(subparsers)
    
    return parser


def setup_logging_from_args(args: argparse.Namespace) -> None:
    """コマンドライン引数からロギングを設定
    
    Args:
        args: パース済みの引数
    """
    from .logging_config import configure_root_logger
    
    level = "DEBUG" if args.verbose else args.log_level
    configure_root_logger(level=level, log_file=args.log_file)
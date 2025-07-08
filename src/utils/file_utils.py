"""
ファイル出力ユーティリティ

プロジェクト全体で統一的なファイル出力管理を提供します。
"""

from datetime import datetime
from pathlib import Path
from typing import Literal


def get_output_path(
    category: Literal["backtest", "screening", "reports"],
    filename: str,
    base_dir: Path | None = None,
) -> Path:
    """
    統一的な出力パスを生成します。

    Args:
        category: 出力カテゴリ（backtest, screening, reports）
        filename: ファイル名
        base_dir: ベースディレクトリ（指定しない場合はデフォルトを使用）

    Returns:
        出力ファイルのフルパス
    """
    if base_dir is None:
        # src/utils/から2階層上がプロジェクトルート
        project_root = Path(__file__).resolve().parent.parent.parent
        base_dir = project_root / "data" / "output"

    output_dir = base_dir / category
    output_dir.mkdir(parents=True, exist_ok=True)

    return output_dir / filename


def timestamped_filename(base_name: str, extension: str) -> str:
    """
    タイムスタンプ付きのファイル名を生成します。

    Args:
        base_name: ベースとなるファイル名（拡張子なし）
        extension: ファイル拡張子（ドット付き）

    Returns:
        タイムスタンプ付きファイル名

    Example:
        >>> timestamped_filename("fundamental", ".xlsx")
        "fundamental_20240101_123456.xlsx"
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{timestamp}{extension}"


def get_timestamped_output_path(
    category: Literal["backtest", "screening", "reports"],
    base_name: str,
    extension: str,
    base_dir: Path | None = None,
) -> Path:
    """
    タイムスタンプ付きの出力パスを生成します。

    Args:
        category: 出力カテゴリ
        base_name: ベースとなるファイル名
        extension: ファイル拡張子
        base_dir: ベースディレクトリ

    Returns:
        タイムスタンプ付き出力ファイルのフルパス
    """
    filename = timestamped_filename(base_name, extension)
    return get_output_path(category, filename, base_dir)


def ensure_output_dirs() -> None:
    """
    すべての出力ディレクトリが存在することを確認します。
    """
    project_root = Path(__file__).resolve().parent.parent.parent
    base_dir = project_root / "data" / "output"

    categories = ["backtest", "screening", "reports"]
    for category in categories:
        output_dir = base_dir / category
        output_dir.mkdir(parents=True, exist_ok=True)

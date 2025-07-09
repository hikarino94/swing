"""ファイル管理サービス"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, cast

from src.utils.file_utils import get_timestamped_output_path
from src.utils.logging_config import get_logger

logger = get_logger("services.file")


class FileService:
    """ファイル操作を管理するサービス"""

    @staticmethod
    def get_output_dir() -> Path:
        """出力ディレクトリのパスを取得"""
        return Path(__file__).resolve().parent.parent.parent.parent / "data" / "output"

    @staticmethod
    def create_timestamped_path(
        category: Literal["backtest", "screening", "reports"],
        base_name: str,
        extension: str,
    ) -> Path:
        """タイムスタンプ付きのファイルパスを生成

        Args:
            category: カテゴリ（screening, backtest等）
            base_name: ベースファイル名
            extension: 拡張子（.xlsx, .json等）

        Returns:
            生成されたパス
        """
        return get_timestamped_output_path(category, base_name, extension)

    @staticmethod
    def list_result_files(
        category: str | None = None, days: int = 7
    ) -> list[dict[str, Any]]:
        """結果ファイルのリストを取得

        Args:
            category: フィルタするカテゴリ
            days: 取得する日数

        Returns:
            ファイル情報のリスト
        """
        output_dir = FileService.get_output_dir()
        if not output_dir.exists():
            return []

        files = []
        cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)

        # カテゴリごとにファイルを検索
        categories = [category] if category else ["screening", "backtest", "analysis"]

        for cat in categories:
            cat_dir = output_dir / cat
            if not cat_dir.exists():
                continue

            for file_path in cat_dir.glob("**/*"):
                if file_path.is_file() and file_path.stat().st_mtime > cutoff_date:
                    # ファイル情報を収集
                    stat = file_path.stat()
                    relative_path = file_path.relative_to(output_dir)

                    files.append(
                        {
                            "name": file_path.name,
                            "path": str(relative_path),
                            "category": cat,
                            "size": stat.st_size,
                            "modified": datetime.fromtimestamp(
                                stat.st_mtime
                            ).isoformat(),
                            "extension": file_path.suffix,
                        }
                    )

        # 更新日時で降順ソート
        files.sort(key=lambda x: cast(datetime, x["modified"]), reverse=True)
        return files

    @staticmethod
    def read_json_file(filepath: Path) -> dict[str, Any]:
        """JSONファイルを読み込む

        Args:
            filepath: ファイルパス

        Returns:
            JSONデータ
        """
        with open(filepath, encoding="utf-8") as f:
            return cast(dict[str, Any], json.load(f))

    @staticmethod
    def get_safe_download_path(filepath: str) -> Path | None:
        """安全なダウンロードパスを取得

        Args:
            filepath: リクエストされたファイルパス

        Returns:
            安全なパス、または無効な場合None
        """
        output_dir = FileService.get_output_dir()

        try:
            # パスの正規化とセキュリティチェック
            requested_path = Path(filepath)
            full_path = (output_dir / requested_path).resolve()

            # ディレクトリトラバーサル対策
            if not str(full_path).startswith(str(output_dir)):
                logger.warning(f"不正なファイルアクセス試行: {filepath}")
                return None

            if full_path.exists() and full_path.is_file():
                return full_path

            return None

        except Exception as e:
            logger.error(f"ファイルパスの検証エラー: {e}")
            return None

    @staticmethod
    def save_thresholds(thresholds: dict[str, Any]) -> bool:
        """閾値設定を保存

        Args:
            thresholds: 閾値データ

        Returns:
            成功した場合True
        """
        try:
            threshold_file = Path("screening/thresholds.json")
            threshold_file.parent.mkdir(exist_ok=True)

            with open(threshold_file, "w", encoding="utf-8") as f:
                json.dump(thresholds, f, indent=2, ensure_ascii=False)

            logger.info("閾値設定を保存しました")
            return True

        except Exception as e:
            logger.error(f"閾値設定の保存エラー: {e}")
            return False

    @staticmethod
    def load_thresholds() -> dict[str, Any]:
        """閾値設定を読み込む

        Returns:
            閾値データ
        """
        try:
            threshold_file = Path("screening/thresholds.json")
            if threshold_file.exists():
                with open(threshold_file, encoding="utf-8") as f:
                    return cast(dict[str, Any], json.load(f))
            return {}

        except Exception as e:
            logger.error(f"閾値設定の読み込みエラー: {e}")
            return {}

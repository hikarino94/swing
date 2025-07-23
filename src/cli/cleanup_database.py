"""
データベースクリーンアップスクリプト

1ヶ月以上ログインのないユーザーと古い株価データを削除します。
"""

import argparse
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from src.config import DB_PATH, load_config
from src.utils.db_utils import get_db_adapter
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def backup_data_before_deletion(db_path: str, backup_dir: Path) -> Path:
    """削除前にデータをバックアップ

    Args:
        db_path: データベースパス
        backup_dir: バックアップディレクトリ

    Returns:
        バックアップファイルのパス
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"backup_before_cleanup_{timestamp}.db"

    logger.info(f"データベースをバックアップしています: {backup_path}")

    # SQLiteの場合は単純にファイルコピー
    if db_path.endswith(".db"):
        import shutil

        shutil.copy2(db_path, backup_path)
    else:
        logger.warning("PostgreSQLのバックアップは別途pg_dumpを使用してください")

    return backup_path


def get_inactive_users(days: int = 30, dry_run: bool = True) -> list[dict]:
    """指定日数以上ログインしていないユーザーを取得

    Args:
        days: 非アクティブ判定日数
        dry_run: True の場合は取得のみ、削除しない

    Returns:
        非アクティブユーザーのリスト
    """
    cutoff_date = datetime.now() - timedelta(days=days)

    with get_db_adapter() as db:
        # 最終ログイン日が古いまたはNULLのユーザーを検索
        query = """
        SELECT u.id, u.username, u.email, u.created_at,
               MAX(s.last_accessed) as last_login
        FROM users u
        LEFT JOIN sessions s ON u.id = s.user_id
        GROUP BY u.id, u.username, u.email, u.created_at
        HAVING MAX(s.last_accessed) < ? OR MAX(s.last_accessed) IS NULL
        """

        cursor = db.execute(query, (cutoff_date.isoformat(),))
        inactive_users = []

        for row in db.fetchall(cursor):
            user_data = (
                dict(row)
                if hasattr(row, "keys")
                else {
                    "id": row[0],
                    "username": row[1],
                    "email": row[2],
                    "created_at": row[3],
                    "last_login": row[4],
                }
            )
            inactive_users.append(user_data)

    logger.info(f"{len(inactive_users)}人の非アクティブユーザーを検出しました")

    if dry_run:
        logger.info("ドライランモード: 実際の削除は行いません")
        for user in inactive_users:
            logger.info(
                f"  - {user['username']} ({user['email']}) - 最終ログイン: {user['last_login']}"
            )

    return inactive_users


def delete_inactive_users(user_ids: list[int]) -> int:
    """非アクティブユーザーとその関連データを削除

    Args:
        user_ids: 削除するユーザーIDのリスト

    Returns:
        削除したユーザー数
    """
    if not user_ids:
        return 0

    with get_db_adapter() as db:
        try:
            # カスケード削除（関連データも含めて削除）
            # 1. セッション削除
            db.execute(
                "DELETE FROM sessions WHERE user_id IN ({})".format(
                    ",".join(["?"] * len(user_ids))
                ),
                user_ids,
            )

            # 2. 保有銘柄削除
            db.execute(
                "DELETE FROM holdings WHERE user_id IN ({})".format(
                    ",".join(["?"] * len(user_ids))
                ),
                user_ids,
            )

            # 3. デイトレード記録削除
            db.execute(
                "DELETE FROM daytrade_stocks WHERE user_id IN ({})".format(
                    ",".join(["?"] * len(user_ids))
                ),
                user_ids,
            )
            db.execute(
                "DELETE FROM daytrade_futures WHERE user_id IN ({})".format(
                    ",".join(["?"] * len(user_ids))
                ),
                user_ids,
            )

            # 4. ユーザー削除
            cursor = db.execute(
                "DELETE FROM users WHERE id IN ({})".format(
                    ",".join(["?"] * len(user_ids))
                ),
                user_ids,
            )

            deleted_count = (
                cursor.rowcount if hasattr(cursor, "rowcount") else len(user_ids)
            )
            db.commit()

            logger.info(f"{deleted_count}人のユーザーとその関連データを削除しました")
            return deleted_count

        except Exception as e:
            logger.error(f"ユーザー削除中にエラーが発生しました: {e}")
            db.rollback()
            raise


def get_old_price_data(days: int = 30, dry_run: bool = True) -> dict:
    """古い株価データの統計を取得

    Args:
        days: 何日前のデータを削除対象とするか
        dry_run: True の場合は統計のみ、削除しない

    Returns:
        削除対象データの統計情報
    """
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    with get_db_adapter() as db:
        # 削除対象データの件数を取得
        query = "SELECT COUNT(*) as count FROM prices WHERE date < ?"
        cursor = db.execute(query, (cutoff_date,))
        result = db.fetchone(cursor)

        count = result["count"] if hasattr(result, "keys") else result[0]

        # データ容量の推定（1レコード約100バイトと仮定）
        estimated_size_mb = (count * 100) / (1024 * 1024)

        stats = {
            "count": count,
            "cutoff_date": cutoff_date,
            "estimated_size_mb": round(estimated_size_mb, 2),
        }

        logger.info(
            f"{cutoff_date}以前の株価データ: {count:,}件 (約{estimated_size_mb:.1f}MB)"
        )

        if dry_run:
            logger.info("ドライランモード: 実際の削除は行いません")

    return stats


def delete_old_price_data(days: int = 30) -> int:
    """古い株価データを削除

    Args:
        days: 何日前のデータを削除対象とするか

    Returns:
        削除したレコード数
    """
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    with get_db_adapter() as db:
        try:
            cursor = db.execute("DELETE FROM prices WHERE date < ?", (cutoff_date,))
            deleted_count = cursor.rowcount if hasattr(cursor, "rowcount") else 0

            # 関連するテクニカル指標も削除
            db.execute(
                "DELETE FROM technical_indicators WHERE date < ?", (cutoff_date,)
            )

            db.commit()

            logger.info(f"{deleted_count:,}件の古い株価データを削除しました")
            return deleted_count

        except Exception as e:
            logger.error(f"株価データ削除中にエラーが発生しました: {e}")
            db.rollback()
            raise


def cleanup_database(config: dict, dry_run: bool = True, force: bool = False) -> dict:
    """データベースのクリーンアップを実行

    Args:
        config: クリーンアップ設定
        dry_run: True の場合は実際の削除を行わない
        force: 確認なしで削除を実行

    Returns:
        実行結果の統計情報
    """
    results = {
        "inactive_users": {"count": 0, "deleted": 0},
        "old_prices": {"count": 0, "deleted": 0},
        "backup_path": None,
    }

    # 設定が無効の場合は何もしない
    if not config.get("enabled", False):
        logger.info("データクリーンアップは無効に設定されています")
        return results

    # バックアップ処理
    if config.get("backup_before_delete", True) and not dry_run:
        backup_dir = Path("data/backups")
        backup_dir.mkdir(parents=True, exist_ok=True)
        results["backup_path"] = str(backup_data_before_deletion(DB_PATH, backup_dir))

    # 非アクティブユーザーの処理
    inactive_days = config.get("inactive_user_days", 30)
    inactive_users = get_inactive_users(inactive_days, dry_run)
    results["inactive_users"]["count"] = len(inactive_users)

    if not dry_run and inactive_users:
        if (
            force
            or input(
                f"\n{len(inactive_users)}人のユーザーを削除しますか？ (y/N): "
            ).lower()
            == "y"
        ):
            user_ids = [u["id"] for u in inactive_users]
            deleted = delete_inactive_users(user_ids)
            results["inactive_users"]["deleted"] = deleted

    # 古い株価データの処理
    old_price_days = config.get("old_price_days", 30)
    price_stats = get_old_price_data(old_price_days, dry_run)
    results["old_prices"]["count"] = price_stats["count"]

    if not dry_run and price_stats["count"] > 0:
        if (
            force
            or input(
                f"\n{price_stats['count']:,}件の古い株価データを削除しますか？ (y/N): "
            ).lower()
            == "y"
        ):
            deleted = delete_old_price_data(old_price_days)
            results["old_prices"]["deleted"] = deleted

    # SQLiteの場合はVACUUMを実行
    if not dry_run and DB_PATH.endswith(".db"):
        logger.info("データベースを最適化しています (VACUUM)...")
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("VACUUM")
        logger.info("データベースの最適化が完了しました")

    return results


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description="データベースクリーンアップツール")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="実際に削除を実行（デフォルトはドライラン）",
    )
    parser.add_argument("--force", action="store_true", help="確認なしで削除を実行")
    parser.add_argument(
        "--inactive-days",
        type=int,
        help="非アクティブユーザー判定日数（設定ファイルの値を上書き）",
    )
    parser.add_argument(
        "--price-days", type=int, help="株価データ保持日数（設定ファイルの値を上書き）"
    )

    args = parser.parse_args()

    # 設定を読み込み
    config = load_config()
    cleanup_config = config.get("data_cleanup", {})

    # コマンドライン引数で上書き
    if args.inactive_days:
        cleanup_config["inactive_user_days"] = args.inactive_days
    if args.price_days:
        cleanup_config["old_price_days"] = args.price_days

    # 実行
    dry_run = not args.execute
    results = cleanup_database(cleanup_config, dry_run=dry_run, force=args.force)

    # 結果表示
    print("\n=== クリーンアップ結果 ===")
    print(
        f"非アクティブユーザー: {results['inactive_users']['count']}人検出, "
        f"{results['inactive_users']['deleted']}人削除"
    )
    print(
        f"古い株価データ: {results['old_prices']['count']:,}件検出, "
        f"{results['old_prices']['deleted']:,}件削除"
    )

    if results["backup_path"]:
        print(f"\nバックアップ: {results['backup_path']}")

    if dry_run:
        print("\n※ ドライランモードで実行しました。実際の削除は行われていません。")
        print("実際に削除を実行するには --execute オプションを付けて実行してください。")


if __name__ == "__main__":
    main()

"""ヘルスチェックエンドポイント"""

from flask import Blueprint, jsonify

from src.utils.db_utils import get_db_adapter
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

health_bp = Blueprint("health", __name__)


@health_bp.route("/health")
def health_check():
    """ヘルスチェックエンドポイント

    データベース接続を含む基本的なヘルスチェックを実行します。
    """
    status = {"status": "healthy", "checks": {}}
    is_healthy = True

    # データベース接続チェック
    try:
        with get_db_adapter() as db:
            cursor = db.execute("SELECT 1")
            db.fetchone(cursor)
        status["checks"]["database"] = "ok"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        status["checks"]["database"] = "failed"
        is_healthy = False

    # 全体のステータス
    if not is_healthy:
        status["status"] = "unhealthy"
        return jsonify(status), 503

    return jsonify(status), 200

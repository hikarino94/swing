"""
ポートフォリオ可視化関連のルート
"""

from typing import cast

from flask import Blueprint, jsonify
from flask import request as flask_request

from src.auth import login_required
from src.types.flask_types import RequestWithUser
from src.utils.logging_config import get_logger

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)

# ロガーの設定
logger = get_logger("portfolio.visualize")

# Blueprintの作成（url_prefixなし）
visualize_bp = Blueprint("portfolio_visualize", __name__)


@visualize_bp.route("/visualize/composition", methods=["GET"])
@login_required
def get_portfolio_composition():
    """ポートフォリオ構成円グラフを取得"""
    try:
        from src.portfolio.visualizers import CompositionVisualizer

        visualizer = CompositionVisualizer(request.current_user.id)
        result = visualizer.create_chart()

        if "error" in result:
            return jsonify({"success": False, "error": result["error"]})

        return jsonify({"success": True, "data": result})
    except Exception as e:
        logger.error(f"ポートフォリオ構成グラフ取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@visualize_bp.route("/visualize/performance", methods=["GET"])
@login_required
def get_portfolio_performance():
    """ポートフォリオパフォーマンス推移を取得"""
    try:
        from src.portfolio.visualizers import PerformanceVisualizer

        visualizer = PerformanceVisualizer(request.current_user.id)
        result = visualizer.create_chart()

        if "error" in result:
            return jsonify({"success": False, "error": result["error"]})

        return jsonify({"success": True, "data": result})
    except Exception as e:
        logger.error(f"パフォーマンス推移取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@visualize_bp.route("/visualize/heatmap", methods=["GET"])
@login_required
def get_portfolio_heatmap():
    """ポートフォリオヒートマップを取得"""
    try:
        from src.portfolio.visualizers import HeatmapVisualizer

        visualizer = HeatmapVisualizer(request.current_user.id)
        result = visualizer.create_chart()

        if "error" in result:
            return jsonify({"success": False, "error": result["error"]})

        return jsonify({"success": True, "data": result})
    except Exception as e:
        logger.error(f"ヒートマップ取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})

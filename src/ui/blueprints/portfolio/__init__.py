"""
ポートフォリオBlueprint
"""

from flask import Blueprint

from . import transactions  # noqa: F401
from .base import portfolio_base_bp
from .holdings import holdings_bp
from .visualize import visualize_bp

# メインのportfolio Blueprintを作成
portfolio_bp = Blueprint("portfolio", __name__, url_prefix="/api/portfolio")

# サブBlueprintを登録
portfolio_bp.register_blueprint(portfolio_base_bp)
portfolio_bp.register_blueprint(holdings_bp)
portfolio_bp.register_blueprint(visualize_bp)

__all__ = ["portfolio_bp"]

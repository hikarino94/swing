"""
保有銘柄管理Blueprint
"""

from flask import Blueprint

holdings_bp = Blueprint("holdings", __name__, url_prefix="/api/holdings")

# ルートのインポート（Blueprintの定義後に行う）
from . import routes  # noqa: E402

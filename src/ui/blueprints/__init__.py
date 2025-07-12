"""
Web UIのBlueprintモジュール
"""

from .auth import auth_bp
from .backtest import backtest_bp
from .fetch import fetch_bp
from .portfolio import portfolio_bp
from .results import results_bp
from .screening import screening_bp
from .utils import utils_bp

__all__ = [
    "auth_bp",
    "fetch_bp",
    "screening_bp",
    "backtest_bp",
    "utils_bp",
    "results_bp",
    "portfolio_bp",
]

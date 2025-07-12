"""
バックテスト関連のルート定義
"""

import sys
from typing import cast

from flask import Blueprint, jsonify
from flask import request as flask_request

from src.auth import admin_required, login_required
from src.types.flask_types import RequestWithUser, get_json_value
from src.ui.common import run_command, timestamped_path

# Blueprint作成
backtest_bp = Blueprint("backtest", __name__, url_prefix="/api/backtest")

# 型付きrequest
request: RequestWithUser = cast(RequestWithUser, flask_request)


@backtest_bp.route("/fundamental", methods=["POST"])
@login_required
@admin_required
def backtest_fundamental():
    """ファンダメンタルバックテスト"""
    output_file = timestamped_path("backtest", "fundamental", ".json")
    cmd = [sys.executable, "backtest/backtest_statements.py"]

    hold_days = get_json_value(request, "hold_days")
    if hold_days:
        cmd.extend(["--hold", str(hold_days)])

    entry_offset = get_json_value(request, "entry_offset")
    if entry_offset:
        cmd.extend(["--entry-offset", str(entry_offset)])

    capital = get_json_value(request, "capital")
    if capital:
        cmd.extend(["--capital", str(capital)])

    start_date = get_json_value(request, "start_date")
    if start_date:
        cmd.extend(["--start", start_date])

    end_date = get_json_value(request, "end_date")
    if end_date:
        cmd.extend(["--end", end_date])

    cmd.extend(["--json", output_file])

    result = run_command(" ".join(cmd), "ファンダメンタルバックテスト")
    result["output_file"] = output_file if result["success"] else None
    return jsonify(result)


@backtest_bp.route("/technical", methods=["POST"])
@login_required
@admin_required
def backtest_technical():
    """テクニカルバックテスト"""
    output_file = timestamped_path("backtest", "technical", ".json")
    cmd = [sys.executable, "backtest/backtest_technical.py"]

    hold_days = get_json_value(request, "hold_days")
    if hold_days:
        cmd.extend(["--hold-days", str(hold_days)])

    stop_loss = get_json_value(request, "stop_loss")
    if stop_loss:
        cmd.extend(["--stop-loss", str(stop_loss)])

    capital = get_json_value(request, "capital")
    if capital:
        cmd.extend(["--capital", str(capital)])

    start_date = get_json_value(request, "start_date")
    if start_date:
        cmd.extend(["--start", start_date])

    end_date = get_json_value(request, "end_date")
    if end_date:
        cmd.extend(["--end", end_date])

    cmd.extend(["--json", output_file])

    result = run_command(" ".join(cmd), "テクニカルバックテスト")
    result["output_file"] = output_file if result["success"] else None
    return jsonify(result)


@backtest_bp.route("/ml", methods=["POST"])
@login_required
@admin_required
def backtest_ml():
    """MLバックテスト"""
    output_file = timestamped_path("backtest", "ml", ".json")
    cmd = [sys.executable, "backtest/backtest_ml.py"]

    top = get_json_value(request, "top")
    if top:
        cmd.extend(["--top", str(top)])

    capital = get_json_value(request, "capital")
    if capital:
        cmd.extend(["--capital", str(capital)])

    start_date = get_json_value(request, "start_date")
    if start_date:
        cmd.extend(["--start", start_date])

    end_date = get_json_value(request, "end_date")
    if end_date:
        cmd.extend(["--end", end_date])

    cmd.extend(["--json", output_file])

    result = run_command(" ".join(cmd), "MLバックテスト")
    result["output_file"] = output_file if result["success"] else None
    return jsonify(result)

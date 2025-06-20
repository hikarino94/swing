from __future__ import annotations

import json
import subprocess
import shlex
from pathlib import Path
from typing import Tuple

from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)


def run_command(cmd: str) -> Tuple[str, int]:
    """Run a shell command and return output and exit code."""
    proc = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
    output = proc.stdout + proc.stderr
    return output, proc.returncode


@app.route("/")
def index():
    """Render the main page with all forms."""
    # List Excel and JSON files for results and analysis tabs
    xlsx_files = sorted(Path(".").glob("*.xlsx"))
    json_files = sorted(Path(".").glob("*.json"))
    threshold_path = Path("screening/thresholds.json")
    thresholds = {}
    if threshold_path.is_file():
        with threshold_path.open("r", encoding="utf-8") as f:
            thresholds = json.load(f)
    return render_template(
        "index.html",
        xlsx_files=xlsx_files,
        json_files=json_files,
        thresholds=thresholds,
    )


@app.post("/run/<cmd_name>")
def run(cmd_name: str):
    """Handle form submission and execute commands."""
    form = request.form
    cmd = ""

    if cmd_name == "fetch_quotes":
        cmd = "python fetch/daily_quotes.py"
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "listed_info":
        cmd = "python fetch/listed_info.py"
    elif cmd_name == "statements":
        cmd = f"python fetch/statements.py {form.get('mode', '1')}"
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "screen_fund":
        cmd = (
            f"python screening/screen_statements.py --lookback {form.get('lookback')} "
            f"--recent {form.get('recent')}"
        )
        if form.get("as_of"):
            cmd += f" --as-of {form['as_of']}"
    elif cmd_name == "screen_tech":
        cmd = f"python screening/screen_technical.py {form.get('cmd', 'indicators')}"
        if form.get("as_of"):
            cmd += f" --as-of {form['as_of']}"
        if form.get("lookback"):
            cmd += f" --lookback {form['lookback']}"
    elif cmd_name == "screen_ml":
        cmd = (
            f"python screening/screen_ml.py screen --top {form.get('top', '30')} "
            f"--lookback {form.get('lookback', '1095')}"
        )
        if form.get("retrain"):
            cmd += " --retrain"
    elif cmd_name == "backtest_stmt":
        out = form.get("xlsx", "trades.xlsx")
        cmd = (
            f"python backtest/backtest_statements.py --hold {form.get('hold')} "
            f"--entry-offset {form.get('offset')} --capital {form.get('capital')} "
            f"--xlsx {out}"
        )
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "backtest_tech":
        cmd = (
            f"python backtest/backtest_technical.py --start {form.get('start')} "
            f"--hold-days {form.get('hold')} --stop-loss {form.get('stop')} "
            f"--capital {form.get('capital')} --outfile {form.get('outfile')}"
        )
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "update_token":
        cmd = "python update_idtoken.py"
        if form.get("mail"):
            cmd += f" --mail {form['mail']}"
        if form.get("password"):
            cmd += f" --password {form['password']}"
    elif cmd_name == "db_summary":
        cmd = "python db/db_summary.py"
    elif cmd_name == "list_signals":
        cmd = (
            f"python db/list_signals.py {form.get('kind')} --limit {form.get('limit')}"
        )
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "analyze_json":
        cmd = "python backtest/analyze_backtest_json.py"
        for fname in request.form.getlist("files"):
            cmd += f" {fname}"
        if form.get("side"):
            cmd += f" --side {form['side']}"
        if form.get("show_trades"):
            cmd += " --show-trades"
    else:
        return redirect(url_for("index"))

    output, code = run_command(cmd)
    return render_template("output.html", command=cmd, output=output, code=code)


@app.post("/thresholds")
def save_thresholds():
    """Update thresholds JSON file."""
    threshold_path = Path("screening/thresholds.json")
    data = {k: float(v) for k, v in request.form.items()}
    with threshold_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

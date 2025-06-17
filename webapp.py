"""Simple Flask web interface for swimg commands."""

from __future__ import annotations

import os
import shlex
import subprocess
from functools import wraps
from flask import (
    Flask,
    render_template_string,
    request,
    redirect,
    url_for,
    session,
)
from werkzeug.security import check_password_hash

from update_idtoken import _load_account, DEFAULT_ACCOUNT

LOGIN_ACCOUNT = os.environ.get("LOGIN_ACCOUNT", "login.json")

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "secret")
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

HTML = """
<!doctype html>
<title>swimg web</title>
<h1>swimg Web Interface</h1>
<p><a href="{{ url_for('logout') }}">ログアウト</a></p>

<h2>株価取得</h2>
<form method="post">
  <input type="hidden" name="action" value="fetch_quotes">
  開始日 <input name="start" size="10">
  終了日 <input name="end" size="10">
  <input type="submit" value="実行">
</form>

<hr>
<h2>上場情報取得</h2>
<form method="post">
  <input type="hidden" name="action" value="listed_info">
  <input type="submit" value="実行">
</form>

<hr>
<h2>財務諸表取得</h2>
<form method="post">
  <input type="hidden" name="action" value="statements">
  モード <input name="mode" size="2" value="1">
  開始日 <input name="start" size="10">
  終了日 <input name="end" size="10">
  <input type="submit" value="実行">
</form>

<hr>
<h2>財務スクリーニング</h2>
<form method="post">
  <input type="hidden" name="action" value="screen_statements">
  参照期間 <input name="lookback" value="1095" size="6">
  開示閾値 <input name="recent" value="7" size="4">
  基準日 <input name="as_of" size="10">
  <input type="submit" value="実行">
</form>

<hr>
<h2>テクニカルスクリーニング</h2>
<form method="post">
  <input type="hidden" name="action" value="screen_technical">
  コマンド <input name="command" value="indicators" size="10">
  対象日 <input name="as_of" size="10">
  過去参照日数 <input name="lookback" value="50" size="5">
  <input type="submit" value="実行">
</form>

<hr>
<h2>ファンダメンタルバックテスト</h2>
<form method="post">
  <input type="hidden" name="action" value="backtest_statements">
  保有日数 <input name="hold" value="40" size="4">
  エントリーオフセット <input name="entry_offset" value="1" size="4">
  資金 <input name="capital" value="1000000" size="8">
  開始日 <input name="start" size="10">
  終了日 <input name="end" size="10">
  出力ファイル <input name="xlsx" value="trades.xlsx" size="12">
  <input type="submit" value="実行">
</form>

<hr>
<h2>テクニカルバックテスト</h2>
<form method="post">
  <input type="hidden" name="action" value="backtest_technical">
  開始日 <input name="start" size="10">
  終了日 <input name="end" size="10">
  保有日数 <input name="hold_days" value="5" size="4">
  損切り率 <input name="stop_loss" value="5" size="4">
  資金 <input name="capital" value="1000000" size="8">
  出力ファイル <input name="outfile" value="backtest_results.xlsx" size="18">
  <input type="submit" value="実行">
</form>

<hr>
<h2>IDトークン更新</h2>
<form method="post">
  <input type="hidden" name="action" value="update_token">
  メール <input name="mail">
  パスワード <input name="password" type="password">
  <input type="submit" value="実行">
</form>

{% if output %}
<h2>Output</h2>
<pre>{{ output }}</pre>
{% endif %}
"""

LOGIN_HTML = """
<!doctype html>
<title>Login</title>
<h1>Login</h1>
{% if error %}<p style="color:red;">{{ error }}</p>{% endif %}
<form method="post">
メール <input name="mail">
パスワード <input type="password" name="password">
<input type="submit" value="ログイン">
</form>
"""


def run_command(cmd: str) -> str:
    """Run *cmd* and return combined stdout/stderr."""
    proc = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
    return f"$ {cmd}\n" + proc.stdout + proc.stderr


def login_required(func):
    """Decorator to require login."""

    @wraps(func)
    def _wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return func(*args, **kwargs)

    return _wrapper


@app.route("/", methods=["GET", "POST"])
@login_required
def index():
    output = ""
    if request.method == "POST":
        action = request.form.get("action")
        if action == "fetch_quotes":
            cmd = "python fetch/daily_quotes.py"
            if request.form.get("start"):
                cmd += f" --start {request.form['start']}"
            if request.form.get("end"):
                cmd += f" --end {request.form['end']}"
            output = run_command(cmd)
        elif action == "listed_info":
            output = run_command("python fetch/listed_info.py")
        elif action == "statements":
            cmd = "python fetch/statements.py"
            mode = request.form.get("mode") or "1"
            cmd += f" {mode}"
            if request.form.get("start"):
                cmd += f" --start {request.form['start']}"
            if request.form.get("end"):
                cmd += f" --end {request.form['end']}"
            output = run_command(cmd)
        elif action == "screen_statements":
            cmd = "python screening/screen_statements.py"
            look = request.form.get("lookback") or "1095"
            recent = request.form.get("recent") or "7"
            cmd += f" --lookback {look} --recent {recent}"
            if request.form.get("as_of"):
                cmd += f" --as-of {request.form['as_of']}"
            output = run_command(cmd)
        elif action == "screen_technical":
            cmd = "python screening/screen_technical.py"
            cmd += f" {request.form.get('command', 'indicators')}"
            if request.form.get("as_of"):
                cmd += f" --as-of {request.form['as_of']}"
            if request.form.get("lookback"):
                cmd += f" --lookback {request.form['lookback']}"
            output = run_command(cmd)
        elif action == "backtest_statements":
            cmd = "python backtest/backtest_statements.py"
            hold = request.form.get("hold") or "40"
            off = request.form.get("entry_offset") or "1"
            cap = request.form.get("capital") or "1000000"
            xlsx = request.form.get("xlsx") or "trades.xlsx"
            cmd += f" --hold {hold} --entry-offset {off} --capital {cap} --xlsx {xlsx}"
            if request.form.get("start"):
                cmd += f" --start {request.form['start']}"
            if request.form.get("end"):
                cmd += f" --end {request.form['end']}"
            output = run_command(cmd)
        elif action == "backtest_technical":
            cmd = "python backtest/backtest_technical.py"
            start = request.form.get("start")
            if start:
                cmd += f" --start {start}"
            end = request.form.get("end")
            if end:
                cmd += f" --end {end}"
            hold = request.form.get("hold_days") or "5"
            stop = request.form.get("stop_loss") or "5"
            cap = request.form.get("capital") or "1000000"
            out = request.form.get("outfile") or "backtest_results.xlsx"
            cmd += f" --hold-days {hold} --stop-loss {stop} --capital {cap} --outfile {out}"
            output = run_command(cmd)
        elif action == "update_token":
            cmd = "python update_idtoken.py"
            if request.form.get("mail"):
                cmd += f" --mail {request.form['mail']}"
            if request.form.get("password"):
                cmd += f" --password {request.form['password']}"
            output = run_command(cmd)
    return render_template_string(HTML, output=output)


@app.route("/login", methods=["GET", "POST"])
def login():
    a_mail, a_pwd, a_hash = _load_account(LOGIN_ACCOUNT)
    if not a_mail and not a_pwd and not a_hash:
        a_mail, a_pwd, a_hash = _load_account(DEFAULT_ACCOUNT)
    error = ""
    if request.method == "POST":
        mail = request.form.get("mail", "")
        password = request.form.get("password", "")
        ok = False
        if mail == a_mail:
            if a_hash:
                ok = check_password_hash(a_hash, password)
            else:
                ok = password == a_pwd
        if ok:
            session["logged_in"] = True
            return redirect(url_for("index"))
        error = "ログインに失敗しました"
    return render_template_string(LOGIN_HTML, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    host = os.environ.get("FLASK_HOST", "127.0.0.1")
    port = int(os.environ.get("FLASK_PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "1") == "1"
    app.run(host=host, port=port, debug=debug)

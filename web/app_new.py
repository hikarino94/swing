from __future__ import annotations

import json
import logging
import os
import queue
import shlex
import subprocess
import threading
import uuid
from datetime import datetime
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

app = Flask(__name__)
app.secret_key = "swing_trading_secret_key_2025"


# Jinja2フィルターを追加
@app.template_filter("datetime_format")
def datetime_format(timestamp):
    """Unix timestampを読みやすい日時形式に変換"""
    try:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return "Unknown"


# アプリケーションログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("web_app.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ログメッセージのキュー
log_queue = queue.Queue(maxsize=1000)

# 実行中のタスク管理
running_tasks = {}
task_outputs = {}


class LogHandler(logging.Handler):
    """ログをキューに保存するハンドラー"""

    def emit(self, record):
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "message": self.format(record),
        }
        try:
            log_queue.put_nowait(log_entry)
        except queue.Full:
            # キューが満杯の場合は古いものを削除
            try:
                log_queue.get_nowait()
                log_queue.put_nowait(log_entry)
            except queue.Empty:
                pass


# ログハンドラーを追加
log_handler = LogHandler()
log_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
log_handler.setFormatter(formatter)
logging.getLogger().addHandler(log_handler)


def run_command_async(cmd: str, task_id: str) -> None:
    """Run a shell command asynchronously and store results."""
    logger.info(f"🚀 コマンド実行開始: {cmd} (ID: {task_id})")

    # タスク開始を記録
    running_tasks[task_id] = {"command": cmd, "status": "running", "start_time": datetime.now(), "pid": None}

    try:
        # PATH設定を追加
        env = os.environ.copy()
        env["PATH"] = f"{os.path.expanduser('~/.local/bin')}:{env.get('PATH', '')}"

        # プロジェクトルートディレクトリを設定
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        proc = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            cwd=project_root,  # 作業ディレクトリを明示的に設定
            bufsize=1,
            universal_newlines=True,
        )

        # PIDを記録
        running_tasks[task_id]["pid"] = proc.pid
        logger.info(f"📊 プロセス開始 PID: {proc.pid}")

        output_lines = []

        # リアルタイムで出力を読み取り
        if proc.stdout:
            for line in iter(proc.stdout.readline, ""):
                if line:
                    output_lines.append(line.rstrip())
                    logger.info(f"[{task_id[:8]}] {line.rstrip()}")

        proc.wait()

        full_output = "\n".join(output_lines)

        # タスク完了を記録
        running_tasks[task_id].update(
            {
                "status": "completed" if proc.returncode == 0 else "failed",
                "end_time": datetime.now(),
                "return_code": proc.returncode,
            }
        )

        task_outputs[task_id] = {"output": full_output, "return_code": proc.returncode, "command": cmd}

        if proc.returncode == 0:
            logger.info(f"✅ コマンド実行完了: {cmd} (ID: {task_id})")
        else:
            logger.error(f"❌ コマンド実行失敗: {cmd} (ID: {task_id}, Code: {proc.returncode})")

    except Exception as e:
        logger.error(f"💥 コマンド実行エラー: {e} (ID: {task_id})")
        running_tasks[task_id].update({"status": "error", "end_time": datetime.now(), "error": str(e)})
        task_outputs[task_id] = {"output": f"エラーが発生しました: {e}", "return_code": -1, "command": cmd}


@app.route("/")
def index():
    """Render the main page with all forms."""
    # List Excel and JSON files for results and analysis tabs
    xlsx_files = sorted(Path(".").glob("*.xlsx"))
    json_files = sorted(Path(".").glob("*.json"))

    # 閾値設定ファイルを読み込み
    threshold_path = Path("src/analysis/thresholds.json")
    thresholds = {}
    if threshold_path.is_file():
        with threshold_path.open("r", encoding="utf-8") as f:
            thresholds = json.load(f)

    return render_template(
        "index_new.html",
        xlsx_files=xlsx_files,
        json_files=json_files,
        thresholds=thresholds,
    )


@app.post("/run/<cmd_name>")
def run(cmd_name: str):
    """Handle form submission and execute commands."""
    form = request.form
    cmd = ""

    # タスクIDを生成
    task_id = str(uuid.uuid4())

    if cmd_name == "fetch_quotes":
        cmd = "python3 fetch/daily_quotes.py"
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
        if form.get("workers"):
            cmd += f" --workers {form['workers']}"
    elif cmd_name == "listed_info":
        cmd = "python3 fetch/listed_info.py"
    elif cmd_name == "statements":
        cmd = f"python3 fetch/statements.py {form.get('mode', '1')}"
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "screen_fund":
        cmd = (
            f"python3 screening/screen_statements.py --lookback {form.get('lookback')} "
            f"--recent {form.get('recent')}"
        )
        if form.get("as_of"):
            cmd += f" --as-of {form['as_of']}"
    elif cmd_name == "screen_tech":
        cmd = f"python3 screening/screen_technical.py {form.get('cmd', 'indicators')}"
        if form.get("as_of"):
            cmd += f" --as-of {form['as_of']}"
        if form.get("lookback"):
            cmd += f" --lookback {form['lookback']}"
    elif cmd_name == "screen_ml":
        cmd = (
            f"python3 screening/screen_ml.py screen --top {form.get('top', '30')} "
            f"--lookback {form.get('lookback', '1095')}"
        )
        if form.get("retrain"):
            cmd += " --retrain"
    elif cmd_name == "backtest_stmt":
        out = form.get("xlsx", "trades.xlsx")
        cmd = (
            f"python3 backtest/backtest_statements.py --hold {form.get('hold')} "
            f"--offset {form.get('offset')} --capital {form.get('capital')} "
            f"--xlsx {out}"
        )
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "backtest_tech":
        cmd = (
            f"python3 backtest/backtest_technical.py --start {form.get('start')} "
            f"--hold-days {form.get('hold')} --stop-loss {form.get('stop')} "
            f"--capital {form.get('capital')} --outfile {form.get('outfile')}"
        )
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "update_token":
        cmd = "python3 update_idtoken.py"
        if form.get("mail"):
            cmd += f" --mail {form['mail']}"
        if form.get("password"):
            cmd += f" --password {form['password']}"
    elif cmd_name == "db_summary":
        cmd = "python3 db/db_summary.py"
    elif cmd_name == "list_signals":
        cmd = f"python3 db/list_signals.py {form.get('kind')} --limit {form.get('limit')}"
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "analyze_json":
        cmd = "python3 backtest/analyze_backtest_json.py"
        for fname in request.form.getlist("files"):
            cmd += f" {fname}"
        if form.get("side"):
            cmd += f" --side {form['side']}"
        if form.get("show_trades"):
            cmd += " --show-trades"
    else:
        flash("未知のコマンドです", "error")
        return redirect(url_for("index"))

    # 非同期でコマンドを実行
    thread = threading.Thread(target=run_command_async, args=(cmd, task_id))
    thread.daemon = True
    thread.start()

    # 実行中ページにリダイレクト
    return redirect(url_for("task_status", task_id=task_id))


@app.route("/thresholds")
def thresholds():
    """閾値設定ページを表示"""
    threshold_path = Path("src/analysis/thresholds.json")
    thresholds = {}
    if threshold_path.is_file():
        with threshold_path.open("r", encoding="utf-8") as f:
            thresholds = json.load(f)
    return render_template("thresholds.html", thresholds=thresholds)


@app.post("/thresholds")
def save_thresholds():
    """Update thresholds JSON file."""
    threshold_path = Path("src/analysis/thresholds.json")

    try:
        data = {}
        for k, v in request.form.items():
            try:
                data[k] = float(v)
            except ValueError:
                flash(f"無効な値です: {k} = {v}", "error")
                return redirect(url_for("thresholds"))

        # バックアップを作成
        if threshold_path.exists():
            backup_path = threshold_path.with_suffix(f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            import shutil

            shutil.copy2(threshold_path, backup_path)

        with threshold_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        flash("閾値設定を保存しました", "success")
        logger.info(f"閾値設定を更新しました: {len(data)}個のパラメータ")

    except Exception as e:
        flash(f"保存中にエラーが発生しました: {e}", "error")
        logger.error(f"閾値設定保存エラー: {e}")

    return redirect(url_for("thresholds"))


@app.route("/results")
def results():
    """結果ファイル一覧ページ"""
    xlsx_files = sorted(Path(".").glob("*.xlsx"))
    json_files = sorted(Path(".").glob("*.json"))
    return render_template("results.html", xlsx_files=xlsx_files, json_files=json_files)


@app.route("/logs")
def logs():
    """ログ表示ページ"""
    return render_template("logs_new.html")


@app.route("/api/logs")
def api_logs():
    """ログデータをJSON形式で返すAPI"""
    logs = []
    try:
        while True:
            log_entry = log_queue.get_nowait()
            logs.append(log_entry)
    except queue.Empty:
        pass

    # 最新100件に制限
    return jsonify(logs[-100:])


@app.route("/status")
def status():
    """アプリケーション状態確認"""
    status_info = {
        "status": "running",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "log_count": log_queue.qsize(),
        "running_tasks": len([t for t in running_tasks.values() if t["status"] == "running"]),
        "total_tasks": len(running_tasks),
    }
    return jsonify(status_info)


@app.route("/task/<task_id>")
def task_status(task_id: str):
    """タスクの実行状況を表示"""
    if task_id not in running_tasks:
        flash("指定されたタスクが見つかりません", "error")
        return redirect(url_for("index"))

    task_info = running_tasks[task_id]
    return render_template("task_status_new.html", task_id=task_id, task_info=task_info)


@app.route("/api/task/<task_id>")
def api_task_status(task_id: str):
    """タスク状態をJSONで返す"""
    if task_id not in running_tasks:
        return jsonify({"error": "Task not found"}), 404

    task_info = running_tasks[task_id].copy()

    # 時刻を文字列に変換
    if "start_time" in task_info:
        task_info["start_time"] = task_info["start_time"].strftime("%Y-%m-%d %H:%M:%S")
    if "end_time" in task_info:
        task_info["end_time"] = task_info["end_time"].strftime("%Y-%m-%d %H:%M:%S")

    # 結果がある場合は追加
    if task_id in task_outputs:
        task_info["output"] = task_outputs[task_id]

    return jsonify(task_info)


@app.route("/api/tasks")
def api_all_tasks():
    """全タスクの状態を返す"""
    tasks = {}
    for task_id, task_info in running_tasks.items():
        task_copy = task_info.copy()
        if "start_time" in task_copy:
            task_copy["start_time"] = task_copy["start_time"].strftime("%Y-%m-%d %H:%M:%S")
        if "end_time" in task_copy:
            task_copy["end_time"] = task_copy["end_time"].strftime("%Y-%m-%d %H:%M:%S")
        tasks[task_id] = task_copy

    return jsonify(tasks)


@app.route("/api/system-info")
def api_system_info():
    """システム情報API"""
    import platform

    import psutil

    # プロジェクトルート
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # ディスク使用量
    disk_usage = psutil.disk_usage(project_root)

    # データベースファイルサイズ
    db_path = Path(project_root) / "db" / "stock.db"
    db_size = db_path.stat().st_size if db_path.exists() else 0

    return jsonify(
        {
            "system": {
                "platform": platform.system(),
                "python_version": platform.python_version(),
                "cpu_count": psutil.cpu_count(),
                "memory_total": psutil.virtual_memory().total,
                "memory_available": psutil.virtual_memory().available,
            },
            "storage": {
                "disk_total": disk_usage.total,
                "disk_used": disk_usage.used,
                "disk_free": disk_usage.free,
                "db_size": db_size,
            },
            "app": {
                "tasks_running": len([t for t in running_tasks.values() if t["status"] == "running"]),
                "tasks_total": len(running_tasks),
                "log_queue_size": log_queue.qsize(),
            },
        }
    )


def log_startup_info():
    """起動情報をログに出力"""
    port = int(os.environ.get("PORT", 8080))
    logger.info("=" * 60)
    logger.info("🚀 Swing Trading Web App 起動中...")
    logger.info(f"📅 起動時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    logger.info("🌐 アクセスURL:")
    logger.info(f"   メイン: http://localhost:{port}")
    logger.info(f"   ログ: http://localhost:{port}/logs")
    logger.info(f"   ステータス: http://localhost:{port}/status")
    logger.info(f"   閾値設定: http://localhost:{port}/thresholds")
    logger.info(f"   結果一覧: http://localhost:{port}/results")
    logger.info("")
    logger.info("🛠️ 機能:")
    logger.info("   ・ リアルタイムログ監視")
    logger.info("   ・ 非同期タスク実行")
    logger.info("   ・ 進捗状況確認")
    logger.info("   ・ 閾値設定管理")
    logger.info("   ・ 結果ファイル管理")
    logger.info("=" * 60)
    logger.info("✅ アプリケーションが正常に起動しました")
    logger.info("💬 コマンド実行時はリアルタイムで進捗状況を表示します")
    print("\n" + "=" * 60)
    print("🚀 Swing Trading Web App 起動完了!")
    print(f"🌐 ブラウザでアクセス: http://localhost:{port}")
    print(f"📊 リアルタイムログ: http://localhost:{port}/logs")
    print(f"⚙️ 閾値設定: http://localhost:{port}/thresholds")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    # 起動情報をログ出力
    log_startup_info()

    try:
        print("🔄 サーバー開始中... (Ctrl+C で終了)")
        port = int(os.environ.get("PORT", 8080))
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("🛑 アプリケーションを終了しています...")
        print("\n🛑 アプリケーションを終了しました")
    except Exception as e:
        logger.error(f"❌ エラーが発生しました: {e}")
        print(f"\n❌ エラー: {e}")
        raise

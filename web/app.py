from __future__ import annotations

import json
import logging
import os
import queue
import shlex
import subprocess
import sys
import threading
import uuid
from datetime import datetime
from pathlib import Path

from app_terminal_logger import TerminalLogger, terminal_logger, terminal_print
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
    handlers=[
        logging.FileHandler("web_app.log", encoding="utf-8"),
        logging.StreamHandler(stream=sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Werkzeugのログレベルを上げて、アクセスログを抑制
logging.getLogger("werkzeug").setLevel(logging.WARNING)

# ログメッセージのキュー
log_queue: queue.Queue = queue.Queue(maxsize=1000)

# 実行中のタスク管理
running_tasks = {}
task_outputs = {}


class LogHandler(logging.Handler):
    """ログをキューに保存するハンドラー（Web表示用）"""

    def emit(self, record):
        # Webには簡潔なメッセージのみ表示
        # デバッグ情報や詳細なログはスキップ
        message = self.format(record)

        # 特定のキーワードを含むログはWebに表示しない
        skip_keywords = [
            "form data:",
            "processed kind:",
            "final command:",
            "Invalid kind value:",
            "📊 プロセス開始 PID:",
            "task_outputs",
        ]

        if any(keyword in message for keyword in skip_keywords):
            return

        # 長すぎるログは要約
        if len(message) > 200:
            message = message[:197] + "..."

        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "message": message,
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
    # ターミナルには詳細情報を表示
    TerminalLogger.log_command_start(task_id, cmd)
    terminal_logger.info(f"タスク開始: {task_id} - {cmd}")

    # Webには簡潔なメッセージ
    logger.info(f"🚀 コマンド実行開始: {cmd.split()[3] if len(cmd.split()) > 3 else 'タスク'}")

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
        TerminalLogger.log_process_start(proc.pid)
        terminal_logger.info(f"プロセス起動 PID: {proc.pid}")

        output_lines = []
        line_count = 0

        # リアルタイムで出力を読み取り
        if proc.stdout:
            for line in iter(proc.stdout.readline, ""):
                if line:
                    stripped_line = line.rstrip()
                    output_lines.append(stripped_line)
                    line_count += 1

                    # ターミナルには全ての出力を表示
                    TerminalLogger.log_output_line(task_id, stripped_line)

                    # Webには重要な行のみ表示（10行ごとに進捗表示）
                    if line_count % 10 == 0:
                        logger.info(f"処理中... ({line_count}行処理済み)")

        proc.wait()

        full_output = "\n".join(output_lines)

        # ターミナルに完了情報を表示
        TerminalLogger.log_command_end(proc.returncode == 0, proc.returncode, line_count)
        terminal_logger.info(f"タスク完了: {task_id} - 終了コード: {proc.returncode}")

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
            logger.info("✅ タスク完了")
        else:
            logger.error(f"❌ タスク失敗 (終了コード: {proc.returncode})")

    except Exception as e:
        TerminalLogger.log_error(e)
        terminal_logger.error(f"タスクエラー: {task_id} - {e}")
        logger.error("💥 タスクエラー")
        running_tasks[task_id].update({"status": "error", "end_time": datetime.now(), "error": str(e)})
        task_outputs[task_id] = {"output": f"エラーが発生しました: {e}", "return_code": -1, "command": cmd}


@app.route("/")
def index():
    """Redirect to menu interface."""
    return redirect(url_for("menu"))


@app.route("/classic")
def classic():
    """Render the classic interface with all forms."""
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


@app.route("/menu")
def menu():
    """Render the menu-driven interface."""
    return render_template("index_menu_new.html")


@app.post("/run/<cmd_name>")
def run(cmd_name: str):
    """Handle form submission and execute commands."""
    form = request.form
    cmd = ""

    # タスクIDを生成
    task_id = str(uuid.uuid4())

    if cmd_name == "fetch_quotes":
        cmd = "python3 -m src.api.daily_quotes"
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
        if form.get("workers"):
            cmd += f" --workers {form['workers']}"
    elif cmd_name == "listed_info":
        cmd = "python3 -m src.api.listed_info"
    elif cmd_name == "statements":
        cmd = f"python3 -m src.api.statements {form.get('mode', '1')}"
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "screen_fund":
        cmd = (
            f"python3 -m src.analysis.screen_statements --lookback {form.get('lookback')} "
            f"--recent {form.get('recent')}"
        )
        if form.get("as_of"):
            cmd += f" --as-of {form['as_of']}"
    elif cmd_name == "screen_tech":
        cmd = f"python3 -m src.analysis.screen_technical {form.get('cmd', 'indicators')}"
        if form.get("as_of"):
            cmd += f" --as-of {form['as_of']}"
        if form.get("lookback"):
            cmd += f" --lookback {form['lookback']}"
    elif cmd_name == "screen_ml":
        cmd = (
            f"python3 -m src.analysis.screen_ml screen --top {form.get('top', '30')} "
            f"--lookback {form.get('lookback', '1095')}"
        )
        if form.get("retrain"):
            cmd += " --retrain"
    elif cmd_name == "backtest_stmt":
        out = form.get("xlsx", "trades.xlsx")
        cmd = (
            f"python3 -m src.backtest.backtest_statements --hold {form.get('hold')} "
            f"--offset {form.get('offset')} --capital {form.get('capital')} "
            f"--xlsx {out}"
        )
        if form.get("start"):
            cmd += f" --start {form['start']}"
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "backtest_tech":
        cmd = (
            f"python3 -m src.backtest.backtest_technical --start {form.get('start')} "
            f"--hold-days {form.get('hold')} --stop-loss {form.get('stop')} "
            f"--capital {form.get('capital')} --outfile {form.get('outfile')}"
        )
        if form.get("end"):
            cmd += f" --end {form['end']}"
    elif cmd_name == "update_token":
        cmd = "python3 -m tools.update_idtoken"
        mail = form.get("mail")
        pwd = form.get("password")
        if not mail or not pwd:
            try:
                from src.utils.config import get_config_manager

                account = get_config_manager().get_account_info()
                mail = mail or account.get("mailaddress")
                pwd = pwd or account.get("password")
            except Exception as e:  # pylint: disable=broad-except
                logger.warning("account.jsonの読み込みに失敗: %s", e)
                terminal_print(f"account.json load failed: {e}")

        if mail:
            cmd += f" --mail {mail}"
        if pwd:
            cmd += f" --password {pwd}"
    elif cmd_name == "db_summary":
        cmd = "python3 -m data.db.db_summary"
    elif cmd_name == "list_signals":
        # フォームデータのデバッグ出力（ターミナルにも表示）
        form_dict = dict(form)
        terminal_print(f"[DEBUG] list_signals - form data: {form_dict}")
        logger.info(f"list_signals - form data: {form_dict}")

        kind = form.get("kind")
        limit = form.get("limit", "100")

        # kindが取得できない、None、'None'文字列の場合はデフォルト値を使用
        if not kind or str(kind).strip() in ["", "None", "null", "undefined"]:
            terminal_print(f"[DEBUG] kind value is empty or invalid: '{kind}', using default 'fund'")
            kind = "fund"

        # limitも同様に処理
        if not limit or str(limit).strip() in ["", "None", "null", "undefined"]:
            terminal_print(f"[DEBUG] limit value is empty or invalid: '{limit}', using default '100'")
            limit = "100"

        # 値を文字列として確実に処理
        kind = str(kind).strip()
        limit = str(limit).strip()

        # 有効な値かチェック
        if kind not in ["fund", "tech"]:
            terminal_print(f"[WARNING] Invalid kind value: '{kind}', using default 'fund'")
            logger.warning(f"Invalid kind value: '{kind}', using default 'fund'")
            kind = "fund"

        terminal_print(f"[DEBUG] list_signals - processed kind: '{kind}', limit: '{limit}'")
        logger.info(f"list_signals - processed kind: '{kind}', limit: '{limit}'")

        cmd = f"python3 -m data.db.list_signals {kind} --limit {limit}"

        start_date = form.get("start")
        end_date = form.get("end")

        if start_date and str(start_date).strip():
            cmd += f" --start {start_date}"
        if end_date and str(end_date).strip():
            cmd += f" --end {end_date}"

        terminal_print(f"[DEBUG] list_signals - final command: {cmd}")
        logger.info(f"list_signals - final command: {cmd}")
    elif cmd_name == "analyze_json":
        cmd = "python3 -m src.backtest.analyze_backtest_json"
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

    # Ajaxリクエストの場合はJSONレスポンスを返す
    if request.headers.get("Content-Type") == "application/json" or request.headers.get("Accept") == "application/json":
        return jsonify(
            {"success": True, "task_id": task_id, "command": cmd, "message": f"タスクを開始しました (ID: {task_id})"}
        )

    # 通常のフォーム送信の場合は実行中ページにリダイレクト
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
    return render_template("logs.html")


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


@app.route("/api/results")
def api_results():
    """結果ファイル一覧をJSON形式で返すAPI"""
    try:
        # プロジェクトルートディレクトリを取得
        project_root = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        # Excel and JSON files in project root
        xlsx_files = [f.name for f in sorted(project_root.glob("*.xlsx"))]
        json_files = [f.name for f in sorted(project_root.glob("*.json"))]

        # data/exports ディレクトリからもファイルを取得
        exports_dir = project_root / "data" / "exports"
        if exports_dir.exists():
            xlsx_files.extend([f"data/exports/{f.name}" for f in sorted(exports_dir.glob("*.xlsx"))])
            json_files.extend([f"data/exports/{f.name}" for f in sorted(exports_dir.glob("*.json"))])

        all_files = xlsx_files + json_files

        return jsonify(
            {
                "success": True,
                "files": all_files,
                "xlsx_files": xlsx_files,
                "json_files": json_files,
                "count": len(all_files),
            }
        )

    except Exception as e:
        logger.error(f"結果ファイル一覧取得エラー: {e}")
        return jsonify({"success": False, "error": str(e), "files": []}), 500


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
    return render_template("task_status.html", task_id=task_id, task_info=task_info)


@app.route("/api/task/<task_id>")
def api_task_status(task_id: str):
    """タスク状態をJSONで返す"""
    if task_id not in running_tasks:
        return jsonify({"error": "Task not found"}), 404

    task_info = running_tasks[task_id].copy()

    # 時刻を文字列に変換
    if "start_time" in task_info:
        start_time = task_info["start_time"]
        if isinstance(start_time, datetime):
            task_info["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    if "end_time" in task_info:
        end_time = task_info["end_time"]
        if isinstance(end_time, datetime):
            task_info["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")

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
            start_time = task_copy["start_time"]
            if isinstance(start_time, datetime):
                task_copy["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        if "end_time" in task_copy:
            end_time = task_copy["end_time"]
            if isinstance(end_time, datetime):
                task_copy["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
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
    db_path = Path(project_root) / "data" / "db" / "stock.db"
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
    # ターミナルへの直接出力
    print("\n" + "=" * 60)
    print("🚀 Swing Trading Web App 起動完了!")
    print(f"🌐 ブラウザでアクセス: http://localhost:{port}")
    print(f"📊 リアルタイムログ: http://localhost:{port}/logs")
    print(f"⚙️ 閾値設定: http://localhost:{port}/thresholds")
    print("=" * 60)
    print("📝 コマンド実行時の詳細ログはこのターミナルに表示されます")
    print("=" * 60 + "\n")

    # Webアプリ用のログ
    logger.info("✅ アプリケーションが正常に起動しました")


if __name__ == "__main__":
    # 起動情報をログ出力
    log_startup_info()

    # ターミナルログの初期テスト
    terminal_logger.info("ターミナルログシステム起動確認")
    print("📝 ターミナルログ出力テスト - この行が見えていればログは正常に動作しています")

    try:
        print("🔄 サーバー開始中... (Ctrl+C で終了)")
        port = int(os.environ.get("PORT", 8080))
        # threaded=Trueを追加して、スレッドでのログ出力を確実にする
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
    except KeyboardInterrupt:
        logger.info("🛑 アプリケーションを終了しています...")
        print("\n🛑 アプリケーションを終了しました")
    except Exception as e:
        logger.error(f"❌ エラーが発生しました: {e}")
        print(f"\n❌ エラー: {e}")
        raise

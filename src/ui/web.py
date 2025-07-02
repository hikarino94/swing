#!/usr/bin/env python3
"""
Swing Trading Tool - モダンなWeb UI版
タブ型インターフェースでGUIアプリの機能を統合
"""

import json
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, render_template, request, send_file

# プロジェクトルートをPYTHONPATHに追加
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.config import DB_PATH
from src.utils.file_utils import get_timestamped_output_path

# プロジェクトルートとテンプレートディレクトリのパスを設定
project_root = Path(__file__).resolve().parent.parent.parent
template_dir = project_root / "templates"

app = Flask(__name__, template_folder=str(template_dir))
app.config["SECRET_KEY"] = (
    "your-secret-key-here"  # 本番環境では環境変数から取得すること
)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max file size


def timestamped_path(category, base_name, extension):
    """タイムスタンプ付きのファイルパスを生成"""
    return str(get_timestamped_output_path(category, base_name, extension))


def run_command(command, description="コマンド実行中"):
    """コマンドを実行し、結果を返す"""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            shell=True,
            cwd=project_root,  # プロジェクトルート
        )
        return {
            "success": result.returncode == 0,
            "output": result.stdout,
            "error": result.stderr,
            "description": description,
        }
    except Exception as e:
        return {
            "success": False,
            "output": "",
            "error": str(e),
            "description": description,
        }


@app.route("/")
def index():
    """メインページ"""
    return render_template("index.html")


@app.route("/api/fetch/quotes", methods=["POST"])
def fetch_quotes():
    """株価データ取得"""
    data = request.json
    cmd = [sys.executable, "fetch/daily_quotes.py"]

    if data.get("start_date"):
        cmd.extend(["--start", data["start_date"]])
    if data.get("end_date"):
        cmd.extend(["--end", data["end_date"]])

    return jsonify(run_command(" ".join(cmd), "株価データ取得"))


@app.route("/api/fetch/listed", methods=["POST"])
def fetch_listed():
    """上場情報取得"""
    cmd = [sys.executable, "fetch/listed_info.py"]
    return jsonify(run_command(" ".join(cmd), "上場情報取得"))


@app.route("/api/fetch/statements", methods=["POST"])
def fetch_statements():
    """財務諸表取得"""
    data = request.json
    cmd = [sys.executable, "fetch/statements.py"]

    mode = data.get("mode", "2")  # デフォルトは日次取得モード
    cmd.append(mode)

    if data.get("start_date"):
        cmd.extend(["--start", data["start_date"]])
    if data.get("end_date"):
        cmd.extend(["--end", data["end_date"]])

    return jsonify(run_command(" ".join(cmd), f"財務諸表{mode}"))


@app.route("/api/screen/fundamental", methods=["POST"])
def screen_fundamental():
    """ファンダメンタルスクリーニング"""
    data = request.json
    cmd = [sys.executable, "screening/screen_statements.py"]

    if data.get("lookback"):
        cmd.extend(["--lookback", str(data["lookback"])])
    if data.get("recent"):
        cmd.extend(["--recent", str(data["recent"])])
    if data.get("as_of"):
        cmd.extend(["--as-of", data["as_of"]])

    result = run_command(" ".join(cmd), "ファンダメンタルスクリーニング")

    # スクリーニング成功時、DBから結果を取得してExcelファイルを生成
    if result["success"]:
        try:
            output_file = timestamped_path("screening", "fundamental", ".xlsx")
            conn = sqlite3.connect(DB_PATH)

            # 最新のシグナルを取得
            query = """
                SELECT fs.*, li.company_name
                FROM fundamental_signals fs
                LEFT JOIN listed_info li ON fs.LocalCode = li.code
                WHERE DATE(fs.created_at) = DATE('now')
                ORDER BY fs.created_at DESC
            """
            df = pd.read_sql(query, conn)
            conn.close()

            if not df.empty:
                # Excelファイルに出力
                with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                    df.to_excel(writer, sheet_name="Signals", index=False)

                    # 列幅の自動調整
                    worksheet = writer.sheets["Signals"]
                    for i, col in enumerate(df.columns):
                        max_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
                        worksheet.set_column(i, i, min(max_len, 50))

                result["output_file"] = output_file
            else:
                result["output_file"] = None
                result["message"] = "スクリーニング結果がありません"
        except Exception as e:
            result["error"] += f"\nExcel出力エラー: {str(e)}"
            result["output_file"] = None
    else:
        result["output_file"] = None

    return jsonify(result)


@app.route("/api/screen/technical", methods=["POST"])
def screen_technical():
    """テクニカルスクリーニング"""
    data = request.json
    cmd = [sys.executable, "screening/screen_technical.py"]

    action = data.get("action", "screen")
    cmd.append(action)

    if action == "screen":
        if data.get("as_of"):
            cmd.extend(["--as-of", data["as_of"]])
        if data.get("lookback"):
            cmd.extend(["--lookback", str(data["lookback"])])

    result = run_command(" ".join(cmd), f"テクニカル{action}")

    # screen実行成功時、DBから結果を取得してExcelファイルを生成
    if result["success"] and action == "screen":
        try:
            output_file = timestamped_path("screening", "technical", ".xlsx")
            conn = sqlite3.connect(DB_PATH)

            # 最新のシグナルを取得
            as_of_date = data.get("as_of")
            if as_of_date:
                query = """
                    SELECT ti.*, li.company_name
                    FROM technical_indicators ti
                    LEFT JOIN listed_info li ON ti.code = li.code
                    WHERE ti.signal_date = ?
                    AND (ti.signals_count >= 3 OR ti.signals_short_count >= 3)
                    ORDER BY ti.signals_count DESC, ti.signals_short_count DESC
                """
                df = pd.read_sql(query, conn, params=[as_of_date])
            else:
                query = """
                    SELECT ti.*, li.company_name
                    FROM technical_indicators ti
                    LEFT JOIN listed_info li ON ti.code = li.code
                    WHERE ti.signal_date = (SELECT MAX(signal_date) FROM technical_indicators)
                    AND (ti.signals_count >= 3 OR ti.signals_short_count >= 3)
                    ORDER BY ti.signals_count DESC, ti.signals_short_count DESC
                """
                df = pd.read_sql(query, conn)

            conn.close()

            if not df.empty:
                # Excelファイルに出力
                with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                    df.to_excel(writer, sheet_name="Signals", index=False)

                    # 列幅の自動調整
                    worksheet = writer.sheets["Signals"]
                    for i, col in enumerate(df.columns):
                        max_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
                        worksheet.set_column(i, i, min(max_len, 50))

                result["output_file"] = output_file
            else:
                result["output_file"] = None
                result["message"] = "スクリーニング結果がありません"
        except Exception as e:
            result["error"] += f"\nExcel出力エラー: {str(e)}"
            result["output_file"] = None
    else:
        result["output_file"] = None

    return jsonify(result)


@app.route("/api/screen/ml", methods=["POST"])
def screen_ml():
    """MLスクリーニング"""
    data = request.json
    cmd = [sys.executable, "screening/screen_ml.py"]

    action = data.get("action", "screen")
    cmd.append(action)

    if action == "train":
        if data.get("force"):
            cmd.append("--force")
    elif action == "screen":
        if data.get("top"):
            cmd.extend(["--top", str(data["top"])])
        if data.get("lookback"):
            cmd.extend(["--lookback", str(data["lookback"])])
        # MLスクリーニングはExcel出力をサポートしていないため、結果をテキストで取得

    result = run_command(" ".join(cmd), f"ML{action}")
    # MLスクリーニングはテキスト出力のみなのでoutput_fileはNone
    result["output_file"] = None
    return jsonify(result)


@app.route("/api/backtest/fundamental", methods=["POST"])
def backtest_fundamental():
    """ファンダメンタルバックテスト"""
    data = request.json
    output_file = timestamped_path("backtest", "fundamental", ".json")
    cmd = [sys.executable, "backtest/backtest_statements.py"]

    if data.get("hold_days"):
        cmd.extend(["--hold", str(data["hold_days"])])
    if data.get("entry_offset"):
        cmd.extend(["--entry-offset", str(data["entry_offset"])])
    if data.get("capital"):
        cmd.extend(["--capital", str(data["capital"])])
    if data.get("start_date"):
        cmd.extend(["--start", data["start_date"]])
    if data.get("end_date"):
        cmd.extend(["--end", data["end_date"]])

    cmd.extend(["--json", output_file])

    result = run_command(" ".join(cmd), "ファンダメンタルバックテスト")
    result["output_file"] = output_file if result["success"] else None
    return jsonify(result)


@app.route("/api/backtest/technical", methods=["POST"])
def backtest_technical():
    """テクニカルバックテスト"""
    data = request.json
    output_file = timestamped_path("backtest", "technical", ".json")
    cmd = [sys.executable, "backtest/backtest_technical.py"]

    if data.get("hold_days"):
        cmd.extend(["--hold-days", str(data["hold_days"])])
    if data.get("stop_loss"):
        cmd.extend(["--stop-loss", str(data["stop_loss"])])
    if data.get("capital"):
        cmd.extend(["--capital", str(data["capital"])])
    if data.get("start_date"):
        cmd.extend(["--start", data["start_date"]])
    if data.get("end_date"):
        cmd.extend(["--end", data["end_date"]])

    cmd.extend(["--json", output_file])

    result = run_command(" ".join(cmd), "テクニカルバックテスト")
    result["output_file"] = output_file if result["success"] else None
    return jsonify(result)


@app.route("/api/backtest/ml", methods=["POST"])
def backtest_ml():
    """MLバックテスト"""
    data = request.json
    output_file = timestamped_path("backtest", "ml", ".json")
    cmd = [sys.executable, "backtest/backtest_ml.py"]

    if data.get("top"):
        cmd.extend(["--top", str(data["top"])])
    if data.get("capital"):
        cmd.extend(["--capital", str(data["capital"])])
    if data.get("start_date"):
        cmd.extend(["--start", data["start_date"]])
    if data.get("end_date"):
        cmd.extend(["--end", data["end_date"]])

    cmd.extend(["--json", output_file])

    result = run_command(" ".join(cmd), "MLバックテスト")
    result["output_file"] = output_file if result["success"] else None
    return jsonify(result)


@app.route("/api/utils/update_token", methods=["POST"])
def update_token():
    """IDトークン更新"""
    data = request.json
    email = data.get("email", "").strip()
    password = data.get("password", "").strip()

    # メールアドレスまたはパスワードが空の場合、account.jsonから読み込む
    if not email or not password:
        try:
            with open("config/account.json") as f:
                account_data = json.load(f)
                if not email:
                    email = account_data.get("mailaddress", "")
                if not password:
                    password = account_data.get("password", "")
        except FileNotFoundError:
            return jsonify(
                {
                    "success": False,
                    "error": "config/account.jsonが見つかりません。メールアドレスとパスワードを入力してください。",
                }
            )
        except json.JSONDecodeError:
            return jsonify(
                {"success": False, "error": "config/account.jsonの形式が不正です。"}
            )

    if not email or not password:
        return jsonify(
            {
                "success": False,
                "error": "メールアドレスとパスワードを入力するか、config/account.jsonに設定してください。",
            }
        )

    # update_idtoken.pyにメールアドレスとパスワードを渡す
    cmd = [
        sys.executable,
        "src/cli/update_idtoken.py",
        "--mail",
        email,
        "--password",
        password,
    ]
    result = run_command(" ".join(cmd), "IDトークン更新")

    return jsonify(result)


@app.route("/api/utils/db_summary", methods=["GET"])
def db_summary():
    """DBサマリー取得"""
    cmd = [sys.executable, "db/db_summary.py"]
    return jsonify(run_command(" ".join(cmd), "DBサマリー"))


@app.route("/api/utils/list_signals", methods=["POST"])
def list_signals():
    """シグナル一覧取得"""
    data = request.json
    cmd = [sys.executable, "db/list_signals.py"]

    signal_type = data.get("type", "fund")
    cmd.append(signal_type)

    if data.get("start_date"):
        cmd.extend(["--start", data["start_date"]])
    if data.get("end_date"):
        cmd.extend(["--end", data["end_date"]])
    if data.get("limit"):
        cmd.extend(["--limit", str(data["limit"])])

    return jsonify(run_command(" ".join(cmd), f"{signal_type}シグナル一覧"))


@app.route("/api/utils/analyze_json", methods=["POST"])
def analyze_json():
    """JSON分析"""
    data = request.json
    files = data.get("files", [])

    if not files:
        return jsonify({"success": False, "error": "ファイルが選択されていません"})

    cmd = [sys.executable, "backtest/analyze_backtest_json.py"] + files

    if data.get("show_trades"):
        cmd.append("--show-trades")
    if data.get("side"):
        cmd.extend(["--side", data["side"]])

    return jsonify(run_command(" ".join(cmd), "JSON分析"))


@app.route("/api/utils/thresholds", methods=["GET", "POST"])
def thresholds():
    """閾値設定の取得/更新"""
    threshold_file = "screening/thresholds.json"

    if request.method == "GET":
        try:
            with open(threshold_file) as f:
                return jsonify({"success": True, "data": json.load(f)})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

    else:  # POST
        try:
            data = request.json
            with open(threshold_file, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return jsonify({"success": True, "message": "閾値設定を保存しました"})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})


@app.route("/api/results/list", methods=["GET"])
def list_results():
    """結果ファイル一覧取得"""
    result_types = request.args.get("types", "xlsx,json").split(",")
    category = request.args.get("category", "")
    files = []

    # data/output/以下のファイルを検索
    project_root = Path(__file__).resolve().parent.parent.parent
    output_dir = project_root / "data" / "output"

    # カテゴリが指定されている場合はそのディレクトリのみ検索
    if category:
        search_dirs = (
            [output_dir / category] if (output_dir / category).exists() else []
        )
    else:
        search_dirs = [
            output_dir / cat
            for cat in ["backtest", "screening", "reports"]
            if (output_dir / cat).exists()
        ]

    for search_dir in search_dirs:
        for ext in result_types:
            pattern = f"*.{ext}"
            for file_path in search_dir.glob(pattern):
                if not file_path.name.startswith("."):
                    relative_path = file_path.relative_to(output_dir)
                    files.append(
                        {
                            "name": file_path.name,
                            "path": str(relative_path),
                            "category": relative_path.parent.name,
                            "size": file_path.stat().st_size,
                            "modified": datetime.fromtimestamp(
                                file_path.stat().st_mtime
                            ).isoformat(),
                            "type": ext,
                        }
                    )

    files.sort(key=lambda x: x["modified"], reverse=True)
    return jsonify({"success": True, "files": files})


@app.route("/api/results/download/<path:filepath>")
def download_result(filepath):
    """結果ファイルダウンロード"""
    try:
        # パスのセキュリティチェック
        safe_path = Path(filepath)
        if ".." in safe_path.parts:
            raise ValueError("Invalid file path")

        project_root = Path(__file__).resolve().parent.parent.parent
        full_path = project_root / "data" / "output" / safe_path

        if not full_path.exists() or not full_path.is_file():
            raise FileNotFoundError(f"File not found: {filepath}")

        return send_file(full_path, as_attachment=True)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 404


if __name__ == "__main__":
    # デバッグモードで起動（本番環境では無効にすること）
    app.run(host="0.0.0.0", port=5000, debug=True)

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
from src.utils.logging_config import get_logger

# ロガーの設定
logger = get_logger("web")

# プロジェクトルートとテンプレートディレクトリのパスを設定
project_root = Path(__file__).resolve().parent.parent.parent
template_dir = project_root / "templates"

app = Flask(__name__, template_folder=str(template_dir))
app.config["SECRET_KEY"] = (
    "your-secret-key-here"  # 本番環境では環境変数から取得すること
)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max file size

# Flaskのログレベルを設定（開発環境）
if app.debug:
    app.logger.setLevel("DEBUG")
else:
    app.logger.setLevel("WARNING")


def timestamped_path(category, base_name, extension):
    """タイムスタンプ付きのファイルパスを生成"""
    return str(get_timestamped_output_path(category, base_name, extension))


def run_command(command, description="コマンド実行中"):
    """コマンドを実行し、結果を返す"""
    try:
        # ターミナルに処理開始を表示
        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {description} 開始")
        print(f"コマンド: {command}")
        print(f"{'='*60}\n")

        # リアルタイム出力のために、stdout/stderrを同時に処理
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # stderrもstdoutに統合
            text=True,
            shell=True,
            cwd=project_root,
            bufsize=1,  # 行バッファリング
            universal_newlines=True,
        )

        output_lines = []
        # リアルタイムで出力を表示
        for line in iter(process.stdout.readline, ""):
            if line:
                # ターミナルに表示（改行なし、flushで即座に表示）
                print(f"[{description}] {line}", end="", flush=True)
                output_lines.append(line)

        # プロセスの終了を待つ
        process.wait()

        # 終了メッセージ
        print(f"\n{'='*60}")
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {description} 完了 (exit code: {process.returncode})"
        )
        print(f"{'='*60}\n")

        return {
            "success": process.returncode == 0,
            "output": "".join(output_lines),
            "error": "" if process.returncode == 0 else "".join(output_lines),
            "description": description,
        }
    except Exception as e:
        error_msg = f"コマンド実行エラー: {str(e)}"
        print(f"\n[ERROR] {error_msg}\n")
        return {
            "success": False,
            "output": "",
            "error": error_msg,
            "description": description,
        }


@app.route("/")
def index():
    """メインページ"""
    logger.info("メインページへのアクセス")
    return render_template("index.html")


@app.route("/api/fetch/quotes", methods=["POST"])
def fetch_quotes():
    """株価データ取得"""
    logger.info("株価データ取得APIが呼び出されました")
    print(
        f"\n[API] 株価データ取得リクエストを受信しました - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        data = request.json
        cmd = [sys.executable, "fetch/daily_quotes.py"]

        if data.get("start_date"):
            cmd.extend(["--start", data["start_date"]])
            print(f"[API] 開始日: {data['start_date']}")
        if data.get("end_date"):
            cmd.extend(["--end", data["end_date"]])
            print(f"[API] 終了日: {data['end_date']}")

        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), "株価データ取得")

        if result["success"]:
            logger.info("株価データ取得が正常に完了しました")
            print("[API] 株価データ取得が正常に完了しました")
        else:
            logger.error(f"株価データ取得でエラーが発生しました: {result['error']}")
            print(f"[API] エラー: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"株価データ取得APIでエラーが発生しました: {str(e)}")
        print(f"[API] 例外エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/fetch/listed", methods=["POST"])
def fetch_listed():
    """上場情報取得"""
    logger.info("上場情報取得APIが呼び出されました")
    print(
        f"\n[API] 上場情報取得リクエストを受信しました - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        cmd = [sys.executable, "fetch/listed_info.py"]
        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), "上場情報取得")

        if result["success"]:
            logger.info("上場情報取得が正常に完了しました")
            print("[API] 上場情報取得が正常に完了しました")
        else:
            logger.error(f"上場情報取得でエラーが発生しました: {result['error']}")
            print(f"[API] エラー: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"上場情報取得APIでエラーが発生しました: {str(e)}")
        print(f"[API] 例外エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/fetch/statements", methods=["POST"])
def fetch_statements():
    """財務諸表取得"""
    logger.info("財務諸表取得APIが呼び出されました")
    print(
        f"\n[API] 財務諸表取得リクエストを受信しました - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        data = request.json
        cmd = [sys.executable, "fetch/statements.py"]

        mode = data.get("mode", "2")  # デフォルトは日次取得モード
        cmd.append(mode)
        print(f"[API] モード: {mode}")

        if data.get("start_date"):
            cmd.extend(["--start", data["start_date"]])
            print(f"[API] 開始日: {data['start_date']}")
        if data.get("end_date"):
            cmd.extend(["--end", data["end_date"]])
            print(f"[API] 終了日: {data['end_date']}")

        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), f"財務諸表{mode}")

        if result["success"]:
            logger.info(f"財務諸表取得（モード{mode}）が正常に完了しました")
            print(f"[API] 財務諸表取得（モード{mode}）が正常に完了しました")
        else:
            logger.error(f"財務諸表取得でエラーが発生しました: {result['error']}")
            print(f"[API] エラー: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"財務諸表取得APIでエラーが発生しました: {str(e)}")
        print(f"[API] 例外エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


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
                LEFT JOIN listed_info li ON fs.code = li.code
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
                logger.info("スクリーニング結果はありませんでした")
        except Exception as e:
            logger.error(f"Excel出力エラー: {str(e)}")
            result["error"] += f"\nExcel出力エラー: {str(e)}"
            result["output_file"] = None
    else:
        result["output_file"] = None

    if result["success"]:
        logger.info("ファンダメンタルスクリーニングが正常に完了しました")
    else:
        logger.error(
            f"ファンダメンタルスクリーニングでエラーが発生しました: {result['error']}"
        )

    return jsonify(result)


@app.route("/api/screen/technical", methods=["POST"])
def screen_technical():
    """テクニカルスクリーニング"""
    logger.info("テクニカルスクリーニングAPIが呼び出されました")
    try:
        data = request.json
        cmd = [sys.executable, "screening/screen_technical.py"]

        action = data.get("action", "screen")
        cmd.append(action)

        # indicatorsとscreenの両方でas_ofとlookbackパラメータを渡す
        if data.get("as_of"):
            cmd.extend(["--as-of", data["as_of"]])
        if data.get("lookback"):
            cmd.extend(["--lookback", str(data["lookback"])])

        logger.info(f"実行コマンド: {' '.join(cmd)}")
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
                            max_len = (
                                max(df[col].astype(str).str.len().max(), len(col)) + 2
                            )
                            worksheet.set_column(i, i, min(max_len, 50))

                    result["output_file"] = output_file
                else:
                    result["output_file"] = None
                    result["message"] = "スクリーニング結果がありません"
                    logger.info("スクリーニング結果はありませんでした")
            except Exception as e:
                logger.error(f"Excel出力エラー: {str(e)}")
                result["error"] += f"\nExcel出力エラー: {str(e)}"
                result["output_file"] = None
        else:
            result["output_file"] = None

        if result["success"] and action == "screen":
            logger.info("テクニカルスクリーニングが正常に完了しました")
        elif not result["success"]:
            logger.error(
                f"テクニカルスクリーニングでエラーが発生しました: {result['error']}"
            )

        return jsonify(result)
    except Exception as e:
        logger.error(f"テクニカルスクリーニングAPIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


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
    logger.info("IDトークン更新APIが呼び出されました")
    try:
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
        logger.info(
            f"実行コマンド: {' '.join(cmd[:4])} *** ***"
        )  # パスワード部分はマスク
        result = run_command(" ".join(cmd), "IDトークン更新")

        if result["success"]:
            logger.info("IDトークン更新が正常に完了しました")
        else:
            logger.error(f"IDトークン更新でエラーが発生しました: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"IDトークン更新APIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/utils/db_summary", methods=["GET"])
def db_summary():
    """DBサマリー取得"""
    logger.info("DBサマリー取得APIが呼び出されました")
    try:
        cmd = [sys.executable, "db/db_summary.py"]
        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), "DBサマリー")

        if result["success"]:
            logger.info("DBサマリー取得が正常に完了しました")
        else:
            logger.error(f"DBサマリー取得でエラーが発生しました: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"DBサマリー取得APIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/utils/list_signals", methods=["POST"])
def list_signals():
    """シグナル一覧取得"""
    logger.info("シグナル一覧取得APIが呼び出されました")
    try:
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

        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), f"{signal_type}シグナル一覧")

        if result["success"]:
            logger.info(f"{signal_type}シグナル一覧取得が正常に完了しました")
        else:
            logger.error(
                f"{signal_type}シグナル一覧取得でエラーが発生しました: {result['error']}"
            )

        return jsonify(result)
    except Exception as e:
        logger.error(f"シグナル一覧取得APIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/utils/analyze_json", methods=["POST"])
def analyze_json():
    """JSON分析"""
    logger.info("JSON分析APIが呼び出されました")
    try:
        data = request.json
        files = data.get("files", [])
        analysis_type = data.get("analysis_type", "basic")  # basic or advanced

        if not files:
            logger.warning("JSON分析でファイルが選択されていません")
            return jsonify({"success": False, "error": "ファイルが選択されていません"})

        if analysis_type == "advanced":
            # 高度な分析を使用
            cmd = [sys.executable, "backtest/analyze_json_advanced.py"] + files

            # 高度な分析のオプション
            if data.get("export_excel"):
                cmd.append("--export-excel")
            if data.get("export_pdf"):
                cmd.append("--export-pdf")
            if data.get("compare"):
                cmd.append("--compare")

            output_dir = Path("data/output/analysis")
            output_dir.mkdir(parents=True, exist_ok=True)
            cmd.extend(["--output-dir", str(output_dir)])
        else:
            # 基本分析を使用
            cmd = [sys.executable, "backtest/analyze_backtest_json.py"] + files

        # 共通オプション
        if data.get("show_trades"):
            cmd.append("--show-trades")
        if data.get("side"):
            cmd.extend(["--side", data["side"]])

        logger.info(f"実行コマンド: {' '.join(cmd)}")
        result = run_command(" ".join(cmd), "JSON分析")

        # 高度な分析の場合、生成されたファイルのパスも返す
        if result["success"] and analysis_type == "advanced":
            analysis_files = list(Path("data/output/analysis").glob("*"))
            # 最新のファイルを取得
            latest_files = sorted(
                analysis_files, key=lambda p: p.stat().st_mtime, reverse=True
            )[:5]
            result["generated_files"] = [
                {"name": f.name, "path": str(f.relative_to(Path.cwd()))}
                for f in latest_files
            ]
            logger.info(f"高度な分析で生成されたファイル: {result['generated_files']}")

        if result["success"]:
            logger.info("JSON分析が正常に完了しました")
        else:
            logger.error(f"JSON分析でエラーが発生しました: {result['error']}")

        return jsonify(result)
    except Exception as e:
        logger.error(f"JSON分析APIでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


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
    logger.info(f"結果ファイルダウンロードが要求されました: {filepath}")
    try:
        # パスのセキュリティチェック
        safe_path = Path(filepath)
        if ".." in safe_path.parts:
            logger.warning(f"不正なファイルパスが指定されました: {filepath}")
            raise ValueError("Invalid file path")

        project_root = Path(__file__).resolve().parent.parent.parent
        full_path = project_root / "data" / "output" / safe_path

        if not full_path.exists() or not full_path.is_file():
            logger.warning(f"ファイルが存在しません: {filepath}")
            raise FileNotFoundError(f"File not found: {filepath}")

        logger.info(f"ファイルをダウンロードします: {filepath}")
        return send_file(full_path, as_attachment=True)
    except Exception as e:
        logger.error(f"ファイルダウンロードでエラーが発生しました: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 404


if __name__ == "__main__":
    # デバッグモードで起動（本番環境では無効にすること）
    logger.info("Web UIサーバーを起動します")
    logger.info("http://localhost:5000 でアクセスできます")

    print("\n" + "=" * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Swing Trading Tool Web UI")
    print("=" * 60)
    print("サーバーを起動しています...")
    print("URL: http://localhost:5000")
    print("Ctrl+C で終了")
    print("=" * 60 + "\n")

    app.run(host="0.0.0.0", port=5000, debug=True)

#!/usr/bin/env python3
"""
Swing Trading Tool - モダンなWeb UI版
タブ型インターフェースでGUIアプリの機能を統合
"""

import gzip
import json
import os
import secrets
import sqlite3
import subprocess
import sys
from datetime import datetime
from functools import wraps
from pathlib import Path

import pandas as pd
from flask import (
    Flask,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.serving import WSGIRequestHandler

# プロジェクトルートをPYTHONPATHに追加
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

# データベース初期化のインポート
from db.db_schema import init_schema
from src.auth import AuthManager, admin_required, login_required
from src.auth.admin_setup import create_admin_from_env
from src.auth.models import Session
from src.config import get_db_path
from src.portfolio import PortfolioManager, SBICSVParser
from src.utils.file_utils import get_timestamped_output_path
from src.utils.logging_config import get_logger

# ロガーの設定
logger = get_logger("web")

# プロジェクトルートとテンプレートディレクトリのパスを設定
project_root = Path(__file__).resolve().parent.parent.parent
template_dir = project_root / "templates"

app = Flask(__name__, template_folder=str(template_dir))

# セキュアなシークレットキーの設定
secret_key = os.environ.get("SECRET_KEY")
if not secret_key:
    # シークレットキーをファイルに保存して再利用
    secret_key_file = project_root / "config" / ".secret_key"
    if secret_key_file.exists():
        secret_key = secret_key_file.read_text().strip()
    else:
        secret_key = secrets.token_urlsafe(32)
        secret_key_file.parent.mkdir(exist_ok=True)
        secret_key_file.write_text(secret_key)
        secret_key_file.chmod(0o600)  # 所有者のみ読み書き可能

app.config["SECRET_KEY"] = secret_key
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max file size
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SECURE"] = False  # 本番環境ではTrue
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["PERMANENT_SESSION_LIFETIME"] = 3600 * 24 * 30  # 30日間
app.config["SESSION_COOKIE_NAME"] = "swing_session"


# データベース初期化
def init_database():
    """データベースが存在しない場合は初期化"""
    db_path = Path(get_db_path())
    if not db_path.exists():
        logger.info("データベースが存在しません。初期化を開始します...")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        init_schema(db_path)
        logger.info("データベースの初期化が完了しました")
    else:
        # テーブルが存在するか確認
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
                )
                if not cursor.fetchone():
                    logger.info(
                        "usersテーブルが存在しません。スキーマを再作成します..."
                    )
                    init_schema(db_path)
                    logger.info("スキーマの再作成が完了しました")
        except Exception as e:
            logger.error(f"データベースチェックでエラー: {e}")
            init_schema(db_path)

    # 環境変数から管理者ユーザーを作成
    create_admin_from_env()


# アプリケーション起動時にデータベースを初期化
init_database()


# CSRFトークン生成
def generate_csrf_token():
    if "_csrf_token" not in session:
        session["_csrf_token"] = secrets.token_hex(16)
    return session["_csrf_token"]


# テンプレートにCSRFトークンを渡す
app.jinja_env.globals["csrf_token"] = generate_csrf_token

# WSL2ネットワーク問題対策の設定
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0  # キャッシュ無効化
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False  # JSON圧縮
app.config["PROPAGATE_EXCEPTIONS"] = True

# Werkzeugの設定調整
WSGIRequestHandler.protocol_version = "HTTP/1.1"


def compress_response(f):
    """レスポンスをgzip圧縮するデコレータ"""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        response = make_response(f(*args, **kwargs))

        # テキストベースのコンテンツのみ圧縮
        if response.mimetype.startswith(
            ("text/", "application/json", "application/javascript")
        ):
            response.data = gzip.compress(response.data)
            response.headers["Content-Encoding"] = "gzip"
            response.headers["Content-Length"] = len(response.data)

        return response

    return decorated_function


# チャンク転送エンコーディングを有効化
@app.after_request
def after_request(response):
    # 小さなチャンクサイズでレスポンスを送信
    response.direct_passthrough = False
    return response


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

    # テスト環境では認証をスキップ
    if app.config.get("TESTING"):
        from src.auth.models import User

        test_user = User(
            id=1,
            username="testuser",
            email="test@example.com",
            password_hash="",  # nosec B106 - テスト用の空パスワード
            role="admin",
        )
        return render_template("index.html", user=test_user, portfolio_only=False)

    # 未ログインの場合はログインページへリダイレクト
    if "session_id" not in session:
        return redirect(url_for("login"))

    user = AuthManager.get_user_by_session(session["session_id"])
    if not user:
        session.clear()
        return redirect(url_for("login"))

    # ポートフォリオ専用ユーザーの場合は権限をチェック
    if user.role == "portfolio_only":
        # ポートフォリオタブのみ表示するようにユーザー情報を渡す
        return render_template("index.html", user=user, portfolio_only=True)

    return render_template("index.html", user=user, portfolio_only=False)


@app.route("/login", methods=["GET", "POST"])
def login():
    """ログインページ"""
    if request.method == "GET":
        # 既にログイン済みの場合はリダイレクト
        if "session_id" in session:
            user = AuthManager.get_user_by_session(session["session_id"])
            if user:
                return redirect(url_for("index"))
        return render_template("login.html", error=None)

    # POST: ログイン処理
    username_or_email = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    remember_me = request.form.get("remember_me") == "on"

    user, session_id, error = AuthManager.login(
        username_or_email, password, remember_me
    )

    if user and session_id:
        session["session_id"] = session_id
        # Remember Meが有効な場合はセッションを永続化
        if remember_me:
            session.permanent = True
        # リダイレクト先の処理
        next_url = session.pop("next_url", None)
        return redirect(next_url or url_for("index"))

    return render_template("login.html", error=error)


@app.route("/register", methods=["GET", "POST"])
def register():
    """新規登録ページ"""
    if request.method == "GET":
        return render_template("register.html", error=None)

    # POST: 登録処理
    username = request.form.get("username", "").strip()
    email = request.form.get("email", "").strip()
    password = request.form.get("password", "")
    password_confirm = request.form.get("password_confirm", "")

    # パスワード確認
    if password != password_confirm:
        return render_template("register.html", error="パスワードが一致しません")

    # 新規登録ユーザーは常にポートフォリオ専用ユーザーとして作成
    success, message = AuthManager.register_user(
        username, email, password, role="portfolio_only"
    )

    if success:
        # 登録成功したら自動的にログイン
        user, session_id, _ = AuthManager.login(username, password)
        if user and session_id:
            session["session_id"] = session_id
            return redirect(url_for("index"))

    return render_template("register.html", error=message)


@app.route("/logout")
def logout():
    """ログアウト処理"""
    session_id = session.get("session_id")
    if session_id:
        AuthManager.logout(session_id)
        session.clear()
    return redirect(url_for("login"))


@app.route("/api/fetch/quotes", methods=["POST"])
@login_required
@admin_required
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
@login_required
@admin_required
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
@login_required
@admin_required
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
@login_required
@admin_required
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
            conn = sqlite3.connect(get_db_path())

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
@login_required
@admin_required
def screen_technical():
    """テクニカルスクリーニング"""
    logger.info("テクニカルスクリーニングAPIが呼び出されました")
    try:
        data = request.json
        # 高速版を使用
        cmd = [sys.executable, "screening/screen_technical_fast.py"]

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
                conn = sqlite3.connect(get_db_path())

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
@login_required
@admin_required
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
        if data.get("as_of"):
            cmd.extend(["--as-of", data["as_of"]])
        # MLスクリーニングはExcel出力をサポートしていないため、結果をテキストで取得

    result = run_command(" ".join(cmd), f"ML{action}")
    # MLスクリーニングはテキスト出力のみなのでoutput_fileはNone
    result["output_file"] = None
    return jsonify(result)


@app.route("/api/backtest/fundamental", methods=["POST"])
@login_required
@admin_required
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
@login_required
@admin_required
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
@login_required
@admin_required
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
@login_required
@admin_required
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
@login_required
@admin_required
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
@login_required
@admin_required
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
@login_required
@admin_required
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
@login_required
@admin_required
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
@login_required
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
@login_required
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


# ポートフォリオ管理API
@app.route("/api/portfolio/funds", methods=["GET"])
@login_required
def get_funds():
    """投資信託一覧を取得"""
    try:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # 投資信託の保有情報を取得
        cursor.execute(
            """
            SELECT
                fh.fund_id,
                fm.fund_name,
                fh.account_name,
                fh.account_type,
                fh.quantity,
                fh.average_price,
                fh.market_value,
                fh.profit_loss,
                fh.profit_loss_ratio,
                fh.dividend_method,
                fh.updated_at,
                fp.nav as current_nav,
                fp.date as nav_date
            FROM fund_holdings fh
            JOIN fund_master fm ON fh.fund_id = fm.fund_id
            LEFT JOIN (
                SELECT fund_id, nav, date,
                       ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY date DESC) as rn
                FROM fund_prices
            ) fp ON fh.fund_id = fp.fund_id AND fp.rn = 1
            WHERE fh.user_id = ? AND fh.deleted_at IS NULL
            ORDER BY fh.account_type, fm.fund_name
            """,
            (request.current_user.id,),
        )

        funds_data = []
        total_value = 0
        total_profit_loss = 0

        for row in cursor.fetchall():
            fund_data = {
                "fund_id": row[0],
                "fund_name": row[1],
                "account_name": row[2],
                "account_type": row[3],
                "quantity": row[4],
                "average_price": row[5],
                "market_value": row[6],
                "profit_loss": row[7],
                "profit_loss_ratio": row[8],
                "dividend_method": row[9],
                "updated_at": row[10],
                "current_nav": row[11],
                "nav_date": row[12],
            }

            # 現在価値を再計算（基準価額がある場合）
            if row[11] is not None:  # current_nav
                fund_data["market_value"] = (
                    row[4] * row[11] / 10000
                )  # 口数 × 基準価額 / 10000
                fund_data["profit_loss"] = fund_data["market_value"] - (
                    row[4] * row[5] / 10000
                )
                fund_data["profit_loss_ratio"] = (
                    (fund_data["profit_loss"] / (row[4] * row[5] / 10000) * 100)
                    if row[5] > 0
                    else 0
                )

            funds_data.append(fund_data)

            if fund_data["market_value"]:
                total_value += fund_data["market_value"]
            if fund_data["profit_loss"]:
                total_profit_loss += fund_data["profit_loss"]

        conn.close()

        # 集計情報
        aggregate = {
            "total_funds": len(funds_data),
            "total_value": total_value,
            "total_profit_loss": total_profit_loss,
            "total_profit_loss_ratio": (
                (total_profit_loss / (total_value - total_profit_loss) * 100)
                if total_value > total_profit_loss
                else 0
            ),
        }

        return jsonify({"success": True, "funds": funds_data, "aggregated": aggregate})

    except Exception as e:
        logger.error(f"投資信託取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/holdings", methods=["GET"])
@login_required
def get_holdings():
    """保有銘柄一覧を取得（株式と投資信託を統合）"""
    try:
        from src.portfolio.models import Holding

        # 集約フラグを取得
        aggregate = request.args.get("aggregate", "false").lower() == "true"

        holdings_data = []

        # 株式の保有銘柄を取得
        if aggregate:
            # 銘柄コードで集約
            holdings_data = PortfolioManager.aggregate_holdings_by_code(
                request.current_user.id
            )
        else:
            # 通常の一覧取得
            holdings = Holding.find_all_by_user(request.current_user.id)
            for h in holdings:
                holdings_data.append(
                    {
                        "type": "stock",  # 株式であることを示す
                        "code": h.code,
                        "company_name": h.company_name or "",
                        "account_name": h.account_name,
                        "account_type": getattr(
                            h, "account_type", "特定"
                        ),  # デフォルトは特定
                        "quantity": h.quantity,
                        "average_price": h.average_price,
                        "market_value": h.market_value,
                        "profit_loss": h.profit_loss,
                        "profit_loss_ratio": h.profit_loss_ratio,
                        "updated_at": h.updated_at,
                        # 株価指標データ
                        "expected_per": h.expected_per,
                        "actual_pbr": h.actual_pbr,
                        "dividend_yield": h.dividend_yield,
                        "expected_eps": h.expected_eps,
                        "actual_bps": h.actual_bps,
                        "expected_dividend": h.expected_dividend,
                        "lending_type": h.lending_type,
                    }
                )

        # 投資信託の保有情報を追加
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        if aggregate:
            # 集約表示の場合は投資信託も集約する
            cursor.execute(
                """
                SELECT
                    fm.fund_id,
                    fm.fund_name,
                    SUM(fh.quantity) as total_quantity,
                    SUM(fh.quantity * fh.average_price) / NULLIF(SUM(fh.quantity), 0) as weighted_avg_price,
                    SUM(fh.market_value) as total_market_value,
                    SUM(fh.profit_loss) as total_profit_loss,
                    COUNT(DISTINCT fh.account_name) as account_count,
                    GROUP_CONCAT(DISTINCT fh.account_name) as account_names,
                    GROUP_CONCAT(DISTINCT fh.account_type) as account_types,
                    MAX(fh.updated_at) as updated_at,
                    fp.nav as current_nav,
                    fp.date as nav_date
                FROM fund_holdings fh
                JOIN fund_master fm ON fh.fund_id = fm.fund_id
                LEFT JOIN (
                    SELECT fund_id, nav, date
                    FROM fund_prices fp1
                    WHERE date = (
                        SELECT MAX(date) FROM fund_prices fp2
                        WHERE fp2.fund_id = fp1.fund_id
                    )
                ) fp ON fh.fund_id = fp.fund_id
                WHERE fh.user_id = ? AND fh.quantity > 0 AND fh.deleted_at IS NULL
                GROUP BY fm.fund_id, fm.fund_name, fp.nav, fp.date
                ORDER BY fm.fund_name
            """,
                (request.current_user.id,),
            )
        else:
            # 通常表示の場合
            cursor.execute(
                """
                SELECT
                    fm.fund_id,
                    fm.fund_name,
                    fh.account_name,
                    fh.account_type,
                    fh.quantity,
                    fh.average_price,
                    fh.market_value,
                    fh.profit_loss,
                    fh.profit_loss_ratio,
                    fh.dividend_method,
                    fh.updated_at,
                    fp.nav as current_nav,
                    fp.date as nav_date
                FROM fund_holdings fh
                JOIN fund_master fm ON fh.fund_id = fm.fund_id
                LEFT JOIN (
                    SELECT fund_id, nav, date
                    FROM fund_prices fp1
                    WHERE date = (
                        SELECT MAX(date) FROM fund_prices fp2
                        WHERE fp2.fund_id = fp1.fund_id
                    )
                ) fp ON fh.fund_id = fp.fund_id
                WHERE fh.user_id = ? AND fh.deleted_at IS NULL
                ORDER BY fm.fund_name
            """,
                (request.current_user.id,),
            )

        for row in cursor.fetchall():
            if aggregate:
                # 集約表示の場合
                fund_data = {
                    "type": "fund",  # 投資信託であることを示す
                    "fund_id": row[0],
                    "fund_name": row[1],
                    "total_quantity": row[2],  # 合計口数
                    "weighted_avg_price": row[3] or 0,  # 加重平均価格
                    "total_market_value": row[4],  # 合計評価額
                    "total_profit_loss": row[5],  # 合計損益
                    "account_count": row[6],
                    "account_names": row[7],
                    "account_types": row[8],
                    "profit_loss_ratio": 0,
                    "updated_at": row[9],
                    "current_nav": row[10],
                    "nav_date": row[11],
                    # 集約表示用の追加フィールド
                    "quantity": row[2],  # total_quantityと同じ値を設定
                    "average_price": row[3] or 0,
                    "market_value": row[4],
                    "profit_loss": row[5],
                    # 株式にはあるが投資信託にはない項目をNullで埋める
                    "code": None,
                    "company_name": row[1],  # fund_nameを使用
                    "expected_per": None,
                    "actual_pbr": None,
                    "dividend_yield": None,
                    "expected_eps": None,
                    "actual_bps": None,
                    "expected_dividend": None,
                    "lending_type": None,
                }

                # 損益率の計算
                total_cost = (
                    fund_data["total_quantity"]
                    * fund_data["weighted_avg_price"]
                    / 10000
                )
                if total_cost > 0:
                    fund_data["profit_loss_ratio"] = (
                        fund_data["total_profit_loss"] / total_cost
                    ) * 100

                # 現在価値を再計算（基準価額がある場合）
                if row[10] is not None:  # current_nav
                    fund_data["total_market_value"] = (
                        row[2] * row[10] / 10000
                    )  # 合計口数 × 基準価額 / 10000
                    fund_data["market_value"] = fund_data["total_market_value"]
                    fund_data["total_profit_loss"] = (
                        fund_data["total_market_value"] - total_cost
                    )
                    fund_data["profit_loss"] = fund_data["total_profit_loss"]
                    if total_cost > 0:
                        fund_data["profit_loss_ratio"] = (
                            fund_data["total_profit_loss"] / total_cost * 100
                        )
            else:
                # 通常表示の場合
                fund_data = {
                    "type": "fund",  # 投資信託であることを示す
                    "fund_id": row[0],
                    "fund_name": row[1],
                    "account_name": row[2],
                    "account_type": row[3],
                    "quantity": row[4],
                    "average_price": row[5],
                    "market_value": row[6],
                    "profit_loss": row[7],
                    "profit_loss_ratio": row[8],
                    "dividend_method": row[9],
                    "updated_at": row[10],
                    "current_nav": row[11],
                    "nav_date": row[12],
                    # 株式にはあるが投資信託にはない項目をNullで埋める
                    "code": None,
                    "company_name": row[1],  # fund_nameを使用
                    "expected_per": None,
                    "actual_pbr": None,
                    "dividend_yield": None,
                    "expected_eps": None,
                    "actual_bps": None,
                    "expected_dividend": None,
                    "lending_type": None,
                }

                # 現在価値を再計算（基準価額がある場合）
                if row[11] is not None:  # current_nav
                    fund_data["market_value"] = (
                        row[4] * row[11] / 10000
                    )  # 口数 × 基準価額 / 10000
                    fund_data["profit_loss"] = fund_data["market_value"] - (
                        row[4] * row[5] / 10000
                    )
                    fund_data["profit_loss_ratio"] = (
                        (fund_data["profit_loss"] / (row[4] * row[5] / 10000) * 100)
                        if row[5] > 0
                        else 0
                    )

            holdings_data.append(fund_data)

        conn.close()

        return jsonify(
            {"success": True, "holdings": holdings_data, "aggregated": aggregate}
        )
    except Exception as e:
        logger.error(f"保有銘柄取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/holdings/upload", methods=["POST"])
@login_required
def upload_holdings():
    """保有銘柄CSVアップロード"""
    try:
        if "file" not in request.files:
            return jsonify({"success": False, "error": "ファイルが選択されていません"})

        file = request.files["file"]
        if file.filename == "":
            return jsonify({"success": False, "error": "ファイルが選択されていません"})

        # 口座名を取得（デフォルトは "default"）
        account_name = request.form.get("account_name", "default").strip()
        if not account_name:
            account_name = "default"

        # CSVを読み込み（バイト列として渡してエンコーディングを自動検出）
        csv_content = file.read()
        logger.info(
            f"保有銘柄CSVアップロード開始: {file.filename} (口座: {account_name})"
        )

        # 解析（エンコーディング検出はパーサー側で実施）
        holdings_data = SBICSVParser.parse_holdings_csv(csv_content)

        # 保有銘柄を追加（更新ではなく追加）
        updated, new = PortfolioManager.update_holdings_from_csv(
            request.current_user.id, holdings_data, account_name
        )

        # 時価評価を更新
        PortfolioManager.update_market_values(request.current_user.id)

        logger.info(f"保有銘柄追加完了: {new}件（口座: {account_name}）")
        return jsonify(
            {
                "success": True,
                "message": f"保有銘柄を追加しました（{new}件、口座: {account_name}）",
                "updated": updated,
                "new": new,
                "account_name": account_name,
            }
        )
    except ValueError as e:
        logger.error(f"保有銘柄アップロードエラー（値エラー）: {str(e)}")
        return jsonify({"success": False, "error": str(e)})
    except Exception as e:
        logger.error(f"保有銘柄アップロードエラー: {str(e)}", exc_info=True)
        return jsonify(
            {"success": False, "error": f"アップロードに失敗しました: {str(e)}"}
        )


@app.route("/api/portfolio/transactions", methods=["GET"])
@login_required
def get_transactions():
    """取引履歴一覧を取得"""
    try:
        from src.portfolio.models import Transaction

        # パラメータ取得
        code = request.args.get("code")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        transactions = Transaction.find_all_by_user(
            request.current_user.id, code, start_date, end_date
        )

        trans_data = []
        for t in transactions:
            # 売買金額と実現損益を計算
            if t.transaction_type == "buy":
                buy_amount = t.quantity * t.price + (t.commission or 0)
                sell_amount = 0
                realized_profit = 0
            else:  # sell
                buy_amount = 0
                sell_amount = t.quantity * t.price - (t.commission or 0) - (t.tax or 0)

                # 売却時点での実現損益を計算
                # 売却時点での平均取得価格を計算するため、この取引より前の履歴を参照
                conn = sqlite3.connect(get_db_path())
                cursor = conn.cursor()
                try:
                    # この売却より前の取引で平均取得価格を計算
                    cursor.execute(
                        """
                        SELECT
                            SUM(CASE WHEN transaction_type = 'buy' THEN quantity ELSE -quantity END) as net_quantity,
                            SUM(CASE WHEN transaction_type = 'buy' THEN quantity * price + COALESCE(commission, 0) ELSE 0 END) as total_cost
                        FROM transactions
                        WHERE user_id = ? AND code = ?
                        AND (transaction_date < ? OR (transaction_date = ? AND id < ?))
                    """,
                        (
                            request.current_user.id,
                            t.code,
                            t.transaction_date,
                            t.transaction_date,
                            t.id,
                        ),
                    )

                    row = cursor.fetchone()
                    if row and row[0] and row[0] > 0 and row[1]:
                        # 売却時点での平均取得価格
                        avg_cost_at_sell = row[1] / row[0]
                        # 実現損益 = 売却額 - 取得コスト
                        cost_basis = t.quantity * avg_cost_at_sell
                        realized_profit = sell_amount - cost_basis
                    else:
                        # 保有していない銘柄の売却（デイトレードなど）
                        realized_profit = 0
                finally:
                    conn.close()

            trans_data.append(
                {
                    "id": t.id,
                    "code": t.code,
                    "company_name": getattr(t, "company_name", "") or "",
                    "transaction_date": t.transaction_date,
                    "transaction_type": t.transaction_type,
                    "detailed_type": getattr(t, "detailed_type", "") or "",
                    "quantity": t.quantity,
                    "price": t.price,
                    "commission": t.commission,
                    "tax": t.tax,
                    "total_amount": t.total_amount,
                    "buy_amount": buy_amount,
                    "sell_amount": sell_amount,
                    "realized_profit": getattr(t, "realized_profit", None)
                    or realized_profit,
                    "remarks": t.remarks,
                }
            )

        return jsonify({"success": True, "transactions": trans_data})
    except Exception as e:
        logger.error(f"取引履歴取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/transactions/upload", methods=["POST"])
@login_required
def upload_transactions():
    """取引履歴CSVアップロード"""
    try:
        if "file" not in request.files:
            return jsonify({"success": False, "error": "ファイルが選択されていません"})

        file = request.files["file"]
        if file.filename == "":
            return jsonify({"success": False, "error": "ファイルが選択されていません"})

        # CSVを読み込み（バイト列として渡してエンコーディングを自動検出）
        csv_content = file.read()
        logger.info(f"取引履歴CSVアップロード開始: {file.filename}")

        # 解析（エンコーディング検出はパーサー側で実施）
        try:
            transactions_data = SBICSVParser.parse_transactions_csv(csv_content)
            logger.info(f"CSV解析完了: {len(transactions_data)}件の取引を検出")
        except Exception as e:
            logger.error(f"CSV解析エラー: {str(e)}")
            return jsonify(
                {
                    "success": False,
                    "error": f"CSVファイルの解析に失敗しました: {str(e)}",
                }
            )

        if not transactions_data:
            return jsonify(
                {"success": False, "error": "取引データが見つかりませんでした"}
            )

        # 取引履歴をインポート
        try:
            imported = PortfolioManager.import_transactions_from_csv(
                request.current_user.id, transactions_data
            )
            logger.info(f"取引履歴インポート完了: {imported}件")

            # 部分的な成功も成功として扱う
            if imported > 0:
                total_count = len(transactions_data)
                if imported < total_count:
                    message = f"取引履歴を部分的にインポートしました（{imported}/{total_count}件）"
                    logger.warning(
                        f"一部の取引がインポートされませんでした: {total_count - imported}件"
                    )
                else:
                    message = f"取引履歴をインポートしました（{imported}件）"

                return jsonify(
                    {
                        "success": True,
                        "message": message,
                        "imported": imported,
                        "total": total_count,
                        "partial": imported < total_count,
                    }
                )
            else:
                return jsonify(
                    {
                        "success": False,
                        "error": "取引をインポートできませんでした。データの形式を確認してください。",
                        "imported": 0,
                        "total": len(transactions_data),
                    }
                )
        except Exception as e:
            logger.error(f"インポート処理エラー: {str(e)}", exc_info=True)
            return jsonify(
                {
                    "success": False,
                    "error": f"インポート処理でエラーが発生しました: {str(e)}",
                }
            )

    except ValueError as e:
        logger.error(f"取引履歴アップロードエラー（値エラー）: {str(e)}")
        return jsonify({"success": False, "error": str(e)})
    except Exception as e:
        logger.error(f"取引履歴アップロードエラー: {str(e)}", exc_info=True)
        return jsonify(
            {"success": False, "error": f"アップロードに失敗しました: {str(e)}"}
        )


@app.route("/api/portfolio/summary", methods=["GET"])
@login_required
def get_portfolio_summary():
    """ポートフォリオサマリーを取得"""
    try:
        summary = PortfolioManager.get_portfolio_summary(request.current_user.id)
        return jsonify({"success": True, "summary": summary})
    except Exception as e:
        logger.error(f"ポートフォリオサマリー取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/holdings/delete", methods=["POST"])
@login_required
def delete_holdings():
    """保有銘柄を削除"""
    try:
        data = request.json
        delete_type = data.get("type", "all")  # all or account
        account_name = data.get("account_name")

        if delete_type == "account" and not account_name:
            return jsonify({"success": False, "error": "口座名が指定されていません"})

        if delete_type == "account":
            # 特定口座の保有銘柄を削除
            deleted = PortfolioManager.delete_holdings_by_account(
                request.current_user.id, account_name
            )
            message = f"口座 '{account_name}' の保有銘柄を削除しました（{deleted}件）"
        else:
            # 全保有銘柄を削除
            deleted = PortfolioManager.delete_all_holdings(request.current_user.id)
            message = f"全ての保有銘柄を削除しました（{deleted}件）"

        logger.info(message)
        return jsonify({"success": True, "message": message, "deleted": deleted})
    except Exception as e:
        logger.error(f"保有銘柄削除エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/accounts", methods=["GET"])
@login_required
def get_accounts():
    """口座名一覧を取得"""
    try:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT DISTINCT account_name
            FROM holdings
            WHERE user_id = ?
            ORDER BY account_name
        """,
            (request.current_user.id,),
        )

        accounts = [row[0] for row in cursor.fetchall()]
        conn.close()

        return jsonify({"success": True, "accounts": accounts})
    except Exception as e:
        logger.error(f"口座名取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/indicators/update", methods=["POST"])
@login_required
def update_stock_indicators():
    """保有銘柄の株価指標を一括更新"""
    try:
        # リクエストボディから銘柄コードリストを取得（オプション）
        data = request.get_json() or {}
        codes = data.get("codes", None)

        # 株価指標を更新
        updated_count = PortfolioManager.update_stock_indicators(
            request.current_user.id, codes
        )

        if updated_count > 0:
            return jsonify(
                {
                    "success": True,
                    "message": f"{updated_count}件の株価指標を更新しました",
                    "updated": updated_count,
                }
            )
        else:
            return jsonify(
                {
                    "success": True,
                    "message": "更新対象の銘柄がありませんでした",
                    "updated": 0,
                }
            )

    except Exception as e:
        logger.error(f"株価指標更新エラー: {str(e)}", exc_info=True)
        return jsonify(
            {"success": False, "error": f"株価指標の更新に失敗しました: {str(e)}"}
        )


@app.route("/api/portfolio/stocks/search", methods=["GET"])
@login_required
def search_stocks():
    """銘柄検索API（listed_infoテーブルから部分一致検索）"""
    try:
        query = request.args.get("q", "").strip()
        if not query:
            return jsonify(
                {"success": False, "error": "検索キーワードを入力してください"}
            )

        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # 銘柄コードまたは会社名で部分一致検索
        # 4桁コードの検索にも対応（末尾0埋めを考慮）
        cursor.execute(
            """
            SELECT code, company_name, market_code, market_name,
                   sector17_code, sector17_name, sector33_code, sector33_name
            FROM listed_info
            WHERE delete_flag = 0
            AND (
                code LIKE ? OR
                code LIKE ? OR
                company_name LIKE ?
            )
            ORDER BY
                CASE
                    WHEN code = ? THEN 1
                    WHEN code = ? THEN 2
                    WHEN code LIKE ? THEN 3
                    WHEN code LIKE ? THEN 4
                    ELSE 5
                END,
                code
            LIMIT 50
            """,
            (
                f"{query}%",  # 銘柄コードの前方一致
                f"{query}0",  # 4桁コードに0を付けた完全一致
                f"%{query}%",  # 会社名の部分一致
                query,  # 完全一致を優先
                f"{query}0",  # 4桁に0を付けた完全一致
                f"{query}%",  # 前方一致
                f"{query}0%",  # 4桁に0を付けた前方一致
            ),
        )

        stocks = []
        for row in cursor.fetchall():
            # 銘柄コードから末尾の0を除去して4桁表示
            display_code = row[0].rstrip("0") if row[0].endswith("0") else row[0]
            stocks.append(
                {
                    "code": display_code,
                    "full_code": row[0],  # 5桁のフルコード（DB用）
                    "company_name": row[1] or "",
                    "market_code": row[2] or "",
                    "market_name": row[3] or "",
                    "sector17_code": row[4] or "",
                    "sector17_name": row[5] or "",
                    "sector33_code": row[6] or "",
                    "sector33_name": row[7] or "",
                }
            )

        conn.close()

        return jsonify({"success": True, "stocks": stocks})
    except Exception as e:
        logger.error(f"銘柄検索エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/holdings/add", methods=["POST"])
@login_required
def add_holding():
    """保有銘柄を手動で追加"""
    try:
        data = request.json
        code = data.get("code", "").strip()
        account_name = data.get("account_name", "default").strip()
        quantity = data.get("quantity")
        average_price = data.get("average_price")

        # バリデーション
        if not code:
            return jsonify({"success": False, "error": "銘柄コードは必須です"})
        if not quantity or quantity <= 0:
            return jsonify(
                {"success": False, "error": "数量は正の数を入力してください"}
            )
        if not average_price or average_price <= 0:
            return jsonify(
                {"success": False, "error": "平均取得価格は正の数を入力してください"}
            )

        # 既存の保有銘柄をチェック
        from src.portfolio.models import Holding

        existing = Holding.find_by_user_code_and_account(
            request.current_user.id, code, account_name
        )

        if existing:
            # 既存の保有銘柄がある場合は数量と平均価格を更新
            total_quantity = existing.quantity + quantity
            total_cost = (existing.quantity * existing.average_price) + (
                quantity * average_price
            )
            existing.quantity = total_quantity
            existing.average_price = total_cost / total_quantity
        else:
            # 新規追加
            existing = Holding(
                user_id=request.current_user.id, code=code, account_name=account_name
            )
            existing.quantity = quantity
            existing.average_price = average_price

        # 保存
        if existing.save():
            # 時価評価を更新
            PortfolioManager.update_market_values(request.current_user.id)
            logger.info(
                f"保有銘柄追加成功: {code} {quantity}株 @{average_price}円 (口座: {account_name})"
            )
            return jsonify({"success": True, "message": "保有銘柄を追加しました"})
        else:
            return jsonify({"success": False, "error": "保有銘柄の保存に失敗しました"})

    except Exception as e:
        logger.error(f"保有銘柄追加エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/holdings/update", methods=["POST"])
@login_required
def update_holding():
    """保有銘柄を編集"""
    try:
        data = request.json
        code = data.get("code", "").strip()
        account_name = data.get("account_name", "").strip()
        quantity = data.get("quantity")
        average_price = data.get("average_price")

        # バリデーション
        if not code or not account_name:
            return jsonify({"success": False, "error": "銘柄コードと口座名は必須です"})
        if quantity is not None and quantity < 0:
            return jsonify({"success": False, "error": "数量は0以上を入力してください"})
        if average_price is not None and average_price <= 0:
            return jsonify(
                {"success": False, "error": "平均取得価格は正の数を入力してください"}
            )

        from src.portfolio.models import Holding

        holding = Holding.find_by_user_code_and_account(
            request.current_user.id, code, account_name
        )

        if not holding:
            return jsonify(
                {"success": False, "error": "指定された保有銘柄が見つかりません"}
            )

        # 更新
        if quantity is not None:
            holding.quantity = quantity
        if average_price is not None:
            holding.average_price = average_price

        # 保存
        if holding.save():
            # 時価評価を更新
            PortfolioManager.update_market_values(request.current_user.id)
            logger.info(
                f"保有銘柄更新成功: {code} {holding.quantity}株 @{holding.average_price}円 (口座: {account_name})"
            )
            return jsonify({"success": True, "message": "保有銘柄を更新しました"})
        else:
            return jsonify({"success": False, "error": "保有銘柄の更新に失敗しました"})

    except Exception as e:
        logger.error(f"保有銘柄更新エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/holdings/delete/<code>/<account_name>", methods=["DELETE"])
@login_required
def delete_single_holding(code, account_name):
    """特定の保有銘柄を削除"""
    try:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        cursor.execute(
            """
            DELETE FROM holdings
            WHERE user_id = ? AND code = ? AND account_name = ?
            """,
            (request.current_user.id, code, account_name),
        )

        deleted = cursor.rowcount
        conn.commit()
        conn.close()

        if deleted > 0:
            logger.info(f"保有銘柄削除成功: {code} (口座: {account_name})")
            return jsonify({"success": True, "message": "保有銘柄を削除しました"})
        else:
            return jsonify(
                {"success": False, "error": "指定された保有銘柄が見つかりません"}
            )

    except Exception as e:
        logger.error(f"保有銘柄削除エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/transactions/add", methods=["POST"])
@login_required
def add_transaction():
    """取引履歴を手動で追加"""
    try:
        data = request.json
        code = data.get("code", "").strip()
        transaction_date = data.get("transaction_date", "").strip()
        transaction_type = data.get("transaction_type", "").strip()
        quantity = data.get("quantity")
        price = data.get("price")
        commission = data.get("commission", 0)
        tax = data.get("tax", 0)
        remarks = data.get("remarks", "").strip()

        # バリデーション
        if not code:
            return jsonify({"success": False, "error": "銘柄コードは必須です"})
        if not transaction_date:
            return jsonify({"success": False, "error": "取引日は必須です"})
        if transaction_type not in ["buy", "sell"]:
            return jsonify(
                {
                    "success": False,
                    "error": "取引種別は buy または sell を指定してください",
                }
            )
        if not quantity or quantity <= 0:
            return jsonify(
                {"success": False, "error": "数量は正の数を入力してください"}
            )
        if not price or price <= 0:
            return jsonify(
                {"success": False, "error": "価格は正の数を入力してください"}
            )

        # 取引を作成
        from src.portfolio.models import Transaction

        transaction = Transaction(
            user_id=request.current_user.id,
            code=code,
            transaction_date=transaction_date,
            transaction_type=transaction_type,
            quantity=quantity,
            price=price,
        )
        transaction.commission = commission
        transaction.tax = tax
        transaction.total_amount = quantity * price
        transaction.remarks = remarks

        # 詳細タイプを設定
        if transaction_type == "buy":
            transaction.detailed_type = "新規買い"
        else:
            transaction.detailed_type = "新規売り"

        # 保存
        if transaction.save():
            logger.info(
                f"取引追加成功: {transaction_date} {code} {transaction_type} {quantity}株 @{price}円"
            )
            return jsonify({"success": True, "message": "取引を追加しました"})
        else:
            return jsonify({"success": False, "error": "取引の保存に失敗しました"})

    except Exception as e:
        logger.error(f"取引追加エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/transactions/update/<int:transaction_id>", methods=["POST"])
@login_required
def update_transaction(transaction_id):
    """取引履歴を編集"""
    try:
        data = request.json

        # バリデーション
        transaction_type = data.get("transaction_type")
        if transaction_type and transaction_type not in ["buy", "sell"]:
            return jsonify(
                {
                    "success": False,
                    "error": "取引種別は buy または sell を指定してください",
                }
            )

        quantity = data.get("quantity")
        if quantity is not None and quantity <= 0:
            return jsonify(
                {"success": False, "error": "数量は正の数を入力してください"}
            )

        price = data.get("price")
        if price is not None and price <= 0:
            return jsonify(
                {"success": False, "error": "価格は正の数を入力してください"}
            )

        # 取引を取得して所有者を確認
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT user_id FROM transactions
            WHERE id = ?
            """,
            (transaction_id,),
        )

        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify(
                {"success": False, "error": "指定された取引が見つかりません"}
            )

        if row[0] != request.current_user.id:
            conn.close()
            return jsonify(
                {"success": False, "error": "この取引を編集する権限がありません"}
            )

        # 更新クエリを構築
        update_fields = []
        update_values = []

        if data.get("transaction_date"):
            update_fields.append("transaction_date = ?")
            update_values.append(data["transaction_date"])

        if transaction_type:
            update_fields.append("transaction_type = ?")
            update_values.append(transaction_type)
            # 詳細タイプも更新
            update_fields.append("detailed_type = ?")
            update_values.append(
                "新規買い" if transaction_type == "buy" else "新規売り"
            )

        if quantity is not None:
            update_fields.append("quantity = ?")
            update_values.append(quantity)

        if price is not None:
            update_fields.append("price = ?")
            update_values.append(price)

        if "commission" in data:
            update_fields.append("commission = ?")
            update_values.append(data["commission"])

        if "tax" in data:
            update_fields.append("tax = ?")
            update_values.append(data["tax"])

        if "remarks" in data:
            update_fields.append("remarks = ?")
            update_values.append(data["remarks"])

        # total_amountを再計算
        if quantity is not None or price is not None:
            # 現在の値を取得
            cursor.execute(
                "SELECT quantity, price FROM transactions WHERE id = ?",
                (transaction_id,),
            )
            current = cursor.fetchone()
            calc_quantity = quantity if quantity is not None else current[0]
            calc_price = price if price is not None else current[1]
            update_fields.append("total_amount = ?")
            update_values.append(calc_quantity * calc_price)

        if not update_fields:
            conn.close()
            return jsonify({"success": False, "error": "更新する項目がありません"})

        # 更新実行
        update_values.append(transaction_id)
        cursor.execute(
            f"""
            UPDATE transactions
            SET {', '.join(update_fields)}
            WHERE id = ?
            """,
            update_values,
        )

        conn.commit()
        conn.close()

        logger.info(f"取引更新成功: ID={transaction_id}")
        return jsonify({"success": True, "message": "取引を更新しました"})

    except Exception as e:
        logger.error(f"取引更新エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route(
    "/api/portfolio/transactions/delete/<int:transaction_id>", methods=["DELETE"]
)
@login_required
def delete_transaction(transaction_id):
    """取引履歴を削除"""
    try:
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # 所有者確認と削除を同時に実行
        cursor.execute(
            """
            DELETE FROM transactions
            WHERE id = ? AND user_id = ?
            """,
            (transaction_id, request.current_user.id),
        )

        deleted = cursor.rowcount
        conn.commit()
        conn.close()

        if deleted > 0:
            logger.info(f"取引削除成功: ID={transaction_id}")
            return jsonify({"success": True, "message": "取引を削除しました"})
        else:
            return jsonify(
                {"success": False, "error": "指定された取引が見つかりません"}
            )

    except Exception as e:
        logger.error(f"取引削除エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/visualize/composition", methods=["GET"])
@login_required
def get_portfolio_composition():
    """ポートフォリオ構成円グラフを取得"""
    try:
        from src.portfolio.visualization import PortfolioVisualizer

        visualizer = PortfolioVisualizer(request.current_user.id)
        result = visualizer.create_composition_pie_charts()

        if "error" in result:
            return jsonify({"success": False, "error": result["error"]})

        return jsonify({"success": True, "data": result})
    except Exception as e:
        logger.error(f"ポートフォリオ構成グラフ取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/visualize/performance", methods=["GET"])
@login_required
def get_portfolio_performance():
    """ポートフォリオパフォーマンス推移を取得"""
    try:
        from src.portfolio.visualization import PortfolioVisualizer

        days = request.args.get("days", 180, type=int)
        visualizer = PortfolioVisualizer(request.current_user.id)
        result = visualizer.create_performance_charts(days)

        if "error" in result:
            return jsonify({"success": False, "error": result["error"]})

        return jsonify({"success": True, "data": result})
    except Exception as e:
        logger.error(f"パフォーマンス推移取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/visualize/heatmap", methods=["GET"])
@login_required
def get_portfolio_heatmap():
    """ポートフォリオヒートマップを取得"""
    try:
        from src.portfolio.visualization import PortfolioVisualizer

        visualizer = PortfolioVisualizer(request.current_user.id)
        result = visualizer.create_heatmap()

        if "error" in result:
            return jsonify({"success": False, "error": result["error"]})

        return jsonify({"success": True, "data": result})
    except Exception as e:
        logger.error(f"ヒートマップ取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/portfolio/transactions/performance", methods=["GET"])
@login_required
def get_transaction_performance():
    """取引履歴のパフォーマンスを計算"""
    try:
        period = request.args.get("period", "all")  # all, 1y, 6m, 3m, 1m
        include_holdings = (
            request.args.get("include_holdings", "false").lower() == "true"
        )

        # 期間に応じて開始日を設定
        end_date = datetime.now().strftime("%Y-%m-%d")
        if period == "1y":
            start_date = (datetime.now() - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
        elif period == "6m":
            start_date = (datetime.now() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
        elif period == "3m":
            start_date = (datetime.now() - pd.DateOffset(months=3)).strftime("%Y-%m-%d")
        elif period == "1m":
            start_date = (datetime.now() - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
        else:
            start_date = None

        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        # 期間内の取引を取得
        if start_date:
            cursor.execute(
                """
                SELECT t.*, li.company_name
                FROM transactions t
                LEFT JOIN listed_info li ON t.code = li.code
                WHERE t.user_id = ? AND t.transaction_date >= ?
                ORDER BY t.transaction_date, t.id
                """,
                (request.current_user.id, start_date),
            )
        else:
            cursor.execute(
                """
                SELECT t.*, li.company_name
                FROM transactions t
                LEFT JOIN listed_info li ON t.code = li.code
                WHERE t.user_id = ?
                ORDER BY t.transaction_date, t.id
                """,
                (request.current_user.id,),
            )

        columns = [desc[0] for desc in cursor.description]
        transactions = []
        for row in cursor.fetchall():
            trans = dict(zip(columns, row, strict=False))
            transactions.append(trans)

        # 銘柄ごとのパフォーマンスを計算（シンプルな総額ベース）
        stock_performance = {}

        # 取引データを銘柄ごとに集計
        for trans in transactions:
            code = trans["code"]

            # 現物売り（決済売りでremarksが信用でない）はパフォーマンス集計から除外
            # 新規売りは全て信用取引（空売り）なので含める
            if (
                trans["transaction_type"] == "sell"
                and trans.get("detailed_type") == "決済売り"
                and trans.get("remarks", "") != "信用"
            ):
                continue

            if code not in stock_performance:
                stock_performance[code] = {
                    "code": code,
                    "company_name": trans.get("company_name", ""),
                    "total_buy_amount": 0,
                    "total_sell_amount": 0,
                    "total_buy_quantity": 0,
                    "total_sell_quantity": 0,
                    "realized_profit": 0,
                    "net_quantity": 0,
                    "average_buy_price": 0,
                    "transactions": [],
                }

            sp = stock_performance[code]
            sp["transactions"].append(trans)

            if trans["transaction_type"] == "buy":
                # 買付金額（手数料込み）
                buy_amount = trans["quantity"] * trans["price"] + (
                    trans["commission"] or 0
                )
                sp["total_buy_amount"] += buy_amount
                sp["total_buy_quantity"] += trans["quantity"]
                sp["net_quantity"] += trans["quantity"]

            else:  # sell
                # 売却金額（手数料・税金控除後）
                sell_amount = (
                    trans["quantity"] * trans["price"]
                    - (trans["commission"] or 0)
                    - (trans["tax"] or 0)
                )
                sp["total_sell_amount"] += sell_amount
                sp["total_sell_quantity"] += trans["quantity"]
                sp["net_quantity"] -= trans["quantity"]

            # 実現損益はtransactionsテーブルの値を使用（NULLは0として扱う）
            # 買い・売り両方で実現損益が記録されている場合があるため、全ての取引で加算
            realized_profit = trans.get("realized_profit") or 0
            sp["realized_profit"] += realized_profit

        # 銘柄ごとに平均買付価格を計算
        for _code, sp in stock_performance.items():
            if sp["total_buy_quantity"] > 0:
                sp["average_buy_price"] = (
                    sp["total_buy_amount"] / sp["total_buy_quantity"]
                )

        # 含み損益の計算は行わない
        for _code, sp in stock_performance.items():
            sp["unrealized_profit"] = 0
            sp["current_price"] = None
            sp["market_value"] = None

        # 全体のパフォーマンスサマリー
        total_realized_profit = sum(
            sp["realized_profit"] for sp in stock_performance.values()
        )
        total_buy_amount = sum(
            sp["total_buy_amount"] for sp in stock_performance.values()
        )
        total_sell_amount = sum(
            sp["total_sell_amount"] for sp in stock_performance.values()
        )

        summary = {
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "total_realized_profit": total_realized_profit,
            "total_profit": total_realized_profit,  # 実現損益のみ
            "total_buy_amount": total_buy_amount,
            "total_sell_amount": total_sell_amount,
            "profit_rate": (
                (total_realized_profit / total_buy_amount * 100)
                if total_buy_amount > 0
                else 0
            ),
            "transaction_count": len(transactions),
            "stock_count": len(stock_performance),
        }

        # 月別損益の計算
        monthly_pnl = {}
        for trans in transactions:
            # 現物売りは除外
            if (
                trans["transaction_type"] == "sell"
                and trans.get("detailed_type") == "決済売り"
                and trans.get("remarks", "") != "信用"
            ):
                continue

            if (
                trans["transaction_type"] == "sell"
                and trans.get("realized_profit") is not None
            ):
                month = trans["transaction_date"][:7]  # YYYY-MM
                if month not in monthly_pnl:
                    monthly_pnl[month] = 0
                monthly_pnl[month] += trans["realized_profit"]

        # 累積損益の計算
        cumulative_pnl = []
        cumulative_profit = 0
        for month in sorted(monthly_pnl.keys()):
            cumulative_profit += monthly_pnl[month]
            cumulative_pnl.append(
                {
                    "month": month,
                    "monthly_profit": monthly_pnl[month],
                    "cumulative_profit": cumulative_profit,
                }
            )

        # 取引時間帯分布（仮データ - 実際の時間データがある場合は使用）
        trading_hours = {
            "9-10": len([t for t in transactions if t["transaction_type"] == "buy"])
            // 4,
            "10-11": len([t for t in transactions if t["transaction_type"] == "buy"])
            // 4,
            "11-12": len([t for t in transactions if t["transaction_type"] == "buy"])
            // 4,
            "13-14": len([t for t in transactions if t["transaction_type"] == "buy"])
            // 4,
            "14-15": len([t for t in transactions if t["transaction_type"] == "sell"])
            // 2,
        }

        # 保有期間分布の計算
        holding_periods = {
            "1日以内": 0,
            "1週間以内": 0,
            "1ヶ月以内": 0,
            "3ヶ月以内": 0,
            "3ヶ月超": 0,
        }

        # 保有銘柄を含める場合の処理
        if include_holdings:
            conn = sqlite3.connect(get_db_path())
            cursor = conn.cursor()

            # 現在の保有銘柄を取得
            cursor.execute(
                """
                SELECT h.code, li.company_name, h.quantity, h.average_price,
                       h.market_value, h.profit_loss, h.account_type
                FROM holdings h
                LEFT JOIN listed_info li ON h.code = li.code
                WHERE h.user_id = ? AND h.deleted_at IS NULL
                """,
                (request.current_user.id,),
            )

            for row in cursor.fetchall():
                code = row[0]

                if code in stock_performance:
                    # 取引履歴にある銘柄の場合、sourceを'both'に更新
                    stock_performance[code]["source"] = "both"
                else:
                    # 取引履歴にない銘柄の場合、新規追加
                    stock_performance[code] = {
                        "code": code,
                        "company_name": row[1] or "",
                        "total_buy_amount": row[2] * row[3],  # 数量 × 平均取得価格
                        "total_sell_amount": 0,
                        "total_buy_quantity": row[2],
                        "total_sell_quantity": 0,
                        "realized_profit": 0,
                        "net_quantity": row[2],
                        "average_buy_price": row[3],
                        "unrealized_profit": row[5] or 0,  # profit_loss
                        "current_price": (
                            row[4] / row[2] if row[2] > 0 else None
                        ),  # market_value / quantity
                        "market_value": row[4],
                        "transactions": [],
                        "source": "holdings",  # 保有のみ
                    }

            # ソース情報を既存の取引履歴銘柄に追加
            for _code, sp in stock_performance.items():
                if "source" not in sp:
                    sp["source"] = "transaction"  # 取引履歴のみ

            # サマリーを再計算（保有銘柄を含める場合）
            total_realized_profit = sum(
                sp["realized_profit"] for sp in stock_performance.values()
            )
            total_unrealized_profit = sum(
                sp["unrealized_profit"] for sp in stock_performance.values()
            )
            total_buy_amount = sum(
                sp["total_buy_amount"] for sp in stock_performance.values()
            )
            total_sell_amount = sum(
                sp["total_sell_amount"] for sp in stock_performance.values()
            )
            total_profit = total_realized_profit + total_unrealized_profit

            summary.update(
                {
                    "total_realized_profit": total_realized_profit,
                    "total_unrealized_profit": total_unrealized_profit,
                    "total_profit": total_profit,
                    "total_buy_amount": total_buy_amount,
                    "total_sell_amount": total_sell_amount,
                    "profit_rate": (
                        (total_profit / total_buy_amount * 100)
                        if total_buy_amount > 0
                        else 0
                    ),
                    "stock_count": len(stock_performance),
                }
            )

        conn.close()

        # 結果を返す
        result = {
            "summary": summary,
            "stock_performance": list(stock_performance.values()),
            "monthly_pnl": monthly_pnl,
            "cumulative_pnl": cumulative_pnl,
            "trading_hours": trading_hours,
            "holding_periods": holding_periods,
        }

        return jsonify({"success": True, "data": result})
    except Exception as e:
        logger.error(f"取引パフォーマンス取得エラー: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


if __name__ == "__main__":
    import argparse

    # コマンドライン引数のパーサーを作成
    parser = argparse.ArgumentParser(description="Swing Trading Tool Web UI")
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="サーバーのポート番号 (デフォルト: 5000)",
    )
    args = parser.parse_args()

    # 期限切れセッションのクリーンアップ
    try:
        cleaned = Session.cleanup_expired()
        if cleaned > 0:
            logger.info(f"期限切れセッションを{cleaned}件削除しました")
    except Exception as e:
        logger.warning(f"セッションクリーンアップエラー: {str(e)}")

    # デバッグモードで起動（本番環境では無効にすること）
    logger.info(f"Web UIサーバーを起動します (ポート: {args.port})")
    logger.info(f"http://localhost:{args.port} でアクセスできます")

    print("\n" + "=" * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Swing Trading Tool Web UI")
    print("=" * 60)
    print("サーバーを起動しています...")
    print(f"URL: http://localhost:{args.port}")
    print("Ctrl+C で終了")
    print("=" * 60 + "\n")

    # WSL2環境での実行を検出
    if "WSL_DISTRO_NAME" in os.environ:
        print("\n[警告] WSL2環境で実行しています")
        print("家庭内LANからのアクセスで問題が発生する場合は、以下を試してください：")
        print("1. sudo ip link set dev eth0 mtu 1450")
        print("2. python src/ui/web_production.py (Waitressサーバーを使用)")
        print("3. WSL2_NETWORK_FIX.md を参照\n")

    # チャンクサイズを小さくしてレスポンスを送信
    app.run(
        host="0.0.0.0", port=args.port, debug=True, threaded=True, use_reloader=True
    )

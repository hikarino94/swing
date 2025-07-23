"""
共通処理とユーティリティ関数
"""

import gzip
import os
import secrets
import subprocess
from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import make_response, session
from werkzeug.serving import WSGIRequestHandler

from src.utils.file_utils import get_timestamped_output_path
from src.utils.logging_config import get_logger

# ロガーの設定
logger = get_logger("web.common")

# プロジェクトルート
project_root = Path(__file__).resolve().parent.parent.parent

# WSL2ネットワーク問題対策の設定
WSGIRequestHandler.protocol_version = "HTTP/1.1"


def get_secret_key():
    """セキュアなシークレットキーを取得または生成

    本番環境では必ず環境変数SECRET_KEYを設定すること。
    開発環境でのみファイルベースのキー生成を使用。
    """
    secret_key = os.environ.get("SECRET_KEY")
    if not secret_key:
        # 本番環境では環境変数の設定を必須とする
        if os.environ.get("ENVIRONMENT") == "production":
            raise ValueError(
                "本番環境ではSECRET_KEY環境変数の設定が必須です。"
                "fly secrets set SECRET_KEY=your-secret-key で設定してください。"
            )

        # 開発環境でのみファイルベースのキー生成を許可
        secret_key_file = project_root / "config" / ".secret_key"
        if secret_key_file.exists():
            secret_key = secret_key_file.read_text().strip()
        else:
            secret_key = secrets.token_urlsafe(32)
            secret_key_file.parent.mkdir(exist_ok=True)
            secret_key_file.write_text(secret_key)
            secret_key_file.chmod(0o600)  # 所有者のみ読み書き可能
    return secret_key


def generate_csrf_token():
    """CSRFトークン生成"""
    if "_csrf_token" not in session:
        session["_csrf_token"] = secrets.token_hex(16)
    return session["_csrf_token"]


def validate_csrf_token(request):
    """CSRFトークンの検証"""
    token = session.get("_csrf_token", None)
    if not token:
        return False

    # フォームデータまたはJSONからトークンを取得
    if request.is_json:
        provided_token = request.json.get("csrf_token")
    else:
        provided_token = request.form.get("csrf_token")

    return token == provided_token


def compress_response(f):
    """レスポンスをgzip圧縮するデコレータ"""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        response = make_response(f(*args, **kwargs))

        # テキストベースのコンテンツのみ圧縮
        if response.mimetype and response.mimetype.startswith(
            ("text/", "application/json", "application/javascript")
        ):
            response.data = gzip.compress(response.data)
            response.headers["Content-Encoding"] = "gzip"
            response.headers["Content-Length"] = len(response.data)

        return response

    return decorated_function


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
            bufsize=1,  # 行単位でバッファリング
        )

        output_lines = []
        # リアルタイムで出力を表示
        for line in process.stdout or []:
            print(line.rstrip())  # 末尾の改行を除いて表示
            output_lines.append(line)

        process.wait()
        output = "".join(output_lines)

        # 終了ステータスを表示
        print(f"\n{'='*60}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {description} 完了")
        print(f"終了コード: {process.returncode}")
        print(f"{'='*60}\n")

        if process.returncode != 0:
            error_msg = output.strip() if output.strip() else "エラーが発生しました"
            logger.error(f"コマンド実行エラー: {error_msg}")
            return {"success": False, "error": error_msg, "description": description}

        # 特定のコマンドの出力を解析
        if "list_signals.py" in command:
            # list_signals.pyの出力をそのまま返す
            return {"success": True, "output": output, "description": description}
        elif "db_summary.py" in command:
            # db_summary.pyの出力をそのまま返す
            return {"success": True, "output": output, "description": description}

        return {
            "success": True,
            "output": output if output else "コマンドが正常に実行されました",
            "description": description,
        }

    except Exception as e:
        error_msg = f"コマンド実行中にエラーが発生しました: {str(e)}"
        logger.error(error_msg)
        print(f"\n[エラー] {error_msg}\n")
        return {"success": False, "error": error_msg, "description": description}

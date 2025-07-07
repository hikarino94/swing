#!/usr/bin/env python3
"""
本番環境用のWeb UIサーバー設定
WSL2のネットワーク問題に対応
"""
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    from waitress import serve
except ImportError:
    print("Waitressがインストールされていません。")
    print("以下のコマンドでインストールしてください：")
    print("pip install waitress")
    sys.exit(1)

from werkzeug.serving import WSGIRequestHandler

# プロジェクトルートをPYTHONPATHに追加
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.ui.web import app
from src.utils.logging_config import get_logger

logger = get_logger("web_production")

# バッファサイズを調整
WSGIRequestHandler.protocol_version = "HTTP/1.1"

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Swing Trading Tool - 本番サーバー"
    )
    print("=" * 60)
    print("Waitressサーバーを起動しています...")
    print("URL: http://localhost:5000")
    print("     http://0.0.0.0:5000 (LAN内の他の端末からアクセス可能)")
    print("Ctrl+C で終了")
    print("=" * 60 + "\n")

    # WSL2環境での実行を検出
    if "WSL_DISTRO_NAME" in os.environ:
        print("[情報] WSL2環境で実行しています")
        print("MTUサイズが小さい場合は以下のコマンドを実行してください：")
        print("sudo ip link set dev eth0 mtu 1500")
        print("または: bash scripts/fix_wsl2_network.sh\n")

    logger.info("Waitressサーバーを起動します")

    # Waitressサーバーで起動（本番環境推奨）
    # WSL2環境でのMTU問題に対応するための設定
    serve(
        app,
        host="0.0.0.0",
        port=5000,
        threads=4,
        connection_limit=100,
        cleanup_interval=30,
        channel_timeout=120,  # タイムアウトを長めに
        recv_bytes=8192,  # 受信バッファを小さく
        send_bytes=4096,  # 送信バッファをさらに小さく（MTU 1280対応）
        outbuf_overflow=1048576,  # 出力バッファオーバーフローサイズ
        asyncore_use_poll=True,
        # MTUが小さい環境でのパケット分割を考慮
        map=None,
        _quiet=False,  # ログ出力を有効化
    )

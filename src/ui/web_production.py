#!/usr/bin/env python3
"""
本番環境用のWeb UIサーバー設定
WSL2のネットワーク問題に対応
"""
import sys
from pathlib import Path

from waitress import serve
from werkzeug.serving import WSGIRequestHandler

# プロジェクトルートをPYTHONPATHに追加
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.ui.web import app

# バッファサイズを調整
WSGIRequestHandler.protocol_version = "HTTP/1.1"

if __name__ == "__main__":
    print(
        """
    ========================================
    Swing Trading Tool - 本番サーバー
    ========================================

    URL: http://localhost:5000

    WSL2ネットワーク最適化設定で起動しています。
    Ctrl+C で終了
    """
    )

    # Waitressサーバーで起動（本番環境推奨）
    # - 接続タイムアウトを長めに設定
    # - チャンクサイズを小さく設定
    # - バッファサイズを調整
    serve(
        app,
        host="0.0.0.0",
        port=5000,
        threads=4,
        connection_limit=100,
        cleanup_interval=30,
        channel_timeout=120,  # タイムアウトを長めに
        recv_bytes=8192,  # 受信バッファを小さく
        send_bytes=8192,  # 送信バッファを小さく
        outbuf_overflow=1048576,  # 出力バッファオーバーフローサイズ
        asyncore_use_poll=True,
    )

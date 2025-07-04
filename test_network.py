#!/usr/bin/env python3
"""
WSL2ネットワーク問題調査用の軽量テストサーバー
"""
import subprocess
import time

from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route("/test/light")
def test_light():
    """軽量ページ（数バイト）"""
    return "OK"


@app.route("/test/medium")
def test_medium():
    """中程度のページ（1KB）"""
    return "X" * 1024


@app.route("/test/heavy")
def test_heavy():
    """重いページ（100KB）"""
    return "X" * (100 * 1024)


@app.route("/test/very-heavy")
def test_very_heavy():
    """非常に重いページ（1MB）"""
    return "X" * (1024 * 1024)


@app.route("/test/info")
def test_info():
    """ネットワーク情報"""
    info = {
        "client_ip": request.remote_addr,
        "host": request.host,
        "url": request.url,
        "headers": dict(request.headers),
        "mtu_info": subprocess.run(["ip", "link", "show", "eth0"], capture_output=True, text=True).stdout,
        "time": time.time(),
    }
    return jsonify(info)


@app.route("/test/chunked")
def test_chunked():
    """チャンク送信テスト"""

    def generate():
        for i in range(10):
            yield f"Chunk {i}\n" * 100
            time.sleep(0.1)

    return app.response_class(generate(), mimetype="text/plain")


if __name__ == "__main__":
    print(
        """
    ========================================
    WSL2 ネットワーク問題調査サーバー
    ========================================

    以下のエンドポイントでテスト可能です：
    - http://localhost:8080/test/light     (数バイト)
    - http://localhost:8080/test/medium    (1KB)
    - http://localhost:8080/test/heavy     (100KB)
    - http://localhost:8080/test/very-heavy (1MB)
    - http://localhost:8080/test/info      (ネットワーク情報)
    - http://localhost:8080/test/chunked   (チャンク送信)

    家庭内LANから上記URLにアクセスして、どのサイズからタイムアウトするか確認してください。
    """
    )

    # デバッグモードOFF、スレッドモードON
    app.run(host="0.0.0.0", port=8080, debug=False, threaded=True)

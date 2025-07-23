# Python 3.12 slimイメージを使用（軽量化のため）
FROM python:3.12-slim

# 作業ディレクトリの設定
WORKDIR /app

# システムパッケージの更新とPostgreSQLクライアントライブラリのインストール
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Pythonの依存関係をコピーしてインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# PostgreSQL用ドライバーを追加インストール
RUN pip install --no-cache-dir psycopg2-binary

# アプリケーションコードをコピー
COPY . .

# 非rootユーザーの作成（セキュリティのため）
RUN useradd -m -u 1001 appuser && \
    chown -R appuser:appuser /app

# データディレクトリの作成と権限設定
RUN mkdir -p /app/data/output /app/data/logs /app/db/models && \
    chown -R appuser:appuser /app/data /app/db

# 非rootユーザーに切り替え
USER appuser

# 環境変数の設定
ENV PYTHONUNBUFFERED=1 \
    ENVIRONMENT=production \
    DATABASE_TYPE=postgres \
    FLASK_ENV=production \
    PORT=8080

# ヘルスチェック用のエンドポイント
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# アプリケーションの起動
EXPOSE 8080
CMD ["./start_app.sh"]

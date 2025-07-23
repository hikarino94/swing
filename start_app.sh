#!/bin/bash
# アプリケーション起動スクリプト（本番環境用）

set -e

echo "Starting Swing Trading Tool..."

# データベースタイプの確認
if [ "$DATABASE_TYPE" = "postgres" ]; then
    echo "Using PostgreSQL database"

    # PostgreSQLが起動するまで待機（最大30秒）
    echo "Waiting for PostgreSQL to be ready..."
    for i in {1..30}; do
        if python -c "from src.database import get_database_adapter; db = get_database_adapter(); db.connect(); print('Database connected')" 2>/dev/null; then
            echo "PostgreSQL is ready!"
            break
        fi
        echo "Waiting for PostgreSQL... ($i/30)"
        sleep 1
    done

    # スキーマの初期化
    echo "Initializing database schema..."
    python db/db_schema_postgres.py || echo "Schema already exists or partially created"
else
    echo "Using SQLite database"
    # SQLiteの場合は通常のスキーマ初期化
    python db/db_schema.py
fi

# 管理者ユーザーの作成
echo "Creating admin user if needed..."
python src/auth/admin_setup.py

echo "Starting web server..."
exec gunicorn --bind 0.0.0.0:${PORT:-8080} \
    --workers ${WORKERS:-2} \
    --threads ${THREADS:-4} \
    --worker-class sync \
    --timeout 120 \
    --access-logfile - \
    --error-logfile - \
    src.ui.web:app

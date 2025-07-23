#!/bin/bash
# Fly.ioへのデプロイスクリプト

set -e  # エラーが発生したらスクリプトを終了

echo "=== Swing Trading Tool - Fly.io デプロイスクリプト ==="
echo ""

# 1. Fly CLIのインストール確認
if ! command -v fly &> /dev/null; then
    echo "❌ Fly CLIがインストールされていません"
    echo "以下のコマンドでインストールしてください:"
    echo "curl -L https://fly.io/install.sh | sh"
    exit 1
fi

# 2. Fly.ioにログイン
echo "📝 Fly.ioにログインしています..."
fly auth login

# 3. アプリが存在しない場合は作成
if ! fly status --app swing-trading-tool &> /dev/null; then
    echo "🚀 新しいアプリを作成しています..."
    fly launch --name swing-trading-tool --region nrt --no-deploy
else
    echo "✅ 既存のアプリを使用します: swing-trading-tool"
fi

# 4. PostgreSQLデータベースの作成（存在しない場合）
echo "🗄️  PostgreSQLデータベースをチェックしています..."
if ! fly postgres list | grep -q "swing-db"; then
    echo "📊 PostgreSQLデータベースを作成しています..."
    fly postgres create --name swing-db --region nrt --initial-cluster-size 1 --vm-size shared-cpu-1x

    # データベースをアプリにアタッチ
    echo "🔗 データベースをアプリにアタッチしています..."
    fly postgres attach swing-db --app swing-trading-tool
else
    echo "✅ 既存のデータベースを使用します: swing-db"
fi

# 5. 永続ストレージボリュームの作成（存在しない場合）
echo "💾 永続ストレージをチェックしています..."
if ! fly volumes list --app swing-trading-tool | grep -q "data"; then
    echo "📁 永続ストレージボリュームを作成しています..."
    fly volumes create data --app swing-trading-tool --region nrt --size 1
else
    echo "✅ 既存のストレージボリュームを使用します"
fi

# 6. シークレットの設定
echo ""
echo "🔐 シークレットを設定します..."
echo "以下の環境変数を設定する必要があります:"

# SECRET_KEYの生成と設定
if ! fly secrets list --app swing-trading-tool | grep -q "SECRET_KEY"; then
    echo ""
    echo "SECRET_KEYを生成しています..."
    SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    echo "Generated SECRET_KEY: $SECRET_KEY"
    fly secrets set SECRET_KEY="$SECRET_KEY" --app swing-trading-tool
else
    echo "✅ SECRET_KEYは既に設定されています"
fi

# 管理者アカウント情報の設定
echo ""
echo "管理者アカウント情報を設定してください:"
read -p "ADMIN_USERNAME (default: admin): " ADMIN_USERNAME
ADMIN_USERNAME=${ADMIN_USERNAME:-admin}

read -p "ADMIN_EMAIL: " ADMIN_EMAIL
if [ -z "$ADMIN_EMAIL" ]; then
    echo "❌ ADMIN_EMAILは必須です"
    exit 1
fi

read -s -p "ADMIN_PASSWORD: " ADMIN_PASSWORD
echo ""
if [ -z "$ADMIN_PASSWORD" ]; then
    echo "❌ ADMIN_PASSWORDは必須です"
    exit 1
fi

# シークレットを設定
fly secrets set \
    ADMIN_USERNAME="$ADMIN_USERNAME" \
    ADMIN_EMAIL="$ADMIN_EMAIL" \
    ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    --app swing-trading-tool

# 7. デプロイ実行
echo ""
echo "🚀 アプリケーションをデプロイしています..."
fly deploy --app swing-trading-tool

# 8. デプロイ後の確認
echo ""
echo "✅ デプロイが完了しました！"
echo ""
echo "📌 アプリケーションのURL:"
fly info --app swing-trading-tool | grep "Hostname"

echo ""
echo "📊 アプリケーションのステータス:"
fly status --app swing-trading-tool

echo ""
echo "🔍 ログを確認するには:"
echo "fly logs --app swing-trading-tool"

echo ""
echo "🌐 アプリケーションを開くには:"
echo "fly open --app swing-trading-tool"

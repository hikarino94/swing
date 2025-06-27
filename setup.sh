#!/bin/bash

# Ubuntu用 自動セットアップスクリプト
# git clone直後にこのスクリプトを実行してください

set -e  # エラー時に停止

echo "🚀 Swing Trading System - Ubuntu セットアップ開始"

# システムパッケージの更新とtkinterのインストール
echo "📦 システムパッケージを更新中..."
sudo apt update

echo "🖥️  tkinter (GUI用) をインストール中..."
sudo apt install -y python3-tk

# Pythonのバージョン確認
echo "🐍 Python バージョン確認:"
python3 --version

# pipを最新化
echo "📦 pip を最新化中..."
python3 -m pip install --upgrade pip

# 必要な依存関係をインストール
echo "📚 Python依存関係をインストール中..."
pip install -r requirements.txt

# pre-commitのセットアップ
echo "🔧 pre-commit フックをセットアップ中..."
pip install pre-commit
pre-commit install

# データベース初期化
echo "🗄️  データベースを初期化中..."
python3 data/db/db_schema.py

# 設定ファイルのサンプル作成
echo "⚙️  設定ファイルのサンプルを作成中..."
if [ ! -f "config/idtoken.json" ]; then
    mkdir -p config
    echo '{"idToken": "YOUR_JQUANTS_API_TOKEN_HERE"}' > config/idtoken.json
    echo "📝 config/idtoken.json サンプルを作成しました。J-Quants APIトークンを設定してください。"
fi

if [ ! -f "config/account.json" ]; then
    mkdir -p config
    echo '{"mailaddress": "your_email@example.com", "password": "your_password"}' > config/account.json
    echo "📝 config/account.json サンプルを作成しました。J-Quantsアカウント情報を設定してください。"
fi

if [ ! -f "config/thresholds.json" ]; then
    mkdir -p config
    echo '{
  "roe": 10.0,
  "roa": 5.0,
  "debt_ratio": 0.5,
  "current_ratio": 1.5,
  "market_cap_min": 10000000000,
  "volume_min": 100000
}' > config/thresholds.json
    echo "📝 config/thresholds.json サンプルを作成しました。"
fi

# 動作確認
echo "🧪 動作確認中..."
python3 -c "import pandas, requests, sklearn, openpyxl, flask, tkinter; print('✅ 全ての依存関係が正常にインストールされました')"

echo ""
echo "🎉 セットアップ完了！"
echo ""
echo "📋 次のステップ:"
echo "1. config/idtoken.json にJ-Quants APIトークンを設定"
echo "2. config/account.json にJ-Quantsアカウント情報を設定"
echo "3. データを取得: python3 src/api/daily_quotes.py"
echo "4. GUIを起動: python3 src/gui/gui.py"
echo "5. Webインターフェース起動: python3 src/gui/web.py"
echo ""
echo "📖 詳細は CLAUDE.md を参照してください"

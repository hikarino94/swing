# Renderデプロイメントガイド

## Renderの設定

### Build Command
```bash
pip install -r requirements.txt && python init_db.py
```

### Pre-Deploy Command
なし（設定不要）

### Start Command
```bash
gunicorn --bind 0.0.0.0:$PORT --worker-class eventlet -w 1 src.ui.web:app
```

## 環境変数の設定

Renderのダッシュボードで以下の環境変数を設定してください：

### 必須の環境変数
- `PYTHONPATH`: `/opt/render/project/src`
- `FLASK_ENV`: `production`
- `DATABASE_PATH`: `/opt/render/project/src/db/stock.db`

### 認証情報（重要）
以下の環境変数をRenderのダッシュボードで設定する必要があります：

- `JQUANTS_MAIL`: J-Quants APIのメールアドレス
- `JQUANTS_PASSWORD`: J-Quants APIのパスワード
- `WEB_USERNAME`: Webアプリのログインユーザー名（任意）
- `WEB_PASSWORD`: Webアプリのログインパスワード（任意）

## デプロイ手順

1. GitHubリポジトリをRenderに接続
2. 上記のBuild CommandとStart Commandを設定
3. 環境変数を設定
4. デプロイを実行

## 注意事項

- データベースファイルは初回デプロイ時に自動生成されます
- 認証情報は環境変数で管理し、コードにハードコードしないでください
- デプロイ後、初回アクセス時にデータベースの初期化が必要な場合があります

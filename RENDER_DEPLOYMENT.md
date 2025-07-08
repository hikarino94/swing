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

#### J-Quants API認証情報
- `JQUANTS_MAIL`: J-Quants APIのメールアドレス
- `JQUANTS_PASSWORD`: J-Quants APIのパスワード

#### 管理者ユーザー設定（自動作成）
以下の環境変数を設定すると、初回デプロイ時に自動的に管理者ユーザーが作成されます：
- `ADMIN_USERNAME`: 管理者ユーザー名
- `ADMIN_EMAIL`: 管理者メールアドレス
- `ADMIN_PASSWORD`: 管理者パスワード（8文字以上）

注意: これらの環境変数が設定されていない場合、管理者ユーザーは作成されません。

## デプロイ手順

1. GitHubリポジトリをRenderに接続
2. 上記のBuild CommandとStart Commandを設定
3. 環境変数を設定
4. デプロイを実行

## 注意事項

- データベースファイルは初回デプロイ時に自動生成されます
- 認証情報は環境変数で管理し、コードにハードコードしないでください
- デプロイ後、初回アクセス時にデータベースの初期化が必要な場合があります

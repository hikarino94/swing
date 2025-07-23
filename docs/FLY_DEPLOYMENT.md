# Fly.io デプロイメントガイド

このドキュメントでは、Swing Trading ToolをFly.ioにデプロイする手順を説明します。

## 前提条件

1. Fly CLIのインストール
```bash
curl -L https://fly.io/install.sh | sh
```

2. Fly.ioアカウントの作成
```bash
fly auth signup
```

## アーキテクチャ

### データベース構成

- **開発環境**: SQLite（ローカルファイル）
- **本番環境**: PostgreSQL（Fly.io Postgres）

環境変数`DATABASE_TYPE`により自動的に切り替わります。

### 公開機能

一般ユーザー向けには以下の2機能のみを公開：
- ポートフォリオ管理
- 取引管理（デイトレード記録）

## デプロイ手順

### 1. 自動デプロイ（推奨）

```bash
./scripts/deploy.sh
```

このスクリプトは以下を自動的に実行します：
- アプリケーションの作成
- PostgreSQLデータベースの作成とアタッチ
- 永続ストレージボリュームの作成
- 必要なシークレットの設定
- アプリケーションのデプロイ

### 2. 手動デプロイ

#### ステップ1: アプリケーションの作成

```bash
fly launch --name swing-trading-tool --region nrt
```

#### ステップ2: PostgreSQLデータベースの作成

```bash
# データベースクラスタの作成
fly postgres create --name swing-db --region nrt

# アプリケーションにアタッチ
fly postgres attach swing-db --app swing-trading-tool
```

#### ステップ3: 永続ストレージの作成

```bash
fly volumes create data --app swing-trading-tool --region nrt --size 1
```

#### ステップ4: シークレットの設定

```bash
# シークレットキーの生成
python scripts/generate_secret_key.py

# 環境変数の設定
fly secrets set SECRET_KEY=your-generated-secret-key
fly secrets set ADMIN_USERNAME=admin
fly secrets set ADMIN_EMAIL=admin@example.com
fly secrets set ADMIN_PASSWORD=your-secure-password
```

#### ステップ5: デプロイ

```bash
fly deploy
```

## 環境変数

### 必須環境変数

| 変数名 | 説明 | 例 |
|--------|------|-----|
| SECRET_KEY | Flaskセッション用シークレットキー | ランダムな32文字 |
| ADMIN_USERNAME | 管理者ユーザー名 | admin |
| ADMIN_EMAIL | 管理者メールアドレス | admin@example.com |
| ADMIN_PASSWORD | 管理者パスワード | セキュアなパスワード |

### 自動設定される環境変数

| 変数名 | 説明 | デフォルト値 |
|--------|------|-------------|
| DATABASE_URL | PostgreSQL接続URL | Fly.ioが自動設定 |
| DATABASE_TYPE | データベースタイプ | postgres |
| ENVIRONMENT | 実行環境 | production |
| PORTFOLIO_ONLY_MODE | ポートフォリオ機能のみ有効化 | true |

## データベース管理

### 初回起動時

アプリケーション起動時に自動的に：
1. データベーススキーマが作成されます
2. 管理者ユーザーが作成されます

### データベース接続確認

```bash
fly ssh console
cd /app
python -c "from src.database import get_database_adapter; db = get_database_adapter(); db.connect(); print('Database connected successfully')"
```

### バックアップ

```bash
# データベースのバックアップ
fly postgres backup create --app swing-db
```

## 運用管理

### ログの確認

```bash
# リアルタイムログ
fly logs

# 過去のログ
fly logs --since 1h
```

### アプリケーションの状態確認

```bash
fly status
```

### SSHアクセス

```bash
fly ssh console
```

### スケーリング

```bash
# インスタンス数の変更
fly scale count 2

# リソースの変更
fly scale vm shared-cpu-2x --memory 1024
```

## トラブルシューティング

### データベース接続エラー

1. DATABASE_URLが正しく設定されているか確認
```bash
fly secrets list
```

2. PostgreSQLが起動しているか確認
```bash
fly postgres list
```

### メモリ不足エラー

```bash
# メモリを増やす
fly scale memory 1024
```

### デプロイ失敗

1. ビルドログを確認
```bash
fly deploy --verbose
```

2. ヘルスチェックの確認
```bash
curl https://swing-trading-tool.fly.dev/health
```

## セキュリティ考慮事項

1. **HTTPS強制**: Fly.ioはデフォルトでHTTPSを強制します
2. **シークレット管理**: すべての機密情報は環境変数で管理
3. **セッションセキュリティ**: SESSION_COOKIE_SECUREが本番環境で有効
4. **CSRFプロテクション**: すべてのフォームでCSRFトークンを使用

## 参考リンク

- [Fly.io Documentation](https://fly.io/docs/)
- [Fly.io Postgres](https://fly.io/docs/postgres/)
- [Fly.io Volumes](https://fly.io/docs/reference/volumes/)

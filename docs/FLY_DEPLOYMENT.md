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

### 🆓 無料枠制限と課金対策

Fly.ioの無料枠を最大限活用し、課金を最小限に抑える設定を適用しています：

#### 無料枠の内容
- **アプリケーション**: 3個のshared-cpu-1x VM（256MB RAM）
- **PostgreSQL**: Development構成（256MB RAM、1GBストレージ）
- **ボリューム**: 3GBまで

#### 課金対策
1. **リソース制限**
   - VMメモリ: 256MB（無料枠の最小値）
   - CPU: shared-cpu-1x
   - マシン数: 1台に固定

2. **自動スケーリング無効化**
   - `min_machines_running = 0`（アイドル時は完全停止）
   - `auto_stop_machines = true`（トラフィックがないと自動停止）
   - `auto_start_machines = true`（リクエスト時のみ起動）

3. **データベース最適化**
   - PostgreSQL Development構成を使用
   - ストレージ: 1GBに制限

#### 課金監視
```bash
# 現在の課金状況を確認
fly billing --app swing-trading-tool

# リソース使用状況を確認
fly status --app swing-trading-tool
```

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
# データベースクラスタの作成（無料枠のDevelopment構成）
fly postgres create --name swing-db --region nrt \
    --initial-cluster-size 1 \
    --vm-size shared-cpu-1x \
    --volume-size 1 \
    --development

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

⚠️ **注意**: スケーリングは課金が発生します。無料枠を維持する場合は以下の制限内で運用してください：

```bash
# 無料枠を維持する場合はスケーリングを避ける
# マシン数は1、メモリは256MBに留める

# どうしても必要な場合（課金発生）
# fly scale count 2
# fly scale vm shared-cpu-2x --memory 1024
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
# ⚠️ メモリを増やすと課金が発生します
# 無料枠を維持する場合は、代わりに以下を試してください：
# 1. 不要な機能を無効化
# 2. キャッシュの最適化
# 3. データベースクエリの最適化

# どうしても必要な場合（課金発生）
# fly scale memory 1024
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

## 課金発生を防ぐためのチェックリスト

✅ **必ず確認**：
- [ ] `fly.toml`で`memory_mb = 256`に設定されている
- [ ] `min_machines_running = 0`に設定されている
- [ ] PostgreSQLはDevelopment構成を使用している
- [ ] ボリュームは1GB以下に設定されている
- [ ] マシン数は1台のみ

⚠️ **課金発生するアクション**：
- スケーリング（scale count、scale memoryなど）
- 追加のマシン起動
- プレミアム機能の使用
- 3GBを超えるボリューム

## 参考リンク

- [Fly.io Documentation](https://fly.io/docs/)
- [Fly.io Postgres](https://fly.io/docs/postgres/)
- [Fly.io Volumes](https://fly.io/docs/reference/volumes/)
- [Fly.io Pricing](https://fly.io/pricing/)

# 開発ガイド

## 環境構築

### 自動セットアップ（推奨）

```bash
python setup_environment.py
```

このスクリプトは以下を自動的に実行します：
- Python仮想環境の作成
- 依存関係のインストール（本番・開発環境）
- 設定ファイルの初期化
- データベースの初期化
- pre-commitフックの設定
- 必要なディレクトリ構造の作成

### 開発ツール

#### Makefile

よく使うコマンドをMakefileにまとめています：

```bash
make help         # 利用可能なコマンド一覧
make setup        # 環境構築
make test         # テスト実行
make lint         # コード品質チェック
make format       # コードフォーマット
make docs         # ドキュメントビルド
```

#### Pre-commit

コミット前に自動的にコード品質をチェックします：

- Ruff: 高速なPython linter
- Black: コードフォーマッター
- isort: import文の整理
- MyPy: 型チェック
- Bandit: セキュリティチェック

手動実行：
```bash
make pre-commit
```

## 設定管理

### 中央設定管理

`config.py`モジュールで設定を一元管理しています：

```python
from config import config, DB_PATH

# データベースパス
db_path = config.db_path

# APIエンドポイント
auth_url = config.get_api_endpoint("auth")

# カスタム設定値
custom_value = config.get("custom.key", default="default_value")
```

### 設定ファイル

- `config.json`: アプリケーション全体の設定
- `account.json`: J-Quants認証情報（.gitignoreに含まれる）
- `screening/thresholds.json`: スクリーニング閾値

## コーディング規約

### Python

- PEP 8準拠（Black/Ruffで自動整形）
- 型ヒントの使用を推奨
- docstringはGoogle/NumPyスタイル

### インポート順序

1. 標準ライブラリ
2. サードパーティライブラリ
3. ローカルモジュール

isortが自動的に整理します。

### 命名規則

- 変数・関数: snake_case
- クラス: PascalCase
- 定数: UPPER_SNAKE_CASE
- プライベート: 先頭に_を付ける

## テスト

### テストの実行

```bash
# 全テスト実行
make test

# 特定のテストのみ
pytest tests/test_config.py

# カバレッジレポート付き
pytest --cov=. --cov-report=html
```

### テストの書き方

- pytestを使用
- `tests/`ディレクトリに配置
- ファイル名は`test_*.py`
- テスト関数名は`test_*`

### フィクスチャ

共通のテストフィクスチャは`tests/conftest.py`に定義：

- `temp_db`: テスト用一時データベース
- `sample_config`: テスト用設定ファイル
- `mock_idtoken`: テスト用トークンファイル

## CI/CD

### GitHub Actions

`.github/workflows/ci.yml`で以下を自動実行：

1. **Lint & Type Check**: コード品質チェック
2. **Test**: ユニットテスト
3. **Validate Config**: 設定ファイルの妥当性チェック
4. **Build**: パッケージビルド

### ブランチ戦略

- `main`: 本番環境（保護されたブランチ）
- `develop`: 開発環境
- `feature/*`: 機能開発
- `bugfix/*`: バグ修正

## データベース

### スキーマ管理

`db/db_schema.py`でスキーマを定義。変更時は以下を実行：

```bash
# データベース再初期化
python db/db_schema.py

# 現在の状態確認
python db/db_summary.py
```

### パス管理

全てのモジュールで統一されたDBパスを使用：

```python
from config import DB_PATH

conn = sqlite3.connect(DB_PATH)
```

## セキュリティ

### 認証情報

- 認証情報は絶対にコミットしない
- `.gitignore`で除外設定済み
- `account.json`はローカルにのみ存在

### セキュリティスキャン

Banditで自動スキャン：

```bash
make lint  # Banditも実行される
```

## リリース

### バージョニング

セマンティックバージョニングを使用：
- MAJOR.MINOR.PATCH (例: 1.2.3)
- `pyproject.toml`でバージョン管理

### リリース手順

1. バージョン番号を更新
2. CHANGELOGを更新
3. タグを作成
4. GitHubでリリースを作成

## トラブルシューティング

### よくある問題

1. **import エラー**
   - `sys.path`の設定を確認
   - 仮想環境が有効か確認

2. **データベースエラー**
   - DBパスが正しいか確認
   - 初期化されているか確認

3. **APIエラー**
   - トークンの有効期限を確認
   - レート制限に注意

### デバッグ

ログレベルを変更してデバッグ：

```json
// config.json
{
  "logging": {
    "level": "DEBUG"
  }
}
```
# Windows環境構築ガイド

## 概要

このドキュメントは、Windows環境での株式分析ツール（Swing Trade Analysis Tool）の環境構築手順を説明します。

## 必要なシステム要件

- **OS**: Windows 10 または Windows 11
- **Python**: 3.9以上
- **メモリ**: 4GB以上推奨
- **ディスク容量**: 2GB以上の空き容量
- **ネットワーク**: インターネット接続（J-Quants API利用のため）

## 事前準備

### 1. Pythonのインストール

1. [Python公式サイト](https://www.python.org/downloads/)からPython 3.9以上をダウンロード
2. インストーラーを実行し、**必ず「Add Python to PATH」にチェックを入れる**
3. コマンドプロンプトまたはPowerShellで確認：
   ```cmd
   python --version
   ```

### 2. J-Quants APIアカウントの作成

1. [J-Quants](https://jpx-jquants.com/)でアカウントを作成
2. メールアドレスとパスワードを控えておく

## 自動環境構築

### 方法1: バッチファイルを使用（推奨）

1. プロジェクトフォルダを開く
2. `setup.bat` を**右クリック**→**「管理者として実行」**を選択
3. 画面の指示に従って進行

### 方法2: PowerShellスクリプトを使用

1. PowerShellを**管理者として実行**
2. 実行ポリシーを変更：
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```
3. プロジェクトフォルダに移動：
   ```powershell
   cd C:\\path\\to\\swing
   ```
4. スクリプトを実行：
   ```powershell
   .\\setup.ps1
   ```

## 手動環境構築

自動構築が失敗した場合は、以下の手順で手動構築を行ってください。

### 1. 仮想環境の作成

```cmd
# プロジェクトフォルダに移動
cd C:\\path\\to\\swing

# 仮想環境を作成
python -m venv venv

# 仮想環境を有効化
venv\\Scripts\\activate.bat
```

### 2. 依存関係のインストール

```cmd
# pipをアップグレード
python -m pip install --upgrade pip

# 本番環境の依存関係をインストール
pip install -r requirements.txt

# 開発環境の依存関係をインストール（オプション）
pip install -r requirements-dev.txt
```

### 3. 設定ファイルの作成

```cmd
# 設定ファイルをコピー
copy config.json.example config.json
copy account.json.example account.json
copy screening\\thresholds.json.example screening\\thresholds.json
```

### 4. 認証情報の設定

`account.json`を編集して、J-Quants APIの認証情報を設定：
```json
{
    \"mailaddress\": \"your-email@example.com\",
    \"password\": \"your-password\"
}
```

### 5. データベースの初期化

```cmd
python db\\db_schema.py
```

### 6. pre-commitフックの設定（オプション）

```cmd
pip install pre-commit
pre-commit install
```

## アプリケーションの起動

### 1. 仮想環境の有効化

```cmd
venv\\Scripts\\activate.bat
```

### 2. 認証トークンの取得

```cmd
python src\\cli\\update_idtoken.py
```

### 3. データの取得

```cmd
# 上場銘柄情報を取得
python fetch\\listed_info.py

# 日次株価データを取得
python fetch\\daily_quotes.py

# 財務諸表データを取得
python fetch\\statements.py
```

### 4. アプリケーションの起動

#### Webインターフェース（推奨）
```cmd
python -m src.ui.web
```
ブラウザで `http://localhost:5005` にアクセス

#### デスクトップGUI（レガシー）
```cmd
python -m src.ui.legacy.gui
```

## 主要なコマンド

### データ取得
```cmd
# 日次株価データ
python fetch\\daily_quotes.py --start 2024-01-01 --end 2024-12-31

# 財務諸表データ
python fetch\\statements.py 2

# 上場銘柄情報
python fetch\\listed_info.py
```

### スクリーニング
```cmd
# ファンダメンタルスクリーニング
python screening\\screen_statements.py

# テクニカルスクリーニング
python screening\\screen_technical.py screen

# 機械学習スクリーニング
python screening\\screen_ml.py screen
```

### バックテスト
```cmd
# ファンダメンタルバックテスト
python backtest\\backtest_statements.py --show

# テクニカルバックテスト
python backtest\\backtest_technical.py --show

# 機械学習バックテスト
python backtest\\backtest_ml.py --show
```

### その他
```cmd
# データベースの概要表示
python db\\db_summary.py

# スケジューラー起動
python -m src.cli.scheduler

# ログビューア
python scripts\\log_viewer.py list
```

## トラブルシューティング

### よくある問題と解決策

#### 1. 「python コマンドが見つかりません」エラー

**解決策**: Pythonのインストール時に「Add Python to PATH」を選択していない可能性があります。
- Pythonを再インストールするか、環境変数PATHにPythonのパスを追加してください

#### 2. 「管理者権限が必要です」エラー

**解決策**: コマンドプロンプトまたはPowerShellを管理者として実行してください。

#### 3. 依存関係のインストールに失敗

**解決策**:
- ネットワーク接続を確認
- ファイアウォール設定を確認
- 以下のコマンドで個別にインストールを試行：
  ```cmd
  pip install --upgrade pip
  pip install -r requirements.txt --no-cache-dir
  ```

#### 4. データベースの初期化に失敗

**解決策**:
- `db`フォルダが存在することを確認
- データベースファイルのアクセス権限を確認
- 既存の`stock.db`ファイルを削除して再実行

#### 5. J-Quants APIの認証に失敗

**解決策**:
- `account.json`の認証情報を確認
- J-Quants APIの利用規約を確認
- 以下のコマンドで手動でトークンを取得：
  ```cmd
  python src\\cli\\update_idtoken.py --mail your-email@example.com
  ```

### ログの確認

問題が発生した場合は、以下のログファイルを確認してください：

```cmd
# ログファイル一覧
python scripts\\log_viewer.py list

# 特定のログファイル表示
python scripts\\log_viewer.py view scheduler.log

# エラーログの検索
python scripts\\log_viewer.py search ERROR
```

## 開発者向け情報

### コード品質の確保

```cmd
# リンターの実行
ruff check --fix .

# コードフォーマット
black .

# テストの実行
pytest --cov=. --cov-report=html
```

### 設定ファイルの説明

- `config.json`: アプリケーション全般の設定
- `account.json`: J-Quants API認証情報
- `screening/thresholds.json`: スクリーニング閾値設定
- `config/login.json`: Webアプリケーション認証設定（オプション）

## サポート

問題が解決しない場合は、以下の情報を含めて開発者に連絡してください：

1. Windows版とPythonのバージョン
2. エラーメッセージ
3. 実行したコマンド
4. ログファイルの内容

---

**重要**: このツールは投資判断の参考情報を提供するものであり、投資結果について一切の責任を負いません。投資は自己責任で行ってください。

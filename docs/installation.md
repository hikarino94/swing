# インストール

## 自動セットアップ（推奨）

最も簡単な方法は、提供されている環境構築スクリプトを使用することです：

```bash
# リポジトリをクローン
git clone https://github.com/yourusername/swing.git
cd swing

# 環境構築スクリプトを実行
python setup_environment.py
```

このスクリプトは以下を自動的に実行します：
- Python仮想環境の作成
- 必要なパッケージのインストール
- 設定ファイルの初期化
- データベースのセットアップ
- pre-commitフックの設定

## 手動セットアップ

### 1. 仮想環境の作成

```bash
# 仮想環境を作成
python -m venv venv

# 仮想環境を有効化
# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

### 2. 依存関係のインストール

```bash
# 本番環境の依存関係
pip install -r requirements.txt

# 開発環境の依存関係（オプション）
pip install -r requirements-dev.txt
```

### 3. 設定ファイルの準備

```bash
# 設定ファイルをコピー
cp config.json.example config.json
cp account.json.example account.json
cp screening/thresholds.json.example screening/thresholds.json
```

### 4. J-Quants認証情報の設定

`account.json`を編集して、J-Quantsの認証情報を入力：

```json
{
  "mailaddress": "your-email@example.com",
  "password": "your-password"
}
```

### 5. データベースの初期化

```bash
python db/db_schema.py
```

### 6. pre-commitフックの設定（開発者向け）

```bash
pre-commit install
```

## 動作確認

インストールが正しく完了したか確認：

```bash
# トークンの取得
python update_idtoken.py

# データベースの状態確認
python db/db_summary.py
```

## トラブルシューティング

### Windowsでのエラー

Windowsで`unicodeescape`エラーが発生する場合は、パスの区切り文字に注意してください。

### 依存関係の競合

依存関係で問題が発生した場合は、仮想環境を削除して再作成してください：

```bash
# 仮想環境を削除
rm -rf venv  # Windows: rmdir /s venv

# 再度セットアップ
python setup_environment.py
```
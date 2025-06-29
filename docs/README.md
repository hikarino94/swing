# ドキュメント

このディレクトリには、Swing Trade Analysis Toolのドキュメントソースが含まれています。

## ドキュメントのビルド

### 必要なパッケージのインストール

```bash
pip install -r requirements-dev.txt
```

### ドキュメントのビルド

```bash
# Makefileを使用
make docs

# または直接実行
cd docs
sphinx-build -b html . _build/html
```

### ドキュメントの確認

```bash
# ローカルサーバーを起動
make serve-docs

# または直接実行
cd docs/_build/html
python -m http.server 8000
```

ブラウザで http://localhost:8000 を開いてドキュメントを確認できます。

## ドキュメントの構成

- `index.rst` - トップページ
- `getting_started.md` - はじめに
- `installation.md` - インストール手順
- `configuration.md` - 設定の詳細
- `modules/` - 各モジュールの詳細説明
- `api/` - APIリファレンス（自動生成）

## ドキュメントの書き方

- reStructuredText (.rst) またはMarkdown (.md) で記述
- Google/NumPyスタイルのdocstringを使用
- 日本語で記述

## 自動生成

APIドキュメントは、Pythonソースコードのdocstringから自動生成されます。`sphinx.ext.autodoc`拡張を使用しています。
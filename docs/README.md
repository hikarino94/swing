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

ブラウザで <http://localhost:8000> を開いてドキュメントを確認できます。

## ドキュメントの構成

- `index.rst` - トップページ
- `getting_started.md` - はじめに
- `installation.md` - インストール手順
- `configuration.md` - 設定の詳細
- `modules/` - 各モジュールの詳細説明
- `api/` - APIリファレンス（自動生成）

### 最近の更新

- 包括的なテストスイートの追加
- セットアップスクリプトによる環境構築の自動化
- 中央設定管理システムの導入
- CI/CDパイプラインの整備

## ドキュメントの書き方

- reStructuredText (.rst) またはMarkdown (.md) で記述
- Google/NumPyスタイルのdocstringを使用
- 日本語で記述

## 自動生成

APIドキュメントは、Pythonソースコードのdocstringから自動生成されます。`sphinx.ext.autodoc`拡張を使用しています。

## ドキュメントの更新

ドキュメントを更新する際は、以下の点に注意してください：

1. 新機能を追加した場合は、対応するドキュメントを追加
2. APIの変更がある場合は、docstringを更新
3. コマンドラインオプションが変更された場合は、使用例を更新
4. テストの追加・変更に伴い、テスト関連のドキュメントを更新

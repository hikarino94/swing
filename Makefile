# Makefile for Swing Trade Analysis Tool

.PHONY: help setup install install-dev clean test lint format docs serve-docs \
        db-init db-summary fetch-all backtest-all pre-commit

# デフォルトターゲット
help:
	@echo "利用可能なコマンド:"
	@echo "  make setup         - 完全な環境構築（仮想環境作成含む）"
	@echo "  make install       - 本番環境の依存関係をインストール"
	@echo "  make install-dev   - 開発環境の依存関係をインストール"
	@echo "  make clean         - キャッシュファイルやビルド成果物を削除"
	@echo "  make test          - テストを実行"
	@echo "  make lint          - コードの静的解析を実行"
	@echo "  make format        - コードフォーマットを実行"
	@echo "  make docs          - ドキュメントをビルド"
	@echo "  make serve-docs    - ドキュメントをローカルサーバーで確認"
	@echo "  make db-init       - データベースを初期化"
	@echo "  make db-summary    - データベースの概要を表示"
	@echo "  make fetch-all     - 全データを取得"
	@echo "  make backtest-all  - 全バックテストを実行"
	@echo "  make pre-commit    - pre-commitフックを実行"
	@echo "  make update-token  - J-Quants認証トークンを更新"
	@echo "  make run-scheduler - スケジューラーを起動"
	@echo "  make run-gui       - GUIアプリケーションを起動"
	@echo "  make run-web       - Webサーバーを起動"

# 環境構築
setup:
	python3 scripts/setup_environment.py

# 依存関係のインストール
install:
	pip install -r requirements.txt

install-dev: install
	pip install -r requirements-dev.txt

# クリーンアップ
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/
	rm -f bandit-report.json

# テスト実行
test:
	pytest -v --cov=. --cov-report=html --cov-report=term

# 静的解析
lint:
	ruff check .
	mypy . --ignore-missing-imports
	bandit -r . -f json -o bandit-report.json || true

# コードフォーマット
format:
	black .
	isort .
	ruff check --fix .

# ドキュメント
docs:
	cd docs && sphinx-build -b html . _build/html

serve-docs: docs
	cd docs/_build/html && python3 -m http.server 8000

# データベース操作
db-init:
	python3 db/db_schema.py

db-summary:
	python3 db/db_summary.py

# データ取得
fetch-quotes:
	python3 fetch/daily_quotes.py

fetch-listed:
	python3 fetch/listed_info.py

fetch-statements:
	python3 fetch/statements.py 2

fetch-all: fetch-listed fetch-quotes fetch-statements

# スクリーニング
screen-fundamental:
	python3 screening/screen_statements.py

screen-technical:
	python3 screening/screen_technical.py screen

screen-ml:
	python3 screening/screen_ml.py screen

# バックテスト
backtest-fundamental:
	python3 backtest/backtest_statements.py

backtest-technical:
	python3 backtest/backtest_technical.py

backtest-ml:
	python3 backtest/backtest_ml.py

backtest-all: backtest-fundamental backtest-technical backtest-ml

# Pre-commit
pre-commit:
	pre-commit run --all-files

# トークン更新
update-token:
	python3 src/cli/update_idtoken.py

# スケジューラー起動
run-scheduler:
	python3 -m src.cli.scheduler

# GUI起動
run-gui:
	python3 -m src.ui.legacy.gui

# Webサーバー起動
run-web:
	python3 -m src.ui.web

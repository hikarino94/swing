# CLAUDE.md

## 重要な指示

- 常に日本語で会話する
- コミットは日本語で実施する
- 修正した箇所は初心者でもわかりやすいように説明してください
- コードの変更理由と効果を具体的に説明してください
- 何か修正を加える場合は他に影響がないか必ず確認し、影響範囲を全て修正してください
- 修正を加えた後はLintを実行して構文エラーがないことを確認する

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python-based Japanese stock market analysis toolkit using J-Quants API data. The project provides:
- Stock screening (fundamental, technical, ML-based)
- Backtesting capabilities
- Both GUI (Tkinter) and web (Flask) interfaces
- Automated data fetching and scheduling

## Core Commands

### Development Setup
```bash
# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt  # 開発用ツール（pytest, coverage等）

# Setup pre-commit hooks for code quality
pip install pre-commit
pre-commit install

# Initialize database
python db/db_schema.py
```

### Code Quality & Linting
```bash
# Run linting (automatically runs on commit)
ruff check --fix .
black .

# Run tests
pytest

# Run tests with coverage
pytest --cov=. --cov-report=html --cov-report=term
```

### Key Operations
```bash
# Database summary
python db/db_summary.py

# Update J-Quants authentication token
python update_idtoken.py

# Fetch daily stock quotes
python fetch/daily_quotes.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]

# Run fundamental screening
python screening/screen_statements.py [--lookback DAYS] [--recent DAYS] [--as-of YYYY-MM-DD]

# Run technical screening
python screening/screen_technical.py screen [--as-of YYYY-MM-DD] [--lookback DAYS]

# Run ML screening
python screening/screen_ml.py train
python screening/screen_ml.py screen [--top N] [--lookback DAYS]

# List screening signals
python db/list_signals.py fund [--start YYYY-MM-DD] [--end YYYY-MM-DD]
python db/list_signals.py tech [--start YYYY-MM-DD] [--end YYYY-MM-DD]

# Run backtests
python backtest/backtest_statements.py [--hold DAYS] [--capital AMOUNT] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--show]
python backtest/backtest_technical.py [--hold-days DAYS] [--capital AMOUNT] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--show]
python backtest/backtest_ml.py [--top N] [--capital AMOUNT] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--show]

# Analyze backtest results
python -m backtest.analyze_backtest_json result.json [--show-trades] [--side long|short]

# Start GUI (Legacy)
python -m src.ui.legacy.gui

# Start web interface
python -m src.ui.web

# Start scheduler for automated data updates
python -m src.cli.scheduler

# View logs (command line)
python scripts/log_viewer.py list                         # ログファイル一覧
python scripts/log_viewer.py view scheduler.log           # ログファイル表示
python scripts/log_viewer.py tail scheduler.log -n 100    # 末尾100行表示
python scripts/log_viewer.py tail scheduler.log -f        # リアルタイム追跡
python scripts/log_viewer.py search ERROR                 # エラーログ検索
```

## Architecture & Data Flow

### Core Modules
- **`fetch/`**: Data acquisition from J-Quants API (daily_quotes, listed_info, statements)
- **`db/`**: SQLite database management and schema (stock.db with WAL mode)
- **`screening/`**: Stock filtering algorithms (fundamental, technical, ML-based)
- **`backtest/`**: Strategy validation and performance analysis
- **`templates/`**: Flask web interface templates

### Database Tables
- `prices`: Daily stock quotes
- `listed_info`: Company information (delete_flag付き)
- `statements`: Financial statements
- `fundamental_signals`: Fundamental screening results
- `technical_indicators`: Technical analysis signals (複数指標のフラグ付き)

### Configuration Files
- `config/account.json`: J-Quants API credentials (mail/password)
- `config/login.json`: Web app authentication (optional, falls back to account.json)
- `config/idtoken.json`: J-Quants API token (auto-generated)
- `screening/thresholds.json`: Screening parameter configuration
- `config/config.json`: General application configuration
- `pyproject.toml`: Project metadata and tool configuration

### Typical Workflow
1. **Data Collection**: Run fetch scripts to populate database
2. **Signal Generation**: Execute screening modules to identify opportunities
3. **Strategy Validation**: Run backtests to evaluate signal performance
4. **Analysis**: Review results via GUI/web interface or analyze JSON outputs

### Code Quality Standards
- **Ruff**: Linting with automatic fixes
- **Black**: Code formatting
- **Pre-commit hooks**: Enforced on every commit
- **Testing**: Comprehensive test suite using pytest (tests/ directory)
- **Coverage**: テストカバレッジレポート生成 (tests/reports/htmlcov/)

### Key Dependencies
- pandas: Data manipulation
- requests: API communication
- Flask: Web interface with Werkzeug security
- schedule: Automated tasks
- XlsxWriter: Excel output generation
- scikit-learn: Machine learning models
- tkinter: Desktop GUI interface
- pytest: Testing framework
- coverage: Test coverage analysis

## Important Notes

- All sensitive files (config/\*.json credentials, \*.db) are in .gitignore
- Scheduler runs daily at 20:00 (quotes), 20:30 (statements), Monday 6:00 (listed info)
- Results are timestamped and saved to data/output/ as both Excel and JSON formats
- Database uses SQLite with WAL mode for concurrent access
- Supports both long and short trading strategies in backtests
- Test suite includes comprehensive unit and integration tests
- ML models are saved as pickle files (db/models/ml_screen_model.pkl)
- Web app supports password hashing for secure authentication
- Backtesting supports various parameters: stop-loss, holding period, capital allocation

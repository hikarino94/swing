# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
必ず日本語で回答してください。
## Project Overview

This is a Japanese stock market analysis and trading system that integrates with J-Quants API for fetching stock data, screening stocks based on fundamental/technical/ML criteria, and backtesting trading strategies.

## Refactored Architecture (2025)

### Common Utilities (`/utils/`)
- **`config.py`** - Centralized configuration management (JSON files, tokens)
- **`db_utils.py`** - Database connection management with context managers
- **`jquants_client.py`** - J-Quants API client with retry and rate limiting
- **`logging_config.py`** - Unified logging configuration
- **`cli_utils.py`** - Common command-line argument parsing utilities
- **`common.py`** - Shared utility functions (date handling, file I/O, etc.)
- **`exceptions.py`** - Custom exception classes for better error handling
- **`screening_utils.py`** - Screening common utilities (price data, technical indicators, export)
- **`backtest_utils.py`** - Backtesting engine and utilities (signals, trades, results)

### Refactored Modules
#### `/fetch/` - Data Fetching
- **`daily_quotes.py`** - Refactored with DailyQuotesFetcher class and common utilities
- ~~`listed_info.py`~~ - Pending refactoring
- ~~`statements.py`~~ - Pending refactoring

#### `/screening/` - Stock Screening
- **`screen_statements.py`** - Refactored with service-oriented architecture:
  - `StatementsFetcher` - Database data retrieval
  - `FeaturesCalculator` - Financial metrics calculation  
  - `FundamentalScreener` - Filtering logic
  - `SignalsSaver` - Result persistence
  - `FundamentalScreeningService` - Orchestration layer

### Design Patterns Applied
1. **Service Layer Pattern** - Business logic separated from data access
2. **Factory Pattern** - Common creation of managers and clients
3. **Strategy Pattern** - Pluggable screening and backtesting algorithms
4. **Repository Pattern** - Unified data access interfaces

## Essential Commands

### Setup & Development
```bash
# Install dependencies and setup pre-commit hooks
pip install -r requirements.txt
pip install pre-commit
pre-commit install

# Initialize database
python db/db_schema.py

# Database info and signals
python db/db_summary.py
python db/list_signals.py [fund|tech] [--start DATE --end DATE]
```

### Code Quality
- Pre-commit hooks run `black` and `ruff` automatically on commit
- No manual lint/test commands - formatting happens via pre-commit

### Data Fetching
```bash
# Daily stock prices (current day or date range)
python fetch/daily_quotes.py [--start DATE --end DATE]

# Financial statements (mode 1=bulk by company, mode 2=by date/period) 
python fetch/statements.py [1|2] [--start DATE --end DATE]

# Listed company information
python fetch/listed_info.py
```

### Stock Screening
```bash
# Fundamental analysis screening
python screening/screen_statements.py [--lookback N --recent N --as-of DATE]

# Technical indicators screening  
python screening/screen_technical.py [indicators|screen] [--as-of DATE --lookback N]

# Machine learning screening
python screening/screen_ml.py [train|screen] [--top N --lookback N]
```

### Backtesting
```bash
# Fundamental signals backtest
python backtest/backtest_statements.py [--hold N --capital N --start DATE --end DATE --xlsx FILE --json FILE --show]

# Technical signals backtest  
python backtest/backtest_technical.py [--start DATE --end DATE --hold-days N --stop-loss N --capital N --outfile FILE --show]

# ML model backtest
python backtest/backtest_ml.py [--start DATE --end DATE --top N --capital N --outfile FILE --show]

# Analyze backtest results
python backtest/analyze_backtest_json.py FILE.json [--side long|short --show-trades]
```

### Applications
```bash
# Desktop GUI interface
python gui.py

# Web interface (Flask)
python web.py  

# Automated scheduler for data updates
python scheduler.py
```

## Architecture

### Database Schema (SQLite: `db/stock.db`)
- `prices` - Daily stock price data
- `statements` - Financial statement data  
- `listed_info` - Company listing information
- `fundamental_signals` - Fundamental analysis results
- `technical_indicators` - Technical analysis results

### Core Modules
- **`/fetch/`** - J-Quants API integration for data retrieval
- **`/screening/`** - Stock screening algorithms (fundamental, technical, ML)
- **`/backtest/`** - Strategy backtesting and performance analysis
- **`/db/`** - Database schema and utility functions

### Configuration Files Required
- `idtoken.json` - J-Quants API token: `{"idToken": "YOUR_TOKEN"}`
- `account.json` - J-Quants credentials for token refresh (optional)
- `login.json` - Web app authentication (optional, falls back to account.json)
- `screening/thresholds.json` - Screening parameter configuration

### Key Integration Points
- All modules use the central SQLite database for data persistence
- J-Quants API requires valid `idToken` for data access
- Screening results are stored as signals for backtesting
- Results export to Excel/JSON formats with timestamps

### Automated Workflows
- `scheduler.py` runs daily data fetches (20:00 prices, 20:30 statements, Mon 6:00 listings)
- Pre-commit hooks ensure code formatting with black/ruff
- Backtest results include comprehensive performance metrics and trade details
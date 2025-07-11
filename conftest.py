"""pytest configuration for the entire project"""

import os
import sys
from pathlib import Path

import pytest

# Add project root to Python path FIRST
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Also ensure PYTHONPATH is set for subprocess calls
os.environ["PYTHONPATH"] = str(project_root)

# Set test environment variables
os.environ.setdefault("TESTING", "1")
os.environ.setdefault("FLASK_ENV", "testing")

# Configure pytest
pytest_plugins: list[str] = []


@pytest.fixture(autouse=True)
def _setup_test_env(monkeypatch):
    """Automatically set up test environment for all tests."""
    # Ensure consistent environment variables
    monkeypatch.setenv("TESTING", "1")
    monkeypatch.setenv("FLASK_ENV", "testing")

    # Set database path for tests
    if "DATABASE_PATH" not in os.environ:
        test_db_path = project_root / "test_stock.db"
        monkeypatch.setenv("DATABASE_PATH", str(test_db_path))

    # Ensure PYTHONPATH is set correctly
    monkeypatch.setenv("PYTHONPATH", str(project_root))

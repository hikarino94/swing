"""pytest configuration for the entire project"""

import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Also ensure PYTHONPATH is set for subprocess calls
os.environ["PYTHONPATH"] = str(project_root)

# Configure pytest
pytest_plugins: list[str] = []

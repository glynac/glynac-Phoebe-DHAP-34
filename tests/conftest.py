"""
Pytest configuration file.
Adds the parent directory to Python path so tests can import utils module.
"""

import sys
from pathlib import Path

# Add the parent directory (airflow-dag-configs) to Python path
# This allows imports like: from utils.validators import ConfigValidator
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))

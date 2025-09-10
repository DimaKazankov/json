"""
Pytest configuration and fixtures for the Flink Kafka test suite.
"""

import sys
from pathlib import Path

# Add the project root to Python path for all tests
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

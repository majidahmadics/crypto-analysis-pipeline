"""Convenience script to run the ETL pipeline."""

import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

from src.main import main

if __name__ == "__main__":
    main()
# Lets `pytest` import the `mapreduce` and `jobs` packages from the repository root.
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

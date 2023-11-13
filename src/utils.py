"""Utility functions for the project."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from rich import print

THIS_DIR = Path(__file__).parent.absolute()
DATA_DIR = THIS_DIR.parent / "data"


def write_json(data: list[dict], path: Path, indent: int = 2):
    """Write JSON data to the provided path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing to [bold]{path}[/bold]")
    with open(path, "w") as f:
        json.dump(data, f, indent=indent)
    # Use os.subprocess to gzip the file
    subprocess.run(["gzip", "-9f", path])

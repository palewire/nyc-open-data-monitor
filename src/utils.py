"""Utility functions for the project."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from dateutil.parser import parse as dateparse
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


def get_sorted_file_list(
    data_dir: Path = DATA_DIR / "raw", ext: str = ".json.gz"
) -> list[Path]:
    """Return the JSON files from our clean data directory in reverse chronological order."""
    # Get all the JSON files
    file_list = list(data_dir.glob(f"*{ext}"))

    # Parse them
    file_tuples = []
    for f in file_list:
        if "additions" in f.stem or "latest" in f.stem:
            continue
        file_tuples.append((dateparse(f.stem.replace(".json", "")), f))

    # Sort them
    sorted_list = sorted(file_tuples, key=lambda x: x[0], reverse=True)

    # Return the path objects
    return [t[1] for t in sorted_list]

"""Utility functions for the project."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from dateutil.parser import parse as dateparse
import pandas as pd
from rich import print

THIS_DIR = Path(__file__).parent.absolute()
DATA_DIR = THIS_DIR.parent / "data"


def get_latest_download() -> pd.DataFrame:
    """Read in the latest download."""
    # Get the latest file
    latest_file = get_sorted_file_list()[0]

    # Read it in
    df = pd.read_json(latest_file, compression="gzip")

    # Set the the filename as a column
    df["filename"] = latest_file.stem.replace(".json", "")

    # Parse the filename as a date
    df["scrape_date"] = pd.to_datetime(df["filename"])

    # Return the result
    return df.apply(parse_row, axis=1)


def parse_row(row: dict) -> dict:
    """Parse a row of raw data and return only what we will keep for analysis."""
    return pd.Series(
        {
            "scrape_date": row["scrape_date"],
            "id": row["resource"]["id"],
            "name": safestr(row["resource"]["name"]),
            "type": safestr(row["resource"]["type"]),
            "update_date": pd.to_datetime(row["resource"]["updatedAt"]),
            "creation_date": pd.to_datetime(row["resource"]["createdAt"]),
            "creator": safestr(row["creator"]["display_name"]),
            "permalink": row["permalink"],
            "category": safestr(row["classification"].get("domain_category")),
            "description": clean_description(row["resource"]["description"]),
        }
    )


def safestr(value: str | None) -> str | None:
    """Return a string representation of a value."""
    # If the value is None, return None
    if not value or not value.strip():
        return None

    # Strip leading and trailing whitespace
    value = value.strip()

    # Replace multiple whitespaces with a single space
    value = " ".join(value.split())

    # Return the result
    return value


def clean_description(value: str) -> str | None:
    """Clean the description field."""
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    # Replace newlines with spaces
    value = value.replace("\n", " ")
    # Replace two or more spaces with one space
    value = " ".join(value.split())
    # Return the result
    return value


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

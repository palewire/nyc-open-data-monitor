"""Download the latest additions and updates to the NYC Open Data portal."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import click
import pytz
import requests
from retry import retry
from rich import print

from . import utils


@click.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=True,
    help="Print verbose output.",
)
def download(verbose: bool) -> None:
    """Download the latest additions and updates to the NYC Open Data portal."""
    this_dir = Path(__file__).parent.absolute()
    data_dir = this_dir.parent / "data" / "raw"

    # Get the current time
    tz = pytz.timezone("America/New_York")
    now = datetime.now(tz=tz)

    # Get the data
    data = get_url(verbose=verbose)

    # Write them out
    if verbose:
        print(f"Writing to [bold]{data_dir}[/bold]")
    utils.write_json(data["results"], data_dir / f"{now.isoformat()}.json")
    utils.write_json(data["results"], data_dir / "latest.json")


@retry()
def get_url(
    domains: str | list[str] = "data.cityofnewyork.us",
    search_context: str = "data.cityofnewyork.us",
    limit: int = 10000,
    order: str = "updatedAt",
    verbose: bool = True,
) -> dict:
    """Connect with the Socrata API."""
    url = "http://api.us.socrata.com/api/catalog/v1"
    params = {
        "domains": domains,
        "search_context": search_context,
        "limit": limit,
        "order": order,
    }
    if verbose:
        print(f"Downloading [bold]{url}[/bold]")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    if verbose:
        print(f"Found [bold]{len(data['results'])}[/bold] results.")
    return data


if __name__ == "__main__":
    download()

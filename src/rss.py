"""Create an RSS feed with the most recently created datasets."""
from __future__ import annotations

import click
import pandas as pd
from feedgen.feed import FeedGenerator

from . import utils


@click.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=True,
    help="Print verbose output.",
)
def rss(verbose: bool) -> None:
    """Create an RSS feed with the most recently created datasets."""
    clean_dir = utils.DATA_DIR / "clean"
    # Get the latest data
    df = pd.read_csv(
        clean_dir / "microdata.csv",
        parse_dates=["scrape_date", "creation_date", "update_date"],
    )

    # Sort by created date and list the 20 most recent
    df = df.sort_values("creation_date", ascending=False).head(20)

    # Convert into a list of dictionaries
    item_list = df.to_dict(orient="records")

    # Create a feed
    feed = FeedGenerator()
    feed.title("New datasets from opendata.cityofnewyork.us")
    feed.link(href="https://github.com/palewire/nyc-open-data-monitor")
    feed.description("The latest datasets posted to New York City's data portal")

    # Add the items
    for item in item_list:
        entry = feed.add_entry(order="append")
        entry.id(item["id"])
        entry.title(item["name"])
        entry.link(href=item["permalink"])
        entry.description(item["description"])
        entry.pubDate(item["creation_date"])
        entry.contributor(name=item["creator"])
        if item["category"]:
            entry.category(term=item["category"])

    # Write the feed
    feed.rss_file(clean_dir / "latest-created.rss")


if __name__ == "__main__":
    rss()

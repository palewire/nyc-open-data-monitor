"""Transform raw data into records ready for analysis."""
from __future__ import annotations

import click
import pandas as pd
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
def transform(verbose: bool) -> None:
    """Transform raw data into records ready for analysis."""
    # Get list of raw files
    file_list = utils.get_sorted_file_list()

    # Read them in as a dataframe
    df_list = []
    for f in file_list:
        df = pd.read_json(f, compression="gzip")
        # Set the the filename as a column
        df["filename"] = f.stem.replace(".json", "")
        # Parse the filename as a date
        df["scrape_date"] = pd.to_datetime(df["filename"])
        # Append to the list
        df_list.append(df)

    # Combine them all
    master_df = pd.concat(df_list).apply(parse_row, axis=1)

    # Count by day
    if verbose:
        print("Count by day")
    count_by_day = master_df.groupby("scrape_date").size()
    count_by_day.name = "n"
    count_by_day.to_csv(utils.DATA_DIR / "clean" / "observations.csv", header=True)

    # Drop duplicates
    latest_df = master_df.sort_values("scrape_date").drop_duplicates(
        subset=["id"], keep="first"
    )

    # Write it out as csv
    out_path = utils.DATA_DIR / "clean" / "latest.csv"
    if verbose:
        print(f"Writing {len(latest_df)} records to [bold]{out_path}[/bold]")
    latest_df.to_csv(out_path, index=False)


def parse_row(row: dict) -> dict:
    """Parse a row of data and return only what we will keep for analysis."""
    return pd.Series(
        {
            "scrape_date": row["scrape_date"],
            "id": row["resource"]["id"],
            "name": row["resource"]["name"],
            "description": row["resource"]["description"],
            "attribution": row["resource"]["attribution"],
            "attribution_link": row["resource"]["attribution_link"],
            "type": row["resource"]["type"],
            "updatedAt": pd.to_datetime(row["resource"]["updatedAt"]),
            "createdAt": pd.to_datetime(row["resource"]["createdAt"]),
            "publication_date": pd.to_datetime(row["resource"]["publication_date"]),
            "permalink": row["permalink"],
            "creator": row["creator"]["display_name"],
        }
    )


if __name__ == "__main__":
    transform()

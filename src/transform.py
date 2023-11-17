"""Transform raw data into records ready for analysis."""
from __future__ import annotations

import click
import pandas as pd
from rich import print
from rich.progress import track

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
    for f in track(file_list, description="Reading files"):
        df = pd.read_json(f, compression="gzip")
        # Set the the filename as a column
        df["filename"] = f.stem.replace(".json", "")
        # Parse the filename as a date
        df["scrape_date"] = pd.to_datetime(df["filename"])
        # Append to the list
        df_list.append(df)

    # Combine them all
    master_df = pd.concat(df_list).apply(parse_row, axis=1)

    # Count by scrape
    count_by_day = master_df.groupby("scrape_date").size()
    count_by_day.name = "n"
    clean_dir = utils.DATA_DIR / "clean"
    count_by_day.to_csv(clean_dir / "observations.csv", header=True)

    # Drop duplicates
    latest_df = master_df.sort_values("scrape_date").drop_duplicates(
        subset=["id"], keep="first"
    )

    # Type counts
    type_counts = latest_df.groupby("type").size()
    type_counts.name = "n"
    type_counts.to_csv(clean_dir / "type_counts.csv", header=True)

    # Creator counts
    creator_counts = latest_df.groupby("creator").size()
    creator_counts.name = "n"
    creator_counts.to_csv(clean_dir / "creator_counts.csv", header=True)

    # Category counts
    category_counts = latest_df.groupby("category").size()
    category_counts.name = "n"
    category_counts.to_csv(clean_dir / "category_counts.csv", header=True)

    # Monthly totals of new records
    monthly_totals = latest_df.groupby(pd.Grouper(key="creation_date", freq="M")).size()
    monthly_totals.name = "n"
    monthly_totals.to_csv(clean_dir / "creations_by_month.csv", header=True, index=True)

    # Calculate how many days since the last update to each dataset
    # Use the maximum scrape date for comparison.
    latest_scrape_date = latest_df["scrape_date"].max()
    latest_df["days_since_update"] = (
        latest_scrape_date - latest_df["update_date"]
    ).dt.days

    # Aggregate the distribution of days since update
    days_since_update = (
        latest_df.groupby("days_since_update").size().reset_index(name="n")
    )
    days_since_update.to_csv(
        clean_dir / "days_since_update.csv", header=True, index=False
    )

    # Drop that column
    latest_df = latest_df.drop(columns=["days_since_update"])

    # Get the previous scraped data
    out_path = clean_dir / "microdata.csv"
    prev_df = pd.read_csv(out_path)

    # Identify new records
    latest_ids = set(latest_df["id"])
    prev_ids = set(prev_df["id"])
    new_ids = latest_ids - prev_ids
    print(f"Found [bold]{len(new_ids)}[/bold] new records")

    # Write out the new records
    new_df = latest_df[latest_df["id"].isin(new_ids)]
    new_path = clean_dir / "new.csv"
    if verbose:
        print(
            f"Writing [bold]{len(new_df)}[/bold] new records to [bold]{new_path}[/bold]"
        )
    new_df.to_csv(new_path, index=False)

    # Write out the full dataset
    if verbose:
        print(f"Writing {len(latest_df)} records to [bold]{out_path}[/bold]")
    latest_df.to_csv(out_path, index=False)


def parse_row(row: dict) -> dict:
    """Parse a row of data and return only what we will keep for analysis."""
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


if __name__ == "__main__":
    transform()

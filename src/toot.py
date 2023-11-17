"""Post new datasets to Mastodon."""
from __future__ import annotations

import os
import time

import click
import pandas as pd
from mastodon import Mastodon
from rich import print

from . import utils


@click.command()
def cli():
    """Post latest datasets to Mastodon."""
    data = pd.read_csv(utils.DATA_DIR / "clean" / "new.csv")
    print(f"Tooting {len(data)} datasets")
    api = Mastodon(
        client_id=os.getenv("MASTODON_CLIENT_KEY"),
        client_secret=os.getenv("MASTODON_CLIENT_SECRET"),
        access_token=os.getenv("MASTODON_ACCESS_TOKEN"),
        api_base_url="https://mastodon.palewi.re",
    )
    for obj in data.to_dict(orient="records"):
        text = f"🔢 New dataset: “{obj['name']}” {obj['permalink']}"
        api.status_post(text)
        time.sleep(2)


if __name__ == "__main__":
    cli()

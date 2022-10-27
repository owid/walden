"""Ingest Global Carbon Budget data by the Global Carbon Project."""

from pathlib import Path

import click
from owid.walden import Dataset

# TODO: Change to public on November 11 (date of public release).
PUBLIC = False

# Path to current folder.
CURRENT_DIR = Path(__file__).parent
# List of metadata files with the information (including download URLs) of data files to download.
METADATA_FILES = [
    # Fossil CO2 emissions dataset (long csv file), containing global and national data on fossil fuel CO2 emissions
    # from 1750 until today..
    CURRENT_DIR / "global_carbon_budget_fossil_co2_emissions.meta.yml",
]

@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    for metadata_path in METADATA_FILES:
        # Get information (e.g. download url) from the metadata yaml file.
        metadata = Dataset.from_yaml(metadata_path)

        # Download dataset from source_data_url and add the local file to Walden's cache in: ~/.owid/walden
        dataset = Dataset.download_and_create(metadata)

        # Upload data file to S3.
        if upload:
            dataset.upload(public=PUBLIC)

        # Update walden index file.
        dataset.save()


if __name__ == "__main__":
    main()

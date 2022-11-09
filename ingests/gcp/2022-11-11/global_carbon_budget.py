"""Ingest Global Carbon Budget data by the Global Carbon Project."""

from pathlib import Path

import click

from owid.walden import Dataset
from etl.paths import DATA_DIR

# TODO: Change to public on November 11 (date of public release).
PUBLIC = False

# Path to current folder.
CURRENT_DIR = Path(__file__).parent
# List of metadata files with the information (including download URLs) of data files to download.
METADATA_FILES = [
    # Fossil CO2 emissions dataset (long csv file), containing global and national data on fossil fuel CO2 emissions
    # from 1750 until today.
    CURRENT_DIR / "global_carbon_budget_fossil_co2_emissions.meta.yml",
    ####################################################################################################################
    # TODO: Update source url links in all metadata files once data is public.
    # Global emissions.
    CURRENT_DIR / "global_carbon_budget_global_emissions.meta.yml",
    # National emissions.
    CURRENT_DIR / "global_carbon_budget_national_emissions.meta.yml",
    # National land-use change emissions.
    CURRENT_DIR / "global_carbon_budget_land_use_change_emissions.meta.yml",
    ####################################################################################################################
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

        ################################################################################################################
        # TODO: Remove the if clause and simply use download_and_create once files are public.
        #   Temporarily, the files have been manually copied on etl/data/gcp_temp (using the dataset short name as the
        #   file name and will be loaded from there.
        if metadata.source_data_url is None:
            path_to_file = (DATA_DIR / "gcp_temp" / metadata.short_name).with_suffix("." + metadata.file_extension)
            dataset = Dataset.copy_and_create(path_to_file, metadata)
        else:
            # Download dataset from source_data_url and add the local file to Walden's cache in: ~/.owid/walden
            dataset = Dataset.download_and_create(metadata)
        ################################################################################################################

        # Upload data file to S3.
        if upload:
            dataset.upload(public=PUBLIC)

        # Update walden index file.
        dataset.save()


if __name__ == "__main__":
    main()

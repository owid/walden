"""This script has been autogenerated by `walkthrough walden`."""
import tempfile
from pathlib import Path

import click
from owid.walden import Dataset
from structlog import get_logger

from shared import WCPD_SOURCES_DIR, WCPD_URL, extract_data_from_remote_zip_folder

log = get_logger()


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    metadata = Dataset.from_yaml(Path(__file__).parent / "world_carbon_pricing_sources.meta.yml")

    # Download zipped repository, extract data from files, and concatenated them into one dataframe.
    log.info("Download data from source.")
    data = extract_data_from_remote_zip_folder(zip_url=WCPD_URL, path_to_folder=WCPD_SOURCES_DIR)

    with tempfile.NamedTemporaryFile() as temp_file:
        # Save data into a temporary file.
        log.info("Save data to a temporary file.")
        data.to_csv(temp_file.name + ".zip", index=False, compression="zip")

        # Add the local temporary file to Walden's cache in ~/.owid/walden, with its metadata.
        dataset = Dataset.copy_and_create(temp_file.name + ".zip", metadata)

        # Upload file to S3.
        if upload:
            dataset.upload(public=True)

    # Update PUBLIC walden index with metadata.
    dataset.save()


if __name__ == "__main__":
    main()
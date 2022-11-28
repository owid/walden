"""Walden step for Human Development Report by the UNDP.

This data is reported anually by the UNDP in their website.

This script retrieves data from 2022.
"""

import os
from pathlib import Path
import tempfile
import shutil

import click
from structlog import get_logger

from owid.walden import Dataset, files, add_to_catalog

log = get_logger()


METADATA_URL = "https://hdr.undp.org/sites/default/files/2021-22_HDR/HDR21-22_Composite_indices_metadata.xlsx"


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Initiate dataset
        dataset = Dataset.from_yaml(Path(__file__).parent / "undp_hdr.meta.yml")
        # Prepare data file
        data_filename = prepare_datafile(dataset.metadata, tmp_dir)
        # Add to catalog
        add_to_catalog(dataset, data_filename, upload=upload, public=True)


def prepare_datafile(metadata, directory: str) -> str:
    # Download data
    DATA_URL = metadata["source_data_url"]
    files.download(DATA_URL, os.path.join(directory, "data.csv"))
    # Download metadata
    files.download(METADATA_URL, os.path.join(directory, "metadata.csv"))
    # Compress
    directory_zipped = compress_directory(directory, "undp_hdr")
    return directory_zipped


def compress_directory(directory, short_name):
    """Compress directory."""
    log.info("Zipping data...")
    shutil.make_archive(short_name, "zip", directory)
    return f"{short_name}.zip"


if __name__ == "__main__":
    main()

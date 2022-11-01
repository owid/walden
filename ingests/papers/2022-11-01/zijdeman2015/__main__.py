"""Get life expectancy at birth from Zindeman et al. (2015)."""

from pathlib import Path

import click
from structlog import get_logger

from owid.walden import Catalog, Dataset

log = get_logger()


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    metadata = Dataset.from_yaml(Path(__file__).parent / "meta.yml")

    # download dataset from source_data_url and add the local file to Walden's cache in ~/.owid/walden
    dataset = Dataset.download_and_create(metadata)

    # Update version
    dataset.version = dataset.date_accessed

    # Upload dataset
    dataset.upload_and_save(upload=upload, public=True)


def needs_to_be_updated(dataset: Dataset) -> bool:
    """Check if dataset needs to be updated.

    Retrieves last version of the dataset in Walden and compares it to the current version. Comparison is done by
    string comparing the MD5 checksums of the two datasets.

    TODO: move to class method of Dataset.

    Parameters
    ----------
    dataset : Dataset
        Dataset that was just retrieved.

    Returns
    -------
    bool
        True if dataset in Walden is outdated and a new version is needed.
    """
    log.info("Checking if dataset needs to be updated...")
    try:
        dataset_last = Catalog().find_latest(namespace=dataset.namespace, short_name=dataset.short_name)
        return dataset_last.md5 != dataset.md5
    except ValueError:
        return True


if __name__ == "__main__":
    main()

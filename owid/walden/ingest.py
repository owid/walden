"""Tools to ingest to Walden and Catalog."""

from typing import Optional, Union
from pathlib import Path

from .catalog import Dataset, INDEX_DIR
from .files import iter_docs
from .ui import log


def add_to_catalog(
    metadata: Union[dict, Dataset],
    filename: str,
    upload: bool = False,
    version: Optional[str] = None,
):
    """Add metadata to catalog.

    Additionally, it computes the md5 hash of the file, which is added to the metadata file.

    TODO: Add checks of fields.

    Args:
        metadata (dict): Dictionary with metadata.
        filename (str): Path to local data file. Used to compute the md5 hash.
        upload (bool): True to upload to DigitalOcean.
        version (str): Version to assign to dataset (if not given, it will be set based on dataset metadata).
    """
    # checksum happens in here, copy to cache happens here
    dataset = Dataset.copy_and_create(filename, metadata, version)

    if upload:
        # add it to our DigitalOcean Space and set `owid_cache_url`
        dataset.upload(public=True)

    # save the JSON to the local index
    dataset.save()

    log("ADDED TO CATALOG", f"{dataset.relative_base}.json")


def is_metadata_in_index(namespace, partial_metadata):
    """Read all metadata files in the walden index, under a certain namespace, and return True if partial_metadata is
    contained in any of them, with identical values.

    Args:
        namespace (str): Namespace in walden index.
        partial_metadata (dict): Fields in metadata to check for matches.
    """
    index_dir = Path(INDEX_DIR) / namespace
    metadata_in_index = False
    # If namespace is not found, there are obviously no matches.
    if index_dir.is_dir():
        for filename, index_file in iter_docs(index_dir):
            # Iterate over all files in index and check if any of their metadata matches the fields in partial_metadata.
            dataset_found = True
            for metadata_field in partial_metadata:
                dataset_found &= (
                    index_file.get(metadata_field) == partial_metadata[metadata_field]
                )

            if dataset_found:
                log("INFO", f"Metadata already exists in index file: {filename}")
                metadata_in_index = True

    return metadata_in_index

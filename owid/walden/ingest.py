"""Tools to ingest to Walden and Catalog."""

from typing import Optional, Union

from .catalog import Dataset
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

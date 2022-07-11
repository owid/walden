"""Ingestion of FAOSTAT data to walden S3 catalog and walden index.

This script will check if the snapshots of FAOSTAT datasets we have in walden are up-to-date. If any of the individual
datasets are not up-to-date, this script will:
 * Upload the latest version to S3.
 * Add a new walden index file for the new snapshot.

Files will be stored in S3 as:
* .zip files, for data of each of the individual domains (e.g. faostat_qcl.zip).
* .json files, for metadata (faostat_metadata.json).

Usage:
* To show available options:
```
poetry run python -m ingests.faostat -h
```
* To simply check if any of the datasets needs to be updated (without actually writing to S3 or walden index):
```
poetry run python -m ingests.faostat -r
```
* To check for updates and actually write to S3 and walden index:
```
poetry run python -m ingests.faostat
```

"""

import argparse
import datetime as dt
import json
import tempfile
from pathlib import Path
from typing import cast

import requests
from dateutil import parser

from owid.walden import add_to_catalog, files
from owid.walden.catalog import Dataset, INDEX_DIR
from owid.walden.files import iter_docs
from owid.walden.ui import log

# Version tag to assign to new walden folders (both in S3 bucket and in index).
VERSION = "2022"
# Global namespace for datasets.
NAMESPACE = "wpp"
# URL where data can be manually accessed (used in metadata, but not to actually retrieve the data).
DATA_URL = "https://population.un.org/wpp/Download/"
# Metadata source name.
SOURCE_NAME = "United Nations World Population Prospects 2022"
# Metadata related to license.
LICENSE_URL = "https://www.un.org/en/about-us/terms-of-use#general"
LICENSE_NAME = "CC BY 3.0 IGO"

# Base URL of API, used to download metadata (about countries, elements, items, etc.).
API_BASE_URL = "https://population.un.org/wpp/Download/"
# URL of walden repos and of this script (just to be included to walden index files as a reference).
GIT_URL_TO_WALDEN = "https://github.com/owid/walden/"
GIT_URL_TO_THIS_FILE = f"{GIT_URL_TO_WALDEN}blob/master/ingests/un_wpp.py"


class UNWPPDataset:
    namespace: str = NAMESPACE

    def __init__(self, dataset_metadata: dict):
        self._dataset_metadata = dataset_metadata
        self._dataset_server_metadata = self._load_dataset_server_metadata()

    def _load_dataset_server_metadata(self) -> dict:
        # Fetch only header of the dataset file on the server, which contains additional metadata, like last
        # modification date.
        head_request = requests.head(self.source_data_url)
        dataset_header = head_request.headers
        return cast(dict, dataset_header)

    @property
    def publication_year(self):
        return self.publication_date.year

    @property
    def publication_date(self) -> dt.date:
        return dt.datetime.fromisoformat(self._dataset_metadata["DateUpdate"]).date()

    @property
    def modification_date(self) -> dt.date:
        last_update_date_str = self._dataset_server_metadata["Last-modified"]
        last_update_date = parser.parse(last_update_date_str).date()
        return last_update_date

    @property
    def short_name(self):
        return f"{self.namespace}_{self._dataset_metadata['DatasetCode'].lower()}"

    @property
    def source_data_url(self):
        return self._dataset_metadata["FileLocation"]

    @property
    def metadata(self):
        f"""
        Walden-compatible view of this dataset's metadata.

        Required by the dataset index catalog (more info at {GIT_URL_TO_WALDEN}).
        """
        if self._dataset_metadata["DatasetDescription"] is None:
            # Description is sometimes missing (e.g. in faostat_esb), but a description is required in index.
            self._dataset_metadata["DatasetDescription"] = ""
            print(
                f"WARNING: Description for dataset {self.short_name} is missing. Type"
                " one manually."
            )
        return {
            "namespace": self.namespace,
            "short_name": self.short_name,
            "name": (
                f"{self._dataset_metadata['DatasetName']} - FAO"
                f" ({self.publication_year})"
            ),
            "description": self._dataset_metadata["DatasetDescription"],
            "source_name": SOURCE_NAME,
            "publication_year": self.publication_year,
            "publication_date": str(self.publication_date),
            "date_accessed": VERSION,
            "version": VERSION,
            "url": FAO_DATA_URL,
            "source_data_url": self.source_data_url,
            "file_extension": "zip",
            "license_url": LICENSE_URL,
            "license_name": LICENSE_NAME,
        }

    def to_walden(self):
        """
        Run faostat -> walden pipeline.

        Downloads the dataset from source, uploads it to Walden (DO/S3), creates the corresponding metadata file and
        places it in the walden local project repository.
        """
        with tempfile.NamedTemporaryFile() as f:
            # fetch the file locally
            files.download(self.source_data_url, f.name)

            # add it to walden, both locally, and to our remote file cache
            add_to_catalog(self.metadata, f.name, upload=True)


def load_faostat_catalog():
    datasets = requests.get(FAO_CATALOG_URL).json()["Datasets"]["Dataset"]
    return datasets


def is_dataset_already_up_to_date(source_data_url, source_modification_date):
    """Check if a dataset is already up-to-date in the walden index.

    Iterate over all files in walden index and check if:
    * The URL of the source data coincides with the one of the current dataset.
    * The last time the source data was accessed was more recently than the source's last modification date.

    If those conditions are fulfilled, we consider that the current dataset does not need to be updated.

    Args:
        source_data_url (str): URL of the source data.
        source_modification_date (date): Last modification date of the source dataset.
    """
    index_dir = Path(INDEX_DIR) / NAMESPACE
    dataset_up_to_date = False
    for filename, index_file in iter_docs(index_dir):
        index_file_source_data_url = index_file.get("source_data_url")
        index_file_date_accessed = dt.datetime.strptime(
            index_file.get("date_accessed"), "%Y-%m-%d"
        ).date()
        if (index_file_source_data_url == source_data_url) and (
            index_file_date_accessed > source_modification_date
        ):
            dataset_up_to_date = True

    return dataset_up_to_date

"""Ingestion of FAO data to Walden & Catalog.

Example usage:

```
poetry run python -m ingests.faostat
```

"""

import json
import datetime as dt
import tempfile
from dateutil import parser
from pathlib import Path
from typing import cast

import requests
import click

from owid.walden import files, add_to_catalog
from owid.walden.catalog import INDEX_DIR
from owid.walden.files import iter_docs
from owid.walden.ui import log

# Version tag to assign to walden folders (both in bucket and in index).
VERSION = str(dt.date.today())
NAMESPACE = "faostat"
FAO_DATA_URL = "http://www.fao.org/faostat/en/#data"
SOURCE_NAME = "Food and Agriculture Organization of the United Nations"
LICENSE_URL = "http://www.fao.org/contact-us/terms/db-terms-of-use/en"
LICENSE_NAME = "CC BY-NC-SA 3.0 IGO"
# Codes of datasets to download from FAO and upload to walden bucket.
INCLUDED_DATASETS_CODES = [
    # Food Security and Nutrition: Suite of Food Security Indicators
    "fs",
    # Food Balance: Food Balances (2014-)
    "fbs",
    # Food Balance: Food Balances (-2013, old methodology and population)
    "fbsh",
    # Production: Crops and livestock products
    "qcl",
    # Land, Inputs and Sustainability: Land Use
    "rl",
]
# URL for dataset codes in FAO catalog.
FAO_CATALOG_URL = (
    "http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json"
)
# Base URL of API, used to take a snapshot of various categories used in FAO datasets.
API_BASE_URL = "https://fenixservices.fao.org/faostat/api/v1/en/definitions/domain/{domain}/{category}?output_type=objects"
# Codes of datasets whose metadata should be fetched using the API.
METADATA_TO_FETCH_FROM_API = {
    "QCL": [
        "itemsgroup",
        "area",
        "element",
        "unit",
    ],  # note "itemsgroup" just for this domain
    "FBS": ["itemgroup", "area", "element", "unit"],
    "FBSH": ["itemgroup", "area", "element", "unit"],
}
GIT_URL_TO_WALDEN = "https://github.com/owid/walden/"
GIT_URL_TO_THIS_FILE = f"{GIT_URL_TO_WALDEN}blob/master/ingests/faostat.py"


class FAODataset:
    namespace: str = NAMESPACE

    def __init__(self, dataset_metadata: dict):
        """[summary]

        Args:
            dataset_metadata (dict): Dataset raw metadata.
        """
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
            log("INFO", f"Dataset is already up-to-date (index file: {filename}).")
            dataset_up_to_date = True

    return dataset_up_to_date


class FAOAdditionalMetadata:
    def __init__(self):
        # Assign current date to additional metadata.
        self.publication_date = str(dt.datetime.today().date())
        self.publication_year = dt.datetime.today().date().year

    @property
    def metadata(self):
        return {
            "namespace": NAMESPACE,
            "short_name": f"{NAMESPACE}_metadata",
            "name": f"Metadata and identifiers - FAO ({self.publication_year})",
            "description": "Metadata and identifiers used in FAO datasets",
            "source_name": SOURCE_NAME,
            "url": FAO_DATA_URL,
            "date_accessed": VERSION,
            "version": VERSION,
            "file_extension": "json",
            "license_url": LICENSE_URL,
            "license_name": LICENSE_NAME,
            "publication_year": self.publication_year,
            "publication_date": self.publication_date,
            "source_notes": f"API snapshot captured by script at {GIT_URL_TO_THIS_FILE}",
        }

    @staticmethod
    def _fetch_additional_metadata(output_filename):
        metadata_combined = {}
        # Fetch additional metadata for each domain and category using API.
        for domain, categories in METADATA_TO_FETCH_FROM_API.items():
            log("INFO", f"Fetching additional metadata for domain {domain}.")
            domain_meta = {}
            for category in categories:
                resp = requests.get(
                    API_BASE_URL.format(domain=domain, category=category)
                )
                domain_meta[category] = resp.json()

            metadata_combined[domain] = domain_meta

        # Save additional metadata to temporary local file.
        with open(output_filename, "w") as _output_filename:
            json.dump(metadata_combined, _output_filename, indent=2, sort_keys=True)

        return metadata_combined

    def to_walden(self):
        with tempfile.NamedTemporaryFile() as f:
            # fetch the file locally
            self._fetch_additional_metadata(f.name)

            # add it to walden, both locally, and to our remote file cache
            add_to_catalog(self.metadata, f.name, upload=True)


@click.command()
def main():
    any_dataset_was_updated = False
    # Fetch dataset codes from FAO catalog.
    faostat_catalog = load_faostat_catalog()
    for description in faostat_catalog:
        # Build FAODataset instance
        if description["DatasetCode"].lower() in INCLUDED_DATASETS_CODES:
            faostat_dataset = FAODataset(description)
            # Skip dataset if it already is up-to-date in index.
            if is_dataset_already_up_to_date(
                source_data_url=faostat_dataset.source_data_url,
                source_modification_date=faostat_dataset.modification_date,
            ):
                continue
            else:
                # Download dataset, upload file to walden bucket and add metadata file to walden index.
                faostat_dataset.to_walden()
                any_dataset_was_updated = True

    # Fetch additional metadata only if at least one dataset was updated.
    if any_dataset_was_updated:
        additional_metadata = FAOAdditionalMetadata()
        # Fetch additional metadata from FAO API, upload file to walden bucket and add metadata file to walden index.
        additional_metadata.to_walden()
    else:
        log(
            "INFO",
            f"No need to fetch additional metadata, since all datasets are up-to-date.",
        )


if __name__ == "__main__":
    main()

"""Ingestion of FAO data to Walden & Catalog.

Example usage:

```
poetry run python -m ingests.faostat
```

"""


import datetime as dt
import tempfile
from dateutil import parser
from typing import cast

import requests
import click

from owid.walden import files, add_to_catalog


INCLUDED_DATASETS = [
    "Food Security and Nutrition: Suite of Food Security Indicators",  # FS
    "Production: Crops and livestock products",  # QCL
    "Food Balance: Food Balances (-2013, old methodology and population)",  # FBSH
    "Food Balance: Food Balances (2014-)",  # FBS
]

INCLUDED_DATASETS_CODES = [
    "FS",
    "FBS",
    "FBSH",
    "QCL",
]


class FAODataset:
    namespace: str = "faostat"
    url: str = "http://www.fao.org/faostat/en/#data"
    source_name: str = "Food and Agriculture Organization of the United Nations"
    _extra_metadata = {}

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
        return f"{self.namespace}_{self._dataset_metadata['DatasetCode']}"

    @property
    def source_data_url(self):
        return self._dataset_metadata["FileLocation"]

    @property
    def metadata(self):
        """
        Walden-compatible view of this dataset's metadata.

        Required by the dataset index catalog (more info at https://github.com/owid/walden).
        """
        return {
            "namespace": self.namespace,
            "short_name": f"{self.namespace}_{self._dataset_metadata['DatasetCode']}",
            "name": (
                f"{self._dataset_metadata['DatasetName']} - FAO"
                f" ({self.publication_year})"
            ),
            "description": self._dataset_metadata["DatasetDescription"],
            "source_name": "Food and Agriculture Organization of the United Nations",
            "publication_year": self.publication_year,
            "publication_date": str(self.publication_date),
            "modification_date": str(self.modification_date),
            "date_accessed": str(dt.date.today()),
            "url": self.url,
            "source_data_url": self.source_data_url,
            "file_extension": "zip",
            "license_url": "http://www.fao.org/contact-us/terms/db-terms-of-use/en",
            "license_name": "CC BY-NC-SA 3.0 IGO",
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
    url_datasets = (
        "http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json"
    )
    datasets = requests.get(url_datasets).json()["Datasets"]["Dataset"]
    return datasets


@click.command()
def main():
    faostat_catalog = load_faostat_catalog()
    for description in faostat_catalog:
        # Build FAODataset instance
        if description["DatasetCode"] in INCLUDED_DATASETS_CODES:
            faostat_dataset = FAODataset(description)
            # Run download pipeline
            faostat_dataset.to_walden()


if __name__ == "__main__":
    main()

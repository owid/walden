"""Ingestion of FAO data to Walden & Catalog."""


from datetime import datetime
import os
import tempfile

import requests
import click

from owid.walden.ingest import ingest_to_walden, add_to_catalog


DATASET_NAMES = [
    "Food Security: Suite of Food Security Indicators",
    "Production: Crops",
]


class FAODataset:
    namespace: str = "faostat"
    url: str =  "http://www.fao.org/faostat/en/#data"
    source_name: str = "Food and Agriculture Organization of the United Nations"
    _extra_metadata = {}

    def __init__(self, dataset_metadata: dict, catalog_dir: str):
        """[summary]

        Args:
            dataset_metadata (dict): Dataset raw metadata.
            catalog_dir (str): walden project local directory (clone project from https://github.com/owid/walden).
        """
        self._dataset_metadata = dataset_metadata
        self.catalog_dir = catalog_dir
        self.owid_data_url = None

    @property
    def filename(self):
        return os.path.basename(self._dataset_metadata['FileLocation'])

    @property
    def publication_year(self):
        return datetime.strptime(self._dataset_metadata['DateUpdate'], "%Y-%m-%d").strftime("%Y")

    @property
    def short_name(self):
        return f"{self.namespace}_{self._dataset_metadata['DatasetCode']}"

    @property
    def source_data_url(self):
        return self._dataset_metadata['FileLocation']

    @property
    def walden_dataset_path(self):
        return f"{self.namespace}/{self.publication_year}/{self.filename}"

    @property
    def catalog_dataset_path(self):
        return os.path.join(self.catalog_dir, self.namespace, self.publication_year, self.short_name)

    @property
    def metadata(self):
        """Metadata file.

        Required by the dataset index catalog (more info at https://github.com/owid/walden).
        """
        return {
            "namespace": self.namespace,
            "short_name": f"{self.namespace}_{self._dataset_metadata['DatasetCode']}",
            "name": f"{self._dataset_metadata['DatasetName']} - FAO ({self.publication_year})",
            "description": self._dataset_metadata['DatasetDescription'],
            "source_name": "Food and Agriculture Organization of the United Nations",
            "publication_year": self.publication_year,
            "publication_date": self._dataset_metadata['DateUpdate'],
            "url": self.url,
            "owid_data_url": self.owid_data_url,
            "source_data_url": self.source_data_url,
        }

    def download(self, output_file: str):
        """Download dataset.

        Args:
            output_folder (str): Folder where to store downloaded dataset.

        Returns:
            str: Complete path to dataset in the local folder.
        """
        r = requests.get(self.source_data_url)
        click.echo(click.style("DOWNLOAD   ", fg="blue") + f"{self.source_data_url} -> {output_file}")
        with open(output_file, 'wb') as f:
            f.write(r.content)

    def to_walden(self):
        """Run faostat -> walden pipeline.

        Downloads the dataset from source, uploads it to Walden (DO/S3), creates the corresponding metadata file and
        places it in the walden local project repository.
        """
        with tempfile.NamedTemporaryFile() as f:
            # Download data
            self.download(output_file=f.name)
            # Upload data to Walden
            url = ingest_to_walden(f.name, self.walden_dataset_path, public=True)
            # Create and add metadata
            self.owid_data_url = url
            add_to_catalog(self.metadata, f.name, self.catalog_dataset_path)


def load_datasets_metadata():
    url_datasets = "http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json"
    datasets = requests.get(url_datasets).json()['Datasets']['Dataset']
    return datasets


@click.command()
@click.option(
    "--catalog_dir",
    required=True,
    help="Path to local catalog directory. Find online version at https://github.com/owid/walden/blob/master/index",
)
def main(catalog_dir):
    datasets = load_datasets_metadata()
    for dataset in datasets:
        # Build FAODataset instance
        if dataset["DatasetName"] in DATASET_NAMES:
            faostat_dataset = FAODataset(dataset, catalog_dir)
            # Run download pipeline
            faostat_dataset.to_walden()


if __name__ == "__main__":
    main()

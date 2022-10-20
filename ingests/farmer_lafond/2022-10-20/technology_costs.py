"""Upload to Walden a local data file provided in a private communication.

The data results from the paper J. Doyne Farmer and François Lafond (2016), How predictable is technological progress?
https://www.sciencedirect.com/science/article/pii/S0048733315001699

"""

import click
import pandas as pd

from owid.walden import Dataset
from owid.walden.ingest import add_to_catalog
from shared import CURRENT_DIR

# Path to metadata file.
METADATA_PATH = (CURRENT_DIR / "technology_costs").with_suffix(".meta.yml")

@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
@click.option(
    "--local_data_file",
    type=str,
    help="Local data file to upload to Walden",
    required=True,
)
def main(upload: bool, local_data_file: str) -> None:
    # Load raw data.
    df = pd.read_csv(local_data_file)

    # Get walden metadata.
    dataset = Dataset.from_yaml(METADATA_PATH)

    # Add data to Walden catalog and metadata to Walden index.
    add_to_catalog(metadata=dataset, dataframe=df, upload=upload)

    # Update Walden datasets.
    dataset.save()


if __name__ == "__main__":
    main()

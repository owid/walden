import io
import os
import tempfile
import zipfile
from pathlib import Path

import click
import pandas as pd
import requests
from owid.catalog.frames import repack_frame
from structlog import get_logger

from owid.walden import add_to_catalog
from owid.walden.catalog import Dataset

log = get_logger()


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:

    locations = get_location_hierachies()

    start_years = [1950, 1960, 1970, 1980, 1990, 2000, 2010]
    df_all = []
    metadata = Dataset.from_yaml(Path(__file__).parent / "ihme_child_mortality.meta.yml")

    with tempfile.TemporaryDirectory() as temp_dir:
        for start_year in start_years:
            log.info("Downloading data...", year=start_year)
            end_year = start_year + 9
            url = f"https://ghdx.healthdata.org/sites/default/files/record-attached-files/IHME_GBD_2019_U5M_{start_year}_{end_year}_CT_RT_0.zip"
            r = requests.get(url)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(temp_dir)
            csv_file = f"IHME_GBD_2019_U5M_{start_year}_{end_year}_CT_RT_Y2021M09D01.CSV"
            df = pd.read_csv(os.path.join(temp_dir, csv_file))
            df = df[df["location_name"].isin(locations)]
            df_all.append(df)

        dataset = pd.concat(df_all)
        # consolidate data
        dataset = repack_frame(dataset)
        data_file = os.path.join(temp_dir, f"data.{metadata.file_extension}")
        dataset = dataset.reset_index()
        dataset.to_feather(data_file)
        add_to_catalog(metadata, data_file, upload=upload)


def get_location_hierachies():

    location_hierachies = "https://ghdx.healthdata.org/sites/default/files/record-attached-files/IHME_GBD_2019_GBD_LOCATION_HIERARCHY_Y2022M06D29.XLSX"
    r = requests.get(location_hierachies).content
    xl = pd.ExcelFile(r)
    lh = xl.parse("Sheet1")
    # We only want the global, some of the high level regions and the country level data, levels 0,1 and 3
    lh = lh[lh["Level"].isin([0, 1, 3])]
    locations = lh["Location Nam"].to_list()
    return locations


if __name__ == "__main__":
    main()

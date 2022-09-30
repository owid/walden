import json
import os
import tempfile
from pathlib import Path
from typing import List

import click
import pandas as pd
import requests
from owid.catalog.frames import repack_frame
from structlog import get_logger

from owid.walden import Dataset, add_to_catalog

log = get_logger()
# get the name of each of the causes of death so we can iterate through them


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool):
    with tempfile.TemporaryDirectory() as temp_dir:
        log.info("Creating metadata...")
        metadata = Dataset.from_yaml(Path(__file__).parent / "who_ghe.meta.yml")
        causes = get_causes_list()
        dataset = download_cause_data(causes)
        data_file = os.path.join(temp_dir, f"data.{metadata.file_extension}")
        dataset.to_feather(data_file)
        add_to_catalog(metadata, data_file, upload=upload)


def get_causes_list() -> List[str]:
    url = "https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL?$apply=groupby((DIM_GHECAUSE_TITLE))"
    res = requests.get(url)
    assert res.ok
    value_json = json.loads(res.content)["value"]
    causes = pd.DataFrame.from_records(value_json)["DIM_GHECAUSE_TITLE"].tolist()
    return causes


def get_cause_data(url) -> pd.DataFrame:
    data_json = requests.get(url).json()
    data_df = pd.DataFrame.from_records(data_json["value"])
    return data_df


def download_cause_data(causes) -> pd.DataFrame:
    all_data = []
    for cause in causes:
        log.info("Downloading...", cause=cause)
        url = f"https://frontdoor-l4uikgap6gz3m.azurefd.net/DEX_CMS/GHE_FULL?$filter=DIM_GHECAUSE_TITLE%20eq%20%27{cause}%27%20and%20DIM_SEX_CODE%20eq%20%27BTSX%27and%20DIM_AGEGROUP_CODE%20eq%20%27ALLAges%27&$select=DIM_GHECAUSE_TITLE,DIM_YEAR_CODE,DIM_COUNTRY_CODE,DIM_AGEGROUP_CODE,DIM_SEX_CODE,VAL_DALY_COUNT_NUMERIC,VAL_DEATHS_COUNT_NUMERIC,VAL_DEATHS_RATE100K_NUMERIC,VAL_DEATHS_COUNT_NUMERIC"
        df = get_cause_data(url)
        all_data.append(df)
    all_df = pd.concat(all_data)
    all_df = repack_frame(all_df)
    all_df = all_df.reset_index()

    return all_df


if __name__ == "__main__":
    main()

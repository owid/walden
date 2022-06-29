import json
import yaml
import pandas as pd
import tempfile
import requests
import datetime as dt
import os

from structlog import get_logger
from io import BytesIO
from pathlib import Path

from owid.walden import add_to_catalog
from owid.walden.catalog import Dataset


base_url = "https://unstats.un.org/sdgapi"
log = get_logger()

URL_METADATA = "https://unstats.un.org/sdgs/indicators/SDG_Updateinfo.xlsx"
MAX_RETRIES = 10
CHUNK_SIZE = 8192


def main():
    print("Creating metadata...")
    metadata = create_metadata()
    with tempfile.TemporaryDirectory() as temp_dir:
        # print(type(temp_dir))
        # fetch the file locally
        assert metadata.source_data_url is not None
        print("Downloading data...")
        all_data = download_data()
        print("Saving data...")
        output_file = os.path.join(temp_dir, f"data.{metadata.file_extension}")
        all_data.to_csv(output_file, index=False)
        print("Adding to catalog...")
        add_to_catalog(metadata, output_file, upload=True)  # type: ignore


def create_metadata():
    meta = load_yaml_metadata()
    meta.update(load_external_metadata())

    return Dataset(
        **meta,
        date_accessed=dt.datetime.now().date(),
    )


def load_yaml_metadata() -> dict:
    fpath = Path(__file__).parent / f"{Path(__file__).stem}.meta.yml"
    with open(fpath) as istream:
        meta = yaml.safe_load(istream)
    return meta


def load_external_metadata() -> dict:
    meta_orig = pd.read_excel(URL_METADATA)
    meta_orig.columns = ["updated", "detail"]
    pub_date = meta_orig["detail"].iloc[0].date()

    meta = {
        "name": f"United Nations Sustainable Development Goals - United Nations ({pub_date})",
        "publication_year": pub_date.year,
        "publication_date": f"{pub_date}",
    }
    return meta


def download_data() -> pd.DataFrame:
    # retrieves all goal codes
    print("Retrieving SDG goal codes...")
    url = f"{base_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok

    goals = json.loads(res.content)
    goal_codes = [str(goal["code"]) for goal in goals]

    # retrieves all area codes
    print("Retrieving area codes...")
    url = f"{base_url}/v1/sdg/GeoArea/List"
    res = requests.get(url)
    assert res.ok
    areas = json.loads(res.content)
    area_codes = [str(area["geoAreaCode"]) for area in areas]
    # retrieves csv with data for all codes and areas
    print("Retrieving data...")
    url = f"{base_url}/v1/sdg/Goal/DataCSV"
    all_data = []
    for goal in goal_codes:
        content = download_file(
            url=url, goal=goal, area_codes=area_codes, max_retries=MAX_RETRIES
        )
        df = pd.read_csv(BytesIO(content), low_memory=False)
        all_data.append(df)
    all_data = pd.concat(all_data)

    return all_data


def download_file(
    url: str, goal: int, area_codes: list, max_retries: int, bytes_read: int = 0
) -> bytes:
    """Downloads a file from a url.

    Retries download up to {max_retries} times following a ChunkedEncodingError
    exception.
    """
    log.info(
        f"Downloading data...",
        url=url,
        bytes_read=bytes_read,
        remaining_retries=max_retries,
    )
    if bytes_read:
        headers = {"Range": f"bytes={bytes_read}-"}
    else:
        headers = {}

    content = b""
    try:
        with requests.post(
            url,
            data={"goal": goal, "areaCodes": area_codes},
            headers=headers,
            stream=True,
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                bytes_read += CHUNK_SIZE
                content += chunk
    except requests.exceptions.ChunkedEncodingError:
        if max_retries > 0:
            log.info("Encountered ChunkedEncodingError, resuming download...")
            content += download_file(
                url=url,
                goal=goal,
                area_codes=area_codes,
                max_retries=max_retries - 1,
                bytes_read=bytes_read,
            )
        else:
            log.error(
                "Encountered ChunkedEncodingError, but max_retries has been "
                "exceeded. Download may not have been fully completed."
            )
    return content


if __name__ == "__main__":
    main()

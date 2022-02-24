"""This script should be manually adapted and executed on the event of an update of the CAIT datasets.

On the event of a new update, manually check and update all fixed inputs below. Then execute this script, which will:
* Download greenhouse gas emissions data from CAIT using Climate Watch Data API.
* Compress the data and upload it to S3 Walden bucket.
* Generate the required metadata, including the md5 hash of the compressed file, and write it to the Walden index.

See https://www.climatewatchdata.org/data-explorer/historical-emissions

"""

import argparse
import gzip
import json
import hashlib
import os
import tempfile
from datetime import datetime
from time import sleep

import boto3
import requests
from tqdm.auto import tqdm

########################################################################################################################

# Fixed inputs.

# CAIT API URL.
CAIT_API_URL = "https://www.climatewatchdata.org/api/v1/data/historical_emissions/"
# Number of records to fetch per api request.
API_RECORDS_PER_REQUEST = 500
# Time to wait between consecutive api requests.
TIME_BETWEEN_REQUESTS = 0.1
# Name of institution.
INSTITUTION_NAME = "cait"
DATASET_NAME = "cait_ghg_emissions"
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
# Path to Walden index folder and subfolders, where metadata for this dataset will be stored.
WALDEN_INDEX_DIR = os.path.join(CURRENT_DIR, "..", "owid", "walden", "index")
DATE_TAG = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DATA_ENTRY_DIR = os.path.join(WALDEN_INDEX_DIR, INSTITUTION_NAME, DATE_TAG)
OUTPUT_DATA_ENTRY_FILE = os.path.join(OUTPUT_DATA_ENTRY_DIR, DATASET_NAME + ".json")
# Define S3 base URL.
S3_URL = "https://nyc3.digitaloceanspaces.com"
# Profile name to use for S3 client (as defined in .aws/config).
S3_PROFILE_NAME = "default"
# S3 bucket name and folder where dataset file will be stored.
S3_BUCKET_NAME = "walden"
S3_DATA_FILE = os.path.join(INSTITUTION_NAME, DATE_TAG, DATASET_NAME + ".zip")
# True to make S3 file readable by everyone.
S3_MAKE_FILE_PUBLIC = True
# Publication year of the CAIT dataset.
PUBLICATION_YEAR = "2021"
# Publication date can be found in the notifications area, where they have a log of all updates.
PUBLICATION_DATE = "2021-03-11"
METADATA = {
    "namespace": INSTITUTION_NAME,
    "short_name": DATASET_NAME,
    "name": f"Greenhouse gas emissions by country and sector (CAIT, {PUBLICATION_YEAR})",
    "description": "Total greenhouse gas emissions are measured in tonnes of carbon dioxide equivalents (CO₂e), based "
    "on 100-year global warming potential factors for non-CO₂ gases. This data is published by country "
    "and sector from the CAIT Climate Data Explorer, and downloaded from the Climate Watch Portal.",
    "source_name": "Climate Analysis Indicators Tool",
    "publication_year": PUBLICATION_YEAR,
    "publication_date": PUBLICATION_DATE,
    "date_accessed": DATE_TAG,
    "url": CAIT_API_URL,
    "owid_data_url": os.path.join(S3_URL, S3_BUCKET_NAME, S3_DATA_FILE),
    "file_extension": "zip",
    "license_url": "https://www.climatewatchdata.org/about/permissions",
    "license_name": "Creative Commons CC BY 4.0",
    "access_notes": "Fetched via API using walden/ingests/cait_ghg_emissions.py",
    # MD5 hash will be added later.
    "md5": "",
}

########################################################################################################################


def fetch_all_data_from_api(
    api_url=CAIT_API_URL,
    api_records_per_request=API_RECORDS_PER_REQUEST,
    time_between_requests=TIME_BETWEEN_REQUESTS,
):
    """Fetch all CAIT data from Climate Watch Data API.

    Parameters
    ----------
    api_url : str
        API URL.
    api_records_per_request : int
        Maximum number of records to fetch per API request.
    time_between_requests : float
        Time to wait between consecutive API requests.

    Returns
    -------
    data_all : list
        Raw data (list with one dictionary per record).

    """
    # Start requests session.
    session = requests.Session()
    # The total number of records in the database is returned on the header of each request.
    # Send a simple request to get that number.
    response = session.get(url=api_url)
    total_records = int(response.headers["total"])
    print(f"Total number of records to fetch from API: {total_records}")

    # Number of requests to ensure all pages are requested.
    total_requests = round(total_records / api_records_per_request) + 1
    # Collect all data from consecutive api requests. This could be sped up by parallelizing requests.
    data_all = []
    for page in tqdm(range(1, total_requests + 1)):
        response = session.get(
            url=api_url, json={"page": page, "per_page": api_records_per_request}
        )
        new_data = json.loads(response.content)["data"]
        if len(new_data) == 0:
            print("No more data to fetch.")
            break
        data_all.extend(new_data)
        sleep(time_between_requests)

    return data_all


def save_compressed_data_to_file(data, data_file):
    """Compress data and save it as a zip file.

    Parameters
    ----------
    data : list
        Raw data.
    data_file : str
        Path to output file.

    """
    with gzip.open(data_file, "wt", encoding="UTF-8") as _output_file:
        json.dump(data, _output_file)


def upload_file_to_s3(
    local_file,
    s3_path=S3_DATA_FILE,
    s3_bucket_name=S3_BUCKET_NAME,
    s3_profile_name=S3_PROFILE_NAME,
    s3_url=S3_URL,
    public=S3_MAKE_FILE_PUBLIC,
):
    """Upload a local file to S3.

    Parameters
    ----------
    local_file : str
        Path to file to be uploaded.
    s3_path : str
        Full path (within bucket) to file to be written in S3.
    s3_bucket_name : str
        S3 bucket name.
    s3_profile_name : str
        S3 profile name.
    s3_url : str
        S3 URL.
    public : bool
        True to make S3 file publicly readable.

    """
    session = boto3.Session(profile_name=s3_profile_name)
    client = session.client(service_name="s3", endpoint_url=s3_url)
    extra_args = {"ACL": "public-read"} if public else {}
    client.upload_file(
        Filename=local_file, Bucket=s3_bucket_name, Key=s3_path, ExtraArgs=extra_args
    )


def create_new_data_entry_in_walden(
    metadata, output_data_entry_file=OUTPUT_DATA_ENTRY_FILE
):
    """Create a new data entry in Walden index.

    Parameters
    ----------
    metadata : dict
        Metadata for new dataset.
    output_data_entry_file : str
        Path to file in Walden index to be written.

    """
    output_dir = os.path.dirname(output_data_entry_file)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    with open(output_data_entry_file, "w") as _output_file:
        json.dump(metadata, _output_file, **{"indent": 2})
        _output_file.write("\n")


def create_md5_hash_for_file(data_file):
    """Create MD5 hash for file.

    Parameters
    ----------
    data_file : str
        Path to file.

    Returns
    -------
    md5_hash : str
        MD5 hash for file.

    """
    with open(data_file, "rb") as _data_file:
        md5_hash = hashlib.md5(_data_file.read()).hexdigest()

    return md5_hash


def main():
    print("Fetching CAIT data from API.")
    api_data = fetch_all_data_from_api()

    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = os.path.join(temp_dir, f"file.zip")
        print("Saving fetched data as a compressed temporary file.")
        save_compressed_data_to_file(data=api_data, data_file=output_file)
        print("Getting md5 hash for file.")
        METADATA["md5"] = create_md5_hash_for_file(output_file)
        print(f"Uploading file to S3 in: {S3_BUCKET_NAME}/{S3_DATA_FILE}")
        upload_file_to_s3(local_file=output_file)

    print(f"Creating new walden index in: {OUTPUT_DATA_ENTRY_FILE}")
    create_new_data_entry_in_walden(metadata=METADATA)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download greenhouse gas emissions data from CAIT using the Climate Watch Data API, compress and "
        "upload data to S3, and add metadata to Walden index."
    )
    args = parser.parse_args()
    main()

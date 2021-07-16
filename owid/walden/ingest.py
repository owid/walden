"""Tools to inges to Walden and Catalog."""


import os
import logging
import hashlib
import json

import boto3
from botocore.exceptions import ClientError
import click


def ingest_to_walden(local_path: str, walden_path: str, public: bool = False):
    """Upload file to Walden.

    Args:
        local_path (str): Local path to file.
        walden_path (str): Path where to store the file in Walden.
        public (bool): Set to True to expose the file to the public (read only). Defaults to False.
    """
    path = f"http://walden.nyc3.digitaloceanspaces.com/{walden_path}"
    if public:
        extra_args = {"ACL": "public-read"}
    else:
        extra_args = {}
    session = boto3.Session(profile_name="default")
    client = session.client(
        service_name="s3",
        endpoint_url="https://nyc3.digitaloceanspaces.com",
    )
    try:
        _ = client.upload_file(local_path, "walden", walden_path, ExtraArgs=extra_args)
        click.echo(
            click.style("FILE UPLOADED TO WALDEN   ", fg="blue")
            + f"{local_path} -> {path}"
        )
    except ClientError as e:
        logging.error(e)
        return False
    return path


def _add_md5_to_metadata(metadata: dict, local_path: str):
    with open(local_path, "rb") as f:
        md5 = hashlib.md5(f.read()).hexdigest()
    metadata["md5"] = md5
    return metadata


def add_to_catalog(metadata: dict, local_path: str, catalog_path: str):
    """Add metadata to catalog.

    Additionally, it computes the md5 hash of the file, which is added to the metadata file.

    TODO: Add checks of fields.

    Args:
        metadata (dict): Dictionary with metadata.
        local_path (str): Path to local data file. Used to compute the md5 hash.
        catalog_path (str): Path to catalog directory.
    """
    metadata = _add_md5_to_metadata(metadata, local_path)
    os.makedirs(os.path.dirname(catalog_path), exist_ok=True)
    with open(catalog_path, "w") as f:
        json.dump(metadata, f, indent=2)
        f.write("\n")
    click.echo(click.style("METADATA ADDED TO CATALOG   ", fg="blue") + catalog_path)

#
#  owid_cache.py
#
#  Helpers for working with our cache in DigitalOcean Spaces.
#

from os import path
import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from owid.walden.ui import log, bail

SPACES_ENDPOINT = "https://nyc3.digitaloceanspaces.com"
S3_BASE = "s3://walden.nyc3.digitaloceanspaces.com"
HTTPS_BASE = "https://walden.nyc3.digitaloceanspaces.com"


def upload(filename: str, relative_path: str, public: bool = False) -> Optional[str]:
    """
    Upload file to Walden.

    Args:
        local_path (str): Local path to file.
        walden_path (str): Path where to store the file in Walden.
        public (bool): Set to True to expose the file to the public (read only). Defaults to False.
    """
    dest_path = f"{S3_BASE}/{relative_path}"
    extra_args = {"ACL": "public-read"} if public else {}

    client = connect()
    try:
        client.upload_file(filename, "walden", relative_path, ExtraArgs=extra_args)
    except ClientError as e:
        logging.error(e)
        raise UploadError(e)

    log("UPLOADED", f"{filename} -> {dest_path}")

    if public:
        return f"{HTTPS_BASE}/{relative_path}"

    return None


def connect():
    "Return a connection to Walden's DigitalOcean space."
    check_for_default_profile()

    session = boto3.Session(profile_name="default")
    client = session.client(
        service_name="s3",
        endpoint_url=SPACES_ENDPOINT,
    )
    return client


def check_for_default_profile():
    filename = path.expanduser("~/.aws/config")
    if not path.exists(filename) or "[default]" not in open(filename).read():
        bail(
            """you must set up a config file at ~/.aws/config

it should look like:

[default]
aws_access_key_id = ...
aws_secret_access_key = ...
"""
        )


class UploadError(Exception):
    pass

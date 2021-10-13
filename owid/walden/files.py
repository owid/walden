#
#  files.py
#
#  Helpers for downloading and dealing with files.
#

from os import path, walk
import hashlib
from typing import Iterator, Optional
import json
from shutil import move  # noqa; re-exported for convenience

import requests

from .ui import log


def download(
    url: str, filename: str, expected_md5: Optional[str] = None, quiet: bool = False
) -> None:
    "Download the file at the URL to the given local filename."
    tmp_file = f"{filename}.tmp"
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(tmp_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=2 ** 14):  # 16k
                f.write(chunk)

    if expected_md5:
        if checksum(tmp_file) != expected_md5:
            raise ChecksumDoesNotMatch(f"for file downloaded from {url}")

    move(tmp_file, filename)

    if not quiet:
        log("DOWNLOADED", f"{url} -> {filename}")


def checksum(local_path: str):
    md5 = hashlib.md5()
    chunk_size = 2 ** 20  # 1MB
    with open(local_path, "rb") as f:
        chunk = f.read(chunk_size)
        while chunk:
            md5.update(chunk)
            chunk = f.read(chunk_size)

    return md5.hexdigest()


def iter_docs(folder) -> Iterator[dict]:
    "Iterate over the JSON documents in the catalog."
    for filename in sorted(iter_json(folder)):
        try:
            with open(filename) as istream:
                yield json.load(istream)

        except json.decoder.JSONDecodeError:
            raise RecordWithInvalidJSON(filename)


def iter_json(base_dir: str) -> Iterator[str]:
    for dirname, _, filenames in walk(base_dir):
        for filename in filenames:
            if filename.endswith(".json"):
                yield path.join(dirname, filename)


def verify_md5(filename: str, expected_md5: str) -> None:
    "Throw an exception if the filename does not match the checksum."
    md5 = checksum(filename)
    if md5 != expected_md5:
        raise ChecksumDoesNotMatch(filename)


class RecordWithInvalidJSON(Exception):
    pass


class ChecksumDoesNotMatch(Exception):
    pass

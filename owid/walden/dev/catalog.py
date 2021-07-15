# -*- coding: utf-8 -*-
#
#  catalog.py
#  walden
#

from os import path, walk
import json
from typing import Iterator

import sh

from owid.walden.utils import DATA_DIR, SCHEMA_FILE, INDEX_DIR


def load_schema() -> dict:
    with open(SCHEMA_FILE) as istream:
        return json.load(istream)


def iter_docs() -> Iterator[dict]:
    "Iterate over the JSON documents in the catalog."
    for filename in sorted(iter_json(INDEX_DIR)):
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
    md5 = sh.md5("-q", filename).stdout.decode("utf8").strip()
    if md5 != expected_md5:
        raise ChecksumDoesNotMatch(filename)


def get_local_filename(doc: dict) -> str:
    parts = [DATA_DIR]
    if "namespace" in doc:
        parts.append(doc["namespace"])

    if "publication_year" in doc:
        parts.append(str(doc["publication_year"]))

    elif "publication_date" in doc:
        parts.append(doc["publication_date"])

    else:
        raise ValueError("document has no publication date or year")

    basename = doc["owid_data_url"].split("/")[-1]
    parts.append(basename)

    return path.join(*parts)


class RecordWithInvalidJSON(Exception):
    pass


class ChecksumDoesNotMatch(Exception):
    pass

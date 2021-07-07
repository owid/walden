#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  audit.py
#  walden
#

from os import path

import click
import jsonschema

import catalog

BASE_DIR = path.join(path.dirname(__file__), "..")
SCHEMA_FILE = path.join(BASE_DIR, "schema.json")
INDEX_GLOB = path.join(BASE_DIR, "index", "*.json")


@click.command()
def audit() -> None:
    "Audit files in the index against the schema."
    schema = catalog.load_schema()

    i = 0
    for document in catalog.iter_docs():
        jsonschema.validate(document, schema)
        i += 1

    print(f"{i} catalog entries ok")


if __name__ == "__main__":
    audit()

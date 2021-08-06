#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  audit.py
#  walden
#
import click
import jsonschema

from owid.walden import catalog


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

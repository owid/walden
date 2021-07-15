# -*- coding: utf-8 -*-
#
#  test_catalog.py
#  walden
#

from jsonschema import Draft7Validator, validate

from owid.walden import catalog


def test_schema():
    "Make sure the schema itself is valid."
    schema = catalog.load_schema()
    Draft7Validator.check_schema(schema)


def test_catalog_entries():
    "Make sure every catalog entry matches the schema."
    schema = catalog.load_schema()
    for doc in catalog.iter_docs():
        validate(doc, schema)

#
#  test_catalog.py
#
#  Unit tests for basic catalog and dataset functionality.
#

from jsonschema import Draft7Validator, validate

from owid.walden import catalog


def test_catalog_loads():
    c = catalog.get_catalog()
    assert c


def test_schema():
    "Make sure the schema itself is valid."
    schema = catalog.load_schema()
    Draft7Validator.check_schema(schema)


def test_catalog_entries():
    "Make sure every catalog entry matches the schema."
    schema = catalog.load_schema()
    for doc in catalog.iter_docs():
        validate(doc, schema)

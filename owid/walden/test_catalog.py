#
#  test_catalog.py
#
#  Unit tests for basic catalog and dataset functionality.
#

from jsonschema import Draft7Validator, validate

from owid.walden.catalog import Dataset, Catalog, load_schema, iter_docs


def test_schema():
    "Make sure the schema itself is valid."
    schema = load_schema()
    Draft7Validator.check_schema(schema)


def test_catalog_entries():
    "Make sure every catalog entry matches the schema."
    schema = load_schema()
    for doc in iter_docs():
        validate(doc, schema)


def test_catalog_loads():
    catalog = Catalog()

    # the catalog is not empty
    assert len(catalog) > 0

    # everything in it is a dataset
    for dataset in catalog:
        assert isinstance(dataset, Dataset)

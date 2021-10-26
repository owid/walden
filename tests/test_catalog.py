#
#  test_catalog.py
#
#  Unit tests for basic catalog and dataset functionality.
#

from jsonschema import Draft7Validator, validate
import pytest

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


def test_catalog_find():
    catalog = Catalog()
    matches = catalog.find(namespace="faostat")
    assert len(matches) >= 2
    assert all(isinstance(d, Dataset) for d in matches)


def test_catalog_find_one_success():
    catalog = Catalog()
    dataset = catalog.find_one("who", "2021-07-01", "gho")
    assert isinstance(dataset, Dataset)


def test_catalog_find_one_too_many():
    catalog = Catalog()
    with pytest.raises(Exception):
        catalog.find_one()

    with pytest.raises(Exception):
        catalog.find_one("who")


def test_catalog_find_one_too_few():
    catalog = Catalog()
    with pytest.raises(Exception):
        catalog.find_one("highly_unlikely_namespace")


def test_catalog_find_latest():
    catalog = Catalog()
    dataset = catalog.find_latest("who", "gho")
    assert isinstance(dataset, Dataset)

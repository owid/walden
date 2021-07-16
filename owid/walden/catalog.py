"""Prototype."""


import json


class Dataset:

    def __init__(self, metadata: dict) -> None:
        self.metadata = metadata

    @classmethod
    def from_json(cls, path: str):
        with open(path) as f:
            data = json.load(f)
        return cls(data)

    @property
    def owid_data_url(self):
        return self.metadata.get("owid_data_url")

    def download(self):
        raise NotImplementedError()


class Catalog:
    base_url: str = "http://walden.nyc3.digitaloceanspaces.com/"

    def find_dataset(self) -> Dataset:
        raise NotImplementedError()

    def list_datasets(self) -> list:
        raise NotImplementedError()


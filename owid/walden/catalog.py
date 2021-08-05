"""Prototype."""


from os import path, makedirs
from dataclasses import dataclass
import datetime as dt
import hashlib
from typing import Optional
import json
import shutil

from dataclasses_json import dataclass_json
import requests


# our local copy
CACHE_DIR = path.expanduser("~/.owid/walden")

# our folder of JSON documents
INDEX_DIR = path.join(path.dirname(__file__), "..", "..", "index")


@dataclass_json
@dataclass
class Dataset:
    """
    A specific dataset represented by a data file plus metadata.
    If there are multiple versions, this is just one of them.

    Construct it from a dictionary or JSON:

        > Dataset.from_dict({"md5": "2342332", ...})
        > Dataset.from_json('{"md5": "23423432", ...}')

    Then you can fetch the file of the dataset with:

        > filename = Dataset.ensure_downloaded()

    and begin working with that file.
    """

    # how we identify the dataset
    md5: Optional[str]
    namespace: str  # a short source name
    short_name: str  # a slug, ideally unique, camel_case, no spaces

    # fields that are meant to be shown to humans
    name: str
    description: str
    source_name: str
    url: str
    publication_year: Optional[int]
    publication_date: Optional[dt.date]

    # how to get the data file
    source_data_url: str
    owid_data_url: Optional[str]
    file_extension: str

    @classmethod
    def download_and_create(cls, metadata: dict) -> "Dataset":
        dataset = Dataset.from_dict(metadata)  # type: ignore

        # make sure we have a local copy
        filename = dataset.ensure_downloaded()

        # set the md5
        dataset["md5"] = checksum(filename)

        return dataset

    @classmethod
    def copy_and_create(cls, filename: str, metadata: dict) -> "Dataset":
        """
        Create a new dataset if you already have the file locally.
        """
        dataset = Dataset.from_dict(metadata)  # type: ignore

        # set the md5
        dataset["md5"] = checksum(filename)

        # copy the file into the cache
        dataset.add_to_cache(filename)

        return dataset

    def add_to_cache(self, filename: str) -> None:
        """
        Copy the pre-downloaded file into the cache. This avoids having to
        redownload it if you already have a copy.
        """
        cache_file = self.local_path

        # make the parent folder
        parent_dir = path.dirname(cache_file)
        makedirs(parent_dir)

        shutil.copy(filename, cache_file)

    def save(self) -> None:
        "Save any changes as JSON to the catalog."
        with open(self.index_path, "w") as ostream:
            print(json.dumps(self.to_dict(), indent=2), file=ostream)  # type: ignore

    @property
    def index_path(self) -> str:
        return path.join(
            INDEX_DIR,
            self.namespace,
            self.version,
            f"{self.short_name}.json",
        )

    # if we always want to download to a local directory
    def ensure_downloaded(self) -> str:
        "Download it if it hasn't already been downloaded. Return the local file path."
        filename = self.local_path
        if not path.exists(filename):
            # make the parent folder
            parent_dir = path.dirname(filename)
            makedirs(parent_dir)

            # actually get it
            url = self.owid_data_url or self.source_data_url
            download(url, filename)

        return filename

    def upload(self) -> None:
        "Copy the local file to our cache."
        pass

    @property
    def local_path(self) -> str:
        return path.join(
            CACHE_DIR,
            self.namespace,
            self.version,
            f"{self.short_name}.{self.file_extension}",
        )

    @property
    def version(self) -> str:
        if self.publication_year:
            return str(self.publication_year)

        elif self.publication_date:
            return str(self.publication_date)

        raise ValueError("no versioning field found")


class Catalog:
    base_url: str = "http://walden.nyc3.digitaloceanspaces.com/"

    def find_dataset(self) -> Dataset:
        raise NotImplementedError()

    def list_datasets(self) -> list:
        raise NotImplementedError()


def download(url: str, filename: str) -> None:
    "Download the file at the URL to the given local filename."
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=2 ** 14):  # 16k
                f.write(chunk)


def checksum(local_path: str):
    with open(local_path, "rb") as f:
        md5 = hashlib.md5(f.read()).hexdigest()
    return md5

"""Prototype."""


from os import path, makedirs, unlink as delete
from dataclasses import dataclass
import datetime as dt
from typing import Optional, Iterator, List
import json
import shutil

from dataclasses_json import dataclass_json

from . import owid_cache, files

# our local copy
CACHE_DIR = path.expanduser("~/.owid/walden")

# this repository
BASE_DIR = path.join(path.dirname(__file__), "..", "..")

# our folder of JSON documents
INDEX_DIR = path.join(BASE_DIR, "index")

# the JSONschema that they must match
SCHEMA_FILE = path.join(BASE_DIR, "schema.json")


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
    namespace: str  # a short source name
    short_name: str  # a slug, ideally unique, camel_case, no spaces

    # fields that are meant to be shown to humans
    name: str
    description: str
    source_name: str
    url: str
    date_accessed: str

    # how to get the data file
    file_extension: str

    # optional fields
    source_data_url: Optional[str] = None
    md5: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[dt.date] = None
    owid_data_url: Optional[str] = None

    @classmethod
    def download_and_create(cls, metadata: dict) -> "Dataset":
        dataset = Dataset.from_dict(metadata)  # type: ignore

        # make sure we have a local copy
        filename = dataset.ensure_downloaded()

        # set the md5
        dataset.md5 = files.checksum(filename)

        return dataset

    @classmethod
    def copy_and_create(cls, filename: str, metadata: dict) -> "Dataset":
        """
        Create a new dataset if you already have the file locally.
        """
        dataset = Dataset.from_dict(metadata)  # type: ignore

        # set the md5
        dataset.md5 = files.checksum(filename)

        # copy the file into the cache
        dataset.add_to_cache(filename)

        return dataset

    @classmethod
    def from_file(cls, filename: str) -> "Dataset":
        with open(filename) as istream:
            return cls.from_json(istream.read())  # type: ignore

    def add_to_cache(self, filename: str) -> None:
        """
        Copy the pre-downloaded file into the cache. This avoids having to
        redownload it if you already have a copy.
        """
        cache_file = self.local_path

        # make the parent folder
        parent_dir = path.dirname(cache_file)
        if not path.isdir(parent_dir):
            makedirs(parent_dir)

        shutil.copy(filename, cache_file)

    def save(self) -> None:
        "Save any changes as JSON to the catalog."
        with open(self.index_path, "w") as ostream:
            print(json.dumps(self.to_dict(), indent=2), file=ostream)  # type: ignore

    def delete(self) -> None:
        """
        Remove this dataset record from the local catalog. It will still remain on Github
        unless this change is committed and pushed there. Mostly useful for testing.
        """
        if path.exists(self.index_path):
            delete(self.index_path)

    @property
    def index_path(self) -> str:
        return path.join(INDEX_DIR, f"{self.relative_base}.json")

    @property
    def relative_base(self):
        return path.join(self.namespace, self.version, f"{self.short_name}")

    def ensure_downloaded(self) -> str:
        "Download it if it hasn't already been downloaded. Return the local file path."
        filename = self.local_path
        if not path.exists(filename):
            # make the parent folder
            parent_dir = path.dirname(filename)
            if not path.isdir(parent_dir):
                makedirs(parent_dir)

            # actually get it
            url = self.owid_data_url or self.source_data_url
            files.download(url, filename, expected_md5=self.md5)

        return filename

    def upload(self, public: bool = False) -> None:
        """
        Copy the local file to our cache. If the file is public, it updates the
        `owid_data_url` field.
        """
        # download the file to the local cache if we don't have it already
        self.ensure_downloaded()

        # add it to our remote cache of data files
        dest_path = f"{self.relative_base}.{self.file_extension}"
        cache_url = owid_cache.upload(self.local_path, dest_path, public=public)

        # remember how to access it
        self.owid_data_url = cache_url

    @property
    def local_path(self) -> str:
        return path.join(CACHE_DIR, f"{self.relative_base}.{self.file_extension}")

    @property
    def version(self) -> str:
        if self.publication_date:
            return str(self.publication_date)

        elif self.publication_year:
            return str(self.publication_year)

        raise ValueError("no versioning field found")


class Catalog:
    def __init__(self):
        self.datasets: List[Dataset] = []
        self.refresh()

    def refresh(self):
        self.datasets = [Dataset.from_dict(d) for d in iter_docs()]  # type: ignore

    def __iter__(self):
        yield from iter(self.datasets)

    def __len__(self):
        return len(self.datasets)


def load_schema() -> dict:
    with open(SCHEMA_FILE) as istream:
        return json.load(istream)


def iter_docs() -> Iterator[dict]:
    return files.iter_docs(INDEX_DIR)

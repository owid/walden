#!/usr/bin/env
# -*- coding: utf-8 -*-
#
#  format_json.py
#  walden
#

from os.path import basename
import io
import json
from typing import Iterator

import click

from owid.walden import catalog
from owid.walden.utils import bail, SCHEMA_FILE, INDEX_DIR


@click.command()
@click.option("--check", is_flag=True, help="Only check, do not reformat")
def format_json(check: bool = False) -> None:
    """
    Reformat JSON files to a consistent readable standard.
    """
    for filename in iter_json():
        contents = read(filename)
        try:
            expected_contents = reformat(contents)
        except json.JSONDecodeError:
            bail(f"{basename(filename)} is not a valid JSON file")

        if contents != expected_contents:
            if check:
                bail(
                    f"{basename(filename)} is not well formatted, please run "
                    '"make format"',
                )

            print(f"Reformatting {basename(filename)}")
            write(expected_contents, filename)


def iter_json() -> Iterator[str]:
    yield SCHEMA_FILE
    yield from catalog.iter_json(INDEX_DIR)


def read(filename: str) -> str:
    with open(filename) as istream:
        return istream.read()


def reformat(contents: str) -> str:
    doc = json.loads(contents)
    fake_file = io.StringIO()
    json.dump(doc, fake_file, indent=2)

    # editors leave a newline at the end, we'll do the same
    print(file=fake_file)

    return fake_file.getvalue()


def write(expected_contents: str, filename: str) -> None:
    with open(filename, "w") as ostream:
        ostream.write(expected_contents)


if __name__ == "__main__":
    format_json()

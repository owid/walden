#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  fetch.py
#  walden
#

from os import path

import click
import sh

import catalog


@click.command()
@click.option(
    "--checksum/--no-checksum",
    is_flag=True,
    default=True,
    help="Verify each file against its checksum",
)
def fetch(checksum: bool = True):
    """
    Fetch the full dataset file by file. Previously downloaded files are considered
    cached and are not re-downloaded.
    """
    for document in catalog.iter_docs():
        dest_filename = catalog.get_local_filename(document)
        base_filename = dest_filename[len(catalog.BASE_DIR) + 1 :]
        if not path.exists(dest_filename):
            click.echo(click.style("FETCH   ", fg="blue") + base_filename)
            download_file(document, dest_filename)
        else:
            click.echo(click.style("CACHED  ", fg="green") + base_filename)

        if checksum:
            catalog.verify_md5(dest_filename, document["md5"])


def download_file(doc: dict, dest_filename: str):
    # prefer our cached copy, but fall back to downloading directly
    url = doc.get("owid_data_url") or doc["source_data_url"]

    sh.mkdir("-p", path.dirname(dest_filename))
    sh.wget("-O", dest_filename, url)


if __name__ == "__main__":
    fetch()

# -*- coding: utf-8 -*-
#
#  utils.py
#  walden
#

from os import path
import sys
from typing import NoReturn


# General paths
BASE_DIR = path.join(path.dirname(__file__), "..", "..")
DATA_DIR = path.join(BASE_DIR, "data")
SCHEMA_FILE = path.join(BASE_DIR, "schema.json")
INDEX_DIR = path.join(BASE_DIR, "index")
INGESTS_DIR = path.join(BASE_DIR, "ingests")


def bail(message: str) -> NoReturn:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)

# -*- coding: utf-8 -*-
#
#  utils.py
#  walden
#

import sys
from typing import NoReturn


def bail(message: str) -> NoReturn:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)

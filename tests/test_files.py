#
#  test_files.py
#  walden
#

import tempfile
import hashlib

from owid.walden import files


def test_empty_checksum():
    with tempfile.NamedTemporaryFile() as tmp:
        md5 = files.checksum(tmp.name)

    assert md5 == hashlib.md5().hexdigest()


def test_known_checksum():
    s = "Hello world o/\n"
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write("Hello world o/\n".encode("utf8"))
        tmp.flush()
        md5 = files.checksum(tmp.name)

    assert md5 == hashlib.md5(s.encode("utf8")).hexdigest()

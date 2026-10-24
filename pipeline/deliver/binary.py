"""The one unsigned LEB128 varint every shipped binary file is written in (audit F21, plan 2.9).

The status index (`status_index.py`), paint (`release/paint.py`) and the lake facts (`lake_facts.py`) each carried
their own copy of the writer and the reader; three copies of a byte format are three chances to disagree with the
app's one decoder. These are the only ones. `test_binary.py` round-trips them.

    varint(n)              the bytes of n (7 bits a byte, low first, the high bit set on every byte but the last)
    put_varint(out, n)     the same, appended to a bytearray
    read_varint(b, i)      (n, the index after it)

A negative number is refused (ValueError naming `what`): an unsigned format has no way to say it, and a silent
two's-complement would decode as a huge count.
"""

from __future__ import annotations

from typing import Tuple


def put_varint(out: bytearray, n: int, what: str = "varint") -> None:
    """Append `n` to `out` as an unsigned LEB128 varint."""
    if n < 0:
        raise ValueError(f"{what}: a negative varint {n}")
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)


def varint(n: int, what: str = "varint") -> bytes:
    """`n` as an unsigned LEB128 varint."""
    out = bytearray()
    put_varint(out, n, what)
    return bytes(out)


def read_varint(b: bytes, i: int) -> Tuple[int, int]:
    """The varint starting at `b[i]`, and the index just past it."""
    n = shift = 0
    while True:
        x = b[i]
        i += 1
        n |= (x & 0x7F) << shift
        shift += 7
        if not x & 0x80:
            return n, i

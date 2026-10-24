"""The one varint (plan 2.9): it round-trips, it is the bytes LEB128 says, and every shipped file uses it.

No data.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from pipeline.deliver.binary import put_varint, read_varint, varint

ROOT = Path(__file__).resolve().parents[2]

EDGES = [0, 1, 0x7F, 0x80, 0xFF, 300, 0x3FFF, 0x4000, 2**21 - 1, 2**21, 2**32, 2**63 + 5]


@pytest.mark.parametrize("n", EDGES)
def test_a_varint_round_trips(n):
    b = varint(n)
    assert read_varint(b, 0) == (n, len(b))
    out = bytearray(b"x")
    put_varint(out, n)
    assert bytes(out) == b"x" + b


def test_the_bytes_are_leb128():
    assert varint(0) == b"\x00"
    assert varint(0x7F) == b"\x7f"
    assert varint(0x80) == b"\x80\x01"
    assert varint(300) == b"\xac\x02"
    assert len(varint(2**21 - 1)) == 3 and len(varint(2**21)) == 4


def test_a_run_of_varints_reads_back_in_order():
    ns = list(range(0, 70000, 997)) + EDGES
    buf = b"".join(varint(n) for n in ns)
    i, got = 0, []
    while i < len(buf):
        n, i = read_varint(buf, i)
        got.append(n)
    assert got == ns


def test_a_negative_number_is_refused_naming_the_file():
    with pytest.raises(ValueError, match="paint: a negative varint -1"):
        put_varint(bytearray(), -1, "paint")
    with pytest.raises(ValueError, match="negative"):
        varint(-5)


def test_no_shipped_writer_keeps_its_own_copy():
    """The three former copies import the one module and define no LEB128 loop of their own."""
    for rel in ("pipeline/deliver/status_index.py", "pipeline/deliver/lake_facts.py",
                "pipeline/deliver/release/paint.py"):
        tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
        assert any(isinstance(n, ast.ImportFrom) and n.module == "pipeline.deliver.binary"
                   for n in ast.walk(tree)), rel
        loops = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.BinOp)
                 and isinstance(n.op, ast.BitAnd) and isinstance(n.right, ast.Constant) and n.right.value == 0x7F]
        assert loops == [], f"{rel}:{loops} masks 0x7F — a varint loop of its own"

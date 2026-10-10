"""The envelope's byte layout, pinned on BOTH sides of the language boundary.

`gauge_clim.bands` is the one place in the bundle where a table stores a hand-packed binary
record. Python writes it in `deliver/bundle/build.py`; TypeScript reads it in
`app/packages/data/src/bundle/source.ts`. Nothing in either language checks the other, and a
disagreement about the stride or an offset does not raise — it silently yields a seasonal
band for the wrong time of year, or flow figures off by orders of magnitude, on a screen
whose whole job is to say whether a river is unusually low.

That is the same failure the trust bands had (see test_shared_logic), and it is pinned the
same way: the layout is asserted here as bytes, and the reader is asserted to use those
exact offsets rather than its own.
"""

from __future__ import annotations

import re
import struct
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

#: uint8 pentad, then float32 p10, p25, p50, p75, p90 — little-endian.
STRIDE = 21


def pack(pentad: int, bands: tuple[float, float, float, float, float]) -> bytes:
    """Exactly what build.py writes, kept here so the assertions below are about bytes."""
    return bytes([pentad]) + struct.pack("<5f", *bands)


def test_one_pentad_is_twenty_one_bytes_laid_out_as_declared():
    rec = pack(7, (1.5, 2.5, 3.5, 4.5, 5.5))
    assert len(rec) == STRIDE
    assert rec[0] == 7, "the pentad is the first byte, unsigned"
    # Little-endian float32, five of them, starting at offset 1.
    assert struct.unpack_from("<5f", rec, 1) == (1.5, 2.5, 3.5, 4.5, 5.5)
    # Big-endian would decode to garbage rather than raising, which is the point.
    assert struct.unpack_from(">5f", rec, 1) != (1.5, 2.5, 3.5, 4.5, 5.5)


def test_the_writer_still_writes_this_layout():
    """If build.py's pack string changes, this test is the thing that notices."""
    src = (ROOT / "pipeline/deliver/bundle/build.py").read_text(encoding="utf-8")
    assert 'struct.pack("<5f"' in src, (
        "the bundler no longer packs five little-endian float32 per pentad")
    assert "buf.append(pe)" in src, "the pentad byte is no longer written first"


def test_the_app_reads_the_same_offsets():
    """The reader must use THIS stride and THESE offsets, not its own arithmetic."""
    ts = ROOT / "app/packages/data/src/bundle/source.ts"   # tracked: app/ is in git
    src = ts.read_text(encoding="utf-8")

    assert f"o += {STRIDE}" in src and f"o + {STRIDE} <= bytes.byteLength" in src, (
        f"source.ts no longer walks the envelope in {STRIDE}-byte records")
    assert "dv.getUint8(o)" in src, "the pentad is no longer read from offset 0"
    # The five float32 offsets, and `true` for little-endian on every one of them. A missing
    # `true` defaults to BIG-endian in the DataView API — silently, and wrongly.
    for off in (1, 5, 9, 13, 17):
        assert f"dv.getFloat32(o + {off}, true)" in src, (
            f"source.ts does not read a little-endian float32 at offset {off}")


def test_float32_is_below_the_precision_that_matters():
    """The claim in schema.sql, checked rather than asserted in prose.

    A percentile transferred from a donor gauge is good to about +/-11.7 PERCENTILE POINTS.
    Storing the band to seven significant figures instead of sixteen cannot be what makes it
    wrong, and this pins the margin at roughly eight orders of magnitude.
    """
    worst = 0.0
    for v in (0.0001, 0.8081, 15.80, 742.0, 7179.0, 11400.0):
        back = struct.unpack("<f", struct.pack("<f", v))[0]
        worst = max(worst, abs(back - v) / v)
    assert worst < 1e-6, f"float32 relative error {worst:.2e} is larger than expected"


def test_a_truncated_envelope_is_not_read_as_a_short_one():
    """A blob that is not a whole number of records must lose the TAIL, never shift.

    The reader's loop condition is `o + STRIDE <= length`, so a trailing partial record is
    skipped. Were it `o < length`, a truncated read would decode past the end and produce a
    band from whatever followed.
    """
    good = pack(0, (1, 2, 3, 4, 5)) + pack(1, (6, 7, 8, 9, 10))
    truncated = good[:-4]
    whole = [truncated[o:o + STRIDE] for o in range(0, len(truncated) - STRIDE + 1, STRIDE)]
    assert len(whole) == 1, "the partial second record must be dropped, not decoded"
    assert whole[0][0] == 0

"""PAINT (P08, MINIMAL-PACKAGING §2): the one file a web reader needs before the first coloured map. Format `paint/2`:

    "BCPT"  u8 version (2)
    u32le   n, then the status index (BCSI v2, `pipeline.deliver.status_index`), exactly the bytes of status_index.bin
    the FLOW half, every column a run of LEB128 varints unless said otherwise:
      stations   u32le byte length, then the station ids sorted, UTF-8, joined by "\\n" (the index every column uses)
      runs       count; then per run: gap (sid - previous run's end), count, panel_id
                 (section_panel's sid → panel_id, cut into runs of consecutive sids with one panel)
      members    count; then per row in (panel_id, ord) order: panel_id delta, ord, station index, role (0 up / 1 down),
                 round(area_km2 × 1000), years, regulated (0/1), same_river (0/1), lead (0 none, else its rank: the
                 section's own gauge, which leads the answer — owner 2026-10-22; panel.py "a water's own gauge")
      basins     count; then per row in (basin_id, station) order: basin_id (u16 length + UTF-8), station index,
                 area_km2 present (0/1) + its float64 (8 bytes: the column holds float32 values rounded to 16 digits, which only a
                 double reproduces), years

THE TARGET AREAS (`section_panel.area_km2`, one per section) are NOT here: only a Conditions view reads them. They are
the file `flow-area` (`flow-area/1`: "BCFA", u8 version 1, then one varint per section_panel row in paint's sid order,
round(area_km2 × 1000), exact), fetched with `conditions`. As a SQLite table keyed by sid they cost 0.9 MB gzip; as a
column beside paint's runs, 0.19 MB.

`decode` is the reference decoder, and `test_release.py` proves it reproduces `section_panel`, `panel_member` and
`basin_member` row for row, and the status half byte for byte.

paint/1 had no `lead`; a paint/1 reader refuses a paint/2 file by its version byte, and the other way round.
"""
from __future__ import annotations

import sqlite3
import struct
from typing import Dict, List, Tuple

MAGIC = b"BCPT"
VERSION = 2
FORMAT = "paint/2"


def _uv(out: bytearray, v: int) -> None:
    if v < 0:
        raise ValueError(f"paint: varint {v} is negative")
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)


class _Reader:
    def __init__(self, b: bytes, at: int = 0):
        self.b, self.i = b, at

    def uv(self) -> int:
        shift = v = 0
        while True:
            c = self.b[self.i]
            self.i += 1
            v |= (c & 0x7F) << shift
            if c < 0x80:
                return v
            shift += 7

    def raw(self, n: int) -> bytes:
        out = self.b[self.i:self.i + n]
        self.i += n
        return out


def _milli(x) -> int:
    return int(round(float(x) * 1000))


def encode(status_index: bytes, bundle: sqlite3.Connection) -> bytes:
    stations = [s for (s,) in bundle.execute("SELECT station FROM gauge ORDER BY station")]
    six = {s: i for i, s in enumerate(stations)}
    out = bytearray(MAGIC)
    out.append(VERSION)
    out += struct.pack("<I", len(status_index)) + status_index
    names = "\n".join(stations).encode("utf-8")
    out += struct.pack("<I", len(names)) + names
    sp = bundle.execute("SELECT sid, panel_id FROM section_panel ORDER BY sid").fetchall()
    runs: List[Tuple[int, int, int]] = []
    for sid, pid in sp:
        if runs and runs[-1][0] + runs[-1][1] == sid and runs[-1][2] == pid:
            runs[-1] = (runs[-1][0], runs[-1][1] + 1, pid)
        else:
            runs.append((sid, 1, pid))
    _uv(out, len(runs))
    end = 0
    for start, n, pid in runs:
        _uv(out, start - end)
        _uv(out, n)
        _uv(out, pid)
        end = start + n
    pm = bundle.execute("SELECT panel_id, ord, station, role, area_km2, years, regulated, same_river, lead "
                        "FROM panel_member ORDER BY panel_id, ord").fetchall()
    _uv(out, len(pm))
    prev = 0
    for pid, o, st, role, area, years, reg, same, lead in pm:
        _uv(out, pid - prev)
        prev = pid
        for v in (o, six[st], {"up": 0, "down": 1}[role], _milli(area), years, int(reg), int(same or 0),
                  int(lead or 0)):
            _uv(out, v)
    bm = bundle.execute("SELECT basin_id, station, area_km2, years FROM basin_member "
                        "ORDER BY basin_id, station").fetchall()
    _uv(out, len(bm))
    for bid, st, area, years in bm:
        b = bid.encode("utf-8")
        out += struct.pack("<H", len(b)) + b
        _uv(out, six[st])
        _uv(out, 0 if area is None else 1)
        if area is not None:
            out += struct.pack("<d", area)
        _uv(out, years)
    return bytes(out)


AREA_MAGIC, AREA_FORMAT = b"BCFA", "flow-area/1"
#: flow-area keeps its own version: paint/2 changed the members, not the areas.
AREA_VERSION = 1


def encode_area(bundle: sqlite3.Connection) -> bytes:
    out = bytearray(AREA_MAGIC)
    out.append(AREA_VERSION)
    for (a,) in bundle.execute("SELECT area_km2 FROM section_panel ORDER BY sid"):
        _uv(out, _milli(a))
    return bytes(out)


def decode_area(b: bytes, n: int) -> List[float]:
    if b[:4] != AREA_MAGIC or b[4] != AREA_VERSION:
        raise ValueError("flow-area: not a flow-area/1 file")
    r = _Reader(b, 5)
    out = [r.uv() / 1000 for _ in range(n)]
    if r.i != len(b):
        raise ValueError(f"flow-area: {len(b) - r.i} trailing bytes (or the paint it pairs with has other sections)")
    return out


def decode(b: bytes) -> Dict[str, object]:
    """{status: bytes, stations, section_panel: [(sid, panel_id)], panel_member: [...], basin_member: [...]}"""
    if b[:4] != MAGIC or b[4] != VERSION:
        raise ValueError(f"paint: not a {FORMAT} file")
    (n,) = struct.unpack_from("<I", b, 5)
    status = b[9:9 + n]
    r = _Reader(b, 9 + n)
    (ln,) = struct.unpack("<I", r.raw(4))
    stations = r.raw(ln).decode("utf-8").split("\n") if ln else []
    sids: List[Tuple[int, int]] = []
    end = 0
    for _ in range(r.uv()):
        start = end + r.uv()
        cnt, pid = r.uv(), r.uv()
        sids += [(start + k, pid) for k in range(cnt)]
        end = start + cnt
    section_panel = sids                                    # (sid, panel_id); the areas are in `conditions`
    panel_member, pid = [], 0
    for _ in range(r.uv()):
        pid += r.uv()
        o, si, role, area, years, reg, same, lead = (r.uv() for _ in range(8))
        panel_member.append((pid, o, stations[si], ("up", "down")[role], area / 1000, years, reg, same, lead))
    basin_member = []
    for _ in range(r.uv()):
        (lb,) = struct.unpack("<H", r.raw(2))
        bid = r.raw(lb).decode("utf-8")
        si, has = r.uv(), r.uv()
        area = struct.unpack("<d", r.raw(8))[0] if has else None
        basin_member.append((bid, stations[si], area, r.uv()))
    if r.i != len(b):
        raise ValueError(f"paint: {len(b) - r.i} trailing bytes")
    return {"status": status, "stations": stations, "section_panel": section_panel,
            "panel_member": panel_member, "basin_member": basin_member}

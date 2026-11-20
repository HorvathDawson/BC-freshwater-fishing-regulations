"""(T686, owner 2026-11-16: "(a) Three lossless changes") THREE RELEASE TABLES PACKED, NOTHING DROPPED.

The bundle keeps its tables as they are (the pipeline reads them by row); the release packs three of them, and the app
decodes them (`app/packages/data/src/release/packed.ts`, held to these bytes by one fixture both sides read:
app/packages/data/dev/packed-parity.json, `pipeline/tests/test_release_packed.py` and `app/tools/packed-parity.test.ts`).
Every value decodes to exactly what the old form shipped (the side deliver's proof, T686).

All varints are unsigned LEB128; `zz` is zigzag for a signed value. Nothing is deflated (the file ships gzipped).

near/2 — `place_water (place_id INTEGER PRIMARY KEY, b BLOB NOT NULL)`: ONE ROW PER TOWN (was one per town and water).
    `b` = N, then N × (ord − the previous ord − 1) (ords ascending, the first from −1; a town names a water once), then
    the N `ckm` low bytes, then the N `ckm` high bytes (ckm ≤ 2500: hundredths of a km, as near/1). The reader sorts by
    (ckm, ord) itself: near/1's primary-key order.
down/2 — `section_down (block INTEGER PRIMARY KEY, b BLOB NOT NULL)`: one row per block of 4096 sids (`sid >> 12`).
    `b` = N, then N × (sid − the previous sid − 1) (the first from `block << 12` − 1), then N × zz(down_sid − the
    previous down_sid) (the first from `block << 12`), then N × len′ (len + 1; 0 = NULL), then L, then L × (slot − the
    previous lake slot − 1) (the first from −1): the slots whose `lake` is 1 (every other is NULL, as down/1).
clim/3 (the climate file's `gauge_clim.bands`, file format climate/2) — clim/2's float16 values, transposed: u8 N, then
    the N pentad bytes, then for p10, p25, p50, p75, p90 in turn the N deltas (float16 bits − the previous pentad's, mod
    2^16, the first from 0), written as all 5N low bytes then all 5N high bytes.
"""
from __future__ import annotations

import sqlite3
import struct
from typing import Iterable, List, Optional, Sequence, Tuple

#: THE SWITCH (owner 2026-11-20 09:59, "Ship readers now, formats later (Recommended)"). OFF: a release writes near/1,
#: down/1 and climate/1 exactly as before T686 (byte for byte; every app reads them, so no minAppVersion is derived from
#: them) and keeps their old gates. ON: near/2, down/2, climate/2 and their lowered gates; the release then needs app
#: 0.8.4 (`app_gate`). Flip it to True in the FIRST data release cut after app 0.8.4 is live in BOTH production stores
#: (Google Play production and the App Store) — never before: an older app keeps its data and is asked to update.
#: App 0.8.4 reads both forms, whichever this says.
SHIP_PACKED_FORMATS = False

#: (T679 #8, owner 2026-11-20 09:59 "Ranking's rule, list first") THE OWN-GAUGE FLAG (`flow-own`): OFF until the owner
#: has seen the list of gauges that move between "ON THIS WATER" and "ESTIMATING FROM OTHER WATERS"
#: (handoff/PLAN/app-integration/T679-shots/own-gauge-moves.csv) and said ship it. An optional file: an app before 0.8.4
#: does not know it and keeps its own rule; 0.8.4 reads the flag where the release carries it.
SHIP_OWN_GAUGE_FLAG = False

NEAR_FORMAT = "near/2"
DOWN_FORMAT = "down/2"
CLIMATE_FORMAT = "climate/2"
CLIM_CODEC = "clim/3"

BLOCK_BITS = 12
CKM_MAX = 2500

NEAR_DDL = "CREATE TABLE place_water (place_id INTEGER PRIMARY KEY, b BLOB NOT NULL)"
DOWN_DDL = "CREATE TABLE section_down (block INTEGER PRIMARY KEY, b BLOB NOT NULL)"
NEAR_COLUMNS = ["place_id", "b"]
DOWN_COLUMNS = ["block", "b"]


class PackedError(ValueError):
    pass


def _uv(out: bytearray, v: int, what: str) -> None:
    if v < 0:
        raise PackedError(f"{what}: {v} is not an unsigned varint")
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)


def _zz(v: int) -> int:
    return 2 * v if v >= 0 else -2 * v - 1


def _unzz(u: int) -> int:
    return u // 2 if u % 2 == 0 else -(u + 1) // 2


class _Reader:
    def __init__(self, b: bytes, what: str):
        self.b, self.pos, self.what = b, 0, what

    def uv(self) -> int:
        v, shift = 0, 0
        while True:
            if self.pos >= len(self.b):
                raise PackedError(f"{self.what}: the blob ends inside a varint")
            c = self.b[self.pos]
            self.pos += 1
            v |= (c & 0x7F) << shift
            if c < 0x80:
                return v
            shift += 7

    def take(self, n: int) -> bytes:
        if self.pos + n > len(self.b):
            raise PackedError(f"{self.what}: the blob ends {self.pos + n - len(self.b)} bytes early")
        out = self.b[self.pos:self.pos + n]
        self.pos += n
        return out

    def end(self) -> None:
        if self.pos != len(self.b):
            raise PackedError(f"{self.what}: {len(self.b) - self.pos} bytes after the last column")


# --- near/2 -------------------------------------------------------------------------------------------------------

def encode_near(rows: Iterable[Tuple[int, int]]) -> bytes:
    """One town's `(ckm, ord)` rows → its near/2 blob. Refused: an ord twice, a ckm outside 0..2500."""
    rs = sorted(rows, key=lambda r: r[1])
    out = bytearray()
    _uv(out, len(rs), "near/2")
    prev = -1
    for ckm, o in rs:
        if not 0 <= ckm <= CKM_MAX:
            raise PackedError(f"near/2: ckm {ckm} is outside 0..{CKM_MAX}")
        if o <= prev:
            raise PackedError(f"near/2: water ord {o} twice for one town")
        _uv(out, o - prev - 1, "near/2")
        prev = o
    out += bytes(c & 0xFF for c, _ in rs) + bytes(c >> 8 for c, _ in rs)
    return bytes(out)


def decode_near(b: bytes) -> List[Tuple[int, int]]:
    """A near/2 blob → `[(ckm, ord)]` in near/1's order (ckm, then ord)."""
    r = _Reader(b, "near/2")
    n = r.uv()
    ords, prev = [], -1
    for _ in range(n):
        prev = prev + 1 + r.uv()
        ords.append(prev)
    lo, hi = r.take(n), r.take(n)
    r.end()
    out = [(lo[i] | hi[i] << 8, ords[i]) for i in range(n)]
    if any(c > CKM_MAX for c, _ in out):
        raise PackedError(f"near/2: a ckm past {CKM_MAX}")
    return sorted(out)


def near_blobs(rows: Iterable[Tuple[int, int, int]]) -> List[Tuple[int, bytes]]:
    """`[(place_id, b)]` from near/1 rows `(place_id, ckm, ord)`."""
    by: dict = {}
    for p, c, o in rows:
        by.setdefault(p, []).append((c, o))
    return [(p, encode_near(by[p])) for p in sorted(by)]


def pack_near(con: sqlite3.Connection) -> None:
    """The release's `extra` for near: `place_water` (near/1 rows, copied from the bundle) → near/2."""
    rows = con.execute("SELECT place_id, ckm, ord FROM place_water").fetchall()
    con.execute("DROP TABLE place_water")
    con.execute(NEAR_DDL)
    con.executemany("INSERT INTO place_water (place_id, b) VALUES (?, ?)", near_blobs(rows))


# --- down/2 -------------------------------------------------------------------------------------------------------

DownRow = Tuple[int, int, Optional[int], Optional[int]]          # (sid, down_sid, len, lake)


def encode_down_block(block: int, rows: Sequence[DownRow]) -> bytes:
    """One block's rows (sid ascending, every sid `>> 12 == block`) → its down/2 blob."""
    base = block << BLOCK_BITS
    out = bytearray()
    _uv(out, len(rows), "down/2")
    prev = base - 1
    for sid, _, _, _ in rows:
        if sid >> BLOCK_BITS != block or sid <= prev:
            raise PackedError(f"down/2: sid {sid} is not after {prev} in block {block}")
        _uv(out, sid - prev - 1, "down/2")
        prev = sid
    prev = base
    for _, d, _, _ in rows:
        if d is None:
            raise PackedError("down/2: a row with no down_sid")
        _uv(out, _zz(d - prev), "down/2")
        prev = d
    for _, _, ln, _ in rows:
        _uv(out, 0 if ln is None else ln + 1, "down/2 len")
    lakes = []
    for i, (sid, _, _, lake) in enumerate(rows):
        if lake not in (None, 1):
            raise PackedError(f"down/2: sid {sid} has lake {lake!r} (1 or NULL only)")
        if lake == 1:
            lakes.append(i)
    _uv(out, len(lakes), "down/2")
    prev = -1
    for i in lakes:
        _uv(out, i - prev - 1, "down/2")
        prev = i
    return bytes(out)


def decode_down_block(block: int, b: bytes) -> List[DownRow]:
    r = _Reader(b, f"down/2 block {block}")
    n = r.uv()
    sids, prev = [], (block << BLOCK_BITS) - 1
    for _ in range(n):
        prev = prev + 1 + r.uv()
        sids.append(prev)
    if sids and sids[-1] >> BLOCK_BITS != block:
        raise PackedError(f"down/2 block {block}: sid {sids[-1]} is past the block")
    downs, prev = [], block << BLOCK_BITS
    for _ in range(n):
        prev += _unzz(r.uv())
        downs.append(prev)
    lens = [r.uv() for _ in range(n)]
    lake: List[Optional[int]] = [None] * n
    prev = -1
    for _ in range(r.uv()):
        prev = prev + 1 + r.uv()
        if prev >= n:
            raise PackedError(f"down/2 block {block}: lake slot {prev} past {n} rows")
        lake[prev] = 1
    r.end()
    return [(sids[i], downs[i], None if lens[i] == 0 else lens[i] - 1, lake[i]) for i in range(n)]


def down_blocks(rows: Iterable[DownRow]) -> List[Tuple[int, bytes]]:
    by: dict = {}
    for row in sorted(rows):
        by.setdefault(row[0] >> BLOCK_BITS, []).append(row)
    return [(k, encode_down_block(k, by[k])) for k in sorted(by)]


def pack_down(con: sqlite3.Connection) -> None:
    """The release's `extra` for down: `section_down` (down/1 rows) → down/2."""
    rows = con.execute("SELECT sid, down_sid, len, lake FROM section_down").fetchall()
    con.execute("DROP TABLE section_down")
    con.execute(DOWN_DDL)
    con.executemany("INSERT INTO section_down (block, b) VALUES (?, ?)", down_blocks(rows))


# --- clim/3 -------------------------------------------------------------------------------------------------------

def clim3(clim2: bytes) -> bytes:
    """A clim/2 blob ([u8 pentad, 5 × f16 LE] records) → clim/3 (the same float16 bits, transposed + delta)."""
    if len(clim2) % 11:
        raise PackedError(f"clim/3: a clim/2 blob of {len(clim2)} bytes is not whole records")
    n = len(clim2) // 11
    if n > 255:
        raise PackedError(f"clim/3: {n} pentads")
    recs = [(clim2[i], struct.unpack_from("<5H", clim2, i + 1)) for i in range(0, len(clim2), 11)]
    deltas = []
    for c in range(5):
        prev = 0
        for _, h in recs:
            deltas.append((h[c] - prev) & 0xFFFF)
            prev = h[c]
    return bytes([n]) + bytes(p for p, _ in recs) + bytes(d & 0xFF for d in deltas) + bytes(d >> 8 for d in deltas)


def unclim3(b: bytes) -> bytes:
    """A clim/3 blob → the clim/2 blob it was made from (byte for byte)."""
    if not b:
        raise PackedError("clim/3: an empty blob")
    n = b[0]
    if len(b) != 1 + n + 10 * n:
        raise PackedError(f"clim/3: {len(b)} bytes is not a blob of {n} pentads")
    pent, lo, hi = b[1:1 + n], b[1 + n:1 + 6 * n], b[1 + 6 * n:]
    cols = []
    for c in range(5):
        prev, col = 0, []
        for i in range(n):
            k = c * n + i
            prev = (prev + (lo[k] | hi[k] << 8)) & 0xFFFF
            col.append(prev)
        cols.append(col)
    out = bytearray()
    for i in range(n):
        out.append(pent[i])
        out += struct.pack("<5H", *(cols[c][i] for c in range(5)))
    return bytes(out)


def pack_clim(con: sqlite3.Connection) -> None:
    """clim/2 bands (after the release's float16 step) → clim/3, in place."""
    rows = con.execute("SELECT rowid, bands FROM gauge_clim").fetchall()
    con.executemany("UPDATE gauge_clim SET bands = ? WHERE rowid = ?", [(clim3(b), r) for r, b in rows])

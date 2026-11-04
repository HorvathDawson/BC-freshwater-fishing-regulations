"""(T369) The `lines` release file: the stream key and measure span of every STREAM section, so a link's PLACE (stream
key + a distance along it, AGENTS rule 5) resolves to a section of the release the app holds. Its own file (`lines/1`),
fetched only when a link is made or opened — never on a cold load or a first tap (`contract.WEB_BUDGET`).

The format is pinned beside the app's reader (`app/packages/data/src/regs/line.ts`, `decodeLineBlock` /
`encodeLineBlock`); this writer is held to it byte for byte (`test_section_line.py` and `app/tools/section-line-parity.test.ts` read one
fixture, app/packages/data/dev/section-line-parity.json):

  section_line (block INTEGER PRIMARY KEY, n INTEGER NOT NULL, blk0 INTEGER NOT NULL, m0 INTEGER NOT NULL,
                b BLOB NOT NULL)          + INDEX section_line_head ON section_line (blk0, m0)

One row per block of 4096 handles (`h >> 12`, as `section_akey`); slot i is handle `block << 12 | i`; `n` the slots
used — every block is full up to the last stream handle (a lake's handle sorts after every stream's,
`section_handles.sort_key`, and has no line). Handle 0 is "no section": its slot (block 0, slot 0) is a FILLER that
keeps the block sorted and spans nothing (handle 1's key and start, end = start). `blk0` / `m0`: slot 0's key and start,
so a place's block is one indexed read (the last head at or below it).

`b`, NOT deflated (the file ships gzipped; an uncompressed block is what lets two writers agree byte for byte), is
columns of unsigned LEB128 varints (`zz` = zigzag for a signed value; arithmetic, never 32-bit bit operations — a key can
be below −2^31). A RUN is the slots in a row with one stream key (slots are in strict (key, start) order):

  R                                    the runs
  R × head = zz(key − the previous run's key) × 8 + multi × 4 + closed × 2 + nonzero × 1
                                       (the first run's key from 0); multi: the run has 2+ slots; closed: its last slot
                                       has a stored end; nonzero: its first slot's start is not 0
  count, for each multi run            its slots (all counts sum to n)
  starts: per run, its first start (only when nonzero), then each next slot's start − the previous slot's start
  length, for each closed run          its last slot's end − start
  E, then E × (slot gap, zz(end − next start))
                                       every other slot ends where the next one starts, except these E (a lake between
                                       two pieces of one stream, …); slot gap = slot − the previous exception's − 1
                                       (the first's from −1)

A run that is NOT closed ends OPEN (0xFFFFFFFF): the stream has ONE section in this release (1,020,114 of 1,088,240
stream keys), so its key alone finds it and no length is stored — those lengths were 1.7 of 2.9 MB gzipped. The app's
place for an open section is its start (`lineMid`), a real point on the stream that this release resolves back to it.
A stream split across two blocks is closed in both.
"""
from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple

BLOCK_BITS = 12
SLOTS = 1 << BLOCK_BITS
FORMAT = "lines/1"
DDL = ("CREATE TABLE section_line (block INTEGER PRIMARY KEY, n INTEGER NOT NULL, blk0 INTEGER NOT NULL, "
       "m0 INTEGER NOT NULL, b BLOB NOT NULL)",
       "CREATE INDEX section_line_head ON section_line (blk0, m0)")
COLUMNS = ["block", "n", "blk0", "m0", "b"]

Line = Tuple[int, int, int]          # (blk, from_m, to_m)


def _uv(out: bytearray, v: int) -> None:
    if v < 0:
        raise ValueError(f"section_line: {v} is not an unsigned varint")
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)


def _zz(v: int) -> int:
    return 2 * v if v >= 0 else -2 * v - 1


def _unzz(u: int) -> int:
    return u // 2 if u % 2 == 0 else -(u + 1) // 2


#: the end of a stream's ONE section in this release (module doc)
OPEN = 0xFFFFFFFF


def encode_block(lines: Sequence[Line]) -> bytes:
    """`lines[i]` is slot i, in strict (key, start) order; a run of one slot must end OPEN, a longer run's ends must not."""
    n = len(lines)
    runs: List[Tuple[int, int, int]] = []           # (key, first slot, count)
    for i, (blk, _, _) in enumerate(lines):
        if runs and runs[-1][0] == blk:
            runs[-1] = (blk, runs[-1][1], runs[-1][2] + 1)
        else:
            runs.append((blk, i, 1))
    heads, counts, starts, lengths, exc = bytearray(), bytearray(), bytearray(), bytearray(), []
    prev = 0
    for k, i, c in runs:
        lo, closed = lines[i][1], lines[i + c - 1][2] != OPEN
        if any(lines[j][2] == OPEN for j in range(i, i + c - 1)):
            raise ValueError(f"section_line: stream {k}: only a run's last section may be open-ended")
        _uv(heads, _zz(k - prev) * 8 + (4 if c > 1 else 0) + (2 if closed else 0) + (1 if lo else 0))
        prev = k
        if c > 1:
            _uv(counts, c)
        if lo:
            _uv(starts, lo)
        for j in range(i + 1, i + c):
            _uv(starts, lines[j][1] - lines[j - 1][1])
            if lines[j - 1][2] != lines[j][1]:
                exc.append((j - 1, lines[j - 1][2] - lines[j][1]))
        if closed:
            _uv(lengths, lines[i + c - 1][2] - lines[i + c - 1][1])
    out = bytearray()
    _uv(out, len(runs))
    out += heads + counts + starts + lengths
    _uv(out, len(exc))
    p = -1
    for i, d in exc:
        _uv(out, i - p - 1)
        _uv(out, _zz(d))
        p = i
    return bytes(out)


def decode_block(b: bytes, n: int) -> List[Line]:
    pos = 0

    def uv() -> int:
        nonlocal pos
        v, shift = 0, 0
        while True:
            if pos >= len(b):
                raise ValueError("section_line: a block ends inside a varint")
            c = b[pos]
            pos += 1
            v |= (c & 0x7F) << shift
            if c < 0x80:
                return v
            shift += 7
    r = uv()
    heads, k = [], 0
    for _ in range(r):
        h = uv()
        k += _unzz(h // 8)
        heads.append((k, h & 4, h & 2, h & 1))
    counts = [uv() if multi else 1 for _, multi, _, _ in heads]
    if sum(counts) != n:
        raise ValueError(f"section_line: a block of {n} slots has runs of {sum(counts)}")
    blk, lo, hi = [], [], []
    for (key, _, _, nz), c in zip(heads, counts):
        for j in range(c):
            lo.append((uv() if nz else 0) if j == 0 else lo[-1] + uv())
            blk.append(key)
            hi.append(OPEN)
    i = 0
    for (_, _, closed, _), c in zip(heads, counts):
        for j in range(i, i + c - 1):
            hi[j] = lo[j + 1]
        if closed:
            hi[i + c - 1] = lo[i + c - 1] + uv()
        i += c
    p = -1
    for _ in range(uv()):
        p = p + 1 + uv()
        hi[p] = lo[p + 1] + _unzz(uv())
    if pos != len(b):
        raise ValueError(f"section_line: {len(b) - pos} bytes after a block's last column")
    return list(zip(blk, lo, hi))


def blocks(rows: Iterable[Tuple[int, int, int, int]]) -> List[Tuple[int, int, int, int, bytes]]:
    """`[(block, n, blk0, m0, b)]` from `rows` = (sid, blk, from_m, to_m) of EVERY stream handle (the bundle's
    `section_measure`). Refused unless they are handles 1..N, contiguous, in strict (blk, from) order — a gap or a
    disorder would make the app's binary search land on the wrong section."""
    lines: List[Line] = []
    for i, (sid, blk, lo, hi) in enumerate(sorted(rows)):
        if sid != i + 1:
            raise ValueError(f"section_line: stream handles are not 1..N contiguous (handle {sid} at {i + 1})")
        if not (0 <= lo <= hi < 2 ** 32) or abs(blk) >= 2 ** 52:
            raise ValueError(f"section_line: handle {sid} ({blk}, {lo}, {hi}) does not fit the layout")
        if lines and (blk, lo) <= lines[-1][:2]:
            raise ValueError(f"section_line: handle {sid} ({blk}:{lo}) is not after handle {sid - 1} {lines[-1][:2]}")
        lines.append((blk, lo, hi))
    if not lines:
        raise ValueError("section_line: no stream sections")
    # a stream of ONE section is open-ended (`OPEN`): its key alone finds it
    ends = [lines[i][2] if (i > 0 and lines[i - 1][0] == b) or (i + 1 < len(lines) and lines[i + 1][0] == b) else OPEN
            for i, (b, _, _) in enumerate(lines)]
    lines = [(b, lo, e) for (b, lo, _), e in zip(lines, ends)]
    slots = [(lines[0][0], lines[0][1], lines[0][1])] + lines          # slot 0 = handle 0: the filler
    out = []
    for k in range(0, len(slots), SLOTS):
        bl = slots[k:k + SLOTS]
        out.append((k >> BLOCK_BITS, len(bl), bl[0][0], bl[0][1], encode_block(bl)))
    return out


def write(con, rows: Iterable[Tuple[int, int, int, int]]) -> int:
    """Create and fill `section_line` in `con` (main schema); returns the stream handles written."""
    bs = blocks(rows)
    for stmt in DDL:
        con.execute(stmt)
    con.executemany("INSERT INTO section_line (block, n, blk0, m0, b) VALUES (?, ?, ?, ?, ?)", bs)
    return sum(n for _, n, *_ in bs) - 1

"""Reader for the Province's 2001 vector depth files: ArcInfo export (`.e00`, uncompressed "EXP  0") (T634 item 3).

WHAT IS IN ONE ZIP (FIDQ `downloadBathymetricMap.do?filename=<sheet>.zip`): two coverages.
  * `<lake>.e00` — LINES. ARC section (the vertices), and the arc attribute table `<LAKE>.AAT` with `FCODE` (the
    Province's feature code) and `CONTOUR` (0 for the shore, then the depth of each contour line, negative down).
    Also `<LAKE>.META` (TITLE, WARNING, RESOLUTION "Scale 1:10000; diagrammatic - no projection", TECH_NOTES …) and
    annotation (`TX7`), which this reader skips.
  * `<lake>p.e00` — SOUNDINGS. LAB section (one point per label) and `<LAKE>P.PAT` with `DEPTH` per label.

COORDINATES are sheet metres at true size, NOT placed on the earth (the META says "no projection"); placing is
`pipeline.bathymetry.place`. Depth values are copied as written: the file says nothing about feet or metres in a
field of its own, so `units_stated` reports only what its META / annotation text literally says, else None.

THE FORMAT (the parts read here; precision 2 = single, 3 = double):
  * a section starts with a header line `XXX  2|3` at column 0 and ends with its own terminator;
  * ARC: per arc a line of 7 ints (arc#, cover#, from node, to node, left poly, right poly, n vertices) then the
    vertices, 2 per line at 14 characters each (precision 2) or 1 per line at 21 each (precision 3); ends at arc# -1;
  * LAB: per label `cover#, poly#, x, y` then its box (1 line at precision 2, 2 at precision 3); ends at cover# -1;
  * IFO: per table a header (name in 32 chars, then item count … record count), one definition line per item
    (after the 16-character name: size, -1, start, -1, width, decimals, TYPE …), then the records, each printed as the items' text widths run
    together and wrapped at 80 characters. Text widths by type: 10 date 8, 20 char = size, 30 fixed = size, 40 float 14,
    50 int 6 (size 2) or 11 (size 4), 60 float 14 (size 4) or 24 (size 8). Ends at `EOI`.
Anything the reader does not understand RAISES naming the line: a half-read chart is worse than none.

COMPRESSED exports ("EXP  1", 6 zips, T634 third pass) are decompressed first (`decompress`): following e00compr's
`e00read.c` (1.0.1): physical line breaks dropped; `~}` a line break, `~ ` + c (ord(c) - 32) spaces, `~~` / `~-` a
literal; `~` + h (n = ord(h) - 33) a number (decimal point after n % 15 digits, `E+`/`E-` before the last two when
(n // 15) % 3 is 1/2, last digit dropped when n // 45 is 1), each following char two digits (ord - 33; 92 + the next
char's for 92..99), ending at a space or `~`; a `~` right after a number only ended it, so the char after it is
literal. Checked against GDAL's AVCE00 driver, which
parses the decompressed text (and refuses the compressed one).
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

HEADER = re.compile(r"^(ARC|CNT|LAB|PAL|TOL|TX6|TX7|TXT|SIN|LOG|PRJ|IFO|RXP|RPL|MSK|TOP|MTD)  ([23])\s*$")


class E00Error(ValueError):
    pass


@dataclass
class Arc:
    id: int
    cover_id: int
    points: List[Tuple[float, float]]


@dataclass
class Label:
    cover_id: int
    poly_id: int
    x: float
    y: float


@dataclass
class Table:
    name: str
    items: List[Tuple[str, int, int]]     # (name, internal size, type)
    rows: List[Dict[str, object]]


@dataclass
class Coverage:
    name: str
    precision: int = 2
    arcs: List[Arc] = field(default_factory=list)
    labels: List[Label] = field(default_factory=list)
    tables: Dict[str, Table] = field(default_factory=dict)

    def table(self, suffix: str) -> Optional[Table]:
        """The INFO table whose name ends with `.SUFFIX` (AAT, PAT, META …)."""
        hits = [t for n, t in self.tables.items() if n.upper().endswith("." + suffix.upper())]
        if len(hits) > 1:
            raise E00Error(f"{self.name}: {len(hits)} tables end with .{suffix}")
        return hits[0] if hits else None

    def meta(self) -> Dict[str, str]:
        t = self.table("META")
        return {} if t is None else {str(r["KEY"]).strip(): str(r["VALUE"]).strip() for r in t.rows}


def _floats(line: str, width: int, count: int, where: str) -> List[float]:
    out = []
    for k in range(count):
        s = line[k * width:(k + 1) * width]
        try:
            out.append(float(s))
        except ValueError as e:
            raise E00Error(f"{where}: not a number {s!r} in {line!r}") from e
    return out


def _text_width(size: int, typ: int) -> int:
    if typ == 10:
        return 8
    if typ in (20, 30):
        return size
    if typ == 40:
        return 14
    if typ == 50:
        return 6 if size == 2 else 11
    if typ == 60:
        return 14 if size == 4 else 24
    raise E00Error(f"INFO item type {typ} (size {size}) is not one this reader knows")


def _value(text: str, typ: int):
    s = text.strip()
    if typ in (20, 10):
        return text.rstrip()
    if s == "":
        return None
    if typ in (30, 50):
        return int(s)
    return float(s)


class _Lines:
    def __init__(self, text: str, name: str):
        self.lines = text.splitlines()
        self.i = 0
        self.name = name

    def next(self) -> str:
        if self.i >= len(self.lines):
            raise E00Error(f"{self.name}: file ends inside a section")
        line = self.lines[self.i]
        self.i += 1
        return line

    def where(self) -> str:
        return f"{self.name}:{self.i}"


def _read_arcs(L: _Lines, prec: int) -> List[Arc]:
    arcs = []
    per_line, width = (2, 14) if prec == 2 else (1, 21)
    while True:
        head = L.next().split()
        if len(head) != 7:
            raise E00Error(f"{L.where()}: ARC header wants 7 ints, got {head}")
        n_id, cov, _fn, _tn, _lp, _rp, npts = map(int, head)
        if n_id == -1:
            return arcs
        pts: List[Tuple[float, float]] = []
        while len(pts) < npts:
            line = L.next()
            take = min(per_line, npts - len(pts))
            v = _floats(line, width, 2 * take, L.where())
            pts.extend((v[2 * k], v[2 * k + 1]) for k in range(take))
        arcs.append(Arc(n_id, cov, pts))


def _read_labels(L: _Lines, prec: int) -> List[Label]:
    out = []
    width = 14 if prec == 2 else 21
    while True:
        line = L.next()
        cov, poly = int(line[0:10]), int(line[10:20])
        if cov == -1:
            return out
        x, y = _floats(line[20:], width, 2, L.where())
        for _ in range(1 if prec == 2 else 2):
            L.next()
        out.append(Label(cov, poly, x, y))


def _read_info(L: _Lines) -> Dict[str, Table]:
    tables: Dict[str, Table] = {}
    while True:
        line = L.next()
        if line.strip() == "EOI":
            return tables
        name = line[:32].strip()
        nums = [int(t) for t in re.findall(r"-?\d+", line[32:])]
        if len(nums) < 2:
            raise E00Error(f"{L.where()}: INFO header {line!r}")
        n_items, n_rows = nums[0], nums[-1]
        items = []
        for _ in range(n_items):
            d = L.next()
            tok = re.findall(r"-?\d+", d[16:])
            if len(tok) < 7:
                raise E00Error(f"{L.where()}: INFO item line {d!r}")
            items.append((d[:16].strip(), int(tok[0]), int(tok[6])))
        widths = [_text_width(s, t) for _, s, t in items]
        rec = sum(widths)
        nlines = max(1, math.ceil(rec / 80))
        rows = []
        for _ in range(n_rows):
            buf = "".join(L.next().ljust(80) for _ in range(nlines))[:rec]
            row, pos = {}, 0
            for (iname, _s, typ), w in zip(items, widths):
                row[iname] = _value(buf[pos:pos + w], typ)
                pos += w
            rows.append(row)
        tables[name] = Table(name, items, rows)


def decompress(text: str, name: str = "e00") -> str:
    """A compressed export ("EXP  1") -> the uncompressed text ("EXP  0"), as e00compr's `e00read.c` 1.0.1 does it.
    Raises on a sequence it cannot read."""
    lines = text.split("\n")
    head = lines[0].rstrip("\r")
    if not head.startswith("EXP  1"):
        raise E00Error(f"{name}: not a compressed E00 (first line {head[:20]!r})")
    body = "".join(l.rstrip("\r") for l in lines[1:])
    out: List[str] = ["EXP  0" + head[6:].rstrip() + "\n"]
    i, n, after_number = 0, len(body), False
    while i < n:
        c = body[i]
        i += 1
        if c != "~":
            out.append(c)
            after_number = False
            continue
        if i >= n:
            raise E00Error(f"{name}: a lone ~ at the end")
        c = body[i]
        i += 1
        if c == " ":
            out.append(" " * (ord(body[i]) - 32))
            i += 1
            after_number = False
        elif c == "}":
            out.append("\n")
            after_number = False
        elif after_number:                  # the ~ only ended the number before it: c is a plain character
            out.append(c)
            after_number = False
        elif c in "~-":
            out.append(c)
        elif "!" <= c <= "z":
            h = ord(c) - 33
            dec, odd, expk = h % 15, h // 45, (h // 15) % 3
            buf: List[str] = []
            k = 0
            while i < n and body[i] not in " ~":
                v = ord(body[i]) - 33
                i += 1
                if v == 92 and i < n:
                    v += ord(body[i]) - 33
                    i += 1
                if not 0 <= v <= 99:
                    raise E00Error(f"{name}: digit pair {v} at {i}")
                for d in (v // 10, v % 10):
                    buf.append(str(d))
                    k += 1
                    if k == dec:
                        buf.append(".")
            after_number = i < n
            if odd:
                buf.pop()
            if expk:
                buf[-2:-2] = list("E+" if expk == 1 else "E-")
            out.append("".join(buf))
        else:
            raise E00Error(f"{name}: unknown code ~{c!r} at {i}")
    return "".join(out)


def read_coverage(text: str, name: str = "e00") -> Coverage:
    """One `.e00` file's text -> its arcs, labels and INFO tables (a compressed export is decompressed first)."""
    if text.startswith("EXP  1"):
        text = decompress(text, name)
    L = _Lines(text, name)
    first = L.next()
    if not first.startswith("EXP  0"):
        raise E00Error(f"{name}: not an uncompressed E00 (first line {first[:20]!r})")
    cov = Coverage(name)
    while L.i < len(L.lines):
        line = L.next()
        if line.strip() == "EOS":
            return cov
        m = HEADER.match(line)
        if not m:
            continue                        # inside a section this reader skips (annotation, log, tolerances)
        kind, prec = m.group(1), int(m.group(2))
        if kind == "ARC":
            cov.precision = prec
            cov.arcs = _read_arcs(L, prec)
        elif kind == "LAB":
            cov.labels = _read_labels(L, prec)
        elif kind == "IFO":
            cov.tables = _read_info(L)
    raise E00Error(f"{name}: no EOS line")


# ---- one sheet: lines + soundings ----------------------------------------------------------------------------------

@dataclass
class Sheet:
    lines: Coverage
    points: Optional[Coverage]

    def contours(self) -> List[Tuple[Optional[float], Optional[str], List[Tuple[float, float]]]]:
        """(CONTOUR, FCODE, vertices) per arc, in arc order. The AAT's rows are the arcs in order (row k = arc k)."""
        aat = self.lines.table("AAT")
        if aat is None:
            raise E00Error(f"{self.lines.name}: no .AAT table")
        if len(aat.rows) != len(self.lines.arcs):
            raise E00Error(f"{self.lines.name}: {len(aat.rows)} AAT rows for {len(self.lines.arcs)} arcs")
        return [(r.get("CONTOUR"), (r.get("FCODE") or "").strip() or None, a.points)
                for r, a in zip(aat.rows, self.lines.arcs)]

    def soundings(self) -> List[Tuple[float, float, Optional[float]]]:
        if self.points is None:
            return []
        pat = self.points.table("PAT")
        if pat is None:
            return []
        by_id = {int(r[k]): r.get("DEPTH") for r in pat.rows for k in r if k.endswith("#")}
        return [(lb.x, lb.y, by_id.get(lb.cover_id)) for lb in self.points.labels]

    def text(self) -> str:
        """Every word the sheet's META carries, for the units scan."""
        return " ".join(f"{k} {v}" for k, v in self.lines.meta().items())


#: The sheets print it as a title-block phrase, "DEPTHS IN METRES" / "DEPTHS IN FEET" (kept in the annotation). Only
#: that phrase counts: a bare "ft" or "foot" is a trail, an elevation or an area as often as a depth.
_PHRASE = re.compile(r"\bdepths?\s+(?:are\s+)?in\s+(feet|foot|ft|metres|meters|metre|meter|m)\b", re.I)


def units_stated(texts: Sequence[str]) -> Optional[str]:
    """'feet' | 'metres' | 'both' when the file's own words say "depth(s) in …"; None when they do not. Never
    inferred from the numbers."""
    got = {("feet" if m.group(1).lower() in ("feet", "foot", "ft") else "metres")
           for t in texts for m in _PHRASE.finditer(t)}
    return "both" if len(got) == 2 else (got.pop() if got else None)


def read_zip_all(path) -> List[Sheet]:
    """A FIDQ vector zip -> one Sheet per line coverage (a zip may hold two lakes). The lines coverage is the one with
    an AAT; its soundings are the coverage named the same plus `p` (`donner.e00` + `donnerp.e00`) whose PAT has DEPTH.
    A compressed export (`EXP  1`) is decompressed (`decompress`)."""
    import zipfile
    with zipfile.ZipFile(path) as z:
        covs = [read_coverage(z.read(n).decode("latin-1"), n) for n in sorted(z.namelist())
                if n.lower().endswith(".e00")]
    lines = [c for c in covs if c.table("AAT") is not None]
    points = {c.name.lower(): c for c in covs
              if c.table("PAT") is not None and any(i[0] == "DEPTH" for i in c.table("PAT").items)}
    if not lines:
        raise E00Error(f"{path}: no line coverage")
    out = []
    for c in lines:
        stem = c.name.lower()[:-4]
        out.append(Sheet(c, points.get(stem + "p.e00")))
    return out


def read_zip(path) -> Sheet:
    """The one Sheet of a single-lake zip; raises when the zip holds more than one."""
    sheets = read_zip_all(path)
    if len(sheets) != 1:
        raise E00Error(f"{path}: {len(sheets)} line coverages")
    return sheets[0]


def annotation_words(path) -> List[str]:
    """The raw annotation strings (TX7 text) of every coverage in the zip — only for the units scan."""
    import zipfile
    out = []
    with zipfile.ZipFile(path) as z:
        for n in sorted(z.namelist()):
            if not n.lower().endswith(".e00"):
                continue
            for row in _tat_rows(z.read(n).decode("latin-1"), n):
                out.append(row)
    return out


def _tat_rows(text: str, name: str) -> Iterator[str]:
    cov = read_coverage(text, name)
    for tname, t in cov.tables.items():
        if ".TAT" in tname.upper():
            for r in t.rows:
                if r.get("TEXT"):
                    yield str(r["TEXT"]).strip()

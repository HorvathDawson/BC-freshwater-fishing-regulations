"""The synopsis's printed pages as reading-order text, and the FIGURES a passage prints (T553).

The base rules (the provincial pages and each region's "Regional Regulations" page) are hand-authored entries, and
their only outside check used to be the hand-typed transcriptions in `data/curated/regulations/reference/*.md`. This
module closes the last hop: it reads a PRINTED page of the repo PDF deterministically (no LLM, like
`synopsis_definitions` does for p.80) and says which numbers and date ranges of a passage that page does not print.

`page_text(printed)` — the page in reading order: three columns at the book's fixed gutters (x 202 / 374 pt on every
regional and provincial page), each top to bottom, then any line that runs across a gutter (a box set across the page),
running header kept, footer dropped. A line ending in a hyphen joins the next only when the hyphen splits a word.

`figures(text)` — what a passage prints that a reader acts on: every number (`4`, `50`, `41.15`), every date or date
range (`oct 16-aug 14`, `apr 1-june 30` is `apr 1-jun 30`), and every number with the word printed before it
(`trout 4`, `over 50`, `kokanee 10`), so "Trout: 4" is not satisfied by a 4 anywhere on the page. URLs and phone
numbers are a different thing and are left out.

`unprinted(text, pages)` — the figures of `text` that none of `pages` prints. Pages that are TABLES
(`TABLE_PAGES`: p.5's licence fees, where a label and its price sit in different columns) are checked for numbers and
day numbers only; their word-number pairs are not readable in reading order.

`fee_table()` — p.5's licence fee table read AS A TABLE (T628): each printed row with its label, the group heading
it sits under and its three cells by the column each is printed under (resident / non-resident / non-resident
alien), so a fee stored under the wrong column, or a changed fee, is seen. `unprinted` cannot see either on that page.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import FrozenSet, Iterable, List, Optional, Tuple

#: The column gutters of the provincial and regional pages (pt from the left edge of a 576 pt page). Measured on
#: printed pp. 5-10, 13, 21, 28, 34, 42, 49, 58, 64, 68: the word-free bands are 196-208 and 368-380.
GUTTERS = (202.0, 374.0)
#: Pages whose figures are a table: label and value are in different columns, so only bare numbers are checked.
TABLE_PAGES = frozenset({5})
_FOOT = 0.95          # the running footer (page number, edition) is the bottom 5 % of the page
_SAME_LINE = 3.0      # words whose tops differ by less than this are one printed line (a bold label sits ~1 pt off)

_MON = r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?"
_DATE = re.compile(r"\b" + _MON + r" (\d{1,2})(?:-(?:" + _MON + r" )?(\d{1,2}))?\b")
_NUM = re.compile(r"(?<![\d.])\d+(?:\.\d+)?(?!\d)")
_URL = re.compile(r"\S*(?:https?://|www\.|\.ca/|\.com|\.org)\S*")
_PHONE = re.compile(r"\(?\b\d{3}\)?[ -]\d{3}-\d{4}\b|\b1-\d{3}-\d{3}-\d{4}\b")


def page_lines(page) -> List[str]:
    """A pdfplumber page as its printed lines in reading order (see the module docstring)."""
    p = _dedupe(page)                # the faux-bold headings draw every glyph twice ("RREEGGIIOONN")
    words = [w for w in p.extract_words() if w["bottom"] < p.height * _FOOT]
    words.sort(key=lambda w: (round(w["top"]), w["x0"]))
    frags: List[dict] = []           # runs of words set together (a gap under 6 pt) on one line
    for w in words:
        f = frags[-1] if frags else None
        if f and abs(w["top"] - f["top"]) < 2.5 and 0 <= w["x0"] - f["x1"] < 6:
            f["words"].append(w["text"])
            f["x1"] = w["x1"]
        else:
            frags.append({"top": w["top"], "x0": w["x0"], "x1": w["x1"], "words": [w["text"]]})
    edges = (0.0,) + GUTTERS + (float("inf"),)
    streams: List[List[dict]] = [[] for _ in range(len(edges))]
    for f in frags:
        col = next(i for i in range(len(edges) - 1) if edges[i] <= f["x0"] < edges[i + 1])
        streams[-1 if f["x1"] > edges[col + 1] + 2 else col].append(f)
    out: List[str] = []
    for s in streams:
        s.sort(key=lambda f: f["top"])
        lines: List[List[dict]] = []
        for f in s:
            if lines and f["top"] - lines[-1][0]["top"] < _SAME_LINE:
                lines[-1].append(f)
            else:
                lines.append([f])
        for ln in lines:
            ln.sort(key=lambda f: f["x0"])
            out.append(" ".join(" ".join(f["words"]) for f in ln))
    return out


def _dedupe(page):
    """`page.dedupe_chars()` for this book: a glyph drawn again at the same rounded point is the faux-bold's second
    stroke. Same words as pdfplumber's on every page read here (checked on pp. 5, 7, 13, 58), a tenth of the time."""
    seen = set()

    def keep(o) -> bool:
        if o.get("object_type") != "char":
            return True
        k = (o["text"], round(o["x0"]), round(o["top"]))
        if k in seen:
            return False
        seen.add(k)
        return True
    return page.filter(keep)


def fold(text: str) -> str:
    """The comparison form: lower case, no emphasis/bullets/quote marks, one dash, dashes tight, one space, a day
    number without its leading zero. Both sides of every comparison go through this."""
    t = (text or "").lower().replace("[includes tributaries]", " ")
    t = re.sub(r"[―•▪‣]", " ", t)                       # bar and bullet glyphs: separators
    t = re.sub(r"[‐-—−]", "-", t)
    t = re.sub(r"[‘’]", "'", t)
    t = re.sub(r"[“”]", '"', t)
    t = re.sub(r"(?m)^\s*[>|]\s?", " ", t).replace("*", "").replace("`", "").replace("|", " ")
    t = re.sub(r"\s*-\s*", "-", t)
    t = " ".join(t.split())
    return re.sub(r"\b" + _MON + r"\s+0?(\d{1,2})\b", lambda m: f"{m.group(1)} {int(m.group(2))}", t)


@dataclass(frozen=True)
class Figures:
    dates: FrozenSet[str]
    numbers: FrozenSet[str]
    pairs: FrozenSet[str]


def figures(text: str) -> Figures:
    t = fold(text)
    t = _PHONE.sub(" ", _URL.sub(" ", t))
    dates = set()
    for m in _DATE.finditer(t):
        m1, d1, m2, d2 = m.groups()
        dates.add(f"{m1} {int(d1)}" + (f"-{m2 or m1} {int(d2)}" if d2 else ""))
    numbers = set(_NUM.findall(t))
    pairs = set()
    for para in re.split(r"\n\s*\n", _PHONE.sub(" ", _URL.sub(" ", (text or "").lower()))):
        # a pair never spans a blank line: a heading or a table's end is not "the word printed before" a number
        toks = [x.strip(".-") for x in re.sub(r"[^a-z0-9$/. -]", " ", fold(para)).split()]
        toks = [x for x in toks if x]
        pairs |= {f"{toks[i - 1]} {x}" for i, x in enumerate(toks) if i and re.search(r"\d", x)}
    return Figures(frozenset(dates), frozenset(numbers), frozenset(pairs))


def unprinted(text: str, pages: Iterable[int], pdf_path: Optional[Path] = None) -> List[str]:
    """The figures of `text` that none of the PRINTED `pages` prints, each as `kind: figure`, sorted."""
    pages = tuple(pages)
    if not pages:
        raise ValueError("unprinted: no pages to check against")
    got = figures(text)
    have = figures(" ".join(page_text(n, pdf_path) for n in pages))
    table = any(n in TABLE_PAGES for n in pages)
    out = [f"number: {n}" for n in got.numbers - have.numbers]
    if table:
        # a table page: a date's day may sit in another cell than its month ("April" | "1st to March 31st")
        out += [f"date: {d}" for d in got.dates - have.dates
                if not set(_NUM.findall(d)) <= have.numbers]
    else:
        out += [f"date: {d}" for d in got.dates - have.dates]
        out += [f"pair: {p}" for p in got.pairs - have.pairs]
    return sorted(out)


def _words_key(text: str) -> str:
    return re.sub(r"[^a-z0-9$%/.,;:()' ]", "", fold(text).replace("-", "")).replace(" ", "")


def quoted(quote: str, pages: Iterable[int], pdf_path: Optional[Path] = None) -> bool:
    """Whether the PRINTED `pages` carry `quote` word for word, in reading order (case, emphasis, dashes, hyphens,
    spacing and quote marks aside). A quote that runs from one column into the next, or out of a box, reads False:
    it is checked against its transcription instead."""
    hay = _words_key(" ".join(page_text(n, pdf_path) for n in pages))
    return bool(quote.strip()) and _words_key(quote) in hay


def page_text(printed: int, pdf_path: Optional[Path] = None) -> str:
    """The PRINTED page's reading-order text, words joined across line-end hyphens. A missing PDF or page raises."""
    from pipeline.common.synopsis_pages import default_pdf
    p = Path(pdf_path or default_pdf()).resolve()
    if not p.exists():
        raise FileNotFoundError(f"synopsis_print: {p} not found (tracked in git: data/source/fishing_synopsis.pdf)")
    return _cached(p, printed)


def _pdf_index(pdf, printed: int, p: Path) -> int:
    """The PDF page (1-based) of the PRINTED page in the open `pdf`. The book's offset (PDF - 2 to printed 40, - 6
    after) names the page; its own footer must agree. Reading the whole map (`pdf_page`, every footer: ~30 s) is only
    for a book where it does not. An unnumbered page (the map, printed 41) takes the next numbered page's offset, as
    `synopsis_pages` reads it."""
    from pipeline.common.synopsis_pages import pdf_page, printed_page_number
    guess = printed + (2 if printed <= 40 else 6)
    own = printed_page_number(pdf.pages[guess - 1]) if 0 < guess <= len(pdf.pages) else -1
    if own == printed or (own is None and guess < len(pdf.pages)
                          and printed_page_number(pdf.pages[guess]) == printed + 1):
        return guess
    n = pdf_page(printed, p)
    if not n:
        raise ValueError(f"synopsis_print: {p} has no printed p.{printed}")
    return n


@lru_cache(maxsize=64)
def _cached(p: Path, printed: int) -> str:
    import pdfplumber
    with pdfplumber.open(str(p)) as pdf:
        lines = page_lines(pdf.pages[_pdf_index(pdf, printed, p) - 1])
    txt = ""
    for ln in lines:
        # a line-end hyphen joins with no space and stays: whether it split a word ("regula-tions") or ends a
        # compound ("first-come-first-serve") the page cannot say, so `quoted` compares with hyphens removed
        txt = txt + ln if re.search(r"[a-z]-$", txt) and re.match(r"[a-z]", ln) else f"{txt} {ln}"
    return txt.strip()


#: The printed page of the licence fee table, and its column headings left to right -> the residency each prices.
FEE_PAGE = 5
FEE_COLUMNS = (("RESIDENT*", "resident"), ("NON-RESIDENT*", "non_resident"), ("NON-RESIDENT ALIEN*", "non_resident_alien"))
#: The table's first heading cell, and the printed mark for "not available" in a cell.
_FEE_HEAD, _NOT_SOLD = "LICENCE TYPE", "★"
_FEE = re.compile(r"^\$(\d+\.\d{2})(/day)?(\*+)?$")


@dataclass(frozen=True)
class FeeRow:
    """One printed row of the fee table. `group` is the heading above it ("Basic Licences"; "" before any). `fees` is
    residency -> dollars for the cells that print a price; a ★ cell (not available) is absent. `per_day` holds the
    residencies whose price is printed "/day"; `text` is the row as printed, label then cells left to right."""
    group: str
    label: str
    fees: Tuple[Tuple[str, float], ...]
    per_day: FrozenSet[str]
    text: str


def fee_table(pdf_path: Optional[Path] = None) -> List[FeeRow]:
    """The licence fee table of printed p.5, row by row, top to bottom. Raises when the page does not hold the table as
    this reader knows it (a heading missing, a cell that is neither a price nor ★, a cell under no column, a row with
    other than three cells): a new book's layout is re-read by a person, never guessed at."""
    from pipeline.common.synopsis_pages import default_pdf
    p = Path(pdf_path or default_pdf()).resolve()
    if not p.exists():
        raise FileNotFoundError(f"synopsis_print: {p} not found (tracked in git: data/source/fishing_synopsis.pdf)")
    return list(_fee_rows(p))


@lru_cache(maxsize=4)
def _fee_rows(p: Path) -> Tuple[FeeRow, ...]:
    import pdfplumber
    with pdfplumber.open(str(p)) as pdf:
        page = _dedupe(pdf.pages[_pdf_index(pdf, FEE_PAGE, p) - 1])
        words = [w for w in page.extract_words() if w["bottom"] < page.height * _FOOT]
    words.sort(key=lambda w: (w["top"], w["x0"]))
    lines: List[List[dict]] = []
    for w in words:
        if lines and abs(w["top"] - lines[-1][0]["top"]) < _SAME_LINE:
            lines[-1].append(w)
        else:
            lines.append([w])
    for ln in lines:
        ln.sort(key=lambda w: w["x0"])
    want = _FEE_HEAD + " " + " ".join(h for h, _ in FEE_COLUMNS)
    at = next((i for i, ln in enumerate(lines) if " ".join(w["text"] for w in ln) == want), None)
    if at is None:
        raise ValueError(f"synopsis_print: printed p.{FEE_PAGE} has no fee table heading {want!r}")
    head, centres, k = lines[at], [], len(_FEE_HEAD.split())
    for title, residency in FEE_COLUMNS:           # each column is where its heading is printed
        span = head[k:k + len(title.split())]
        centres.append(((span[0]["x0"] + span[-1]["x1"]) / 2, residency))
        k += len(title.split())
    first_col = min(w["x0"] for w in head[len(_FEE_HEAD.split()):])
    # the table is its ruled box: label lines start at the heading's left edge, and it ends at the first line that
    # does not (the notes under it are set further left)
    left = head[0]["x0"]
    half = min(b[0] - a[0] for a, b in zip(centres, centres[1:])) / 2
    rows: List[FeeRow] = []
    group = ""
    for ln in lines[at + 1:]:
        if abs(ln[0]["x0"] - left) > 2:
            break
        label = [w for w in ln if w["x1"] < first_col - 2 and not _is_cell(w["text"])]
        cells = [w for w in ln if w not in label]
        name = " ".join(w["text"] for w in label)
        if not cells:
            group = name
            continue
        fees, per_day, taken = [], set(), []
        for w in cells:
            mid = (w["x0"] + w["x1"]) / 2
            x, residency = min(centres, key=lambda c: abs(c[0] - mid))
            m = _FEE.match(w["text"])
            if abs(x - mid) > half or (w["text"] != _NOT_SOLD and not m):
                raise ValueError(f"synopsis_print: fee table row {name!r}: {w['text']!r} at x={mid:.0f} is no cell "
                                 f"this reader knows (a price or {_NOT_SOLD} under one of the three columns)")
            taken.append(residency)
            if m:
                fees.append((residency, float(m.group(1))))
                if m.group(2):
                    per_day.add(residency)
        if taken != [r for _, r in FEE_COLUMNS]:
            raise ValueError(f"synopsis_print: fee table row {name!r} prints cells under {taken}, not one under each "
                             f"of {[r for _, r in FEE_COLUMNS]}")
        rows.append(FeeRow(group, name, tuple(fees), frozenset(per_day), " ".join(w["text"] for w in ln)))
    if not rows:
        raise ValueError(f"synopsis_print: printed p.{FEE_PAGE}: the fee table heading has no rows under it")
    return tuple(rows)


def _is_cell(text: str) -> bool:
    return text == _NOT_SOLD or bool(_FEE.match(text))


def _main(argv: Tuple[str, ...]) -> None:   # python -m pipeline.common.synopsis_print 13 — print a page as read
    for a in argv:
        print(f"===== printed p.{a}\n{page_text(int(a))}\n")


if __name__ == "__main__":
    import sys
    _main(tuple(sys.argv[1:]))

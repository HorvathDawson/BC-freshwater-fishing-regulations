"""WHERE A REGION'S OR THE PROVINCE'S RULE IS PRINTED ON ITS PAGE — the highlight boxes of the rules no table row holds
(T1032, owner 2026-11-23 "Yes, in 0.9.0").

A water's rule is boxed by its table row (`row_boxes.json`, T215). A zone, province or area rule ("Single barbless hook:
must be used in all streams of Region 6", the provincial "It Is Unlawful To…" list) prints in running text, so no row
holds it and its page opened with nothing marked. This finds it by DETERMINISTIC TEXT LOCATION in the synopsis PDF the
release ships — no LLM, no credits, no network, nothing written:

- the page's words in the PDF's own text flow (pdfplumber `use_text_flow`: the layout's frames in reading order, so
  a column reads down before the next), each reduced to its letters and digits, lower case (`norm`: so "May 15 – June
  15" and the printed "May 15-June 15" read alike, and a word hyphenated across a line joins up). No `dedupe_chars`:
  on these pages it changes only the faux-bold page numbers, and on a map page it eats real letters ("Bella" → "Bela");
- the rule's text, reduced the same way, must occur on the page EXACTLY ONCE, starting at a word's start and ending at
  a word's end, and be at least `MIN_CHARS` long. Anything else is NOT LOCATED, with its reason (`SHORT`, `ABSENT`,
  `AMBIGUOUS`): it gets no box and is listed (`python -m pipeline.regs.extraction.passage_boxes`). A box is never
  guessed (W46): no fuzzy match, no nearest paragraph, no "the region's whole page".
- the matched words become one box per run of lines in one column (`blocks`), padded by `MARGIN_PT`.

`rule_boxes` tries the rule's own `verbatim` on each of its entry's printed pages; where that is on none of them, the
entry's whole `printed` text (as a water's row box is its entry's row). The boxes are the answers' `book_box`
(`pipeline.deliver.answers.display.decided.book_boxes`): `{pdf, box [x0, top, x1, bottom], size [w, h]}`, points,
top-left origin — the row boxes' shape, so the app's opener (`core/regs/highlight.ts`) takes them unchanged.

    python -m pipeline.regs.extraction.passage_boxes [--export-dir DIR] [--json OUT]
        # coverage: located / total per printed page, every rule not located with its reason
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

#: the shortest reduced text placed: shorter fragments ("Trout: 4" is 6) still must be unique on the page
MIN_CHARS = 6
#: padding around the matched words, in points
MARGIN_PT = 2.0
#: a word on the next line of the same block starts no further below the last than this many line heights
LINE_GAP = 1.8

SHORT, ABSENT, AMBIGUOUS = "short", "absent", "ambiguous"
#: what `rule_boxes` placed a rule by
BY_RULE, BY_ENTRY = "rule", "entry"


def norm(s: str) -> str:
    """Letters and digits only, lower case, after NFKC (ligatures, full-width forms)."""
    return re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKC", s or "").lower())


@dataclass(frozen=True)
class PageText:
    pdf: int                       # 1-based PDF page
    size: Tuple[float, float]      # (width, height), points
    words: Tuple[dict, ...]        # pdfplumber words in text-flow order, those with something left by `norm`
    chars: str                     # every word's `norm`, concatenated
    owner: Tuple[int, ...]         # chars[i] belongs to words[owner[i]]
    starts: frozenset              # offsets in `chars` where a word starts
    ends: frozenset                # offsets in `chars` where a word ends


def page_text(page, pdf_page: int) -> PageText:
    """A pdfplumber page's words, reduced for matching."""
    ws = [w for w in page.extract_words(use_text_flow=True) if norm(w["text"])]
    chars: List[str] = []
    owner: List[int] = []
    starts, ends, pos = set(), set(), 0
    for i, w in enumerate(ws):
        t = norm(w["text"])
        chars.append(t)
        owner.extend([i] * len(t))
        starts.add(pos)
        pos += len(t)
        ends.add(pos)
    return PageText(pdf_page, (float(page.width), float(page.height)), tuple(ws), "".join(chars), tuple(owner),
                    frozenset(starts), frozenset(ends))


def blocks(words: Sequence[dict]) -> List[List[float]]:
    """The matched words (text-flow order) as boxes: a word joins the current box when it is on the box's last line to
    the right of the last word, or on the next line down overlapping the box's columns; otherwise it starts a new one
    (a column break, a jump to another frame)."""
    out: List[List[float]] = []
    last: Optional[dict] = None
    for w in words:
        x0, top, x1, bot = float(w["x0"]), float(w["top"]), float(w["x1"]), float(w["bottom"])
        if last is not None:
            b = out[-1]
            h = float(last["bottom"]) - float(last["top"])
            same_line = abs(top - float(last["top"])) < h / 2 and x0 >= float(last["x1"]) - 1
            next_line = 0 < top - float(last["top"]) <= LINE_GAP * h and x0 < b[2] and x1 > b[0]
            if same_line or next_line:
                b[0], b[1], b[2], b[3] = min(b[0], x0), min(b[1], top), max(b[2], x1), max(b[3], bot)
                last = w
                continue
        out.append([x0, top, x1, bot])
        last = w
    return out


def locate(pt: PageText, text: str) -> Tuple[str, List[dict]]:
    """`text` on the page: ("ok", its boxes) when its reduced form occurs exactly once on word bounds, else (reason, [])."""
    t = norm(text)
    if len(t) < MIN_CHARS:
        return SHORT, []
    hits = [m.start() for m in re.finditer("(?=" + re.escape(t) + ")", pt.chars)
            if m.start() in pt.starts and m.start() + len(t) in pt.ends]
    if not hits:
        return ABSENT, []
    if len(hits) > 1:
        return AMBIGUOUS, []
    a = hits[0]
    idx = sorted(set(pt.owner[a:a + len(t)]))
    w, h = pt.size
    boxes = []
    for x0, top, x1, bot in blocks([pt.words[i] for i in idx]):
        box = [max(0.0, x0 - MARGIN_PT), max(0.0, top - MARGIN_PT), min(w, x1 + MARGIN_PT), min(h, bot + MARGIN_PT)]
        boxes.append({"pdf": pt.pdf, "box": [round(v, 2) for v in box], "size": [round(w, 2), round(h, 2)]})
    return "ok", boxes


class Book:
    """The synopsis PDF, its pages read once each (the file is opened per page read and closed: no handle outlives it,
    so a forked worker never shares one)."""

    def __init__(self, pdf_path: Path):
        self.path = Path(pdf_path)
        self._pages: Dict[int, PageText] = {}

    def page(self, pdf_page: int) -> PageText:
        if pdf_page not in self._pages:
            import pdfplumber
            with pdfplumber.open(str(self.path)) as pdf:
                if not 1 <= pdf_page <= len(pdf.pages):
                    raise ValueError(f"passage_boxes: {self.path} has no PDF page {pdf_page} ({len(pdf.pages)} pages)")
                self._pages[pdf_page] = page_text(pdf.pages[pdf_page - 1], pdf_page)
        return self._pages[pdf_page]


def rule_boxes(book: Book, pages: Sequence[Tuple[int, int]], verbatim: str, printed: str) -> Tuple[Optional[str], List[dict], Dict[int, str]]:
    """A zone / province / area rule's boxes over its entry's `pages` ([(printed, pdf)]): its own `verbatim` wherever it
    is located; on none of them, the entry's `printed` text. Returns (BY_RULE | BY_ENTRY | None, boxes, {printed page:
    why the rule's text is not there}) — None and [] when neither is located on any page."""
    why: Dict[int, str] = {}
    for by, text in ((BY_RULE, verbatim), (BY_ENTRY, printed)):
        got: List[dict] = []
        for printed_page, pdf_page in pages:
            st, bx = locate(book.page(pdf_page), text)
            if st == "ok":
                got.extend(bx)
            elif by == BY_RULE:
                why[printed_page] = st
        if got:
            return by, got, why
    return None, [], why


# --------------------------------------------------------------------------------------------
# The coverage report
# --------------------------------------------------------------------------------------------

def report(doc: dict, book: Book, row_boxes) -> dict:
    """Coverage over every rule a water's rule set holds: per printed page, how many of the rules citing it carry a box
    on it (a water's by its row, `row_boxes(x, doc)`; any other by `rule_boxes`), and every non-water rule located on
    none of its pages, with why."""
    pdfp = {int(k): int(v) for k, v in ((doc.get("guide") or {}).get("pdf_pages") or {}).items()}
    held = sorted({rid for rs in doc["rulesets"].values() for via, m in rs.items() if via != "sections" for rid in m})
    per: Dict[int, Dict[str, int]] = {}
    unlocated = []
    by_count = {BY_RULE: 0, BY_ENTRY: 0, "row": 0}
    for rid in held:
        x = doc["rules"][rid]
        E = doc["entries"].get(x["entry_id"]) or {}
        pages = [(int(p), pdfp[int(p)]) for p in E.get("pages") or []]
        if E.get("kind") == "water" or E.get("kind") is None:
            boxes, by = row_boxes(x, doc), "row"
        else:
            by, boxes, why = rule_boxes(book, pages, x.get("verbatim") or "", E.get("printed") or "")
            if by is None:
                unlocated.append({"rule": rid, "kind": E.get("kind"), "pages": [p for p, _ in pages],
                                  "why": {str(k): v for k, v in sorted(why.items())}, "verbatim": x.get("verbatim")})
        if boxes:
            by_count[by] += 1
        on = {b["pdf"] for b in boxes}
        for p, q in pages:
            c = per.setdefault(p, {"rules": 0, "boxed": 0, "water_rules": 0, "water_boxed": 0})
            k = "water_" if by == "row" else ""
            c[k + "rules"] += 1
            if q in on:
                c[k + "boxed"] += 1
    return {"pages": {str(p): per[p] for p in sorted(per)}, "placed_by": by_count, "unlocated": unlocated}


def main(argv=None) -> int:
    from pipeline.common.curated import GENERATED
    from pipeline.deliver.answers.common import load_export
    from pipeline.deliver.answers.display import decided
    from pipeline.deliver.export.codec import expand
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--export-dir", type=Path, default=GENERATED.regs)
    ap.add_argument("--pdf", type=Path, default=None, help="the synopsis PDF (default: the repo copy)")
    ap.add_argument("--row-boxes", type=Path, default=None, help="the row-boxes sidecar (default: the generated one)")
    ap.add_argument("--json", type=Path, default=None, help="write the whole report here")
    a = ap.parse_args(argv)
    decided.use_book(a.pdf, a.row_boxes)
    data, guide = load_export(a.export_dir)
    doc = expand(data, guide)
    book = Book(decided.book_files()[0])
    rep = report(doc, book, decided.row_boxes_of)
    if a.json:
        a.json.write_text(json.dumps(rep, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print("printed page: zone/province/area rules boxed/citing · water rules boxed/citing")
    for p, c in rep["pages"].items():
        print(f"  p.{p:>3}: {c['boxed']}/{c['rules']} · {c['water_boxed']}/{c['water_rules']}")
    print(f"placed by: {rep['placed_by']}")
    print(f"not located ({len(rep['unlocated'])}): no box")
    for u in rep["unlocated"]:
        print(f"  {u['rule']} p.{u['pages']} {u['why']}: {(u['verbatim'] or '')[:90]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""T321 — DFO's limits wording as a CLOSED REGISTRY (`data/curated/dfo_salmon/wording.json`).

T320's guard was "every NUMBER in a limits cell is read". A restriction written in WORDS passed it as fully read: "one
hour before sunrise to one hour after sunset" dropped the hours, "4 coho per day in the aggregate" read as a plain daily
limit, "(see exception below)" lost its pointer — and a notice that changed a quota in new wording could pass the same
way. So every CLAUSE of a cell must be one DFO has used before, with a known meaning:

    template(clause)   numbers -> N, "FN 1281" -> FN#, spaces squashed, lower case, " .;" trimmed
    clauses(cell)      the cell split at ";", ",", a spaced dash and ". " (T332: not inside a lettered section title
                       "F. Fraser River Watershed - …", not after DFO's "Single,", not before a lower-case word
                       "77 cm. in length"; a species list "sockeye, pink, chum" stays with its clause; a clause's
                       leading dashes are not words — "— FN0372" is "FN0372", "— —" is nothing)
    unknown(cell)      the clauses whose template the registry does not hold (or holds as `rejected`)
    notes(cell)        the clauses whose meaning is `note`: shown on the card in DFO's words (`row_words`)

Each registry entry: `{template, kind, reads, status, rows, pages, example, cell}` —
    kind      structure   read into the rule's structure (`reads`: daily, hatchery, over, wild, monthly, max_cm, hours,
                          gap, hook, fly, bait, no_fishing, release, tbd, species … — what `typed` reads it as)
              note        the card shows the row in DFO's words (`reads` may still name what is read from it)
              provenance  the in-season notice number (FN) or its dash; no meaning of its own
    status    proposed | accepted | rejected. EVERYTHING SEEDED IS A PROPOSAL (owner 2026-10-29): the owner reviews each
              entry. ONLY AN ACCEPTED ENTRY IS KNOWN (T549 item 6): a proposed one — what the builder guessed a clause
              reads as — is unknown like a rejected one, so `build` after DFO prints new wording never clears the
              "new wording" guard before the owner has read the clause.

An UNKNOWN clause makes the row unparsed: its rules carry `review_reason` "new wording: '…'" (`typed.to_rules`), the
verdict `q.u` (no number), the card shows the row in DFO's words ("DFO changed the wording here — read their text"), and
the live feed tells the owner URGENTLY, once per new template (`live`, the Worker's `email.ts`).

THE BUILDER (`python -m pipeline.regs.dfo_salmon.wording build`): reads every saved page (the tracked fixtures, the
cache's raw pages and history) and writes the registry — existing entries keep their kind / reads / status (the counts
refresh); a template not yet in it is added as `proposed`, its kind guessed from what `typed` reads (a guess the owner
reviews). `check` lists the unknown clauses of the saved pages and exits 1 when there are any. No network, no model.

The Worker reads the same file (`app/workers/feeds/src/dfo/wording.ts`): the copy `pipeline.deliver.sync_inputs` puts
on R2 (private/feeds/dfo/wording.json), its only copy (T324: missing or invalid FAILS the DFO job). A parity test holds
the two readers (app/workers/feeds/test/dfo.wording.test.ts).
"""
from __future__ import annotations

import argparse
import functools
import glob
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.common.curated import CURATED, REPO_ROOT

#: T332: /2 — the clause split changed (no section-title / species-list / "Single," / dash scraps), so a /1 registry's
#: templates are not this split's: each reader REFUSES a registry of another format (T324: there is no compiled-in copy)
FORMAT = "dfo-wording/2"
REGISTRY = CURATED.dfo_salmon.wording            # T331: declared in config.yaml (`curated.dfo_salmon.wording`)
#: a typed rule's `review_reason` for a row with a clause the registry does not know
NEW_WORDING = "new wording: "
KINDS = ("structure", "note", "provenance")
STATUSES = ("proposed", "accepted", "rejected")

_SPLIT = re.compile(r"[;,]|\s[–—-]\s|\.\s(?![a-z])")
#: (T332) a lettered section title, "F" / "F. Fraser River Watershed": its dot and its dash are not clause breaks — the
#: split made two scraps of Region 6's "F. Fraser River Watershed - There is no fishing for salmon in the portions of the
#: Fraser River watershed in Region 6." (a watershed SCOPE, section F of the cascade, closed outright — not a limit)
_TITLE = re.compile(r"[A-Z](?:\.\s+[A-Z][A-Za-z ]*)?")
#: (T332) DFO's comma in "Single, barbless hook …": one clause
_GLUED = re.compile(r"single", re.I)
#: (T332) the rest of a species list, "pink, chum" in "No fishing for sockeye, pink, chum.": part of the clause before it
_SPECIES_LIST = re.compile(r"(?:chinook|coho|sockeye|pink|chum)(?:\s+(?:or|and)\s+(?:chinook|coho|sockeye|pink|chum))*"
                           r"(?:\s+salmon)?", re.I)
_DASHES = "–—-"
_FN = re.compile(r"\bFN\s*\d+", re.I)
_NUM = re.compile(r"\d+(?:[.:]\d+)?")
_WS = re.compile(r"\s+")


def template(clause: str) -> str:
    """A clause with its numbers blanked: what the registry is keyed by."""
    c = _FN.sub("FN#", clause)
    c = _NUM.sub("N", c)
    return _WS.sub(" ", c).strip(" .;").lower()


def _clean(c: str) -> str:
    return c.strip().lstrip(_DASHES).strip()


def clauses(cell: str) -> List[str]:
    """The cell's clauses, as printed (stripped, leading dashes and empty ones dropped)."""
    cell = cell or ""
    spans: List[List[int]] = []
    start = 0
    for m in _SPLIT.finditer(cell):
        head = cell[start:m.start()].strip()
        if _TITLE.fullmatch(head) or (m.group(0) == "," and _GLUED.fullmatch(head)):
            continue
        spans.append([start, m.start()])
        start = m.end()
    spans.append([start, len(cell)])
    out: List[List[int]] = []
    for a, b in spans:
        c = _clean(cell[a:b])
        if not c:
            continue
        if out and _SPECIES_LIST.fullmatch(c.strip(" .")):
            out[-1][1] = b
            continue
        out.append([a, b])
    return [_clean(cell[a:b]) for a, b in out]


@functools.lru_cache(maxsize=4)
def _load(path: str) -> Dict[str, dict]:
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if doc.get("format") != FORMAT:
        raise ValueError(f"{path}: not a {FORMAT} registry")
    return {e["template"]: e for e in doc["entries"]}


def registry(path: Optional[Path] = None) -> Dict[str, dict]:
    """template -> entry. Read once per path."""
    return _load(str(path or REGISTRY))


def _known(t: str, reg: Dict[str, dict]) -> Optional[dict]:
    """(T549 item 6) known = the owner ACCEPTED it. A `proposed` entry is the builder's guess: it must not clear the guard."""
    e = reg.get(t)
    return e if e and e.get("status") == "accepted" else None


def unknown(cell: str, reg: Optional[Dict[str, dict]] = None) -> List[str]:
    """The clauses of a cell DFO has not used before (as printed, once each)."""
    reg = registry() if reg is None else reg
    return list(dict.fromkeys(c for c in clauses(cell) if not _known(template(c), reg)))


def notes(cell: str, reg: Optional[Dict[str, dict]] = None) -> List[str]:
    """The clauses of a cell whose meaning is a note in DFO's words."""
    reg = registry() if reg is None else reg
    return [c for c in clauses(cell) if (_known(template(c), reg) or {}).get("kind") == "note"]


def reason(cell: str, reg: Optional[Dict[str, dict]] = None) -> str:
    """"" when every clause is known; else the rule's `review_reason`: "new wording: '…'; '…'"."""
    new = unknown(cell, reg)
    return f"{NEW_WORDING}{'; '.join(repr(c) for c in new)}" if new else ""


def row_words(rec: dict, reg: Optional[Dict[str, dict]] = None) -> Optional[dict]:
    """A scraped row the card must show in DFO's words, or None. `k`: "new" (a clause DFO never used before — the row is
    unparsed), "unread" (T320: a number no reading accounts for), "note" (a known clause whose meaning is DFO's words).
    `{k, sp, d, t}` (+ `new`: the unknown clauses) — species, dates and limits cells as printed."""
    from pipeline.regs.dfo_salmon.quota import unread_reason
    cell = (rec.get("limits_gear") or "").strip()
    out = {"sp": (rec.get("species") or "").strip(), "d": (rec.get("dates") or "").strip(), "t": cell}
    new = unknown(cell, reg)
    if new:
        return {"k": "new", **out, "new": new}
    if unread_reason(cell):
        return {"k": "unread", **out}
    if notes(cell, reg):
        return {"k": "note", **out}
    return None


# ── the builder ──────────────────────────────────────────────────────────────────────────────────────────────────────

def saved_pages(repo: Path = REPO_ROOT) -> List[Tuple[Path, str]]:
    """Every saved DFO region page (tracked fixtures, the cache's history and raw pages) with its slug; Region 5 is an
    index stub (5A and 5B are the pages)."""
    fix = repo / "pipeline/tests/fixtures/dfo_salmon"
    paths = sorted(glob.glob(str(fix / "region*.html")))
    paths += sorted(glob.glob(str(repo / "cache/dfo_salmon/history/region*_*.html")))
    paths += sorted(glob.glob(str(repo / "cache/dfo_salmon/raw/region*-eng.html")))
    out = []
    for p in paths:
        m = re.search(r"region(\w+?)(?:-eng|_\d+)\.html", Path(p).name)
        if m and m.group(1) != "5":
            out.append((Path(p), m.group(1)))
    return out


def saved_rows(pages: Iterable[Tuple[Path, str]]) -> Iterable[Tuple[str, object]]:
    from pipeline.regs.dfo_salmon.parse import parse_region
    for path, slug in pages:
        for r in parse_region(path.read_text(encoding="utf-8", errors="replace"), slug).rows:
            yield path.name, r


def corpus(pages: Iterable[Tuple[Path, str]]) -> Dict[str, dict]:
    """template -> {rows, pages, example (a clause), cell (a whole cell it sat in)} over the saved pages."""
    got: Dict[str, dict] = defaultdict(lambda: {"rows": 0, "pages": set(), "example": None, "cell": None})
    for name, r in saved_rows(pages):
        cell = (r.limits_gear or "").strip()
        for c in clauses(cell):
            d = got[template(c)]
            d["rows"] += 1
            d["pages"].add(name)
            if d["example"] is None:
                d["example"], d["cell"] = c, cell
    return got


#: what `typed` reads a clause as — the builder's GUESS for a new template's `reads` (the owner reviews it)
def _reads_of(clause: str) -> List[str]:
    from pipeline.regs.dfo_salmon import typed as T
    probes = (("daily", (T._RE_DAILY, T._RE_DAILY_OF)), ("hatchery", (T._RE_HATCHERY,)),
              ("over", (T._RE_ONLY_OVER, T._RE_WHICH_MAY, T._RE_OF_WHICH_N)), ("wild", (T._RE_UNMARKED,)),
              ("monthly", (T._RE_MONTHLY,)), ("max_cm", (T._RE_NONE_OVER, T._RE_MAX_SIZE)),
              ("hours", (T._RE_TIMES, T._RE_SOLAR_TIMES)), ("gap", (T._RE_GAP,)), ("hook", (T._RE_BARBLESS,)),
              ("fly", (T._RE_FLY,)), ("bait", (T._RE_BAIT_BAN,)), ("no_fishing", (T._RE_NO_FISHING,)),
              ("release", (T._RE_NON_RETENTION,)), ("tbd", (T._RE_TBD,)), ("notice", (T._RE_FN,)))
    return [name for name, rxs in probes if any(rx.search(clause) for rx in rxs)]


def _guess(t: str, example: str) -> Tuple[str, List[str]]:
    reads = _reads_of(example)
    if reads == ["notice"] or t in ("—", "fn#", "— fn#"):
        return "provenance", reads
    return ("structure" if reads else "note"), reads


def build(path: Path = REGISTRY, pages: Optional[Sequence[Tuple[Path, str]]] = None) -> dict:
    """The registry over the saved pages: existing entries keep their meaning and status, new ones are proposals."""
    old = {e["template"]: e for e in (json.loads(path.read_text(encoding="utf-8"))["entries"] if path.exists() else [])}
    got = corpus(saved_pages() if pages is None else pages)
    entries = []
    for t in sorted(set(old) | set(got), key=lambda t: (-got.get(t, {"rows": 0})["rows"], t)):
        seen = got.get(t)
        if t in old:
            e = dict(old[t])
        else:
            kind, reads = _guess(t, seen["example"])
            e = {"template": t, "kind": kind, "reads": reads, "status": "proposed"}
        if seen:
            e.update(rows=seen["rows"], pages=len(seen["pages"]), example=seen["example"], cell=seen["cell"])
        entries.append(e)
    return {"format": FORMAT,
            "about": "T321: every clause DFO has printed in a limits cell, numbers blanked, and what it means. A clause "
                     "not here is NEW WORDING: the row shows DFO's words, no number, and the owner is emailed. Built "
                     "by `python -m pipeline.regs.dfo_salmon.wording build`; every seeded entry is a proposal.",
            "entries": entries}


def write(doc: dict, path: Path = REGISTRY) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    head = {k: v for k, v in doc.items() if k != "entries"}
    lines = [json.dumps(e, ensure_ascii=False) for e in doc["entries"]]       # one entry a line: reviewable diffs
    body = json.dumps(head, indent=1, ensure_ascii=False)[:-2] + ',\n "entries": [\n  ' + ",\n  ".join(lines) + "\n ]\n}"
    path.write_text(body + "\n", encoding="utf-8")
    _load.cache_clear()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=("build", "check"))
    args = ap.parse_args(argv)
    if args.cmd == "build":
        doc = build()
        write(doc)
        new = sum(1 for e in doc["entries"] if e["status"] == "proposed")
        print(f"{len(doc['entries'])} templates -> {REGISTRY} ({new} proposed)")
        return 0
    bad = 0
    for name, r in saved_rows(saved_pages()):
        for c in unknown(r.limits_gear or ""):
            bad += 1
            print(f"{name}: {template(c)!r} in {r.limits_gear!r}")
    print(f"{bad} unknown clause(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

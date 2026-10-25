"""Curation review phase 3 (T134): what the review screen decides, each in one function.

    coverage     the strip: which words of the row no rule quotes — found by THE comparison
                 normaliser (`squash`), mapped back to the characters a reviewer reads
    tiers        PLAN §3's entry tiers (P1 Z P2 P3 P4 sweep) and the splits-to-verify test
    row boxes    the one-off helper's check that a box is the extraction's row (no credits)
    evidence     a zone passage found on its printed page by text; the Adam River row legible

Reads the curated catalogue and splits.json, and the repo PDF; no generated data except where a
test is marked. Whole values throughout; each key behaviour carries a mutation pin that names the
test the mutant fails.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.parsing.catalogue import clean_verbatim, squash
from pipeline.regs.review import coverage as COV
from pipeline.regs.review import tiers as T

REPO = Path(__file__).resolve().parents[2]


def _corpus() -> list[dict]:
    out = []
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        out += json.loads(p.read_text(encoding="utf-8"))["entries"]
    return out


# --------------------------------------------------------------------------- coverage

def test_the_strip_marks_what_no_rule_quotes():
    got = COV.coverage("**No Fishing** upstream of the falls, Dec 1-May 31; bait ban\nTrout quota = 2",
                       [("a.r1", "No Fishing upstream of the falls, Dec 1–May 31"),
                        ("a.r2", "Trout quota = 2"),
                        ("a.r3", "fly fishing only")])
    assert got == {
        "segments": [{"text": "No Fishing upstream of the falls, Dec 1-May 31", "kind": "quoted"},
                     {"text": "; ", "kind": "gap"},
                     {"text": "bait ban", "kind": "unquoted"},
                     {"text": "\n", "kind": "gap"},
                     {"text": "Trout quota = 2", "kind": "quoted"}],
        "unquoted_chars": 7,
        "missing": ["a.r3"]}


def test_the_adam_river_row_is_quoted_whole():
    adam = next(e for e in _corpus() if e["entry_id"] == "r1:adam_river_except_eve_river@1-10")
    assert COV.entry_coverage(adam) == {
        "segments": [{"text": "Artificial fly only upstream of Eve River, to Hwy 19 bridge",
                      "kind": "quoted"}],
        "unquoted_chars": 0, "missing": []}


def test_the_fold_is_squash_over_every_row_of_the_corpus():
    """The strip finds a quote exactly where the gate does: `_folded` IS `squash`, character for
    character, over every row's text — so the two cannot drift."""
    texts = [e.get("regs_verbatim") or "" for e in _corpus()]
    assert len(texts) > 1000
    assert COV.folded_matches_squash(texts) == []


def test_the_fold_check_is_pinned_by_mutation(monkeypatch):
    """MUTATION: a fold that forgets squash's lower-casing disagrees with squash on the corpus —
    `test_the_fold_is_squash_over_every_row_of_the_corpus` fails on it."""
    real = COV._folded

    def keeps_case(text):
        folded, idx = real(text)
        return "".join(text[i] if text[i].isalpha() else c for c, i in zip(folded, idx)), idx

    monkeypatch.setattr(COV, "_folded", keeps_case)
    assert COV.folded_matches_squash([e.get("regs_verbatim") or "" for e in _corpus()]) != []


def test_the_fold_maps_each_character_back():
    text = clean_verbatim("**Bait** ban —  single   barbless\n> hook")
    folded, idx = COV._folded(text)
    assert folded == squash(text) == "bait ban - single barbless hook"
    # each folded character comes from the character a reviewer reads in that place: the same
    # letter, the dash it normalised, or (a space) a run of white space or a line's marker
    assert all(f == text[i].lower() or (f == "-" and text[i] == "—")
               or (f == " " and (text[i].isspace() or text[i] == ">"))
               for f, i in zip(folded, idx))
    assert sorted(idx) == idx and len(idx) == len(folded)


# --------------------------------------------------------------------------- tiers

def _e(eid, rules=None, matched=("gnis:1",), **kw):
    return {"entry_id": eid, "matched": list(matched), "rules": rules or [], **kw}


CLOSE = {"rule_id": "x.r1", "type": "retention_limit", "take": 0, "may_target": 0,
         "extents": [{"op": "whole"}]}
QUOTA = {"rule_id": "x.r2", "type": "retention_limit", "take": 2, "extents": [{"op": "whole"}]}
CUT = {"op": "between", "splits": ["a", "b"]}


def test_each_tier_whole():
    got = {
        "zone": T.entry_tier(_e("z1:x", [QUOTA], matched=()), binds=False),
        "pointer": T.entry_tier(_e("r1:a@1-1", see=[{"entries": ["r1:b@1-1"]}]), binds=True),
        "closure + cut": T.entry_tier(_e("r1:a@1-1", [dict(CLOSE, extents=[CUT])]), binds=True),
        "closure, no match": T.entry_tier(_e("r1:a@1-1", [CLOSE], matched=()), binds=False),
        "plain closure": T.entry_tier(_e("r1:a@1-1", [CLOSE]), binds=True),
        "quota, two waters": T.entry_tier(_e("r1:a@1-1", [QUOTA], matched=("gnis:1", "gnis:2")), binds=True),
        "quota, flagged": T.entry_tier(_e("r1:a@1-1", [dict(QUOTA, review_reason="?")]), binds=True),
        "plain quota": T.entry_tier(_e("r1:a@1-1", [QUOTA]), binds=True),
    }
    assert got == {"zone": "Z", "pointer": "sweep", "closure + cut": "P1", "closure, no match": "P1",
                   "plain closure": "P2", "quota, two waters": "P3", "quota, flagged": "P3",
                   "plain quota": "P4"}
    # in `tiers.RISKS` order, most urgent first (T135: no match leads P1)
    assert T.risks(_e("r1:a@1-1", [dict(CLOSE, extents=[CUT], unresolved_locators=["x"])],
                      matched=("gnis:1", "gnis:2")), binds=False) == [
        "no match", "cut", "several waters", "flag"]


def test_a_closure_is_the_one_closure_predicate():
    """A closure with a condition ("partial") is a closure; a release you may still fish for is not
    — `rules.closure_grade`, never a second reading. (T135: the partial one's undrawn part is a
    risk factor, so it is P1, not P2 — PLAN §3's P1 starts with the closures with undrawn parts.)"""
    partial = dict(CLOSE, undrawn_part="near the dam")
    release = {"rule_id": "x.r3", "type": "retention_limit", "take": 0, "may_target": 1,
               "extents": [{"op": "whole"}]}
    assert [T.entry_tier(_e("r1:a@1-1", [r]), binds=True) for r in (partial, release)] == ["P1", "P4"]


def test_a_split_to_verify_is_an_open_auto_proposal_or_a_verify_note():
    got = [T.split_needs_verify({"_note": n}) for n in (
        "[auto-proposal L] Hwy 19 bridge …", "Verify: confirm the bridge", "[human-review] done",
        "[osm-candidate] (VERIFY). [human-review 2026-08-06]", "", None)]
    assert got == [True, True, False, False, False, False]
    assert [T.split_grade({"_note": n}) for n in ("[auto-proposal M] x", "y [auto-proposal L]")] \
        == ["M", None]


def test_the_corpus_tiers_match_plan_section_3():
    """PLAN §3 counted P1 227, Z 132, P2 261, P3 116, P4 730, sweep 59 and S0 165 (120 L) on
    2026-10-22. Measured here from the corpus alone (no match = an empty `matched`; the served
    build decides it in the tool): within the day's drift. The old S0 tier (T184: gone; its splits
    are checked by proxy on their entries' maps) is the 150 notes that still ask for a check — a
    rule binding through one carries the `split_to_verify` pre-check."""
    from collections import Counter
    got = Counter(T.entry_tier(e, binds=bool(e.get("matched"))) for e in _corpus())
    assert got["Z"] == 132
    assert sum(got.values()) == 1525
    for tier, plan in {"P1": 227, "P2": 261, "P3": 116, "P4": 730, "sweep": 59}.items():
        assert abs(got[tier] - plan) <= 3, (tier, got[tier], plan)
    splits = [s for wb in json.loads(CURATED.waters.splits.read_text())["waterbodies"]
              for s in wb["splits"]]
    held = [s for s in splits if T.split_needs_verify(s)]
    assert (len(splits), len(held), Counter(T.split_grade(s) for s in held)["L"]) == (409, 150, 119)


def _py_sources():
    for p in sorted((REPO / "pipeline").rglob("*.py")):
        yield p, p.read_text(encoding="utf-8")
    for p in sorted((REPO / "curation-review" / "backend").glob("*.py")):
        yield p, p.read_text(encoding="utf-8")


def test_no_second_copy_of_the_tier_the_grade_the_flag_the_strip_or_the_verdict():
    hits: dict[str, list] = {k: [] for k in (
        "def entry_tier(", "def split_grade(", "def split_needs_verify(", "def flagged(",
        r"\[auto-proposal ([A-Z])\]", "def coverage(", "def verdict_from_answers(", "def find_passage(",
        "def app_part(")}
    for p, text in _py_sources():
        if p.name.startswith("test_"):
            continue
        for k in hits:
            if k in text:
                hits[k].append(str(p.relative_to(REPO)))
    assert hits == {
        "def entry_tier(": ["pipeline/regs/review/tiers.py"],
        "def split_grade(": ["pipeline/regs/review/tiers.py"],
        "def split_needs_verify(": ["pipeline/regs/review/tiers.py"],
        "def flagged(": ["pipeline/regs/review/tiers.py"],
        r"\[auto-proposal ([A-Z])\]": ["pipeline/regs/review/tiers.py"],
        "def coverage(": ["pipeline/regs/review/coverage.py"],
        "def verdict_from_answers(": ["pipeline/regs/review/marks.py"],
        "def find_passage(": ["curation-review/backend/evidence.py"],
        "def app_part(": ["curation-review/backend/answer.py"]}


# --------------------------------------------------------------------------- the row boxes

def test_a_box_from_another_row_is_refused_and_glyph_order_is_only_reported():
    from pipeline.regs.extraction.row_boxes import check_against
    rows = [{"image": "page_016_row_000.png", "water": "ADAM RIVER (except Eve River)",
             "raw_regs": "Artificial fly only"},
            {"image": "page_016_row_001.png", "water": "AHNUHATI RIVER", "raw_regs": "Class II"},
            {"image": "page_018_row_021.png", "water": "HEBER RIVER", "raw_regs": "Fly fishing"},
            {"image": "page_099_row_000.png", "water": "GONE", "raw_regs": ""}]
    boxes = {"page_016_row_000.png": {"water": "ADAM RIVER (except Eve River)",
                                      "raw_regs": "Artificial fly only"},
             "page_016_row_001.png": {"water": "ALICE LAKE", "raw_regs": "Class II"},
             "page_018_row_021.png": {"water": "HEBER RIVER", "raw_regs": "Folpye fnishing"}}
    assert check_against(rows, boxes) == (
        ["page_016_row_001.png: the extractor now reads 'ALICE LAKE' here, the extraction "
         "recorded 'AHNUHATI RIVER'",
         "page_099_row_000.png: no box (the extractor no longer yields this row)"],
        ["page_018_row_021.png"])


@pytest.mark.slow
def test_the_helper_reads_the_adam_river_row_where_the_extractor_cropped_it():
    """The helper runs the extractor itself on page 16 (no credits; ~10 s): the Adam River row's box
    and column divider, in PDF points — the crop the extractor saved is this box at 400 dpi
    (2855 x 109 px; the box is kept to 0.01 pt, so a pixel either way)."""
    from pipeline.common.synopsis_pages import default_pdf
    from pipeline.regs.extraction.row_boxes import _page_boxes
    got = {b["image"]: b for b in _page_boxes((str(default_pdf()), 16))}
    adam = got["page_016_row_000.png"]
    assert {k: adam[k] for k in ("pdf_page", "printed", "box", "div_x", "page_size", "water")} == {
        "pdf_page": 16, "printed": 14, "box": [36.14, 86.88, 539.94, 96.48], "div_x": 173.16,
        "page_size": [576.0, 756.0], "water": "ADAM RIVER (except Eve River)"}
    x0, y0, x1, y1 = adam["box"]
    s = 400 / 72
    w, h = (x1 - x0 + 10) * s, (y1 - y0 + 10) * s
    assert abs(w - 2855) <= 1 and abs(h - 109) <= 1, (w, h)


# --------------------------------------------------------------------------- adversarial review

def test_the_strip_and_the_gate_agree_on_every_quote_of_the_corpus():
    """The strip calls a quote "in the row" exactly when the gate does (`CatalogueEntry`'s
    chain of custody: `squash(quote) in squash(regs_verbatim)` — the RAW row, while the strip
    shows `clean_verbatim` of it). Over every rule and licensing quote of the corpus."""
    disagree, n = [], 0
    for e in _corpus():
        hay = squash(e.get("regs_verbatim") or "")
        missing = set(COV.entry_coverage(e)["missing"])
        for r in (e.get("rules") or []) + (e.get("licensing") or []):
            q = squash(r.get("verbatim") or "")
            if not q:
                continue
            n += 1
            rid = str(r.get("rule_id") or r.get("id"))
            if (q in hay) == (rid in missing):
                disagree.append((e["entry_id"], rid))
    assert n > 3000 and disagree == [], disagree[:10]


def _render_in_threads(render, pages=(5, 9, 14, 20, 26, 31, 40, 47), each: int = 3) -> list[str]:
    """`render(pdf_page)` from len(pages) threads at once, `each` times; the errors raised."""
    import threading
    errs: list[str] = []

    def work(i):
        try:
            for k in range(each):
                render(pages[(i + k) % len(pages)])
        except Exception as ex:  # noqa: BLE001 — the failure is the finding
            errs.append(repr(ex))
    ts = [threading.Thread(target=work, args=(i,)) for i in range(len(pages))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return errs


@pytest.fixture
def backend_mods(monkeypatch):
    import sys
    monkeypatch.syspath_prepend(str(REPO / "curation-review" / "backend"))
    for m in ("evidence", "synopsis_pages"):
        sys.modules.pop(m, None)
    import evidence
    import synopsis_pages
    yield evidence, synopsis_pages
    for m in ("evidence", "synopsis_pages"):
        sys.modules.pop(m, None)


def test_pages_render_from_many_threads_at_once(backend_mods):
    """PDFIUM IS NOT THREAD-SAFE and FastAPI serves these routes from a thread pool: the screen asks
    for a crop and a page at once. Every render holds `synopsis_pages.PDFIUM_LOCK`."""
    evidence, pages = backend_mods
    assert _render_in_threads(lambda n: evidence._render.__wrapped__(n, 1.0)) == []
    assert _render_in_threads(lambda n: pages.page_png.__wrapped__(n + 2, 1.0)) == []


#: The mutation, run in a CHILD process: unlocked concurrent renders corrupt pdfium's process-global state, and every
#: later `PdfDocument` in the same process then fails with "PDFium: Data format error" (T63: this test, run in-process,
#: was the leak behind test_synopsis_pages' 4 order-dependent errors in the full suite).
_MUTATION = """
import contextlib, sys
sys.path.insert(0, sys.argv[1])
sys.path.insert(0, sys.argv[2])
import evidence, synopsis_pages
from pipeline.tests.test_review_screen import _render_in_threads
synopsis_pages.PDFIUM_LOCK = contextlib.nullcontext()
print(len(_render_in_threads(lambda n: evidence._render.__wrapped__(n, 1.0))))
"""


def test_the_render_lock_is_pinned_by_mutation():
    """MUTATION: no lock (`PDFIUM_LOCK` a no-op) — renders in flight together fail, and
    `test_pages_render_from_many_threads_at_once` fails on it. In a child process (see `_MUTATION`)."""
    import subprocess
    import sys
    got = subprocess.run([sys.executable, "-c", _MUTATION, str(REPO / "curation-review" / "backend"), str(REPO)],
                         capture_output=True, text=True, timeout=600, cwd=REPO)
    if got.returncode < 0:     # killed by a signal: unlocked pdfium can crash the process outright — a failure too
        return
    assert got.returncode == 0, got.stderr[-2000:]
    assert int(got.stdout.strip().splitlines()[-1]) > 0, got.stdout


def test_a_split_whose_old_mark_went_stale_is_to_verify_again():
    """A confirmed split point whose anchor or kind changed since is to confirm again: its old mark
    is stale, and the pre-check names it whatever its note says."""
    plain = {"_note": "[human-review 2026-08-06] placed on the bridge"}
    assert [T.split_needs_verify(plain, s) for s in (None, "unmarked", "verified", "stale")] == [
        False, False, False, True]


def _node_review(expr: str):
    """Evaluate `expr` against the frontend's `review.ts` in node (type stripping, no build)."""
    import shutil
    import subprocess
    node = shutil.which("node")
    assert node, "node is needed (the app and the review frontend need it too): install Node >= 22.6"
    src = REPO / "curation-review" / "frontend" / "src" / "review.ts"
    code = f"const R = await import({json.dumps(src.as_uri())}); console.log(JSON.stringify({expr}));"
    got = subprocess.run([node, "--input-type=module", "-e", code], capture_output=True,
                         text=True, timeout=60)
    assert got.returncode == 0, got.stderr[-2000:]
    return json.loads(got.stdout.strip().splitlines()[-1])


def test_a_zone_entry_moves_on_only_when_every_rule_is_marked():
    """Enter on a zone rule opens the next OPEN rule — after it, then wrapping to one before it; the
    entry is done (-1, the screen moves on) only when none is open. It used to look only AFTER the
    rule: marking the last rule first left the earlier ones unmarked and moved to the next entry."""
    got = _node_review(
        "[['r1','r2','r3'], ['r2'], ['r1','r3'], []].map((open) =>"
        " [0, 1, 2].map((at) => R.nextOpenRule(['r1','r2','r3'], at, (k) => open.includes(k))))")
    assert got == [[1, 2, 0],       # all open: the next one, the last wraps to the first
                   [1, -1, 1],      # only r2 open: from r1 or r3 it is r2; marking r2 ends it
                   [2, 2, 0],       # r1 and r3 open: from r3 it wraps back to r1
                   [-1, -1, -1]]    # none open: done

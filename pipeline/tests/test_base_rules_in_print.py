"""THE BASE RULES ARE PINNED TO THE PRINTED BOOK (T553 item 1).

The zone and provincial entries (`z…`, 122 of them) are hand-authored, and their only outside check was
`test_catalogue_entries.py` against `data/curated/regulations/reference/*.md` — typed by hand too. Nothing compared either
with the PDF except the p.80 definitions. Here every NUMBER and DATE an entry's `regs_verbatim` prints, and every number
with the word printed before it ("trout 4", "over 50", "kokanee 10"), must be printed on the entry's own `source_pages`
of the repo PDF, read deterministically by `pipeline.common.synopsis_print` (no LLM). The same for every reference
transcription against the pages it transcribes. A rule's own numbers are tied to its quote by the number guard
(`validate_catalogue`), and its quote to `regs_verbatim` by the model, so the chain now ends at print.

A figure the book does not print is never fixed here: it is an owner question (data vs print, printed page, the line),
listed in `DISAGREE` until the owner answers. `test_every_disagreement_still_holds` refuses a stale one.

Pages are PRINTED numbers (the repo PDF is PDF - 2 to p.40, - 6 after; `synopsis_pages`).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from pipeline.tests import under_test
from pipeline.common.synopsis_print import figures, unprinted

#: The pages each transcription transcribes (PRINTED). region-5's pelican alert is on the map page (41).
REFERENCE_PAGES = {
    "region-1": (13,), "region-2": (21,), "region-3": (28,), "region-4": (34,), "region-5": (41, 42),
    "region-6": (49,), "region-7a": (58,), "region-7b": (64,), "region-8": (68,), "definitions": (80,),
    "licensing": (5, 6, 7),
    # pp. 8-10 and the box text read off pp. 5-6; the Haida Gwaii preamble (p.13) and Region 3's worked example (p.28)
    "provincial-regulations": (5, 6, 8, 9, 10, 13, 28),
}

#: Figures the data holds that the printed page does not print in the same words — OWNER QUESTIONS (T553), never
#: fixed by an agent. Key: (entry id or reference file, figure as `unprinted` names it) -> (printed page, what the data
#: says, what the book prints). None changes a regulation value; each is a wording or provenance question.
DISAGREE = {
    ("z7b:possession_exceptions", "pair: exception 1"): (
        64, "Exception: 1 daily quota for Arctic grayling, bull trout and lake trout.",
        "Exception: possession quota = 1 daily quota for Arctic grayling, 1 daily quota for bull trout, and 1 daily "
        "quota for lake trout."),
    ("region-7b", "pair: exception 1"): (
        64, "Exception: 1 daily quota for Arctic grayling, bull trout and lake trout.",
        "Exception: possession quota = 1 daily quota for Arctic grayling, 1 daily quota for bull trout, and 1 daily "
        "quota for lake trout."),
    ("zp:no_fishing_buffers", "number: 3"): (
        9, "3. Within 23 m downstream of the lower entrance to any fishway, …",
        "• Within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap; (a bullet, no 3.)"),
    ("zp:no_fishing_buffers", "pair: exceptions 3"): (
        9, "There are five basic exceptions: 3. Within 23 m …", "There are five basic exceptions: … • Within 23 m …"),
    ("zp:no_fishing_buffers", "number: 4"): (
        9, "4. Within a 100 m radius of any government facility …",
        "• Within a 100 m radius of any government facility … (a bullet, no 4.)"),
    ("zp:no_fishing_buffers", "pair: leap 4"): (
        9, "… obstacle or leap. 4. Within a 100 m radius …", "… obstacle or leap; • Within a 100 m radius …"),
    ("licensing", "number: 2026"): (
        6, "Fishing Licensing in WILD: … as of the 2026-27 licence year, which starts April 1, 2026.",
        "(not printed: no WILD / FWID paragraph on pp. 5-7 of the repo PDF — likely a later gov.bc.ca printing)"),
    ("licensing", "number: 27"): (
        6, "… as of the 2026-27 licence year …", "(not printed in the repo PDF; see the 2026 line)"),
    ("provincial-regulations", "number: 26"): (
        13, "This supersedes the previous Region 6 administration. … currently all 26 Region 6 zone rules bind to "
            "Haida Gwaii's 25,772 sections …",
        "(our commentary, inside the transcription half of the file; the book prints only the preamble)"),
    ("provincial-regulations", "number: 772"): (
        13, "… Haida Gwaii's 25,772 sections …", "(our commentary; see the 26 line)"),
}


def _zone_entries():
    for p in sorted(under_test.curated().regulations.synopsis.glob("region-*.json")):
        for e in json.loads(p.read_text(encoding="utf-8"))["entries"]:
            if e["entry_id"].startswith("z"):
                yield e


def _printed_part(text: str) -> str:
    """A reference file's transcription: below the preamble's `---`, above the audit, without our headings, code
    spans and "(p.N):" labels."""
    from pipeline.tests.test_catalogue_entries import _transcription_only
    t = _transcription_only(text)
    t = t.split("\n---\n", 1)[1] if "\n---\n" in t else t
    t = re.sub(r"`[^`\n]*`", "", t)
    keep = [ln for ln in t.splitlines()
            if not ln.lstrip().startswith("#") and ln.strip() != "---"
            and not re.search(r"\(p\.[\d, ]+\):\s*$", ln)]
    return "\n".join(keep)


def _found_zone() -> dict:
    return {(e["entry_id"], f): e["source_pages"]
            for e in _zone_entries() for f in unprinted(e["regs_verbatim"], e["source_pages"])}


def _found_reference() -> dict:
    ref = under_test.curated().regulations.reference
    out = {}
    for name, pages in REFERENCE_PAGES.items():
        for f in unprinted(_printed_part((ref / f"{name}.md").read_text(encoding="utf-8")), pages):
            out[(name, f)] = pages
    return out


def test_every_zone_entry_names_its_printed_pages():
    missing = [e["entry_id"] for e in _zone_entries() if not e.get("source_pages")]
    assert not missing, f"zone entries with no source_pages cannot be checked against print: {missing}"


def test_every_base_rule_figure_is_printed_on_its_page():
    """Every number, date range and word-number pair of every zone/provincial `regs_verbatim` is on its
    `source_pages`, except the owner questions in `DISAGREE`."""
    new = {k: v for k, v in _found_zone().items() if k not in DISAGREE}
    assert not new, ("figures a zone entry stores that its printed page does not print — ask the owner (data vs "
                     "print, page, line); never edit the value to pass:\n  "
                     + "\n  ".join(f"{e} p.{pg}: {f}" for (e, f), pg in sorted(new.items())))


def test_every_reference_transcription_figure_is_printed():
    """The hand-typed transcriptions the quote tests rely on: every figure on the pages they transcribe."""
    new = {k: v for k, v in _found_reference().items() if k not in DISAGREE}
    assert not new, ("figures a reference transcription holds that its pages do not print:\n  "
                     + "\n  ".join(f"{n}.md p.{pg}: {f}" for (n, f), pg in sorted(new.items())))


def test_every_disagreement_still_holds():
    """A `DISAGREE` line the data no longer shows is stale: the owner answered and the data moved. Remove it."""
    found = set(_found_zone()) | set(_found_reference())
    stale = sorted(k for k in DISAGREE if k not in found)
    assert not stale, f"stale DISAGREE lines (no longer found; remove them): {stale}"


def test_a_changed_quota_size_or_date_is_caught():
    """Mutation: the Region 1 quota table with one figure changed at a time. Each change is a figure p.13 does not
    print (a 6 is elsewhere on the page — the word-number pair is what catches it)."""
    e = next(x for x in _zone_entries() if x["entry_id"] == "z1:daily_quotas")
    text, pages = e["regs_verbatim"], e["source_pages"]
    assert pages == [13] and "Trout: 4" in text and "1 over 50 cm" in text
    assert unprinted(text, pages) == []
    assert "pair: trout 6" in unprinted(text.replace("Trout: 4", "Trout: 6", 1), pages)
    assert "pair: over 55" in unprinted(text.replace("1 over 50 cm", "1 over 55 cm", 1), pages)
    closure = next(x for x in _zone_entries() if x["entry_id"].startswith("z1:") and "July 15" in x["regs_verbatim"])
    assert unprinted(closure["regs_verbatim"].replace("July 15", "July 16"), closure["source_pages"])


def test_figures_reads_numbers_dates_and_pairs():
    """The reader's own contract, no PDF: what counts as a figure."""
    f = figures("Trout: 4, but not more than 1 over 50 cm. No Fishing July 15 – Aug 31; Sept 15-Oct 31.\n\n"
                "Call (250) 751-7220 or see www.gov.bc.ca/fish-2")
    assert f.dates == {"jul 15-aug 31", "sep 15-oct 31"}
    assert f.numbers == {"4", "1", "50", "15", "31"}
    assert {"trout 4", "than 1", "over 50"} <= f.pairs
    assert not any("751" in x or "7220" in x or "2" == x for x in f.numbers | f.pairs)
    assert "4 1" not in figures("Trout: 4\n\n1 over 50 cm").pairs        # a pair never spans a blank line

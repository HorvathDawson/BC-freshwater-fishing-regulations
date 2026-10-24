"""THE FOUR ANSWERS THE APP KEYED AS EXCLUSIONS on the REAL waters, against the GOLDEN ORACLE: the answers build (this
code, the bundle on this machine) for the waters the 134 golden states name, read at each state's own key and segment
(taps.json), against what the PROTOTYPE drew (app/fixtures/golden/golden.json — never this code's output).
The no-data twin is `test_answers_pipeline_needs.py`; the app's parity (licence, keep, sheets) proves them end to end on
a release built from this code.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import licence as L
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")
GOLDEN = Path(__file__).resolve().parents[2] / "app" / "fixtures" / "golden"
GOLD = json.loads((GOLDEN / "golden.json").read_text())
G = GOLD["states"]
TAPS = json.loads((GOLDEN / "taps.json").read_text())["states"]
IDS = sorted(TAPS)
STAMP = "Steelhead Conservation Surcharge Stamp"
#: page v36 TROUTISH: the rows the Keep tab draws in full; every other row is an Other fish row (oracle side only)
TROUTISH = {"RB", "CT", "EB", "GB", "LT", "DV", "ST", "BT", "CT_ws", "CT_c"}


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    items = sorted({TAPS[i]["item"] for i in IDS})
    return A.build(BUNDLE, EXPORT_DIR, workers=1, items=items, export=(data, guide), log=lambda *_: None)


def _at(m, sid, section):
    tap = TAPS[sid]
    k = m.parts[tap["item"]][tap["partIx"]]
    assert m.segments[k] == tap["starts"]                       # the oracle's own key year
    return m.sections[section][k][tap["s"]]


def _golden_licence(sid, who):
    return (G[sid].get("licence") or {}).get(who) or {}


# 1. the steelhead stamp ------------------------------------------------------------------------------------------
@pytest.mark.needs_bundle
def test_1_the_stamp_is_listed_exactly_where_the_prototype_listed_it(built):
    adult = L.profile_index({"residency": "resident", "age": "16_plus", "guidance": "non_guided", "status": "none"})
    drawn, stored = [], []
    for sid in IDS:
        g = _golden_licence(sid, "adult|resident")
        lic = _at(built, sid, "licence")
        if not g.get("blocks") or "tidal" in lic["holds"]:
            continue                                            # a closed day / tidal water draws no list
        if any(l.startswith(STAMP) for b in g["blocks"] if b["cls"] == "needs" for l in b["lines"]):
            drawn.append(sid)
        if any(d["doc"] == "steelhead_stamp" for d in lic["profiles"][adult].get("documents") or []):
            stored.append(sid)
    assert stored == drawn
    # the seven the prototype dropped at render time are among the waters the stamp is otherwise needed on
    assert {"bc1_501", "bc0_501", "bc1_1010", "bc0_1010", "ape_501", "ape_1010", "x15p2_1010"}.isdisjoint(stored)


# 2. the youth's alternatives -------------------------------------------------------------------------------------
@pytest.mark.needs_bundle
def test_2_the_youth_box_names_the_documents_the_prototype_named(built):
    seen = 0
    for sid in IDS:
        for res in ("non_resident", "non_resident_alien"):
            g = _golden_licence(sid, f"u16|{res}")
            box = next((b["lines"][0] for b in g.get("blocks") or [] if b["cls"] == "classbox youth"), None)
            if box is None:
                continue
            want = re.search(r"holds what this fishing needs \((.*?)\)\. ", box).group(1)
            u = next(x for x in _at(built, sid, "licence")["under_16"] if x["residency"] == res)
            got = ", ".join(" or ".join([b["name"], *(" + ".join(o["names"]) for o in b.get("or") or [])])
                            for b in u["buy"])
            assert got == want, (sid, res)
            seen += 1
    assert seen > 50


# 3. the page map -------------------------------------------------------------------------------------------------
@pytest.mark.needs_bundle
def test_3_every_glossary_and_hatch_page_the_prototype_linked_is_the_export_guides_map(built):
    _, guide = C.load_export(EXPORT_DIR)
    m = guide["guide"]["pdf_pages"]
    pages = set()
    for sheets in [GOLD["sheets"], *(s["sheets"] for s in G.values())]:
        for k, v in sheets.items():
            if (k.startswith("gl:") or k == "hatch") and v != "@":
                pages |= {(int(re.match(r"p\.(\d+)", p["text"]).group(1)), p["pdf"]) for p in v["pages"]}
    assert len(pages) == 7                     # measured: printed 4–8 and 13 (PDF − 2), 80 (PDF − 6, after the centre pages)
    assert sorted((p, m[str(p)]) for p, _ in pages) == sorted(pages)


# 4. Other fish rows' stamps --------------------------------------------------------------------------------------
@pytest.mark.needs_bundle
def test_4_other_fish_rows_carry_the_stamp_lines_the_prototype_drew(built):
    line = lambda x: f"You need {x['name']} › {x['when'][:1].lower()}{x['when'][1:]}"
    hit = []
    for sid in IDS:
        k = G[sid].get("keep") or {}
        if not k.get("otherFish"):
            continue
        inside = {l for gr in k.get("groups") or [] for l in gr.get("lines") or []} \
            | {l for p in k.get("plainRows") or [] for l in p}
        want = [l for l in k.get("lines") or [] if l.startswith("You need ") and l not in inside]
        rows = _at(built, sid, "rows")["rows"]
        got = [line(x) for r in rows if not TROUTISH & set(r["all_members"] or r["members"]) for x in r.get("stamps") or []]
        assert got == want, sid
        hit += [sid] if want else []
    assert len(hit) == 13                                       # + klmb's page-v36 keep line (D19) = the 14 states

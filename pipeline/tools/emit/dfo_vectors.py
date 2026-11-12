"""Emit THE DFO PARITY VECTORS: what the Python reads on the saved DFO pages, for the feeds worker's test to reproduce.

    python -m pipeline.tools.emit.dfo_vectors          # write them
    python -m pipeline.tools.emit.dfo_vectors --check  # fail if the committed file is stale (CI, fast-fail)

WHY THIS EXISTS (T556). The DFO page parser, the location fingerprint, the name key and the row typing exist twice: the
Python (`pipeline/regs/dfo_salmon/` parse / untangle / locations / typed, `pipeline/common/names.py`) mints the
fingerprints the curated entries carry and types the release's rows; the feeds worker (`app/workers/feeds/src/dfo/*.ts`)
re-derives both live. The byte-parity runners went with T350 / T431 (the worker's feed is no longer meant to equal
`live.py`'s byte for byte), which left NO pin between the two copies of the rule: a synonym added on one side re-keys
every place on that side only, and the same page reads two ways. The owner's rule (2026-11-02): the cascade is resolved
in Python and TypeScript only reads results; where a TS copy of a rule must exist (the worker types DFO rows live), ONE
SHARED COMMITTED FIXTURE is the pin.

WHAT IS IN THE FILE. For each saved page (`app/workers/feeds/test/fixtures/dfo/pages/`, the pages the worker's tests
already run on): every location's fingerprint with the texts it was made from, and every table locator's rows, typed
rules and year calendar (`live.type_page`, `live.location_calendar`). Beside them: `fingerprint` / `normalize` /
`landmarks` cases (one per synonym, with the drift the fingerprint must survive), `names.key` cases, and typed rules
for the hand rows of `dfo-rows.json` (the shared rows of T671's two tests). (T676) Per page also: the curated entry file
(`entries/region-<slug>.json`) joined to it (`by_fingerprint`, `reconcile`), and the region header (`header.parse_header`,
`digest`, `resolve`, `deliver.salmon.header_answers`, `quota.header_closure`); `joins` holds hand cases of the join (a
page default, never dormant); `dates` is the robust date reader (`dates.read_dates`) on every recorded cell, the named
shapes and a seeded fuzz.

WHO READS IT. `app/workers/feeds/test/dfo.vectors.test.ts` (the worker must reproduce every value in its
Python-parity mode, and in robust mode — what production runs — everything but the differences that test lists by
name) and `pipeline/tests/test_dfo_vectors.py` (`--check`, so a Python-side change cannot land without the file, and
the file cannot land without the worker agreeing). It depends on the curated wording registry (a clause's reading):
a registry edit that changes a saved page's typing makes `--check` fail until this is re-run.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import asdict
from pathlib import Path

from pipeline.common import names
from pipeline.common.curated import REPO_ROOT
from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import dates as D
from pipeline.regs.dfo_salmon import entries as ENT
from pipeline.regs.dfo_salmon import feed as FD
from pipeline.regs.dfo_salmon import header as H
from pipeline.regs.dfo_salmon import live as L
from pipeline.regs.dfo_salmon import locations as LOC
from pipeline.regs.dfo_salmon import quota as Q
from pipeline.regs.dfo_salmon import typed as T

FORMAT = "dfo-parity-vectors/1"
FIXTURES = REPO_ROOT / "app" / "workers" / "feeds" / "test" / "fixtures" / "dfo"
PAGES = FIXTURES / "pages"
#: the hand rows both languages' row tests read (T671's twin tests): `{cases: [{name, rows: [[species, dates, limits]]}]}`
ROWS = FIXTURES / "dfo-rows.json"
#: (T676) the curated entry files the worker's tests read (`entries/region-<slug>.json`), joined to each saved page
ENTRIES = FIXTURES / "entries"
#: (T676) every date cell DFO has printed in the recorded corpus (the worker's dates test reads it too)
DATE_CELLS = FIXTURES / "date-cells.json"
OUT = REPO_ROOT / "app" / "workers" / "feeds" / "test" / "fixtures" / "dfo" / "python-vectors.generated.json"

_BY = "python -m pipeline.tools.emit.dfo_vectors"

#: (section, water, specific area): each synonym of `locations._SYNONYMS`, the typography the fingerprint folds, and
#: what it must keep apart (a number, a different landmark)
TEXT_CASES = [
    (None, "", ""),
    ("A", "Fraser River", "Downstream of the Hwy. 1 bridge"),
    ("A", "Fraser River", "downstream of the Highway 1 Bridge."),
    ("A", "Fraser River", "Downstream of the Hwy 2 bridge"),
    ("B", "Fraser River", "Downstream of the Hwy. 1 bridge"),
    (None, "Chilliwack R.", "From the fishing boundary sign approx. 100 metres below Slesse Ck. to Tamihi Rd"),
    (None, "Chilliwack River", "from the fishing boundary sign approximately 100 m below Slesse Creek to Tamihi Road"),
    (None, "Stamp R. & Somass R.", "2 kilometres above the #4 marker — tidal waters"),
    (None, "Stamp River and Somass River", "2 km above the 4 marker – tidal waters"),
    (None, "Kitsumkalum River", "Three signs"),
    (None, "Kitsumkalum River", "4 signs"),
    (None, "Cr. Lake", "the Cr mouth, 1 kilometer upstream; 5 meters past the Rd. end"),
    (None, "O’Connor’s Slough", "It's “the” slough"),
    (None, "R.V. Creek", "r. mile 2"),
]

#: names as a curated entry or a DFO page might print them: `names.key` on both sides
NAME_CASES = [
    "", "Fraser River", "  fraser   RIVER ", "O'Connor Lake", "O’Connor Lake", "Oʼ Connor-Lake", "Lac des Roches",
    "Lac-des-Roches", "Rivière à l’Eau Claire", "St. Mary's River", "Mary_Ann Creek", "Hwy #1 (east)", "Čeština",
    "Kwadacha/Finlay", "Müller Straße", "Σίσυφος", "Nass River (upper)", "ʔEsdilagh", "Tŝilhqox Biny", "K’ómoks",
    "X̱wemelch'stn", "Skwxwú7mesh", "100 Mile House", "Lake No. 2", "a—b–c",
]


#: (T676) date cells beyond the recorded corpus: the shapes the release's reader missed, the refusals, the edges
DATE_SHAPES = [
    "Apr 1 until further notice", "July 1 until futher notice", "May 8 2018 until further notice", "Until further notice",
    "from Oct 1 until further notice", "Effective October 15, until further notice", "Aug 2 To Aug 31",
    "July 1 until Aug 8", "October 1 to December 31", "October 1 to \u2026", "1 Oct to 31 Dec", "Oct 1st to Dec 31st",
    "Sept. 15 through Oct 30", "Oct 1 to Dec 31, 2025", "June 15 to July 14 2017", "Nov 01- to Dec 31", "Sep 21-Dec 31",
    "Jan 01\u2013Dec 31", "Oct 1 - 15", "Nov 30 to Jan 15", "Sep 15 to Jan 31", "Aprl 1 to Jun 15", "Ocotber 1 to Dec 31",
    "Setpember 1 to Spet 30", "Jul 1 to Aug 31, Oct 1 to Dec 31", "Jul 1 to Aug 31 and Oct 1 until further notice",
    "August", "Aug 15", "Feb 29 to Mar 4", "*Apr 1 to Mar 31*", "To be determined", "TBD", "", "   ",
    "Jun 31 to Jul 4", "Feb 30 to Mar 4", "see notice", "Weekends in August", "Mar to", "Jum 1 to Jul 4", "13 to 15",
    "Oct\u00a01\u2002to Dec\u202f31", "Oct 1\u2028to Dec 31", "Oct 1 till Dec 31", "Oct 1 up to Dec 31", "Oct - 15",
    "Oct 1 to Nov", "ufn", "Apr 1 UFN", "Agu 3 to Spet 4", "Mat 1 to Jun 2", "Oct 1 to Dec 31 or", "on Oct 1",
    "Oct 1 & Nov 2", "Oct 1; Nov 2 to Nov 9", "Octob 3", "\ufeffOct 1 to Dec 31\ufeff", "Oct 1 to Dec 31.",
]
#: (T676) the seeded fuzz's parts: month spellings (whole, cut, dotted, misspelled, wrong), days (some impossible), the
#: separators, leads and tails the reader knows, and noise it must refuse
_FUZZ = {
    "month": ["Jan", "January", "Feb", "Febr", "Mar", "March", "Apr", "Aprl", "Apr.", "May", "Jun", "June", "Jul",
              "July", "Aug", "Agu", "August", "Sep", "Sept", "Sept.", "Spet", "September", "Setpember", "Oct",
              "Ocotber", "October", "Nov", "Nov.", "November", "Dec", "December", "Decmber", "Smarch", "Jum", "Mat"],
    "day": ["0", "1", "01", "1st", "2nd", "3rd", "4th", "9", "15", "28", "29", "30", "31", "32", "99"],
    "sep": [" to ", " To ", "-", " - ", "\u2013", " \u2014 ", " through ", " thru ", " until ", " till ", " up to ",
            "- to ", " to\u00a0"],
    "lead": ["", "", "", "from ", "Effective ", "starting ", "as of ", "opens ", "on "],
    "tail": ["", "", "", " until further notice", " until futher notification", " UFN", ", 2024", " 2019", ".", " or",
             " and", "*"],
    "noise": ["see notice", "Weekends", "TBD", "to be determined", "&", ";", ",", "and", "??", "\u2028", "\ufeff"],
}


def _fuzz_cells(n: int = 1500, seed: int = 676) -> list:
    """Deterministic rewritings of date cells (the same seed every run, so the file is stable)."""
    rnd = random.Random(seed)
    pick = lambda k: rnd.choice(_FUZZ[k])                                   # noqa: E731

    def point() -> str:
        m, d = pick("month"), pick("day")
        return rnd.choice([f"{m} {d}", f"{d} {m}", m, d, f"{m}{d}"])

    def one() -> str:
        return point() if rnd.random() < 0.2 else f"{point()}{pick('sep')}{point()}"

    out = []
    for _ in range(n):
        body = one() if rnd.random() < 0.75 else rnd.choice([", ", "; ", " and ", " & "]).join(one() for _ in range(2))
        if rnd.random() < 0.1:
            body = f"{body} {pick('noise')}" if rnd.random() < 0.5 else f"{pick('noise')} {body}"
        out.append(f"{pick('lead')}{body}{pick('tail')}")
    return out


def _date_cases() -> list:
    corpus = json.loads(DATE_CELLS.read_text(encoding="utf-8"))["cells"]
    seen, out = set(), []
    for cell in [*corpus, *DATE_SHAPES, *_fuzz_cells()]:
        if cell not in seen:
            seen.add(cell)
            out.append([cell, D.read_dates(cell).to_json()])
    return out


#: (T676) a curated page default (the page's own sentence, never a table row): `reconcile` must never call it dormant
_PAGE_DEFAULT_CASE = {"location_id": "8:page-default", "region": "8", "region_number": 8, "kind": ENT.PAGE_DEFAULT,
                      "precedence": ENT.PAGE_DEFAULT_PRECEDENCE, "water": "", "fingerprints": ["0000000000000000"],
                      "source_text": {}, "status": "active"}


def _entry_dict(slug: str) -> "dict | None":
    """The fixture entry file as the worker reads it: its `scopes` left out — the worker reads none, and the join (and
    the header's names) never consult them (the region 6 fixture predates the residual rule `entries.load` enforces)."""
    p = ENTRIES / f"region-{slug}.json"
    return {k: v for k, v in json.loads(p.read_text(encoding="utf-8")).items() if k != "scopes"} if p.exists() else None


def _entry_file(slug: str, d: "dict | None" = None):
    d = d if d is not None else _entry_dict(slug)
    return ENT._from_dict(d) if d is not None else ENT.load(slug, ENTRIES)


def _join(ef, locs, structure) -> dict:
    """The curated entry file joined to a page: `by_fingerprint` (fingerprint -> location_id) and `reconcile`."""
    return {"by_fingerprint": {fp: loc.location_id for fp, loc in sorted(ef.by_fingerprint().items())},
            "reconcile": asdict(ENT.reconcile(ef, locs, structure))}


def _slug(path: Path) -> str:
    return path.name[len("region"):-len("-eng.html")]


def _calendar(rules: list) -> dict:
    """One locator's year on its own (`live.location_calendar`), with the verdicts and gears it indexes."""
    verdicts, gears = S._Interner(), S._Interner()
    cal = L.location_calendar(rules, verdicts, gears)
    return {**cal, "verdicts": verdicts.items, "gears": gears.items}


def _dump(rules) -> list:
    return [r.model_dump(mode="json", exclude_defaults=True, by_alias=True) for r in rules]


def build() -> dict:
    pages = {}
    waters: set = set()
    files = sorted(PAGES.glob("region*-eng.html"), key=_slug)
    if not files:
        raise SystemExit(f"{_BY}: no saved page under {PAGES}")
    joins = {}
    for path in files:
        slug = _slug(path)
        html = path.read_text(encoding="utf-8")
        typed, locs, structure = L.type_page(html, slug)
        tables = {loc.fingerprint for loc in locs}
        for loc in locs:
            waters.update([loc.water, *loc.aliases])
        ef = _entry_file(slug)
        notes = H.parse_header(html, slug)
        scopes, unresolved = H.resolve(notes, ef)
        if slug == "8":
            # the page-default case rides on region 8's file: the input itself is in the vectors (the worker loads it)
            raw = _entry_dict(slug)
            d = {**raw, "locations": [*raw["locations"], _PAGE_DEFAULT_CASE]}
            joins["page_default"] = {"slug": slug, "entries": d, **_join(_entry_file(slug, d), locs, structure)}
        pages[slug] = {
            # (T676) the curated entries joined to the page, and the region header: notes, digest, names -> items,
            # what the notes resolve to (`deliver.salmon.header_answers`: bucket / shared / sizes / hours / folds) and
            # the header closure (`quota.header_closure`)
            "entries": _join(ef, locs, structure),
            "header": {"notes": notes, "digest": H.digest(notes), "scopes": scopes, "unresolved": unresolved,
                       "answers": S.header_answers(notes), "closure": Q.header_closure(notes, slug)},
            "structure": structure,
            "locations": [{"fingerprint": loc.fingerprint, "section": loc.section, "water": loc.water,
                           "specific_area": loc.specific_area, "landmarks": loc.landmarks} for loc in locs],
            # the table's locators (a page-default sentence is `cascade.statement_locators`' — Python only)
            "locators": [{"fingerprint": l["fingerprint"], "rows": l["rows"], "rules": l["rules"],
                          **({"words": l["words"]} if "words" in l else {}), "calendar": _calendar(l["rules"])}
                         for l in typed["locators"] if l["fingerprint"] in tables],
        }
    cases = json.loads(ROWS.read_text(encoding="utf-8"))["cases"]
    rows = {}
    for case in cases:
        if "robust" in case:
            # (T679 D6) rows that do not type: the robust reader's place (`feed.type_rows`) and the error's type name
            recs = [{"fingerprint": "rows" * 4, "species": sp, "dates": d, "limits_gear": lg} for sp, d, lg in case["rows"]]
            rules, why = FD.type_rows("rows" * 4, recs)
            rows[case["name"]] = {"error": why.split(":")[0], "rules": rules, "calendar": _calendar(rules)}
            continue
        rules = [r for i, (species, dates, limits) in enumerate(case["rows"])
                 for r in _dump(T.to_rules({"species": species, "dates": dates, "limits_gear": limits}, "rows", i))]
        rows[case["name"]] = {"rules": rules, "calendar": _calendar(rules)}
    names_in = sorted({*NAME_CASES, *(w for w in waters if w)})
    return {
        "format": FORMAT,
        "_about": f"GENERATED by `{_BY}` from the saved DFO pages beside this file and dfo-rows.json: what the PYTHON "
                  "reads. The feeds worker's dfo.vectors.test.ts must reproduce it (T556). Do not edit.",
        "typed_format": T.FORMAT,
        "species": list(S.SPECIES),
        "text": [{"section": s, "water": w, "specific_area": a, "fingerprint": LOC.fingerprint(s, w, a),
                  "normalize": [LOC.normalize(w), LOC.normalize(a)], "landmarks": LOC.landmarks(f"{w} {a}")}
                 for s, w, a in TEXT_CASES],
        "names": [[n, names.key(n)] for n in names_in],
        "rows": rows,
        "pages": pages,
        "joins": joins,
        "dates": _date_cases(),
    }


def render() -> str:
    return json.dumps(build(), ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", help="fail if the committed file is stale")
    args = ap.parse_args(argv)
    text = render()
    if args.check:
        if not OUT.exists() or OUT.read_text(encoding="utf-8") != text:
            print(f"{OUT} is stale: the Python reads the saved DFO pages differently than the "
                  f"committed vectors say. Run `{_BY}`, then the worker's dfo.vectors.test.ts must still pass — a "
                  "change on one side only is the drift this file exists to catch (T556).", file=sys.stderr)
            return 1
        return 0
    OUT.write_text(text, encoding="utf-8")
    print(f"wrote {OUT.relative_to(REPO_ROOT)} ({len(text):,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

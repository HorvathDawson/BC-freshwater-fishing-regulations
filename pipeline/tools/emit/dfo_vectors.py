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
for the hand rows of `dfo-rows.json` (the shared rows of T671's two tests).

WHO READS IT. `app/workers/feeds/test/dfo.vectors.test.ts` (the worker must reproduce every value in its
Python-parity mode, and in robust mode — what production runs — everything but the differences that test lists by
name) and `pipeline/tests/test_dfo_vectors.py` (`--check`, so a Python-side change cannot land without the file, and
the file cannot land without the worker agreeing). It depends on the curated wording registry (a clause's reading):
a registry edit that changes a saved page's typing makes `--check` fail until this is re-run.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.common import names
from pipeline.common.curated import REPO_ROOT
from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon import live as L
from pipeline.regs.dfo_salmon import locations as LOC
from pipeline.regs.dfo_salmon import typed as T

FORMAT = "dfo-parity-vectors/1"
FIXTURES = REPO_ROOT / "app" / "workers" / "feeds" / "test" / "fixtures" / "dfo"
PAGES = FIXTURES / "pages"
#: the hand rows both languages' row tests read (T671's twin tests): `{cases: [{name, rows: [[species, dates, limits]]}]}`
ROWS = FIXTURES / "dfo-rows.json"
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
    for path in files:
        slug = _slug(path)
        typed, locs, structure = L.type_page(path.read_text(encoding="utf-8"), slug)
        tables = {loc.fingerprint for loc in locs}
        for loc in locs:
            waters.update([loc.water, *loc.aliases])
        pages[slug] = {
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

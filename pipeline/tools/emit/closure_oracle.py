"""THE ORACLE FOR THE WORKER'S CLOSED-AREA PORT (T573 -> T757): sample inputs and the `locs` the Python live feed emits
for them (`regs.dfo_salmon.closures.live_piece_rules` -> `live.location_calendar`, as `live.run` does).

    python -m pipeline.tools.emit.closure_oracle [--out handoff/PLAN/app-integration/tickets/T573-files]

Writes `closure-oracle.json`: {cases: [{name, says, entry_file, rows, confirmed, expect: {locs, verdicts, gears}}]}.
`entry_file` is the curated places file as the worker reads it (`entries.ts loadEntryFile`), `rows` today's scraped
rows of the page ({fingerprint, specific_area, excludes}: the scrape's own output, so the port is tested apart from
its scraper), `confirmed` the owner's placements (the FEED ignores them: confirmation is the release's matter — bound
or carried — and the feed carries every piece today's row names). `test_dfo_closure_build.py` holds the committed file
to this code.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

OUT = Path("handoff/PLAN/app-integration/tickets/T573-files")
FILE = "closure-oracle.json"

DEADMAN = "Deadman - from the fishing boundary signs approximately 1 km downstream of the Hwy No. 1 Bridge at Savona"
JUNIPER = "Juniper - from the fishing boundary signs 4 km above to the signs at Juniper Beach Provincial Park"
TEXT = "From Kamloops Lake outlet downstream. Please note that no fishing is allowed in the following areas, year-round:"


def _ef():
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.parsing.entry_models import Extent
    place = E.EntryLocation(location_id="3:thompson-river:from-kamloops-lake-outlet", region="3", region_number=3,
                            kind="water", precedence=3, water="Thompson River", water_id="3:thompson-river",
                            fingerprints=["fp-thompson"], source_text={"specific_area": TEXT,
                                                                        "excludes": [DEADMAN, JUNIPER]},
                            binding=E.Binding(extents=[Extent(op="whole")]))
    w = E.WaterBinding(water_id="3:thompson-river", region="3", region_number=3, name="Thompson River",
                       item_ids=["gnis:39492"])
    return E.EntryFile(region="3", region_number=3, region_name="Thompson / Nicola", waters=[w], locations=[place])


def _confirmed():
    from pipeline.regs.dfo_salmon import closures as K
    return [{"key": K.piece_key(DEADMAN), "water_id": "3:thompson-river", "name": "Deadman", "item": DEADMAN,
             "kind": "stretch", "on_item": "gnis:39492",
             "points": [{"kind": "split", "split_id": "thompson_river__a"},
                        {"kind": "split", "split_id": "thompson_river__b"}]}]


CASES = (
    ("a_confirmed_on_todays_row", "Deadman is confirmed and today's row names it: emitted, closed every day",
     [DEADMAN, JUNIPER]),
    ("b_unconfirmed_on_todays_row", "Juniper is not confirmed (the release carries it on the water, `u`) but today's "
     "row names it: emitted too — the feed decides liveness only", [DEADMAN, JUNIPER]),
    ("c_confirmed_dropped_today", "DFO dropped Deadman today: NOT emitted (its place answers there); Juniper still is",
     [JUNIPER]),
)


def build() -> dict:
    from types import SimpleNamespace as NS
    from pipeline.deliver import salmon as S
    from pipeline.regs.dfo_salmon import closures as K
    from pipeline.regs.dfo_salmon import live as L
    ef = _ef()
    cases = []
    for name, says, excludes in CASES:
        rows = [{"fingerprint": "fp-thompson", "specific_area": TEXT, "excludes": list(excludes)}]
        verdicts, gears = S._Interner(), S._Interner()
        verdicts(S.NOT_LISTED)                        # verdicts[0], as `live.run`
        got = K.live_piece_rules(ef, [NS(**r) for r in rows])
        locs = {lid: {"r": "3", **L.location_calendar(rs, verdicts, gears)} for lid, rs in sorted(got.items())}
        cases.append({"name": name, "says": says, "entry_file": ef.to_dict(), "rows": rows,
                      "confirmed": _confirmed(), "rules": got,
                      "expect": {"locs": locs, "verdicts": verdicts.items, "gears": gears.items}})
    return {"_about": __doc__.split("\n\n")[0], "regenerate": "python -m pipeline.tools.emit.closure_oracle",
            "cases": cases}


def text() -> str:
    return json.dumps(build(), indent=1, ensure_ascii=False, sort_keys=True) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / FILE).write_text(text(), encoding="utf-8")
    print(a.out / FILE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

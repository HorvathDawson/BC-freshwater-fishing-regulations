"""PROTOTYPE RUN OF THE CHANGED CODE, read-only, writes only OUT.pkl: rerun THIS checkout's `build_panels.build` on the
live atlas graph with the live donors and the live section -> water map, and save `sid -> (members, area)` where each
member is `(station, role, area, years, regulated, same_river, lead)`.

Donors are rebuilt from the live bundle and HYDAT exactly as the side-channels lane's prototype.py does (its baseline
matched the shipped bundle on all 249,415 sections). The water map is the bundle's `item_section` (water items only
— the bundle holds no `area:` items), as `pipeline/deliver/bundle/build.py` passes `owner`; `live` is the stations
roster's `realtime` flag, as build.py passes it.

Heavy (~6 GB, loads graph.pkl): run through handoff/tools/heavy.sh.

    .venv/bin/python handoff/PLAN/app-data/own-gauge/scripts/rebuild.py OUT.pkl [--no-water]
"""
import collections
import json
import os
import pickle
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, ".")
import pipeline.atlas.gauges.build_panels as bp  # noqa: E402
from pipeline.common.section_handles import read as read_handles  # noqa: E402
from pipeline.deliver.bundle.build import _area_model  # noqa: E402

sys.path.insert(0, "handoff/PLAN/app-data/side-channels/scripts")
from prototype import ATLAS, BUNDLE, MAIN, donors_from_bundle  # noqa: E402


def water_map(ids):
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    by = collections.defaultdict(set)
    for sid, item in db.execute("SELECT s.sid, i.item_id FROM item_section s JOIN item i USING(ord)"):
        by[sid].add(item)
    multi = sum(1 for v in by.values() if len(v) > 1)
    print(f"water map: {len(by):,} sections, {multi:,} in more than one water (first by item id kept)")
    return {ids[sid - 1]: sorted(v)[0] for sid, v in by.items()}


def main(out):
    t = time.time()
    with (ATLAS / "graph.pkl").open("rb") as fh:
        graph = pickle.load(fh)
    model = _area_model(MAIN / "data/source/bc_fisheries_data.gpkg")
    ids, sid_of = read_handles(ATLAS)
    donors = donors_from_bundle(ids)
    # `--no-water`: the same pass without the water map, i.e. nothing leads — the baseline for the counts
    water = None if "--no-water" in sys.argv else water_map(ids)
    print(f"loaded {time.time() - t:.0f}s; {len(donors)} donors")
    stations = json.loads((MAIN / "data/source/bc_hydrometric_stations.json").read_text())
    live = {s["station"] for s in stations if s.get("realtime")}
    measures = collections.defaultdict(set)
    with sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True) as db:
        for st, param in db.execute("SELECT station, parameter FROM gauge_stats"):
            measures[st].add(param)
    if os.environ.get("OWN_LEADS_PER_QUANTITY"):         # experiments only: the shipped value is in panel.py
        bp.OWN_LEADS_PER_QUANTITY = int(os.environ["OWN_LEADS_PER_QUANTITY"])
    panels = bp.build(graph, model, donors, water_of=water, live=live, measures=measures)
    got = {}
    for sec, (pid, area) in panels.by_section.items():
        ms = tuple((m.station, m.role, round(m.area_km2, 3), m.years, int(m.regulated), int(m.same_river), m.lead)
                   for m in panels.members[pid])
        got[sid_of[sec]] = (ms, None if area is None else round(area, 3))
    with open(out, "wb") as fh:
        pickle.dump(got, fh, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"{len(got):,} sections with a panel, {len(panels.members):,} panels, "
          f"{sum(len(v) for v in panels.members.values()):,} member rows ({time.time() - t:.0f}s)")


if __name__ == "__main__":
    main(sys.argv[1])

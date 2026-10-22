"""PROTOTYPE RUN OF THE CHANGED CODE, read-only, writes only into OUT_DIR: rerun THIS checkout's `build_panels.build` on
the live atlas graph with the live donors, the live section -> water map, the measures and the published areas — as
`pipeline/deliver/bundle/build.py` now calls it (no roster `realtime` flag) — once per reserve depth, and save

    OUT_DIR/panels_K{k}_L{l}.pkl   {"by_sid": {sid: (panel_id, area)}, "members": {panel_id: ((station, role, area,
                                    years, regulated, same_river, lead), ...)}}
    OUT_DIR/lengths.pkl            {sid: metres}

Donors and the water map are rebuilt exactly as handoff/PLAN/app-data/own-gauge/scripts/rebuild.py does (its
baseline matched the shipped bundle on every section). `K` is `panel.MEMBERS_PER_QUANTITY`, `L` is
`build_panels.OWN_LEADS_PER_QUANTITY`; the shipped values are in panel.py.

Heavy (~6 GB, loads graph.pkl): run through scripts/heavy.sh.

    .venv/bin/python handoff/PLAN/app-data/live-panels/scripts/build_variants.py OUT_DIR 4:4 6:4 8:4 12:4 8:8
"""
import collections
import json
import pickle
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, ".")
import pipeline.atlas.gauges.build_panels as bp  # noqa: E402
from pipeline.atlas.gauges import panel as P  # noqa: E402
from pipeline.common.section_handles import read as read_handles  # noqa: E402
from pipeline.deliver.bundle.build import _area_model  # noqa: E402

sys.path.insert(0, "handoff/PLAN/app-data/side-channels/scripts")
sys.path.insert(0, "handoff/PLAN/app-data/own-gauge/scripts")
from prototype import ATLAS, BUNDLE, MAIN, donors_from_bundle  # noqa: E402
from rebuild import water_map  # noqa: E402


def main(out_dir, configs):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    t = time.time()
    with (ATLAS / "graph.pkl").open("rb") as fh:
        graph = pickle.load(fh)
    model = _area_model(MAIN / "data/source/bc_fisheries_data.gpkg")
    ids, sid_of = read_handles(ATLAS)
    donors = donors_from_bundle(ids)
    water = water_map(ids)
    stations = json.loads((MAIN / "data/source/bc_hydrometric_stations.json").read_text())
    measures = collections.defaultdict(set)
    with sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True) as db:
        for st, param in db.execute("SELECT station, parameter FROM gauge_stats"):
            measures[st].add(param)
    published = {s["station"]: s["area_km2"] for s in stations if s.get("area_km2")}
    lengths = {sid_of[sec]: float(getattr(n, "length_m", 0.0) or 0.0) for sec, n in graph.nodes.items()
               if sec in sid_of}
    with (out_dir / "lengths.pkl").open("wb") as fh:
        pickle.dump(lengths, fh, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"loaded {time.time() - t:.0f}s; {len(donors)} donors")
    for cfg in configs:
        k, l = (int(x) for x in cfg.split(":"))
        P.MEMBERS_PER_QUANTITY = k
        bp.OWN_LEADS_PER_QUANTITY = l
        panels = bp.build(graph, model, donors, water_of=water, measures=measures, published=published)
        got = {"by_sid": {sid_of[sec]: (pid, None if a is None else round(a, 3))
                          for sec, (pid, a) in panels.by_section.items()},
               "members": {pid: tuple((m.station, m.role, round(m.area_km2, 3), m.years, int(m.regulated),
                                       int(m.same_river), m.lead) for m in ms)
                           for pid, ms in panels.members.items()}}
        with (out_dir / f"panels_K{k}_L{l}.pkl").open("wb") as fh:
            pickle.dump(got, fh, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"K={k} L={l}: {len(got['by_sid']):,} sections, {len(panels.members):,} panels, "
              f"{sum(len(v) for v in panels.members.values()):,} member rows ({time.time() - t:.0f}s)", flush=True)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:] or ["8:4"])

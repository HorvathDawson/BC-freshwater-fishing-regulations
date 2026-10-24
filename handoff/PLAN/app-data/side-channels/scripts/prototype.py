"""PROTOTYPE, read-only, writes only to OUT_DIR: rerun the bundle's panel pass (`build_panels.build`) on the live
atlas graph with the live donors, once as shipped and once per candidate fix, and save `sid -> (members, area)`.

The donors are reconstructed from the live bundle (`gauge.sid`, `gauge_stats`) and HYDAT exactly as
`pipeline/deliver/bundle/build.py` builds them; `baseline` must reproduce the live bundle's section_panel to the
section (checked by compare.py) before any variant's numbers mean anything.

Variants (only `mainstem_of` / `panel_for` are swapped; nothing in the repo is changed):
  baseline    as shipped
  same_wsc    a side channel adopts the largest non-side reach within MAINSTEM_HOPS whose FWA watershed code is
              the channel's OWN code (FWA gives a secondary channel its river's code); no such reach -> the
              channel keeps the panel its own walk gave it
  same_wsc_ns as same_wsc, but no such reach -> no panel (silence rather than the channel's own catchment)
  nested      same_wsc + one same-river donor per direction: of the donors on the same blue line in the same
              role, only the one nearest in size is kept (the rest are subsets of the same water)

    .venv/bin/python handoff/PLAN/app-data/side-channels/scripts/prototype.py OUT_DIR
"""
import collections
import pickle
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, ".")
import pipeline.gauges.consume.panels as bp  # noqa: E402
from pipeline.gauges.consume import panel as pn  # noqa: E402
from pipeline.gauges.consume.panel import MIN_RECORD_YEARS  # noqa: E402
from pipeline.common.section_handles import read as read_handles  # noqa: E402
from pipeline.deliver.bundle.build import _area_model  # noqa: E402

MAIN = Path("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")
ATLAS = MAIN / "data/generated/atlas/full"
BUNDLE = MAIN / "data/generated/bundle/bundle.sqlite"
HYDAT = MAIN / "data/source/hydat.sqlite3"

ORIGINAL_MAINSTEM_OF = bp.mainstem_of
ORIGINAL_PANEL_FOR = bp.panel_for


def mainstem_same_wsc(graph, area_of):
    """`mainstem_of`, but the adopted reach must carry the channel's own watershed code."""
    up, down = collections.defaultdict(list), collections.defaultdict(list)
    for e in graph.edges:
        up[e.to_node].append(e.from_node)
        down[e.from_node].append(e.to_node)
    parent = {}
    for sec, node in graph.nodes.items():
        if not bp._is_side_channel(node) or bp._kind(node) != "stream":
            continue
        mine, best, seen, frontier = area_of(node) or 0.0, None, {sec}, [sec]
        for _hop in range(bp.MAINSTEM_HOPS):
            nxt = []
            for cur in frontier:
                for other in (*up[cur], *down[cur]):
                    if other in seen:
                        continue
                    seen.add(other)
                    o = graph.nodes.get(other)
                    if o is None:
                        continue
                    nxt.append(other)
                    if bp._is_side_channel(o) or bp._kind(o) != "stream" or o.wsc != node.wsc:
                        continue
                    a = area_of(o)
                    if a and a > mine and (best is None or a > best[0]):
                        best = (a, other)
            frontier = nxt
        if best is not None:
            parent[sec] = best[1]
    return parent


def panel_nested(candidates, area_target):
    """`panel_for`, keeping one same-river donor per role: the one nearest the target in size."""
    out = ORIGINAL_PANEL_FOR(candidates, area_target)
    if not area_target:
        return out
    by = {c[0]: c for c in candidates}
    keep, best = [], {}
    for d in out:
        c = by[d.station]
        same = bool(c[6]) if len(c) > 6 else True
        if not same:
            keep.append(d)
            continue
        ratio = max(area_target, c[2]) / min(area_target, c[2])
        if d.role not in best or ratio < best[d.role][0]:
            best[d.role] = (ratio, d)
    keep += [d for _r, d in best.values()]
    keep.sort(key=lambda d: -d.weight)
    return keep


def donors_from_bundle(ids_by_handle):
    with sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True) as db:
        years = {s: int(y or 0) for s, y in db.execute("SELECT station, max(years) FROM gauge_stats GROUP BY station")}
        matched = {s: ids_by_handle[h - 1] for s, h in db.execute("SELECT station, sid FROM gauge WHERE sid IS NOT NULL")}
    with sqlite3.connect(f"file:{HYDAT}?mode=ro", uri=True) as hy:
        regulated = {r[0] for r in hy.execute("SELECT DISTINCT STATION_NUMBER FROM STN_REGULATION WHERE REGULATED=1")}
    return [(st, matched[st], years.get(st, 0), st in regulated) for st in matched
            if years.get(st, 0) >= MIN_RECORD_YEARS]


def run(graph, model, donors, sid_of, variant):
    bp.mainstem_of = ORIGINAL_MAINSTEM_OF
    bp.panel_for = ORIGINAL_PANEL_FOR
    keep_own = True
    if variant in ("same_wsc", "same_wsc_ns", "nested"):
        bp.mainstem_of = mainstem_same_wsc
        keep_own = variant != "same_wsc_ns"
    if variant == "nested":
        bp.panel_for = panel_nested
    panels = bp.build(graph, model, donors)
    out = {}
    for sec, (pid, area) in panels.by_section.items():
        ms = tuple((m.station, m.role, round(m.area_km2, 3), m.years, int(m.regulated), int(m.same_river))
                   for m in panels.members[pid])
        out[sid_of[sec]] = (ms, None if area is None else round(area, 3))
    if not keep_own:
        # silence a side channel the same-wsc rule found no river for, instead of its own catchment's panel
        area_of = lambda n: model.area_km2(n.stream_magnitude, n.wsc)  # noqa: E731
        adopted = mainstem_same_wsc(graph, area_of)
        for sec, node in graph.nodes.items():
            if bp._is_side_channel(node) and bp._kind(node) == "stream" and sec not in adopted:
                out.pop(sid_of.get(sec), None)
    return out


def main(out_dir):
    out_dir = Path(out_dir)
    t = time.time()
    with (ATLAS / "graph.pkl").open("rb") as fh:
        graph = pickle.load(fh)
    model = _area_model(MAIN / "data/source/bc_fisheries_data.gpkg")
    ids, sid_of = read_handles(ATLAS)
    donors = donors_from_bundle(ids)
    print(f"loaded {time.time() - t:.0f}s; {len(donors)} donors")
    for variant in sys.argv[2:] or ("baseline", "same_wsc", "same_wsc_ns", "nested"):
        got = run(graph, model, donors, sid_of, variant)
        with (out_dir / f"panels_{variant}.pkl").open("wb") as fh:
            pickle.dump(got, fh, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"{variant}: {len(got):,} sections with a panel ({time.time() - t:.0f}s)")


if __name__ == "__main__":
    main(sys.argv[1])

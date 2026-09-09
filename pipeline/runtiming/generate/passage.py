"""Where a run PASSES THROUGH, as distinct from where it spawns.

WHY THIS EXISTS. The Pacific Salmon Explorer's own method note is explicit:

    "We adjust run timing estimates depending on the sampling location so that the run
     timing curves shown in the Pacific Salmon Explorer represent RIVER ENTRY TIMING
     regardless of where the data were collected."

So the curve is dated at the moment fish leave salt water — not at the spawning grounds.
That makes the containment join in `index.py` exactly backwards about accuracy: it paints a
Conservation Unit's spawning watershed, often hundreds of river kilometres inland, with a
date measured at the coast.

The fish are real in between. A Babine sockeye is in the lower Skeena in July whether or
not the lower Skeena is inside the Babine CU, and an angler standing on the lower Skeena is
asking about exactly those fish. So every reach between a CU and the sea carries that run,
and this module finds them by walking the atlas's own flow graph downstream.

TWO RELATIONS, NEVER MERGED:

    spawning   the reach is inside the CU polygon. `index.py` produces these.
    passage    the reach is downstream of the CU. The run crosses it on the way in.

They must render differently — a dotted line, a second colour, anything but the same mark —
because "fish spawn here" and "fish swim past here" are different answers to an angler, and
only one of them is a place to look for a fish holding.

WHAT WE HAVE DIRECT DATA FOR, AND WHAT WE DO NOT.

    river entry timing      YES. That is what the curve is, and at the tidal boundary it is
                            exactly right.
    river kilometres        YES. Summed `length_m` down the flow graph to the outlet, so
                            every passage reach knows how far up it sits.
    travel time upriver     NO. Nothing in this dataset says how fast a run climbs, and
                            rates differ by species, river and year. So NO date shift is
                            applied. `km_to_sea` is published instead, and the reader is
                            told the date is river entry — accurate at the mouth and
                            increasingly early the further up you stand.

Inventing a migration speed here would turn a measured curve into a modelled one, silently.
"""

from __future__ import annotations

import argparse
import csv
import json
import pickle
import time
from collections import Counter, defaultdict, deque
from pathlib import Path

from pipeline.common.utils.wsc import trim_wsc
from pipeline.runtiming.runs import read_runs


def descends(ancestor: str, code: str) -> bool:
    """True when `code`'s water drains through `ancestor` — the FWA hierarchy, on group
    boundaries so "400-04" cannot be read as an ancestor of "400-046808"."""
    return bool(ancestor) and (code == ancestor or code.startswith(ancestor + "-"))


def downstream_next(graph) -> dict[str, list[str]]:
    """node -> the nodes it flows into. Usually one; a braid can have several."""
    nxt: dict[str, list[str]] = {}
    for node, idxs in graph.down_adj.items():
        outs = [graph.edges[i].to_node for i in idxs]
        if outs:
            nxt[node] = outs
    return nxt


def km_to_sea(graph, nxt: dict[str, list[str]]) -> dict[str, float]:
    """River kilometres from each node to its outlet, by summing `length_m` downstream.

    Iterative rather than recursive: a coastal mainstem chain is thousands of nodes deep and
    Python's stack is not. `seen` guards against a braid that loops back on itself — the
    graph should be acyclic, but a wrong answer here is silent and a crash is not.
    """
    out: dict[str, float] = {}
    for start in graph.nodes:
        if start in out:
            continue
        stack, seen = [start], set()
        while stack:
            n = stack[-1]
            if n in out:
                stack.pop()
                continue
            kids = [k for k in nxt.get(n, ()) if k in graph.nodes]
            pend = [k for k in kids if k not in out and k not in seen]
            if pend:
                seen.add(n)
                stack.extend(pend[:1])          # one path at a time; the rest on the way back
                continue
            here = graph.nodes[n].length_m or 0.0
            out[n] = here + (min(out[k] for k in kids if k in out) if any(k in out for k in kids) else 0.0)
            seen.discard(n)
            stack.pop()
    return {k: v / 1000.0 for k, v in out.items()}


def main(graph_pkl: Path, gen: Path, max_km: float | None) -> None:
    t0 = time.time()
    graph = pickle.load(open(graph_pkl, "rb"))
    print(f"graph loaded {time.time()-t0:.0f}s — {len(graph.nodes):,} nodes", flush=True)
    nxt = downstream_next(graph)
    km = km_to_sea(graph, nxt)
    print(f"km_to_sea computed {time.time()-t0:.0f}s", flush=True)

    spawn: dict[int, set[str]] = defaultdict(set)
    with open(gen / "section_cu.csv") as f:
        r = csv.reader(f); next(r)
        for sid, cuid, _rel in r:
            spawn[int(cuid)].add(sid)

    runs = read_runs(gen / "runs.json")
    passage: dict[str, list[tuple[int, float]]] = defaultdict(list)
    stats = Counter()
    wsc = {n: trim_wsc(v.wsc) for n, v in graph.nodes.items()}
    leaked, dropped_roots = Counter(), Counter()
    for cuid, secs in spawn.items():
        # EVERY reach this run drains through, as FWA codes. A downstream walk alone is not
        # enough: it does not stop at the river mouth, and the tidal reaches below one river
        # connect to the tidal reaches below the next — which walked `Nass Summer` steelhead
        # 400 reaches up the SKEENA, a different drainage entirely. The Skeena's code root is
        # `400` and the Nass's is `500`, so requiring the passage reach to be an ANCESTOR of
        # a spawning reach refuses it by construction. Same test the gauge shed makes, in the
        # mirror direction: there a reach must drain THROUGH the gauge, here the run must
        # drain through the reach.
        # DROP FOREIGN-DRAINAGE SPILL BEFORE WALKING. A CU polygon simplified to ~200 m
        # can clip a handful of reaches over a divide, and passage AMPLIFIES that: 13 stray
        # Skeena headwater sections on `Nass Summer` steelhead walked the run down 168
        # reaches of the Skeena mainstem, a river it never enters.
        #
        # The threshold separates noise from signal cleanly on this data. Real multi-drainage
        # CUs are coastal aggregates and say so in their names — `Nass-Skeena Estuary` holds
        # 598 sections (1.3%) in its smallest root, `East Vancouver Island-Georgia` 136
        # (1.0%). The spill is one to thirteen sections at two thousandths of a percent.
        roots = Counter(wsc[s].split("-")[0] for s in secs if s in wsc and wsc[s])
        tot = sum(roots.values()) or 1
        keep_roots = {r for r, n in roots.items() if (n / tot >= 0.01 and n >= 5) or n >= 100}
        dropped_roots[cuid] = sum(n for r, n in roots.items() if r not in keep_roots)
        secs = {s for s in secs if s in wsc and wsc[s].split("-")[0] in keep_roots}
        if not secs:
            continue
        codes = {wsc[s] for s in secs if s in wsc and wsc[s]}
        ancestors: set[str] = set()
        for c in codes:
            parts = c.split("-")
            for i in range(1, len(parts) + 1):
                ancestors.add("-".join(parts[:i]))
        seen, q = set(), deque(secs)
        while q:
            n = q.popleft()
            for d in nxt.get(n, ()):
                if d in seen or d in secs or d not in graph.nodes:
                    continue
                if wsc.get(d, "") not in ancestors:
                    leaked[cuid] += 1
                    continue
                # A run does not pass through water it never reaches; the walk only ever
                # goes DOWN, so a cap is about output size, not correctness.
                if max_km is not None and km.get(d, 0.0) > max_km:
                    continue
                seen.add(d)
                q.append(d)
        for d in seen:
            passage[d].append((cuid, round(km.get(d, 0.0), 1)))
        stats[cuid] = len(seen)

    with open(gen / "section_cu_passage.csv", "w") as f:
        f.write("section_id,cuid,relation,km_to_sea\n")
        for sid, lst in passage.items():
            for cuid, k in lst:
                f.write(f"{sid},{cuid},passage,{k}\n")

    rows = sum(len(v) for v in passage.values())
    depth = Counter(len(v) for v in passage.values())
    report = {
        "passage_sections": len(passage), "passage_rows": rows,
        "spawning_sections": len({s for v in spawn.values() for s in v}),
        "median_cus_per_passage_section":
            sorted(len(v) for v in passage.values())[len(passage)//2] if passage else 0,
        "max_cus_on_one_reach": max(depth) if depth else 0,
        "cus_with_passage": sum(1 for v in stats.values() if v),
        "reaches_refused_by_watershed_code": sum(leaked.values()),
        "spawning_sections_dropped_as_spill": sum(dropped_roots.values()),
        "cus_with_spill": sum(1 for v in dropped_roots.values() if v),
        "cus_that_leaked": len(leaked),
        "seconds": round(time.time()-t0, 1),
    }
    (gen / "passage_report.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--graph", type=Path, default=GENERATED.require_build() / "graph.pkl")
    ap.add_argument("--gen", type=Path, default=GENERATED.runtiming)
    ap.add_argument("--max-km", type=float, default=None,
                    help="drop passage reaches further than this from the sea")
    a = ap.parse_args()
    main(a.graph, a.gen, a.max_km)

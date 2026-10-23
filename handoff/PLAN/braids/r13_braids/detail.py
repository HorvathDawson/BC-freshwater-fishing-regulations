"""Detail: the channels the WSC sweep misses (Serpentine, Squamish) and the 8 line/topology conflicts."""
import json, sys
sys.path.insert(0, ".")
from pipeline.common.io.serialize import read_artifact
g = read_artifact("data/generated/atlas/full/graph.pkl")
want_blk = ["355995609", "355996889", "360221035", "360221036", "360221037", "360225339", "360224406",
            "359002030", "354087999", "355992048", "360220099", "360230482", "360226040"]
by = {}
for nid, n in g.nodes.items():
    if n.blk in want_blk:
        by.setdefault(n.blk, []).append(nid)
for b in want_blk:
    for nid in sorted(by.get(b, []), key=lambda s: g.nodes[s].down_m):
        n = g.nodes[nid]
        nbs = []
        for i in g.down_adj.get(nid, []):
            e = g.edges[i]; y = g.nodes.get(e.to_node)
            nbs.append(("dn", e.to_node, y.blk if y else "", y.display_name if y else "", round(y.down_m) if y else None, round(y.up_m) if y else None, e.kind, round(e.at_measure)))
        for i in g.up_adj.get(nid, []):
            e = g.edges[i]; y = g.nodes.get(e.from_node)
            nbs.append(("up", e.from_node, y.blk if y else "", y.display_name if y else "", round(y.down_m) if y else None, round(y.up_m) if y else None, e.kind, round(e.at_measure)))
        print(b, nid, n.display_name, n.wsc, n.gnis_id, [t.name + "/" + t.source.value for t in n.name_tuples], round(n.down_m), round(n.up_m),
              "lb", n.lower_bound.boundary_id if n.lower_bound else None, "ub", n.upper_bound.boundary_id if n.upper_bound else None)
        for x in nbs:
            print("     ", x)
# host lines for serpentine / squamish cuts
res = json.load(open("data/generated/atlas/full/splits.resolved.json"))
for r in res:
    if "serpentine" in r["split_id"] and "168" in r["split_id"] or "ashlu" in r["split_id"] or "kitsumkalum_river_into_skeena_river_u1500m" in r["split_id"] or "exchamsiks" in r["split_id"] or "kay_creek" in r["split_id"] or "08MF075" in r["split_id"]:
        ns = [nid for nid, n in g.nodes.items() if n.blk == r["blk"]][:1]
        n = g.nodes[ns[0]] if ns else None
        print("SPLIT", r, n.display_name if n else None, n.wsc if n else None)

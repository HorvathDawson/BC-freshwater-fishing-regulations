"""Tables from conditions_measure.py runs: `python conditions_table.py <run.json> ...` -> markdown rows."""
import json, sys

for path in sys.argv[1:]:
    r = json.load(open(path))
    name = f"{r['path']} {r['size']} net={r['net']} cpu={r['cpu']}"
    for z, v in r["byZoom"].items():
        print(f"| {name} | {z} | {v['pansGreyMoving']} / {v['pansGreyAfter']} of 9 | {v['greyFramesAfter']} | "
              f"{v['colouredMs']['median']} / {v['colouredMs']['max']} | {v['workMs']['median']} / {v['workMs']['max']} | "
              f"{v['fsCalls']['median']} / {v['fsCalls']['max']} | {v['settledWithValue']['median']} | {v['longtasksMs']} |")
    for c in r["changes"]:
        sync = [x for x in c["colourCost"] if x[0] == "standing"]
        tiles = [x for x in c["colourCost"] if x[0] == "tile" and x[2] > 0]
        print(f"    {c['label']}: settled +{c['settledMs']} ms, {c['sectionsChanged']}/{c['rendered']} rendered changed, "
              f"{c['fsCalls']} setFeatureState ({c['fsMs']} ms), standing sync {sum(x[1] for x in sync):.2f} ms "
              f"x{len(sync)}, tile labels +{sum(x[2] for x in tiles)}, idle +{c['idleMs']} ms, long tasks {c['longtasks']} ms")
    print("    heap MB", r["heapMB"])

"""Tables from app_measure.py runs: `python app_table.py <run.json> ...` -> markdown rows, one per run and zoom."""
import json, sys


def change_row(c):
    tiles = [x for x in c.get("colourCost", []) if x[0] == "tile"]
    sync = [x for x in c.get("colourCost", []) if x[0].startswith("status:")]
    return (f"{c['label']}: {c['fsCalls']} setFeatureState ({c['fsMs']} ms), "
            f"status sync {sum(x[1] for x in sync):.2f} ms, re-tiles {len(tiles)} (+{sum(x[2] for x in tiles)} calls), "
            f"idle +{c['idleMs']} ms, grey frames {c['greyFrames']}/{c['frames']}, long tasks {c['longtasks']} ms, "
            f"paint {c.get('paint', '-')}")


for path in sys.argv[1:]:
    r = json.load(open(path))
    name = f"{r['state']}{' public' if r['public'] else ''} {r['size']} net={r['net']} cpu={r['cpu']}"
    for z, v in r["byZoom"].items():
        print(f"| {name} | {z} | {v['pansGreyMoving']} / {v['pansGreyAfter']} of 9 | {v['greyFramesAfter']} | "
              f"{v['colouredMs']['median']} / {v['colouredMs']['max']} | {v['workMs']['median']} / {v['workMs']['max']} | "
              f"{v['fsCalls']['median']} / {v['fsCalls']['max']} | {v['longtasksMs']} |")
    for c in r["changes"]:
        print("   ", change_row(c))
    print("    heap MB", r["heapMB"])

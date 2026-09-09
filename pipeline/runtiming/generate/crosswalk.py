#!/usr/bin/env python3
"""Crosswalk PSF Pacific Salmon Explorer cuid -> DFO FULL_CU_IN by polygon overlap.

DFO CU names encode run/age codes ("LOWER FRASER RIVER_SU_1.3") while PSF uses cleaned
display names, so names don't join. Both delineate the same watersheds, so overlap does.
"""
import json, math, sys
from shapely.geometry import shape
from shapely.ops import unary_union
from shapely.strtree import STRtree
from shapely import make_valid

# PSF speciesId -> DFO FULL_CU_IN prefixes. Steelhead is provincial, so DFO has none.
SPECIES_PREFIX = {1: ["CK"], 2: ["SEL", "SER"], 3: ["CO"], 4: ["PKE", "PKO"], 5: ["CM"], 6: []}


def prefixes_for(cu):
    """PSF folds DFO's even/odd pink and lake/river sockeye into one species each; the
    run-year and life-history qualifier survives in the CU name, so use it to narrow."""
    ps = SPECIES_PREFIX[cu["species_id"]]
    n = cu["cu_name"].lower()
    if cu["species_id"] == 4:
        if "(even)" in n or "even" in n.split():
            return ["PKE"]
        if "(odd)" in n or "odd" in n.split():
            return ["PKO"]
    if cu["species_id"] == 2:
        if "river-type" in n or "river type" in n:
            return ["SER"]
        if "lake-type" in n or "lake type" in n:
            return ["SEL"]
    return ps

def merc_to_wgs(x, y):
    lon = x/20037508.34*180.0
    lat = math.degrees(2*math.atan(math.exp(math.radians(y/20037508.34*180.0))) - math.pi/2)
    return lon, lat

def load_dfo():
    out = {}
    for slug in ["ck", "cm", "pke", "pko", "sel", "ser"]:
        for f in json.load(open(f"dfo_{slug}.geojson"))["features"]:
            if f.get("geometry"):
                out[f["properties"]["FULL_CU_IN"]] = make_valid(shape(f["geometry"]))
    # DFO's coho REST service returns Chinook rows, so coho comes from the shapefile
    import shapefile
    r = shapefile.Reader("coho_shp/Coho_Salmon_CU_shape/CO_CU_Boundary_En.shp")
    fi = [x[0] for x in r.fields[1:]].index("FULL_CU_IN")
    for sr in r.shapeRecords():
        g = sr.shape.__geo_interface__
        def conv(c, d):
            return [conv(i, d-1) for i in c] if d > 1 else [merc_to_wgs(*p[:2]) for p in c]
        depth = {"Polygon": 2, "MultiPolygon": 3}[g["type"]]
        out[sr.record[fi]] = make_valid(shape({"type": g["type"],
                                               "coordinates": conv(g["coordinates"], depth)}))
    return out

def main():
    data = json.load(open("run_timing.json"))
    psf = {f["properties"]["cuid"]: make_valid(shape(f["geometry"]))
           for f in json.load(open("cu_polygons_z8.geojson"))["features"]}
    dfo = load_dfo()
    print(f"DFO CUs {len(dfo)}  PSF CUs {len(psf)}", flush=True)

    by_prefix = {}
    for k, g in dfo.items():
        by_prefix.setdefault(k.split("-")[0], []).append((k, g))
    trees = {p: (STRtree([g for _, g in v]), [k for k, _ in v]) for p, v in by_prefix.items()}

    matched = 0
    for cu in data["conservation_units"]:
        cu["dfo_full_cu_in"] = None
        cu["dfo_match_iou"] = None
        prefixes = prefixes_for(cu)
        if not prefixes:
            cu["dfo_match_note"] = "no DFO CU: steelhead is managed by the Province of BC"
            continue
        a = psf[cu["cuid"]].simplify(0.002, preserve_topology=False).buffer(0)
        best_iou, best_key, best_cont = 0.0, None, 0.0
        for p in prefixes:
            tree, keys = trees[p]
            for i in tree.query(a):
                b = dfo[keys[i]]
                inter = a.intersection(b).area
                if inter <= 0:
                    continue
                iou = inter/(a.area + b.area - inter)
                cont = inter/a.area                     # share of the PSF CU inside the DFO CU
                if (iou, cont) > (best_iou, best_cont):
                    best_iou, best_key, best_cont = iou, keys[i], cont
        cu["dfo_match_iou"] = round(best_iou, 4) if best_key else None
        cu["dfo_match_containment"] = round(best_cont, 4) if best_key else None
        if best_key is None:
            cu["dfo_match_note"] = "no overlapping DFO CU found"
        elif best_iou >= 0.5:
            cu["dfo_full_cu_in"] = best_key
            cu["dfo_match_note"] = "1:1 boundary match"
            matched += 1
        elif best_cont >= 0.9:
            cu["dfo_full_cu_in"] = best_key
            cu["dfo_match_note"] = ("PSF CU nested inside this DFO CU - PSF splits it finer; "
                                    "not a 1:1 equivalence")
            matched += 1
        else:
            cu["dfo_full_cu_in"] = best_key
            cu["dfo_match_note"] = f"partial overlap only (IoU {best_iou:.2f}) - verify before use"
            matched += 1
    json.dump(data, open("run_timing.json", "w"), indent=1)
    print(f"matched {matched}")

if __name__ == "__main__":
    main()

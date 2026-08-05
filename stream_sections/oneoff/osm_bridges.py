"""First-pass OSM candidate resolver for bridge/highway/rail locators.

For each todo point row that names a highway / road / rail bridge, find where that feature crosses
the target river (gpkg streams ∩ OSM Overpass geometry) and write candidate coord(s) into the row's
notes as `[osm-candidate]` (NEVER auto-curates — the human confirms). Shows ALL candidates when
several cross (dual carriageways, multiple same-name bridges).

  report : write scratchpad_osm_candidates.md + .json, mutate nothing
  apply  : also append [osm-candidate] lines into 14-locators-to-curate.json notes
"""
import json, re, sys, time, warnings
from collections import defaultdict
warnings.filterwarnings('ignore')
import geopandas as gpd, requests
from shapely.geometry import LineString, Point
from pyproj import Transformer

P = "stream_sections/docs/14-locators-to-curate.json"
OVERPASS = "https://overpass-api.de/api/interpreter"
HDR = {'User-Agent': 'bc-fishing-reg-curation/1.0 (ridgedvids@gmail.com)'}
TO3005 = Transformer.from_crs(4326, 3005, always_xy=True)
TO4326 = Transformer.from_crs(3005, 4326, always_xy=True)
STREAMS = gpd.read_file("data/bc_fisheries_data.gpkg", layer="streams")
WMU = gpd.read_file("data/bc_fisheries_data.gpkg", layer="wmu").to_crs(3005)

STOP = {'the', 'fishing', 'boundary', 'main', 'logging', 'pedestrian', 'white', 'triangular',
        'second', 'first', 'old', 'new', 'lower', 'upper', 'a', 'no', 'bridge', 'river', 'creek',
        'valley', 'station', 'forest', 'service', 'park', 'lake', 'road', 'street', 'boulevard',
        'avenue', 'crossing', 'drive', 'way', 'lane', 'north', 'south', 'east', 'west', 'at', 'of'}


def gnis_name(name_verbatim):
    n = re.sub(r'\([^)]*\)', '', name_verbatim)          # drop (see map) / (alias)
    n = re.sub(r'[",]', '', n).strip()
    return n.title()                                      # "NANAIMO RIVER" -> "Nanaimo River"


def feature_filter(txt):
    t = txt.lower()
    refs = [m.group(1).upper() for m in re.finditer(r'\b(?:hwy|highway)\s*([0-9]+[a-z]?)\b', t)]
    names = []
    for mm in re.finditer(r"([A-Za-z][A-Za-z.' ]+?)\s+(road|street|st|avenue|ave|bypass|crossing|drive|bridge)\b", txt, re.I):
        nm = mm.group(1).strip()
        toks = [w for w in nm.split() if w.lower() not in STOP]
        if toks and not re.fullmatch(r'(?i)hwy|highway', toks[-1]):
            names.append(toks[-1])                        # last significant word: "Cedar", "Vedder"
    rail = bool(re.search(r'\b(cnr|cn rail|railway|rail bridge|trestle|train)\b', t))
    power = bool(re.search(r'\b(power\s*line|powerline|transmission|hydro line)\b', t))
    dam = bool(re.search(r'\bdam\b', t))  # \b avoids matching 'Adam'
    return refs, names, rail, power, dam


def cluster(pts, tol=60.0):
    """Merge candidate points within tol metres (dual carriageway = one bridge); keep all clusters."""
    out = []
    for lon, lat, lab in pts:
        x, y = TO3005.transform(lon, lat)
        for c in out:
            if ((c['x'] - x) ** 2 + (c['y'] - y) ** 2) ** 0.5 < tol:
                c['labels'].add(lab)
                break
        else:
            out.append({'x': x, 'y': y, 'lon': lon, 'lat': lat, 'labels': {lab}})
    return out


def find_river(gname):
    lo = STREAMS.GNIS_NAME.astype(str).str.lower()
    sel = STREAMS[lo == gname.lower()]
    if sel.empty:
        key = ' '.join(gname.split()[:2]).lower()
        sel = STREAMS[lo.str.startswith(key)] if len(key) > 4 else sel
    return sel


def resolve_river(gname, mus, refs, names, rail, power, dam):
    sel = find_river(gname).to_crs(3005)
    if sel.empty:
        return None, "no gpkg river"
    mp = WMU[WMU.WILDLIFE_MGMT_UNIT_ID.isin(mus)]           # clip to the row's MU -> beat name collisions
    if not mp.empty:
        poly = mp.union_all()
        insel = sel[sel.intersects(poly)]
        if not insel.empty:
            sel = insel
    river = sel.union_all()
    W, S, E, N = sel.to_crs(4326).total_bounds
    bbox = f'{S-.02},{W-.02},{N+.02},{E+.02}'
    clauses = []
    if refs:
        clauses.append(f'way[highway][ref~"^({"|".join(sorted(set(refs)))})$"]({bbox});')
    for nm in sorted(set(names)):
        clauses.append(f'way[highway][name~"{nm}",i]({bbox});')
        clauses.append(f'way[bridge][name~"{nm}",i]({bbox});')
    if rail:
        clauses.append(f'way[railway=rail]({bbox});')
    if power:                                               # OSM-only man-made: power lines
        clauses.append(f'way[power=line]({bbox});')
    if dam:                                                 # OSM-only man-made: dams (way or node)
        clauses.append(f'way[man_made=dam]({bbox});way[waterway=dam]({bbox});node[man_made=dam]({bbox});')
    if not clauses:
        return None, "no feature"
    q = f'[out:json][timeout:40];({"".join(clauses)});out geom;'
    els = None
    for attempt in range(4):
        try:
            r = requests.post(OVERPASS, data={'data': q}, headers=HDR, timeout=120)
            if r.status_code == 200 and r.text.lstrip()[:1] == '{':
                els = r.json()['elements']
                break
        except Exception:
            pass
        time.sleep(6 * (attempt + 1))  # backoff on rate-limit / gateway errors
    if els is None:
        return None, "overpass fail (rate-limit)"
    pts = []
    for e in els:
        t = e.get('tags', {})
        if t.get('power') == 'line':
            lab = 'powerline'
        elif t.get('man_made') == 'dam' or t.get('waterway') == 'dam':
            lab = 'dam'
        elif t.get('railway') == 'rail':
            lab = 'rail'
        else:
            lab = t.get('ref') or t.get('name') or '?'
        if e.get('type') == 'node' and e.get('lon') is not None:   # dam node: keep if on/near river
            x0, y0 = TO3005.transform(e['lon'], e['lat'])
            if Point(x0, y0).distance(river) < 200:
                pts.append((round(e['lon'], 5), round(e['lat'], 5), lab))
            continue
        g = e.get('geometry') or []
        if len(g) < 2:
            continue
        line = LineString([TO3005.transform(p['lon'], p['lat']) for p in g])
        x = line.intersection(river)
        if x.is_empty:
            continue
        for p in ([x] if x.geom_type == 'Point' else list(getattr(x, 'geoms', []))):
            if p.geom_type == 'Point':
                lon, lat = TO4326.transform(p.x, p.y)
                pts.append((round(lon, 5), round(lat, 5), lab))
    return cluster(pts), None


def main():
    doc = json.load(open(P))
    rows = doc['locators']
    todo = [r for r in rows if r['status'] == 'todo' and r['anchor_kind'] == 'point'
            and re.search(r'\b(hwy|highway|road|bridge|cnr|rail|trestle|street|crossing|bypass|'
                          r'power\s*line|powerline|transmission|dam)\b', r['locator_text'], re.I)]
    by_river = defaultdict(list)
    for r in todo:
        by_river[(gnis_name(r['name_verbatim']), tuple(r.get('mus') or []))].append(r)

    apply = sys.argv[1:2] == ['apply']
    cand_by_id = {}
    L = ["# OSM bridge/highway candidates (first pass — VERIFY each)\n"]
    n_resolved = n_rows_hit = 0
    for (gname, mu), lst in sorted(by_river.items()):
        refs, names, rail, power, dam = [], [], False, False, False
        for r in lst:
            a, b, c, d, e = feature_filter(r['locator_text'])
            refs += a; names += b; rail |= c; power |= d; dam |= e
        clusters, err = resolve_river(gname, list(mu), refs, names, rail, power, dam)
        time.sleep(2.5)  # be polite to public Overpass
        if err:
            L.append(f"## {gname} · MU {list(mu)} — SKIP ({err})\n")
            continue
        if clusters:
            n_resolved += 1
        L.append(f"## {gname} · MU {list(mu)}")
        for r in lst:
            a, b, c, d, e = feature_filter(r['locator_text'])
            hits = []
            for cl in clusters:
                labs = {x.lower() for x in cl['labels']}
                if (any(rf.lower() in labs for rf in a) or any(nm.lower() in ' '.join(labs) for nm in b)
                        or (c and 'rail' in labs) or (d and 'powerline' in labs) or (e and 'dam' in labs)):
                    hits.append(cl)
            if hits:
                n_rows_hit += 1
                cand_by_id[r['id']] = [[h['lon'], h['lat']] for h in hits]
                pretty = "; ".join(f"[{h['lon']}, {h['lat']}] ({'/'.join(sorted(h['labels']))})" for h in hits)
                L.append(f"- **{r['locator_text'][:48]}** → {pretty}")
            else:
                L.append(f"- {r['locator_text'][:48]} → (no crossing found)")
        L.append("")

    from pathlib import Path
    Path("output").mkdir(exist_ok=True)
    open("output/osm_candidates.md", "w").write("\n".join(L) + "\n")
    json.dump(cand_by_id, open("output/osm_candidates.json", "w"), indent=1)
    print(f"rivers with candidates: {n_resolved} | rows with >=1 candidate: {n_rows_hit} "
          f"| todo point-bridge rows scanned: {len(todo)}")

    if apply:
        for r in rows:
            cs = cand_by_id.get(r['id'])
            if cs:
                s = "; ".join(f"[{lo}, {la}]" for lo, la in cs)
                r['notes'] = ((r.get('notes') or '').strip() +
                              f" [osm-candidate 2026-08-05] {s} (OSM Overpass ∩ river; VERIFY).").strip()
        json.dump(doc, open(P, 'w'), indent=2, ensure_ascii=False)
        open(P, 'a').write('\n')
        print(f"APPLIED osm candidates to {len(cand_by_id)} rows")


if __name__ == "__main__":
    main()

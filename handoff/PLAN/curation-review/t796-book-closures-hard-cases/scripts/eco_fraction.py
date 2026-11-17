"""For the ecological-reserve rows (13, 15, 19, 30, 41, 46, 57, 71): what share of each closed STREAM section's length
lies inside an ecological reserve polygon (tiles layer eco_reserve) — a section mostly outside the reserve closed in
whole is an overreach. Lakes are not in the stream layer (not measured). Run with the repo venv; ~4 min (one grep pass).
Writes out/eco_fraction.json."""
import json, subprocess
from collections import Counter
from pathlib import Path
from shapely.geometry import shape
from shapely.strtree import STRtree
here = Path(__file__).parent
LAY = Path('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/generated/tiles/layers')
rows = {r['id']: r for r in json.load(open(here / 'out/rows.json'))}
ids = ['row-13', 'row-15', 'row-19', 'row-30', 'row-41', 'row-46', 'row-57', 'row-71']
sids = {s for i in ids for s in rows[i]['sids']}
pat = here / 'out/eco_pat.txt'; pat.write_text(''.join(f'"section_id":{s},\n' for s in sids))
got = []   # (BSD grep -F with ~2,500 patterns took >12 min) scan the tail of each line for its section_id instead
with open(LAY / 'stream.geojsonl', 'rb') as fh:
    for line in fh:
        k = line.rfind(b'"section_id":')
        if k >= 0:
            e = k + 13
            while line[e:e+1].isdigit(): e += 1
            if int(line[k+13:e]) in sids: got.append(line.decode())
geo = {}
for line in got:
    f = json.loads(line); geo.setdefault(f['properties']['section_id'], []).append(shape(f['geometry']))
polys = [(json.loads(l)['properties']['name'], shape(json.loads(l)['geometry'])) for l in open(LAY / 'eco_reserve.geojsonl')]
tree = STRtree([p for _, p in polys])
out = {}
for i in ids:
    fr = []
    for s in rows[i]['sids']:
        gs = geo.get(s)
        if not gs: continue
        g = gs[0]; L = g.length or 1e-12
        hits = [polys[k] for k in tree.query(g)]
        inside = sum(g.intersection(p).length for _, p in hits)
        fr.append((s, round(min(1, inside / L), 3), sorted({n for n, p in hits if g.intersects(p)})))
    b = Counter('0' if f == 0 else '<25%' if f < .25 else '<50%' if f < .5 else '<90%' if f < .9 else '>=90%' for _, f, _ in fr)
    out[i] = {"measured": len(fr), "of": len(rows[i]['sids']), "bins": dict(b), "low": [x for x in fr if x[1] < .5][:30],
              "reserves": Counter(n for _, _, ns in fr for n in ns).most_common(12)}
    print(i, 'measured', len(fr), 'of', len(rows[i]['sids']), dict(b), out[i]['reserves'][:6])
json.dump(out, open(here / 'out/eco_fraction.json', 'w'), indent=0)

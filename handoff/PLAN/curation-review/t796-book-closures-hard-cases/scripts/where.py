"""Which ecological reserve / park / MU polygon each section of the given rows lies in, from the tiles' layers (the
geometry the app draws): where.py row-41 row-46 ...  (rows with at most ~400 sections; uses grep over stream.geojsonl).
Run with the repo venv (shapely)."""
import json, subprocess, sys
from pathlib import Path
from shapely.geometry import shape
here = Path(__file__).parent
LAY = Path('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/generated/tiles/layers')
rows = {r['id']: r for r in json.load(open(here / 'out/rows.json'))}
want = {s: rid for rid in sys.argv[1:] for s in rows[rid]['sids'][:400]}
pat = here / 'out/where_pat.txt'; pat.write_text(''.join(f'"section_id":{s},\n' for s in want))
got = subprocess.run(['grep', '-F', '-f', str(pat), str(LAY / 'stream.geojsonl')], capture_output=True, text=True).stdout.splitlines()
geoms = {}
for line in got:
    f = json.loads(line); geoms.setdefault(f['properties']['section_id'], []).append(shape(f['geometry']))
def polys(name, key):
    out = []
    for line in open(LAY / name):
        f = json.loads(line); out.append((f['properties'].get(key) or json.dumps(f['properties'])[:80], shape(f['geometry'])))
    return out
layers = {'eco': polys('eco_reserve.geojsonl', 'name'), 'mu': polys('mu.geojsonl', 'mu') if len(sys.argv) < 10 else []}
for rid in sys.argv[1:]:
    print('==', rid, rows[rid]['water'])
    for s in rows[rid]['sids'][:400]:
        gs = geoms.get(s)
        if not gs: print('  ', s, 'no geometry in stream layer (lake?)'); continue
        g = gs[0]
        for k, ps in layers.items():
            hit = [(n, round(g.intersection(p).length / g.length, 3)) for n, p in ps if g.intersects(p)]
            print('  ', s, k, hit[:4], 'len_deg', round(g.length, 5))

"""Build pipeline/docs/curation-review-queue.md from offline_pins.json + synopsis raw regs.
Run from repo root:  PYTHONPATH=. .venv/bin/python pipeline/oneoff/needspin/build_queue.py
NOT applied to waterbody-splits.json - review-only queue."""
import json, os, re
from collections import defaultdict
from pipeline.oneoff.waterbody_splits import load_curation
_HERE = os.path.dirname(os.path.abspath(__file__))

def osm(lat, lon, z=15): return f'https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map={z}/{lat}/{lon}'

pins = json.load(open(os.path.join(_HERE, 'offline_pins.json')))
raw = json.load(open('output/pipeline/extraction/synopsis_raw_data.json'))
# raw index
ridx = defaultdict(list)
for pg in raw:
    for row in pg.get('rows', []):
        ridx[(row.get('water') or '').strip().upper()].append(row)
def norm(s): return re.sub(r'[^A-Z0-9]', '', (s or '').upper())
def mulist(m):
    if isinstance(m, list): return set(str(x).strip() for x in m)
    return set(x.strip() for x in re.split('[ ,]+', m or '') if x.strip())
def raw_for(nv, mus):
    keys = list(ridx.keys()); mus = set(mus)
    cand = [k for k in keys if (norm(nv)[:12] and norm(nv)[:12] in norm(k)) or (norm(k)[:12] and norm(k)[:12] in norm(nv))]
    for k in cand:
        for row in ridx[k]:
            if mus & mulist(row.get('mu')):
                rr = row.get('raw_regs'); return ' \\n'.join(rr) if isinstance(rr, list) else rr
    if cand:
        rr = ridx[cand[0]][0].get('raw_regs'); return ' \\n'.join(rr) if isinstance(rr, list) else rr
    return None

rows = load_curation()
byeid = defaultdict(list)
for r in rows: byeid[r.get('entry_id')].append(r)
todo_eids = [eid for eid, ers in byeid.items() if any(x['status'] == 'todo' for x in ers)]

# confidence buckets by rid
STRONG = {'copper-creek-538967-b','mamin-river-608d92','eve-river-41b1eb','artlish-river-9e60a3',
          'mohun-creek-1fa4a0-a','thorn-creek-b2b2c7-a'}
SPRINGBOARD_FWA = {'deena-creek-af3d2f','north-alouette-river-e3810a','elk-river-s-tributaries-see-exceptions-e09244',
          'chowade-river-42a570','kitimat-river-angling-regulations-for-the-kit-b3b21f','ptarmigan-creek-dd88fc-b',
          'chuckwalla-river-eec2f0','pinkut-creek-03a36e','swift-creek-a32fd8-a','thorn-creek-b2b2c7-b',
          'cranberry-river-699352-a','cranberry-river-699352-b','thompson-river-downstream-of-signs-at-kamloop-1c9ce3',
          'kitimat-river-angling-regulations-for-the-kit-664c72'}
TOWNSITE = {'burton-creek-b25ef3-b','trepanier-river-a999b0-a','capilano-river-496399',
          'hays-creek-in-prince-rupert-6e14c4','ksi-x-anmas-river-formerly-kwinamass-river-fe2f0b'}
NEEDS_FISS = {'ptarmigan-creek-dd88fc-a','white-river-da5ee3'}
NA_LIKELY = {'skeena-river-mainstem-only-c2e0df'}
def bucket(rid):
    for name, s in [('A. STRONG (named FWA/FSR match)',STRONG),('B. SPRINGBOARD — correct FWA channel',SPRINGBOARD_FWA),
                    ('C. SPRINGBOARD — townsite/landmark (FWA name/MU miss)',TOWNSITE),
                    ('D. NEEDS FISS/topo (no offline feature)',NEEDS_FISS),('E. LIKELY n/a (reach label)',NA_LIKELY)]:
        if rid in s: return name
    return 'Z. other'

# assemble per bucket
out = ['# Curation review queue — remaining 24 entries / 28 todo rows',
       '',
       'Generated 2026-08-10. **Not applied** — candidate pins for manual review. Coords shown lat,lon.',
       'Each pin has an OSM springboard link. Method = how the pin was derived + what to eyeball.',
       '', '---', '']

perbucket = defaultdict(list)
for eid in todo_eids:
    ers = byeid[eid]
    todos = [x for x in ers if x['status'] == 'todo']
    nv = ers[0].get('name_verbatim'); mus = ers[0].get('mus') or []
    rr = raw_for(nv, mus)
    block = []
    block.append(f"### {nv}  ·  MU {', '.join(mus)}  ·  `{eid}`")
    block.append('')
    block.append('**Raw reg (verbatim):**')
    rr_disp = (rr or '(raw not found)').replace('\\n', '\n').replace('\n', '\n> ')
    block.append('> ' + rr_disp)
    block.append('')
    block.append('**All locators in this entry:**')
    for x in ers:
        st = x['status']
        mark = {'todo':'🔴 TODO','curated':'✅ curated','not_applicable':'⬜ n/a','deferred':'⏸ defer','manual':'🔧 manual'}.get(st, st)
        c = x.get('coord'); cs = f" — curated {c[1]:.5f},{c[0]:.5f}" if c else ''
        block.append(f"- {mark}: *{x.get('locator_text')}*{cs}")
    block.append('')
    # candidate pins for each todo row
    block.append('**Candidate pin(s) for the TODO row(s):**')
    buck = None
    for x in todos:
        rid = x['id']; buck = bucket(rid)
        p = pins.get(rid, {})
        block.append(f"- `{rid}`  — {p.get('method','(no method)')}")
        for label, latlon in p.get('pins', []):
            if latlon:
                block.append(f"    - **{label}**: `{latlon[0]},{latlon[1]}`  →  [OSM]({osm(*latlon)})")
            else:
                block.append(f"    - **{label}**: _(no coord — springboard is the river/area above)_")
    block.append('')
    block.append('---')
    perbucket[buck].append('\n'.join(block))

for name in sorted(perbucket):
    out.append(f'\n## {name}\n')
    out.extend(perbucket[name])

open('pipeline/docs/curation-review-queue.md','w').write('\n'.join(out))
print('wrote pipeline/docs/curation-review-queue.md')
print('buckets:', {k: len(v) for k,v in perbucket.items()})

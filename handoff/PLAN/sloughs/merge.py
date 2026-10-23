"""Merge by-name / by-shape / by-regs into one deduped list (by wbk) and classify adversarially.

Read-only: bundle.sqlite (ro), section_handles.txt (1-based), the three candidate files.
Writes candidates.json beside this script.
"""
import json, re, sqlite3, sys
from collections import defaultdict, Counter

ROOT = '/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/'
S = '/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/sloughs/'

N = json.load(open(S + 'by-name.json'))
R = json.load(open(S + 'by-regs.json'))
SH = json.load(open(S + 'by-shape.json'))

H = [''] + open(ROOT + 'data/generated/atlas/full/section_handles.txt').read().split('\n')
lake_sid = {}
for sid, h in enumerate(H):
    if h.startswith('lake:'):
        try:
            lake_sid[int(h[5:])] = sid
        except ValueError:
            pass
c = sqlite3.connect(f'file:{ROOT}data/generated/bundle/bundle.sqlite?mode=ro', uri=True)
q = lambda s, *a: c.execute(s, a).fetchall()
items = {o: (iid, name, kind) for o, iid, name, kind in q('select ord,item_id,name,kind from item')}
iid2 = {v[0]: v for v in items.values()}
sec_items = defaultdict(list)
for o, sid in q('select ord,sid from item_section'):
    sec_items[sid].append(items[o])
home = dict(q('select sid,region from section_home'))
ent_by_item = defaultdict(list)
ENT = {}
for eid, iid, name, verb, scope, pages, matched in q(
        'select entry_id,item_id,name,verbatim,scope_note,pages,matched from entry'):
    ENT[eid] = dict(entry_id=eid, name=name, scope_note=scope or None,
                    pages=json.loads(pages) if pages else None,
                    verbatim=(verb or '')[:300])
    ids = set(json.loads(matched or '[]')) | ({iid} if iid else set())
    for i in ids:
        ent_by_item[i].append(eid)

# ---------------------------------------------------------------- gather
recs = defaultdict(lambda: dict(src=[], ev=[]))
shape_all = {x['wbk']: x for x in SH['candidates']}
for x in SH['candidates']:
    if x['confidence'] == 'low':
        continue
    r = recs[x['wbk']]
    r['src'].append(f"by-shape:{x['cls']}/{x['confidence']}")
    r['shape'] = x
for x in R['candidates']:
    r = recs[x['wbk']]
    r['src'].append(f"by-regs:{x['source']}/{x['confidence']}")
    r['regs'] = x
for x in N['candidates']:
    r = recs[x['wbk']]
    r['src'].append(f"by-name:{x['status']}/{x['by_the_book_stream_confidence']}")
    r['name'] = x
# attach by-shape low evidence when another list found it
for w, r in recs.items():
    if 'shape' not in r and w in shape_all:
        r['shape'] = shape_all[w]
        r['src'].append(f"by-shape(low):{shape_all[w]['cls']}")

LAKEWORD = re.compile(r'\b(lake|lakes|reservoir|marsh|pond|ponds|biny|tarn)\b', re.I)
SLOUGHLIKE = re.compile(r'\b(slough|canal|channel|ditch|backwater|cut)\b', re.I)


def outline_of(r):
    for k, f in (('name', 'outline_lonlat'), ('shape', 'outline'), ('regs', 'outline_lonlat')):
        if k in r and r[k].get(f):
            o = r[k][f]
            if o and isinstance(o[0][0], list):  # rings -> first ring
                o = o[0]
            return o
    return None


out = []
for w, r in recs.items():
    sh, rg, nm = r.get('shape'), r.get('regs'), r.get('name')
    sid = lake_sid.get(w)
    cur = sec_items.get(sid, []) if sid else []
    cur_kind = sorted({k for _, _, k in cur})
    cur_name = sorted({n for _, n, _ in cur})
    cur_ids = sorted({i for i, _, _ in cur})
    names = set()
    for src in (sh, rg, nm):
        if src:
            names |= set(src.get('names') or src.get('fwa_names') or [])
    names = sorted(names)
    region = (home.get(sid) if sid else None) or (sh or {}).get('region') or (rg or {}).get('region') or (nm or {}).get('region')
    mu = (sh or {}).get('mu') or (rg or {}).get('mus') or (nm or {}).get('mu')
    if sh:
        lat, lon = sh['lat'], sh['lon']
    else:
        lon, lat = (rg or nm)['centroid_lonlat']
    area = (sh or rg or nm)['area_m2']
    elong = None
    if sh:
        elong = sh['elongation']['min_rect']
    elif rg:
        elong = rg['elongation']
    else:
        elong = nm['elongation']
    width = (sh or {}).get('mean_width_m') or (rg or {}).get('mean_width_m')
    layer = (sh or {}).get('waterbody_layer') or (rg or {}).get('layer') or ((nm or {}).get('fwa_layer') or [None])[0]
    fcode = (sh or {}).get('fwa_feature_code') or (rg or {}).get('feature_code') or ','.join((nm or {}).get('fwa_feature_code') or [])
    # stream it sits on
    stream_name = stream_item = None
    if rg and rg.get('sits_on'):
        stream_name, stream_item = rg['sits_on'][0]['name'], rg['sits_on'][0]['item_id']
    elif sh and sh.get('stream'):
        stream_name, stream_item = sh['stream']['name'], sh['stream']['item_id']
    elif nm and nm.get('stream'):
        stream_name = nm['stream']['name']
        stream_item = (nm['stream'].get('item_ids') or [None])[0]
    stream_kind = iid2.get((stream_item or '').split(' / ')[0], (None, None, None))[2] if stream_item else None
    # regs reach today
    reach = None; missing = []
    if rg is not None:
        reach = rg.get('stream_regs_reach_it_today')
        for so in rg.get('sits_on') or []:
            missing += sorted({x['entry_id'] for x in so.get('rules_stop_and_resume', [])})
    if sh is not None and sh['confidence'] != 'low':
        sr = sh['stream_regs_reach']
        if reach is None:
            reach = False if sr.startswith('no') else ('line_only' if sr.startswith('line_yes') else None)
        missing += sh.get('stream_entries_missing_on_polygon') or []
    if nm is not None and reach is None:
        reach = nm.get('stream_regs_reach_it')
        missing += (nm.get('regs_compare') or {}).get('stream_entries_missing') or []
    missing = sorted(set(missing))
    edge_types = sorted(set(((rg or {}).get('fwa_lines_inside') or {}).get('edge_types') or []))
    rp_both = bool(sh and sh.get('stream') and sh['stream'].get('river_polygon_both_sides'))
    one_sided = bool(sh and sh['cls'] == 'one_sided')
    canal_code = 'GA03950000' in (fcode or '')
    reservoir = 'GB24300000' in (fcode or '')
    sids_ = [i.strip() for i in (stream_item or '').split('/') if i.strip()]
    own_entries = sorted({e for i in sids_ for e in ent_by_item.get(i, [])})
    canal_in_book = [e for e in own_entries if 'canal' in ((ENT[e]['scope_note'] or '') + ENT[e]['name'] + e).lower()]
    cur_entries = sorted({e for i in cur_ids for e in ent_by_item.get(i, [])})
    cat = [ENT[e] for e in sorted(set(own_entries + cur_entries))]
    ev = []
    for src in (nm, sh, rg):
        if src:
            ev += src.get('evidence') or []

    # ------------------------------------------------------------ classify (adversarial)
    cls = None; why = []
    sname = (stream_name or '')
    slough_stream = bool(SLOUGHLIKE.search(sname)) and not re.search(r'\bcreek\b', sname, re.I)
    area_ha = area / 1e4 if area else 0
    named_lake = [n for n in names if LAKEWORD.search(n)] + [n for n in cur_name if LAKEWORD.search(n)]
    is_oxbow = any('OXBOW' in n.upper() for n in cur_name + names) and 'Oxbow Lake' not in names
    lagoon = [n for n in names if 'lagoon' in n.lower()]
    if 'stream' in cur_kind:
        cls = 'EXCLUDE_ALREADY_STREAM'; why.append(f'already part of stream item {cur_ids}')
    elif is_oxbow:
        cls = 'UNSURE'
        why.append('Okanagan River Oxbows (p.70): book calls them "isolated oxbows adjacent to Okanagan River" and exempts '
                   'them from the spring closure (a stream rule) — reads both ways; no stream item touches them on either side; owner call')
    elif named_lake:
        cls = 'LAKE'; why.append(f'carries a lake-type name {sorted(set(named_lake))} — a named lake is a lake by the book')
    elif lagoon:
        if stream_name:
            cls = 'UNSURE'; why.append(f'{lagoon} with {stream_name} running through: could be a river widening; "lagoon" is not a flowing word')
        else:
            cls = 'LAKE'; why.append(f'{lagoon}: named lagoon, no stream through it')
    elif names and not SLOUGHLIKE.search(' '.join(names)) and cur_kind and 'lake' in cur_kind:
        cls = 'LAKE'; why.append(f'own named lake item {cur_name}')
    elif reservoir and not canal_code:
        cls = 'LAKE'; why.append('FWA reservoir code GB24300000')
    elif re.search(r'\b(slough|channel) creek\b', sname, re.I) and not rp_both:
        cls = 'LAKE'; why.append(f'threaded by {sname} — a creek, and unnamed polygons on a named creek stay lakes (flowing.py)')
    elif canal_code:
        if canal_in_book:
            cls = 'STREAM_BY_BOOK'; why.append(f'FWA canal code GA03950000 on {stream_name}; the book row says the water includes the canal ({canal_in_book[0]}: "{ENT[canal_in_book[0]]["scope_note"]}")')
        elif stream_name and own_entries and not one_sided:
            cls = 'STREAM_BY_BOOK'; why.append(f'FWA canal code GA03950000 on {stream_name}\'s course; the book regulates the water ({own_entries[:2]}) and a canal is a stream')
        elif stream_name:
            cls = 'LIKELY_STREAM'; why.append(f'FWA canal code GA03950000 beside/on {stream_name}; canal is a stream by the book but placement needs an eye')
        else:
            cls = 'UNSURE'; why.append('FWA canal code with no named stream through it')
    elif slough_stream:
        if layer == 'wetlands' and area_ha > 50 and (elong or 0) < 4:
            cls = 'LAKE'; why.append(f'{area_ha:.0f} ha round marsh beside {sname}: a marsh, not the slough channel')
        elif layer == 'wetlands':
            cls = 'LIKELY_STREAM'; why.append(f'{sname} (a slough/channel, a stream by the book) runs inside this unnamed wetland; the line already carries the rules, the polygon draws grey')
        else:
            cls = 'STREAM_BY_BOOK'; why.append(f'unnamed polygon on {sname}: a slough/canal/channel is a stream by the book; flowing.py misses it because the slough does not both enter and leave')
    else:
        e = elong or 0
        wide_lake = area_ha > 5 and e < 2.5
        if wide_lake:
            cls = 'LAKE'; why.append(f'{area_ha:.1f} ha, elongation {e:.1f}: a lake-shaped widening the stream passes through')
        elif e < 2.0 and set(edge_types) & {1300, 1350}:
            cls = 'UNSURE'; why.append(f'round (elongation {e:.1f}) but FWA side-channel edge type inside, on {sname}: a backwater pocket or a side channel')
        elif e < 2.0:
            cls = 'LAKE'; why.append(f'round (elongation {e:.1f}): a pond on the stream, not a channel')
        elif rp_both and e >= 4:
            if own_entries and reach is False and (width or 999) <= 120:
                cls = 'STREAM_BY_BOOK'; why.append(f'FWA draws {sname} as a river polygon on both sides; the river\'s own book entry stops at it')
            else:
                cls = 'LIKELY_STREAM'; why.append(f'FWA river polygon on both sides of this piece of {sname}; elongation {e:.1f}')
        elif set(edge_types) & {1300, 1350} and e >= 3:
            if own_entries:
                cls = 'STREAM_BY_BOOK'; why.append(f'side-channel edge type {sorted(set(edge_types)&{1300,1350})} on {sname}; the river\'s own entry ({own_entries[0]}) covers the river and stops here')
            else:
                cls = 'LIKELY_STREAM'; why.append(f'side-channel edge type on {sname}, elongation {e:.1f}')
        elif own_entries and reach is False and e >= 5 and width and width <= 80 and layer != 'wetlands':
            cls = 'STREAM_BY_BOOK'; why.append(f'river-shaped (elongation {e:.1f}, {width} m wide) unnamed piece on {sname}\'s line; the river\'s own book entry ({own_entries[0]}) binds the line on both sides and stops here')
        elif own_entries and reach is False and e >= 3 and width and width <= 60:
            cls = 'LIKELY_STREAM'; why.append(f'elongation {e:.1f}, {width} m wide on {sname}; its own entry binds both sides and stops here')
        elif e >= 4 and (width or 999) <= 300:
            cls = 'LIKELY_STREAM'; why.append(f'elongation {e:.1f}, mean width {width} m on {sname}: river-shaped')
        elif rp_both or (set(edge_types) & {1300, 1350}):
            cls = 'LIKELY_STREAM'; why.append(f'river polygon / side-channel signal on {sname}, elongation {e:.1f}')
        else:
            cls = 'UNSURE'; why.append(f'elongation {e:.1f}, {area_ha:.1f} ha on {sname}: shape alone does not settle it')
        if layer == 'wetlands' and cls in ('STREAM_BY_BOOK', 'LIKELY_STREAM'):
            cls = 'LIKELY_STREAM' if rp_both else 'UNSURE'
            why.append('wetland polygon: the line through it already carries the stream rules; only the drawing differs')
    if 'line_only' == reach:
        pass
    out.append(dict(
        id=w, poly_layer=layer, fwa_feature_code=fcode, name=names or None,
        current_item=[dict(item_id=i, name=n, kind=k) for i, n, k in cur] or None,
        region=region, mu=mu, lat=round(lat, 5), lon=round(lon, 5),
        outline=outline_of(r), area_m2=area, elongation=elong, mean_width_m=width,
        stream_name=stream_name, stream_item=stream_item, stream_item_kind=stream_kind,
        regs_reach_today=reach, entries_missing_on_polygon=missing,
        fwa_edge_types_inside=edge_types, river_polygon_both_sides=rp_both,
        **{'class': cls}, class_reason=why, evidence=ev, found_by=r['src'],
        catalogue_entries=cat))

order = {'STREAM_BY_BOOK': 0, 'LIKELY_STREAM': 1, 'UNSURE': 2, 'LAKE': 3, 'EXCLUDE_ALREADY_STREAM': 4}
out.sort(key=lambda x: (order[x['class']], x['region'] or '', -(x['elongation'] or 0)))
cnt = Counter(x['class'] for x in out)
byreg = defaultdict(Counter)
for x in out:
    byreg[x['class']][x['region']] += 1
summary = dict(
    generated='2026-10-22', total=len(out), by_class=dict(cnt),
    by_class_region={k: dict(sorted(v.items())) for k, v in byreg.items()},
    method=('Union of by-name (79 not-stream), by-shape high+medium (447), by-regs (406); by-shape low rows kept only as '
            'evidence on wbks another list found. Deduped by FWA waterbody key (wbk = the lake:<wbk> section). Current item '
            'rechecked against the live bundle. Classes: STREAM_BY_BOOK = slough/canal/channel water, FWA canal code on a '
            'regulated stream, or a river-polygon/side-channel piece of a river whose own book entry stops at it; '
            'LIKELY_STREAM = river-shaped unnamed piece, or a wetland on a slough line (polygon display only); '
            'LAKE = named lake/reservoir/marsh, reservoir code, round or lake-sized widening, polygon on a "Slough/Channel Creek"; '
            'UNSURE = Okanagan oxbows, lagoons with a river through, unsettled shape. Graph not loaded (reach job held the machine).'))
json.dump(dict(summary=summary, candidates=out), open(S + 'candidates.json', 'w'), indent=1)
print(json.dumps(summary, indent=1))

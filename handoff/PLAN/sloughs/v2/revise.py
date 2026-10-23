"""T55 second sweep: revise the 865 first-sweep candidates with feature-code / line-code / neighbour evidence,
add the census's new candidates, and measure the rules against the owner's marks.

Read-only on the repo (gpkg, bundle.sqlite, section_handles.txt). Run through scripts/heavy.sh.
Stage 1 (geometry + neighbours) caches to OUT/geo_v2.pkl; stage 2 (rules) is cheap and reruns from the cache.
Writes OUT/candidates_v2.json.gz and OUT/sweep2.md.

T154 (2026-10-23): `--dams FILE` reads the provincial dams matched to the candidate polygons (dams.py's
dams_by_wbk.json) as R2 evidence: a STANDING dam (DAM_OPERATION_CODE in `STANDING`) within `DAM_REACH_M` of the
polygon, at the polygon's OUTLET on its own blue line (dams.side_of). `--out DIR` writes the outputs (and a fresh stage-1 cache) there instead of OUT, which stays the inputs'
directory — so a re-run never overwrites the T148 outputs. Without `--dams` the run is the T148 one.
"""
import collections, gzip, json, math, os, pickle, re, sqlite3, sys
import pandas as pd
from shapely import wkb, STRtree
from shapely.geometry import Point
from shapely.ops import unary_union
from pyproj import Transformer

REPO = '/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/'
OUT = '/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/t55v2/'
MARKS = '/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/t55marks/verdicts/'
GPKG = REPO + 'data/source/bc_fisheries_data.gpkg'
BUNDLE = REPO + 'data/generated/bundle/bundle.sqlite'
HANDLES = REPO + 'data/generated/atlas/full/section_handles.txt'
V1 = REPO + 'handoff/PLAN/sloughs/candidates.json.gz'


def _flag(name):
    return sys.argv[sys.argv.index(name) + 1] if name in sys.argv else None


WOUT = (_flag('--out') or OUT).rstrip('/') + '/'
DAMS_FILE = _flag('--dams')
#: T154: the dams that hold water back (the WRIS operation codes); Breached (Failed), Not Constructed, Decommissioned,
#: Removed and Application do not impound anything a polygon could be the widening of
STANDING = {'Active', 'Deactivated'}
#: T154: a dam is evidence for a polygon when its crest line is this close to it — FWA 1:20k positional tolerance; a
#: dam impounding the polygon sits on its edge (Quinsam diversion dam 0 m, South Fork dam on the South Nanaimo 8 m)
DAM_REACH_M = 25.0
DAMS = json.load(open(DAMS_FILE)) if DAMS_FILE else {}

FC = {
    'GB15300000': 'Lake-Definite', 'GB15300130': 'Lake-Indefinite', 'GB15300140': 'Lake-Intermittent',
    'GB24300000': 'Reservoir-Definite', 'GB90100000': 'Reservoir-Indefinite', 'GB90100110': 'Reservoir-Intermittent',
    'EA26700110': 'Settling Basin-Sewage', 'AP90300100': 'Mine-Tailing Pond', 'AP09200000': 'Dump',
    'GA03950000': 'Canal', 'GC17100000': 'Marsh', 'GC30050000': 'Swamp', 'GB11350110': 'Flooded Land-Inundated',
    'WA24111110': 'Construction Line-Main Flow', 'WA24111111': 'Lake Arm', 'WA24111120': 'Main Connector',
    'WA24111130': 'Secondary Flow', 'WA24111150': 'Construction Line (1350)', 'WA24111170': 'Construction Line (1400)',
    'WA24111190': 'Construction Line (1450)', 'WA11410000': 'Flow Connector-Inferred', 'GA08800110': 'Ditch',
    'GA24850000': 'River/Stream-Definite', 'GA24850140': 'River/Stream-Indefinite', 'GA24850150': 'River/Stream-Intermittent',
}
RESERVOIR = {'GB24300000', 'GB90100000', 'GB90100110'}
NOT_FISHING = {'EA26700110', 'AP90300100', 'AP09200000'}
SOFT_LAKE = {'GB15300130', 'GB15300140', 'GB11350110'}       # indefinite / intermittent / flooded land
STREAMISH = {'STREAM_BY_BOOK', 'LIKELY_STREAM'}
R7A = {'7-%d' % i for i in list(range(1, 19)) + list(range(23, 31)) + list(range(37, 42))}
SLOUGHWORD = re.compile(r'\b(slough|canal|channel|backwater)\b', re.I)
LAKEWORD = re.compile(r'\b(lake|lakes|reservoir|pond|ponds|marsh)\b', re.I)
to_ll = Transformer.from_crs(3005, 4326, always_xy=True)


def gpkg_geom(b):
    flags = b[3]; env = (flags >> 1) & 7
    return wkb.loads(bytes(b[8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[env]:]))


def ll(x, y):
    lon, lat = to_ll.transform(x, y)
    return round(lat, 5), round(lon, 5)


def outline(g, max_pts=48):
    """Simplified exterior of the largest part (polygon) or the longest part (line), lon/lat, <= max_pts points."""
    if g.geom_type.startswith('Multi') or g.geom_type == 'GeometryCollection':
        parts = list(g.geoms)
        g = max(parts, key=(lambda p: p.area) if g.geom_type != 'MultiLineString' else (lambda p: p.length))
    coords = None
    tol = max(2.0, math.sqrt(max(g.area, 1)) / 200) if g.geom_type == 'Polygon' else 5.0
    for _ in range(12):
        s = g.simplify(tol, preserve_topology=True)
        cs = list(s.exterior.coords) if s.geom_type == 'Polygon' else list(s.coords)
        coords = cs
        if len(cs) <= max_pts:
            break
        tol *= 1.8
    xs, ys = zip(*[(c[0], c[1]) for c in coords])
    lon, lat = to_ll.transform(xs, ys)
    return [[round(a, 6), round(b, 6)] for a, b in zip(lon, lat)]


# ===================================================================== inputs (light)
v1 = json.load(gzip.open(V1))['candidates']
v1ids = {int(c['id']) for c in v1}
marks = {}
for f in os.listdir(MARKS):
    if f.endswith('.json'):
        m = json.load(open(MARKS + f))
        marks[int(f[:-5])] = dict(verdict=m.get('verdict') or None, note=(m.get('note') or '').strip() or None)

CC = ['wbk', 'layer', 'fc', 'fcs', 'fc_label', 'n1', 'n2', 'sid', 'item', 'item_name', 'item_kind', 'area_m2', 'perim_m',
      'elong', 'mean_width_m', 'lineclass', 'main_flow_name', 'main_flow_blk', 'main_flow_len', 'river_item',
      'river_through', 'river_regulated', 'regulated', 'rule_own', 'rule_stream', 'rule_lake', 'rule_area',
      'rule_entries', 'lines_sig', 'lines_len', 'len_1200', 'len_1250', 'len_1300', 'len_1350', 'len_1400', 'len_1450',
      'len_1475', 'len_1000_1100', 'len_2000s', 'through_ratio', 'tile_layer', 'tile_water', 'tile_minzoom', 'gauge',
      'lat', 'lon', 'fid']
cen = pd.read_parquet(OUT + 'census.parquet', columns=CC)
cen['n1'] = cen.n1.fillna('')
# verification fix V3: a wetland's FEATURE_CODE is the code of its LARGEST piece; a Flooded Land piece inside a
# marsh/swamp wetland is only in fcs (all codes). Read fcs too.
cen['fcs'] = pd.read_parquet(OUT + 'census.parquet', columns=['fcs'])['fcs'].fillna('').values
C = cen.set_index('wbk')
ditch = pd.read_parquet(OUT + 'ditch_blks.parquet')
# verification fix V4: the census's river_regulated is blue-line-wide (an entry on ANY section of the line). Which
# water entries bind the river's sections AT the polygon (its own main-flow sections plus one up- and downstream)
# comes from verify/local_entries.py -> local_entries.json.
LOCAL = {int(k): set(v) for k, v in json.load(open(OUT + 'local_entries.json')).items()}

bd = sqlite3.connect(f'file:{BUNDLE}?mode=ro', uri=True)
items = {o: (iid, name, kind) for o, iid, name, kind in bd.execute('select ord,item_id,name,kind from item')}
item_by_id = {v[0]: v for v in items.values()}
stream_items_by_name = collections.defaultdict(list)
for iid, name, kind in items.values():
    if kind == 'stream':
        stream_items_by_name[name].append(iid)
ENT = {}
ent_by_item = collections.defaultdict(list)
for eid, iid, name, verb, scope, pages, matched in bd.execute(
        'select entry_id,item_id,name,verbatim,scope_note,pages,matched from entry'):
    ENT[eid] = dict(entry_id=eid, name=name, scope_note=scope or None,
                    pages=json.loads(pages) if pages else None, verbatim=(verb or '')[:300], _full=(verb or ''))
    for i in set(json.loads(matched or '[]')) | ({iid} if iid else set()):
        ent_by_item[i].append(eid)
sec_item = {}
for o, s in bd.execute('select ord,sid from item_section'):
    sec_item.setdefault(s, o)
sec_set = dict(bd.execute('select sid,set_id from section_ruleset'))
set_entries = collections.defaultdict(set)
for sid_, eid, via in bd.execute('select set_id, entry_id, via from ruleset'):
    set_entries[sid_].add(eid)


def cat_of(eids):
    return [{k: v for k, v in ENT[e].items() if k != '_full'} for e in sorted(set(eids)) if e in ENT]


# ===================================================================== new-candidate pools (from the census)
def pools():
    P = {}
    unnamed_nonitem = (cen.n1 == '') & (cen.item_kind == 'none')
    # B: not fishing water
    for w in cen.loc[cen.fc.isin(NOT_FISHING), 'wbk']:
        P.setdefault(int(w), 'v2:B-not-fishing')
    # C: manmade canal polygons that are not items
    for w in cen.loc[(cen.fc == 'GA03950000') & (cen.item_kind == 'none'), 'wbk']:
        P.setdefault(int(w), 'v2:C-canal-polygon')
    # D: reservoirs (any code) that are not items, with a named stream item's main flow through
    for w in cen.loc[cen.fc.isin(RESERVOIR) & (cen.item_kind == 'none') & cen.river_through, 'wbk']:
        P.setdefault(int(w), 'v2:D-reservoir-on-river')
    # F: flooded land with a named stream item's line through
    for w in cen.loc[cen.fcs.str.contains('GB11350110') & cen.river_through, 'wbk']:
        P.setdefault(int(w), 'v2:F-flooded-on-river')
    # A: unnamed, non-item lake-indefinite / intermittent with a named stream item's main flow through
    for w in cen.loc[cen.fc.isin({'GB15300130', 'GB15300140'}) & cen.river_through & unnamed_nonitem, 'wbk']:
        P.setdefault(int(w), 'v2:A-soft-lake-on-river')
    # gap: unnamed non-item lake/manmade polygons on a river regulated by a named entry
    for w in cen.loc[cen.layer.isin(['lakes', 'manmade']) & cen.river_regulated & unnamed_nonitem, 'wbk']:
        P.setdefault(int(w), 'v2:gap-on-regulated-river')
    return {w: s for w, s in P.items() if w not in v1ids}


NEW = pools()
WANT = sorted(v1ids | set(NEW))
print('v1', len(v1ids), 'new polygons', len(NEW), collections.Counter(NEW.values()))


# ===================================================================== stage 1: geometry + neighbours (cached)
def stage1():
    gp = sqlite3.connect(f'file:{GPKG}?mode=ro', uri=True)
    raw = pd.read_parquet(OUT + 'polys_raw.parquet', columns=['fid', 'wbk', 'layer'])
    raw = raw[raw.wbk.isin(WANT)]
    geoms = collections.defaultdict(list)
    for r in raw.itertuples():
        b = gp.execute(f'select geom from {r.layer} where fid=?', (int(r.fid),)).fetchone()[0]
        geoms[int(r.wbk)].append(gpkg_geom(b))
    G = {w: (unary_union(v) if len(v) > 1 else v[0]) for w, v in geoms.items()}
    print('geoms', len(G))
    # wmu for MU / region
    mus, mg = [], []
    for mu, b in gp.execute('select WILDLIFE_MGMT_UNIT_ID, geom from wmu'):
        mus.append(mu); mg.append(gpkg_geom(b))
    tree = STRtree(mg)
    out = {}
    NB = 300.0
    for n, w in enumerate(WANT):
        g = G.get(w)
        if g is None:
            continue
        rp = g.representative_point()
        mu = None
        for i in tree.query(rp, predicate='within'):
            mu = mus[i]; break
        minx, miny, maxx, maxy = g.bounds
        bb = (minx - NB, maxx + NB, miny - NB, maxy + NB)
        polys_nb = []
        for lyr in ('lakes', 'manmade', 'wetlands'):
            q = (f'select t.fid, t.WATERBODY_KEY, t.FEATURE_CODE, t.GNIS_NAME_1, t.BLUE_LINE_KEY, t.geom '
                 f'from {lyr} t join rtree_{lyr}_geom r on r.id=t.fid '
                 f'where r.maxx>=? and r.minx<=? and r.maxy>=? and r.miny<=?')
            for fid, wk, fc, nm, blk, b in gp.execute(q, (bb[0], bb[1], bb[2], bb[3])):
                if wk is not None and int(wk) == w:
                    continue
                # only keep what can matter: touching anything, or a named / manmade one nearby
                gg = gpkg_geom(b)
                d = g.distance(gg)
                if d <= 2.0 or (nm and d <= NB):
                    polys_nb.append(dict(layer=lyr, wbk=int(wk) if wk else None, fc=fc, name=nm, blk=blk,
                                         d=round(d, 1), area=round(gg.area)))
        q = ('select t.GNIS_NAME, t.BLUE_LINE_KEY, t.FEATURE_CODE, t.EDGE_TYPE, t.geom from streams t '
             'join rtree_streams_geom r on r.id=t.fid where r.maxx>=? and r.minx<=? and r.maxy>=? and r.miny<=? '
             'and t.GNIS_NAME is not null')
        lines_nb = {}
        for nm, blk, fc, et, b in gp.execute(q, (bb[0], bb[1], bb[2], bb[3])):
            d = g.distance(gpkg_geom(b))
            if d <= NB and (nm not in lines_nb or d < lines_nb[nm]['d']):
                lines_nb[nm] = dict(name=nm, blk=blk, fc=fc, et=et, d=round(d, 1))
        out[w] = dict(mu=mu, rep=ll(rp.x, rp.y), outline=outline(g), polys=polys_nb, lines=list(lines_nb.values()))
        if n % 250 == 0:
            print('neighbours', n, len(WANT), flush=True)
    # named ditch / canal lines that are not stream items (line candidates)
    dl = ditch[(ditch.name.fillna('') != '') & (~ditch.stream_item)]
    lines_c = {}
    for r in dl.itertuples():
        gs = [gpkg_geom(b) for (b,) in gp.execute('select geom from streams where BLUE_LINE_KEY=? and FEATURE_CODE in '
                                                  "('GA08800110','GA03950000')", (int(r.blk),))]
        g = unary_union(gs)
        rp = g.interpolate(0.5, normalized=True) if g.geom_type == 'LineString' else gs[0].interpolate(0.5, normalized=True)
        mu = None
        for i in tree.query(rp, predicate='within'):
            mu = mus[i]; break
        lines_c[int(r.blk)] = dict(mu=mu, rep=ll(rp.x, rp.y), outline=outline(g, 64), length=round(g.length))
    return dict(geo=out, lines_c=lines_c)


if os.path.exists(OUT + 'geo_v2.pkl') and '--fresh' not in sys.argv:
    S1 = pickle.load(open(OUT + 'geo_v2.pkl', 'rb'))
    if set(S1['geo']) < set(WANT) - {w for w in WANT if w not in C.index}:
        S1 = stage1(); pickle.dump(S1, open(WOUT + 'geo_v2.pkl', 'wb'))
else:
    S1 = stage1(); pickle.dump(S1, open(WOUT + 'geo_v2.pkl', 'wb'))
GEO, LINES_C = S1['geo'], S1['lines_c']

# blue lines inside each wanted polygon (for "shares the lake's blue line")
LI = pd.read_parquet(OUT + 'lines_in.parquet', columns=['wbk', 'blk', 'et', 'name'])
LI = LI[LI.wbk.isin(set(WANT) | {p['wbk'] for g in GEO.values() for p in g['polys'] if p['wbk']})]
named_in = set(int(w) for w in LI.loc[LI.name.notna(), 'wbk'])
blks_in = LI.groupby('wbk').blk.agg(lambda s: set(int(x) for x in s)).to_dict()

# blue line -> stream items (for new candidates' stream_item); only the blks we need
need_blk = {int(b) for b in cen.loc[cen.wbk.isin(WANT), 'main_flow_blk'].dropna()}
blk_items = collections.defaultdict(set)
with open(HANDLES) as f:
    for n, line in enumerate(f, 1):
        h = line.rstrip('\n')
        if h.startswith('lake:') or ':' not in h:
            continue
        b = int(h.rsplit(':', 1)[0])
        if b in need_blk:
            o = sec_item.get(n)
            if o is not None and items[o][2] == 'stream':
                blk_items[b].add(items[o][0])


def region_of(mu):
    if not mu:
        return None
    r = mu.split('-')[0]
    if r == '7':
        return '7a' if mu in R7A else '7b'
    return r


# ===================================================================== per-polygon evidence
def line_codes(sig):
    out = {}
    for k, (n, m) in json.loads(sig or '{}').items():
        fc, et = k.split('/')
        out[k] = dict(n=n, m=m, label=FC.get(fc, fc))
    return out


def evidence_for(w, row, stream_name, river_entries):
    g = GEO.get(w, {})
    own_blks = blks_in.get(w, set())
    # dam or reservoir
    dam = []
    if row is not None and row.fc in RESERVOIR:
        dam.append(f'own FWA code {row.fc} ({FC[row.fc]})')
    for p in g.get('polys', []):
        if p['layer'] == 'manmade' and p['d'] <= 2.0 and p['fc'] in RESERVOIR:
            dam.append(f"touches manmade {FC.get(p['fc'], p['fc'])} polygon wbk {p['wbk']}"
                       + (f" ({p['name']})" if p['name'] else ''))
        elif p['d'] <= 2.0 and p['name'] and re.search(r'\breservoir\b', p['name'], re.I):
            dam.append(f"touches {p['name']} (wbk {p['wbk']})")
    # T154: a provincial dam (BC Dams, WRIS) standing on the polygon's edge
    for d in DAMS.get(str(w), []):
        # behind it: the dam is at the polygon's OUTLET on its own blue line (dams.side_of) — a dam at its inlet
        # (Seton Canal below the Seton dam) holds back the water upstream, not this polygon
        if d['d'] <= DAM_REACH_M and d['operation'] in STANDING and d.get('side') == 'outlet':
            dam.append(f"provincial dam {d['name']} ({d['function']}, {d['operation']}"
                       + (f", {d['height_m']:g} m high" if d['height_m'] else '') + f") {d['d']:.0f} m from it")
    # (the owner's notes are NOT evidence here: a rule fed by the mark it is measured against would launder it)
    book_river = [e for e in river_entries if re.search(r'reservoir', ENT[e]['_full'] + (ENT[e]['scope_note'] or ''), re.I)]
    # part of a named lake: a NAMED lakes-layer (or named manmade) polygon touching it
    lakes = []
    for p in g.get('polys', []):
        if p['layer'] in ('lakes', 'manmade') and p['name'] and p['d'] <= 150.0 and p['wbk']:
            shared = bool(own_blks & blks_in.get(p['wbk'], set())) or (p['blk'] in own_blks)
            it = None
            iid = f"wbk:{p['wbk']}"
            if iid in item_by_id:
                it = iid
            lakes.append(dict(name=p['name'], wbk=p['wbk'], item=it, touch_m=p['d'], shares_blue_line=shared,
                              area_m2=p['area']))
    lakes.sort(key=lambda x: (-x['shares_blue_line'], x['touch_m'], -x['area_m2']))
    # naming suggestions: slough / canal / channel names on lines or polygons within 300 m
    sug = []
    for l in g.get('lines', []):
        if SLOUGHWORD.search(l['name']) and not re.search(r'\bcreek\b', l['name'], re.I):
            sug.append(dict(name=l['name'], via='FWA stream line', d=l['d'], items=stream_items_by_name.get(l['name'], [])))
    for p in g.get('polys', []):
        if p['name'] and SLOUGHWORD.search(p['name']) and not re.search(r'\bcreek\b', p['name'], re.I):
            sug.append(dict(name=p['name'], via=f"FWA {p['layer']} polygon", d=p['d'], items=[]))
    best = {}
    for s in sorted(sug, key=lambda s: s['d']):
        best.setdefault(s['name'], s)
    sug = sorted(best.values(), key=lambda s: s['d'])
    return dam, book_river, lakes, sug


# ===================================================================== classification
def shape_class(row, at_piece=True):
    """Base class for a NEW polygon candidate (no first-sweep class): the first sweep's shape rules, simplified.
    at_piece: a river entry binds the river's sections at this polygon (V4); STREAM_BY_BOOK needs it."""
    e, wd, a = row.elong or 0, row.mean_width_m or 999, row.area_m2 or 0
    if row.fc in NOT_FISHING:
        return 'NOT_FISHING_WATER', 'sewage basin / tailing pond / dump: not a fishing water'
    if a > 5e4 and e < 2.5:
        return 'LAKE', f'{a/1e4:.1f} ha, elongation {e:.1f}: a lake-shaped widening'
    if e < 2.0:
        return 'LAKE', f'round (elongation {e:.1f}): a pond on the stream, not a channel'
    if row.len_1300 + row.len_1350 > 0 and e >= 3:
        return 'LIKELY_STREAM', f'secondary-flow (1300/1350) line inside, elongation {e:.1f}'
    if row.river_regulated and at_piece and e >= 5 and wd <= 80:
        return 'STREAM_BY_BOOK', (f'river-shaped (elongation {e:.1f}, {wd:.0f} m wide) unnamed piece on '
                                  f'{row.main_flow_name}, whose book entry binds the river but not this polygon')
    if e >= 4 and wd <= 300:
        return 'LIKELY_STREAM', f'elongation {e:.1f}, mean width {wd:.0f} m on {row.main_flow_name}: river-shaped'
    return 'UNSURE', f'elongation {e:.1f}, {a/1e4:.1f} ha on {row.main_flow_name}: shape alone does not settle it'


def v1_lake_by_shape(reasons):
    t = ' '.join(reasons or [])
    return ('lake-shaped widening' in t) or ('round (elongation' in t)


def revise(base_cls, base_reason, row, ev, v1rec=None):
    """Apply the v2 rules in order. Returns (class, [reasons], [rule ids fired that changed or confirmed])."""
    dam, book_river, lakes, sug = ev
    cls, why, fired = base_cls, [], []
    if row is None:
        return cls, [base_reason], fired
    fc = row.fc
    e, wd, a = row.elong or 0, row.mean_width_m or 999, row.area_m2 or 0
    named_river = bool(row.river_through) or int(row.name) in named_in   # any named FWA line inside (wetlands carry 1000-series)
    mfn = row.main_flow_name if isinstance(row.main_flow_name, str) else None
    sname = mfn or (v1rec or {}).get('stream_name') or 'the river'
    # R0 not fishing water
    if fc in NOT_FISHING:
        return 'NOT_FISHING_WATER', [f'FWA code {fc} ({FC[fc]}): never a fishing water'], ['R0']
    # R1 part of a named lake (touching, no named river main flow of its own through it)
    part = [l for l in lakes if LAKEWORD.search(l['name']) and l['shares_blue_line']]
    # verification fix V1: a shared MAIN FLOW (1200) blue line only says the same creek also runs through the lake
    # (Miller Creek piece: a pond 140 m up an unnamed creek from Gooseneck Lake), and a river-shaped piece is a
    # river reach, not a bay (Obo River piece: elongation 14, 71 m wide). R1 needs no 1200 inside and elongation < 4.
    if part and not named_river and row.len_1200 == 0 and e < 4 and row.layer == 'lakes' and fc != 'GA03950000' and not (v1rec and SLOUGHWORD.search(v1rec.get('stream_name') or '')):
        l = part[0]
        why.append(f"its side line is a blue line of {l['name']} (wbk {l['wbk']}, {l['touch_m']:.0f} m away) and no named "
                   f"river runs through it: a bay or arm of that lake, not the river")
        return 'LAKE', why, ['R1']
    # R2 dam / reservoir
    if dam:
        if book_river:
            why.append(f"dam/reservoir evidence ({dam[0]}) but the book names the reservoir as part of the river "
                       f"({book_river[0]}): kept with the river")
            fired.append('R2-book')
        else:
            why.append(f'impounded water ({"; ".join(dam[:2])}): a widening behind a dam reads as a lake')
            return 'LAKE', why, ['R2']
    # R3 lake arm only
    if row.len_1475 > 0 and row.len_1300 == 0 and not named_river:
        why.append(f'FWA Lake Arm (1475) line inside and no secondary flow or named river: a lake arm')
        return 'LAKE', why, ['R3']
    # R4 soft lake codes on a river's line
    on_line = named_river or row.len_1300 > 0 or bool(v1rec and v1rec.get('stream_name'))
    soft = fc if fc in SOFT_LAKE else ('GB11350110' if 'GB11350110' in (getattr(row, 'fcs', '') or '') else None)
    if soft and on_line:
        round_large = a > 5e4 and e < 2.5
        # verification fix V2: the first R4 promoted every soft-coded piece that was not round AND large, so 375
        # round ponds (perimeter elongation < 2, e.g. a 3.4 ha pond on Tunkwa Creek, elongation 1.0) became
        # LIKELY_STREAM. The R4 marks are all channel-shaped (perimeter elongation 5.6 to 54) except Hatzis Slough
        # (1.1), which is a named slough. R4 now promotes only elongation >= 3 or a slough/channel name.
        sloughy = bool(SLOUGHWORD.search(' '.join([sname or '', row.n1 or ''])))
        if round_large:
            if cls != 'LAKE':
                why.append(f'{FC[soft]} but round and large ({a/1e4:.1f} ha, elongation {e:.1f}): a lake')
                return 'LAKE', why, ['R4-round']
        elif (e >= 3 or sloughy) and (cls == 'UNSURE' or (cls == 'LAKE' and (v1rec is None or v1_lake_by_shape(v1rec.get('class_reason'))))):
            why.append(f'{FC[soft]} on {sname}\'s line, channel-shaped (perimeter elongation {e:.1f})'
                       + (' and slough-named' if sloughy and e < 3 else '') + ': a slough or side channel the river fills')
            return 'LIKELY_STREAM', why, ['R4']
    # R5 secondary flow inside upgrades an UNSURE
    if cls == 'UNSURE' and row.len_1300 > 0 and e >= 3:
        why.append(f'FWA Secondary Flow (1300) line inside ({row.len_1300} m), elongation {e:.1f}: a side channel')
        return 'LIKELY_STREAM', why, ['R5']
    # R6 named river main flow through a narrow polygon upgrades an UNSURE
    if cls == 'UNSURE' and bool(row.river_through) and wd <= 40 and e >= 4:
        why.append(f'{sname} main flow runs through a narrow polygon ({wd:.0f} m, elongation {e:.1f}): the river')
        return 'LIKELY_STREAM', why, ['R6']
    return cls, why or [base_reason], fired


# ===================================================================== build records
recs = []
for c in v1:
    w = int(c['id'])
    row = C.loc[w] if w in C.index else None
    river_entries = [e['entry_id'] for e in (c.get('catalogue_entries') or [])]
    ev = evidence_for(w, row, c.get('stream_name'), river_entries)
    r = dict(c)
    r['class_v1'] = c['class']
    if r.get('region') == '7':          # first sweep wrote Region 7 unlettered; letter it from the MU like the new ones
        mu1 = r.get('mu') if isinstance(r.get('mu'), str) else ((r.get('mu') or [None])[0])
        r['region'] = region_of(mu1) or '7'
    r['class_reason_v1'] = c['class_reason']
    rc, why, fired = revise(c['class'], '; '.join(c['class_reason']), row, ev, c)
    recs.append((w, r, row, ev, rc, why, fired))

for w, src in NEW.items():
    row = C.loc[w]
    g = GEO.get(w, {})
    blk = int(row.main_flow_blk) if pd.notna(row.main_flow_blk) else None
    sitems = sorted(blk_items.get(blk, set())) if blk else []
    river_entries = sorted({e for i in sitems for e in ent_by_item.get(i, []) if e.startswith('r')})
    own_rule = set((row.rule_entries or '').split('|')) - {''}
    at_piece = bool(set(river_entries) & LOCAL.get(w, set()))
    base_cls, base_reason = shape_class(row, at_piece)
    mu = g.get('mu')
    r = dict(
        id=str(w), poly_layer=row.layer, fwa_feature_code=row.fc, name=[row.n1] if row.n1 else None,
        current_item=None, region=region_of(mu), mu=mu, lat=g.get('rep', (row.lat, row.lon))[0],
        lon=g.get('rep', (row.lat, row.lon))[1], outline=g.get('outline'), area_m2=round(float(row.area_m2)),
        elongation=float(row.elong), mean_width_m=float(row.mean_width_m),
        stream_name=(row.main_flow_name if isinstance(row.main_flow_name, str) else None), stream_item=' / '.join(sitems) or None,
        stream_item_kind='stream' if sitems else None, regs_reach_today=bool(row.rule_stream),
        entries_missing_on_polygon=sorted(set(river_entries) - own_rule),
        fwa_edge_types_inside=sorted({int(k.split('/')[1]) for k in json.loads(row.lines_sig or '{}')}),
        river_polygon_both_sides=None, evidence=[
            f"census: {row.fc_label}, line class {row.lineclass}, {row.main_flow_len} m of "
            f"{row.main_flow_name if isinstance(row.main_flow_name, str) else 'no named'} main flow inside (through_ratio {row.through_ratio})",
            f"drawn: tile layer {row.tile_layer} as {row.tile_water} from z{row.tile_minzoom}"
            if row.tile_layer else 'not drawn'],
        found_by=[src], catalogue_entries=cat_of(river_entries), class_v1=None, class_reason_v1=None)
    ev = evidence_for(w, row, row.main_flow_name, river_entries)
    rc, why, fired = revise(base_cls, base_reason, row, ev, None)
    if src == 'v2:C-canal-polygon' and rc not in ('NOT_FISHING_WATER',):
        names = [s['name'] for s in ev[3] if s['d'] <= 50] + ([row.main_flow_name] if row.main_flow_name else [])
        if river_entries:
            rc, why, fired = 'STREAM_BY_BOOK', [f'FWA canal polygon on {row.main_flow_name}; the book regulates that water ({river_entries[0]}) and a canal is a stream'], ['R7-book']
        else:
            rc, why, fired = 'LIKELY_STREAM', [f'FWA canal polygon (GA03950000), not yet a stream item'
                                               + (f'; nearby name {names[0]}' if names else '')], ['R7']
    recs.append((w, r, row, ev, rc, why, fired))

# named ditch / canal LINES that are not stream items
for d in ditch[(ditch.name.fillna('') != '') & (~ditch.stream_item)].itertuples():
    lc = LINES_C.get(int(d.blk), {})
    eids = [e for e in (d.entry_by_name or '').split('|') if e]
    cls = 'STREAM_BY_BOOK' if eids else 'LIKELY_STREAM'
    r = dict(
        id=f'blk:{d.blk}', poly_layer='streams', fwa_feature_code=d.fc, name=[d.name], current_item=None,
        region=region_of(lc.get('mu')), mu=lc.get('mu'), lat=lc.get('rep', (None, None))[0],
        lon=lc.get('rep', (None, None))[1], outline=lc.get('outline'), geom_kind='line', area_m2=None,
        elongation=None, mean_width_m=None, stream_name=d.name, stream_item=None, stream_item_kind=None,
        regs_reach_today=False, entries_missing_on_polygon=eids, fwa_edge_types_inside=[int(x) for x in str(d.edges).split(',') if x],
        river_polygon_both_sides=None,
        evidence=[f'{round(d.m)} m of FWA {"/".join(FC.get(x, x) for x in d.fc.split(","))} line(s) on blue line {d.blk}; '
                  f'{d.sections} sections; no stream item'],
        found_by=['v2:C-named-ditch-line'], catalogue_entries=cat_of(eids), class_v1=None, class_reason_v1=None)
    why = [f'named {"/".join(FC.get(x, x) for x in d.fc.split(","))} "{d.name}" is no stream item and has no sections'
           + (f'; the book names it ({eids[0]})' if eids else '')]
    recs.append((r['id'], r, None, ([], [], [], []), cls, why, ['R7-book' if eids else 'R7']))

# ===================================================================== owner marks, final records
VMAP = {'stream': 'stream', 'not_stream': 'not_stream', 'unsure': 'unsure'}


def as_verdict(cls):
    return 'stream' if cls in STREAMISH else ('unsure' if cls == 'UNSURE' else 'not_stream')


final, exceptions = [], []
for w, r, row, ev, rc, why, fired in recs:
    dam, book_river, lakes, sug = ev
    mk = marks.get(w) if isinstance(w, int) else None
    verdict = (mk or {}).get('verdict')
    cls, reason = rc, list(why)
    if verdict in VMAP:
        want = verdict
        if as_verdict(rc) != want:
            exceptions.append(dict(id=str(w), stream=r.get('stream_name'), verdict=verdict, note=mk.get('note'),
                                   rule_class=rc, rule_reason=why, fired=fired, v1=r.get('class_v1')))
            if want == 'stream':
                cls = r['class_v1'] if r.get('class_v1') in STREAMISH else 'LIKELY_STREAM'
            elif want == 'not_stream':
                cls = 'LAKE'
            else:
                cls = 'UNSURE'
            reason = [f'owner marked {verdict}' + (f' ("{mk["note"]}")' if mk.get('note') else '')
                      + f'; rules alone say {rc} ({"; ".join(why)})']
        else:
            if want == 'unsure':
                cls = 'UNSURE'
            reason = [f'owner marked {verdict}' + (f' ("{mk["note"]}")' if mk.get('note') else '')] + why
    # suggested name: owner note first, then FWA slough/canal names within 250 m
    suggested = None
    note = (mk or {}).get('note') or ''
    m_ = re.search(r'\b([A-Z][a-z]+ (?:Slough|Lake|Canal|Channel))\b', note.title() if note else '')
    if m_ and 'Slough' in m_.group(1):
        suggested = dict(name=m_.group(1), source='owner note', note=note)
    near = [s for s in sug if s['d'] <= 250]
    if not suggested and near and not (row is not None and row.n1):
        s = near[0]
        suggested = dict(name=s['name'], source=s['via'], distance_m=s['d'], items=s['items'])
    if suggested and near:
        suggested['nearby'] = [dict(name=s['name'], via=s['via'], distance_m=s['d'], items=s['items']) for s in near[:4]]
    lc = line_codes(row.lines_sig) if row is not None else {}
    if r.get('geom_kind') == 'line':
        lc = {f'{fc}/{et}': dict(label=FC.get(fc, fc)) for fc in r['fwa_feature_code'].split(',') for et in r['fwa_edge_types_inside']}
    part = [l for l in lakes if LAKEWORD.search(l['name']) and (l['shares_blue_line'] or l['touch_m'] <= 2.0)]
    out = dict(r)
    out.pop('class_reason_v1', None)
    out.update(
        fwa_feature_code=r.get('fwa_feature_code'),
        fwa_feature_label=' / '.join(FC.get(k, k) for k in (r.get('fwa_feature_code') or '').split(',') if k) or None,
        fwa_feature_codes_all=(row.fcs if row is not None else r.get('fwa_feature_code')),
        elongation_perimeter=(float(row.elong) if row is not None and pd.notna(row.elong) else None),
        river_entry_at_piece=(sorted({e['entry_id'] for e in (r.get('catalogue_entries') or [])} & LOCAL.get(w, set()))
                              if isinstance(w, int) and w in LOCAL else None),
        inside_line_codes=lc,
        dam_or_reservoir=dict(value=bool(dam), evidence=dam, book_names_as_river=book_river,
                              owner_note=(note if re.search(r'\bdamm?ed\b|\bdam\b', note, re.I) else None)),
        part_of_named_lake=(dict(part[0], others=[l['name'] for l in part[1:4]]) if part else None),
        suggested_name=suggested,
        class_v1=r.get('class_v1'), rule_class=rc, rule_ids=fired,
        **{'class': cls}, class_reason=reason,
        owner_mark=(dict(verdict=(mk or {}).get('verdict'), note=(mk or {}).get('note')) if mk else None),
        changed=(r.get('class_v1') is not None and cls != r.get('class_v1')),
        drawn=(dict(tile_layer=row.tile_layer, as_=row.tile_water, minzoom=int(row.tile_minzoom))
               if row is not None and isinstance(row.tile_layer, str) else None),
        river_through=(bool(row.river_through) if row is not None else None),
        river_regulated=(bool(row.river_regulated) if row is not None else None),
        lineclass=(row.lineclass if row is not None else 'ditch_canal_line'),
        through_ratio=(float(row.through_ratio) if row is not None and pd.notna(row.through_ratio) else None),
    )
    out['id'] = str(out['id'])
    if out.get('drawn'):
        out['drawn']['as'] = out['drawn'].pop('as_')
    final.append(out)

ORDER = {'STREAM_BY_BOOK': 0, 'LIKELY_STREAM': 1, 'UNSURE': 2, 'LAKE': 3, 'NOT_FISHING_WATER': 4}
final.sort(key=lambda x: (ORDER[x['class']], x['class_v1'] is None, x['region'] or '', -(x['elongation'] or 0)))
json.dump(dict(final=final, exceptions=exceptions), open(WOUT + 'v2_work.json', 'w'), default=str)
print('records', len(final), collections.Counter(x['class'] for x in final))

# ===================================================================== binds (for NOT_FISHING_WATER and every polygon)
for x in final:
    w = int(x['id']) if x['id'].isdigit() else None
    if w is not None and w in C.index:
        sid = C.loc[w, 'sid']
        st = sec_set.get(int(sid)) if pd.notna(sid) else None
        ents = sorted(set_entries.get(st, ())) if st is not None else []
        x['binds'] = dict(water_entries=[e for e in ents if not e.startswith('z')], zone_entries=len([e for e in ents if e.startswith('z')]))
    else:
        x['binds'] = None

# ===================================================================== outputs
def scrub(o):
    if isinstance(o, float) and math.isnan(o):
        return None
    if isinstance(o, dict):
        return {k: scrub(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [scrub(v) for v in o]
    return o


final = scrub(final)
cnt = collections.Counter(x['class'] for x in final)
byreg = collections.defaultdict(collections.Counter)
for x in final:
    byreg[x['class']][x['region'] or '?'] += 1
summary = dict(
    generated='2026-10-23', sweep=2, total=len(final), first_sweep=len(v1),
    new=len(final) - len(v1), by_class=dict(cnt),
    by_class_region={k: dict(sorted(v.items())) for k, v in byreg.items()},
    new_by_source=dict(collections.Counter(x['found_by'][0] for x in final if x['class_v1'] is None)),
    owner_marks=len(marks),
    method=('First sweep (handoff/PLAN/sloughs/candidates.json.gz) revised by rules R0-R7 derived from the owner marks, '
            'plus new candidates from the T55 v2 census (found_by v2:*). class_v1 = first-sweep class; rule_class = '
            'what the v2 rules alone say; class = rule_class unless the owner marked it, in which case the class follows '
            'the owner verdict and any disagreement is listed in sweep2.md as a rule exception.'))
with gzip.open(WOUT + 'candidates_v2.json.gz', 'wt') as f:
    json.dump(dict(summary=summary, candidates=final), f, separators=(',', ':'), default=str, allow_nan=False)

# ---------------------------------------------------------------- sweep2.md
md = []
P = md.append


def table(rows, cols):
    P('| ' + ' | '.join(cols) + ' |'); P('|' + '---|' * len(cols))
    for r in rows:
        P('| ' + ' | '.join('' if v is None else str(v).replace('|', '/') for v in r) + ' |')
    P('')


marked = [x for x in final if x['owner_mark'] and x['owner_mark']['verdict'] in VMAP]
P('# T55 second sweep: sloughs, canals and the lake feature codes\n')
P(f"{len(final)} candidates: the {len(v1)} from the first sweep, revised, plus {len(final) - len(v1)} new ones the census "
  f"found. Owner marks read: {len(marks)} ({dict(collections.Counter((m['verdict'] or 'note-only') for m in marks.values()))}). "
  "Files: candidates_v2.json.gz (same record shape as the first sweep plus the new fields), revise.py.\n")
P('Classes: ' + ', '.join(f'{k} {cnt[k]}' for k in ORDER if cnt[k]) + '.\n')

# rules + support
P('## The rules, and what the owner marks say about each\n')
RULES = [
    ('R0', 'Sewage settling basin, tailing pond or dump (FWA EA26700110 / AP90300100 / AP09200000) is NOT_FISHING_WATER.',
     'No mark touches one (none was in the first sweep). Census: all 275 are drawn as lakes.'),
    ('R1', 'A piece whose inside line is one of a named lake\'s blue lines (the lake within 150 m), with no named river main flow of its own through it, no main-flow (1200) line at all and perimeter elongation under 4, is that LAKE (a bay or arm).',
     'Owner notes: Sproat River piece "could actually be labelled as sproat lake too" (not_stream): its 1300 line is blue line 354087683, a 1450 line inside Sproat Lake 59 m away; Cruickshank "this can be on moat lake" (note only): its 1300 line is blue line 354087804, inside Moat Lake 24 m away. Touching alone is not used: 8 stream marks lie within 150 m of a named lake (Duffey, Nicola, Robert, McCauley, Hatzic...), all on a named river or a named slough.'),
    ('R2', 'A widening behind a dam or a reservoir on a river is a LAKE, unless the river\'s book entry names the reservoir as part of the river.',
     'Owner notes: Quinsam "it is a dammed area", South Nanaimo "dammed area probably a lake?", Bonaparte "probably a lake" (all three unsure). Evidence: a reservoir FWA code, a touching manmade reservoir polygon, a touching polygon named "Reservoir"'
     + (f', and (T154, --dams) a standing provincial dam (BC Dams: {"/".join(sorted(STANDING))}) within {DAM_REACH_M:g} m of the polygon, at its outlet on its own blue line.' if DAMS_FILE else
        '. There is no dam dataset in this run (T154 adds one with --dams).')
     + ' The owner notes are not used as evidence (a rule fed by the mark it is scored against would launder it).'),
    ('R3', 'Lake Arm (1475) lines inside, with no secondary flow and no named river through it, is a LAKE.',
     'No mark has a 1475 line; the rule rests on what the code means.'),
    ('R4', 'Lake-Indefinite, Lake-Intermittent or Flooded Land on a river\'s line leans LIKELY_STREAM (a slough or side channel) when channel-shaped (perimeter elongation 3 or more) or slough/channel-named; round and large (over 5 ha, elongation under 2.5) is LAKE; anything else keeps its shape class (a round pond stays LAKE).',
     'Marks: Raft River and Similkameen pieces (Intermittent), two Crooked River pieces (Indefinite) and Hatzis Slough (Flooded Land) all marked stream; perimeter elongation 5.6 to 54 except Hatzis (1.1, a named slough). R4 fired on none of them (they were already stream classes), so the marks constrain its shape floor, not its firing.'),
    ('R5', 'An UNSURE with a Secondary Flow (1300) line inside and elongation 3 or more leans LIKELY_STREAM.',
     'Marks: 41 of the 89 stream marks have a 1300 line; the one not_stream with 1300 (Sproat) is caught first by R1.'),
    ('R6', 'An UNSURE with a named river main flow (1200) through a polygon 40 m wide or less, elongation 4 or more, leans LIKELY_STREAM.',
     'Marks: river-through stream marks have median width under 30 m; see the width note below.'),
    ('R7', 'A canal polygon or named ditch/canal line that is not a stream item is a new candidate: STREAM_BY_BOOK when a book entry names it, else LIKELY_STREAM.',
     'Marks: Serpentine canal polygon and Seton Canal both marked stream.'),
]
fired_marks = collections.defaultdict(lambda: collections.Counter())
for x in marked:
    v = x['owner_mark']['verdict']
    for rid in x['rule_ids'] or ['(none)']:
        k = rid.split('-')[0]
        fired_marks[k]['agree' if as_verdict(x['rule_class']) == v else 'disagree'] += 1
all_fired = collections.Counter(r.split('-')[0] for x in final for r in x['rule_ids'])
rows = []
for rid, text, sup in RULES:
    fm = fired_marks.get(rid, {})
    rows.append((rid, text, all_fired.get(rid, 0), fm.get('agree', 0), fm.get('disagree', 0), sup))
table(rows, ['id', 'rule', 'fires on (all candidates)', 'marked: agrees', 'marked: disagrees', 'support'])

# agreement
P('## Agreement with the owner marks\n')
P(f'{len(marked)} marks with a verdict (the note-only mark is left out). Rows: owner verdict; columns: the class the rules give before the mark is applied.\n')
vs = ['stream', 'not_stream', 'unsure']
cl = list(ORDER)
for label, key in (('v2 rules', 'rule_class'), ('first sweep', 'class_v1')):
    ct = collections.Counter((x['owner_mark']['verdict'], x[key]) for x in marked)
    P(f'**{label}**\n')
    table([[v] + [ct.get((v, c), 0) for c in cl] for v in vs], ['owner \\ class'] + cl)
    agree = sum(n for (v, c), n in ct.items() if as_verdict(c) == v)
    P(f'Agreement ({label}): {agree} of {len(marked)} (stream = STREAM_BY_BOOK or LIKELY_STREAM; not_stream = LAKE or NOT_FISHING_WATER; unsure = UNSURE).\n')

P('### Rule exceptions (a rule disagrees with a mark; the class follows the owner)\n')
table([(e['id'], e['stream'], e['verdict'], e['note'] or '', e['v1'], e['rule_class'], ','.join(e['fired']) or '(no v2 rule fired; first-sweep class)',
        '; '.join(e['rule_reason'])[:160]) for e in exceptions],
      ['wbk', 'stream', 'owner', 'note', 'v1', 'rules say', 'rule', 'why'])

# width note: river_through by verdict
rt = [x for x in marked if x.get('lineclass') == 'river_through']
P('### Shape does not separate river-through pieces\n')
table([(x['id'], x['stream_name'], x['owner_mark']['verdict'], x['mean_width_m'], x['elongation'], x['through_ratio'])
       for x in sorted(rt, key=lambda x: -(x['mean_width_m'] or 0))[:14]],
      ['wbk', 'stream', 'owner', 'width m', 'elong', 'through_ratio'])
P('Seven stream marks are wider than 45 m (Heakamie 81, Big Bar 79, Dean 75, Cayoosh 69, Lachmach 60, Fiftynine 50, Fiftyseven 47) '
  'while the not_stream Big Bar piece is 51 m and unsure Quinsam 27 m. A width or through-ratio rule that made the dammed '
  'ponds LAKE would turn those stream marks too, so no such rule is used; R2 needs dam evidence instead.\n')

# changes
P('## What changed from the first sweep\n')
ch = [x for x in final if x['class_v1'] is not None]
tm = collections.Counter((x['class_v1'], x['class']) for x in ch if x['changed'])
P(f"{sum(1 for x in ch if x['changed'])} of {len(ch)} first-sweep classes changed.\n")
table([(a, b, n) for (a, b), n in tm.most_common()], ['v1', 'v2', 'n'])
byrule = collections.Counter((x['rule_ids'][0].split('-')[0] if x['rule_ids'] else ('owner mark' if x['owner_mark'] else '-'))
                             for x in ch if x['changed'])
table(sorted(byrule.items(), key=lambda t: -t[1]), ['rule that changed it', 'n'])
P('Examples:\n')
ex = collections.defaultdict(list)
for x in ch:
    if x['changed'] and len(ex[x['class_v1'] + '>' + x['class']]) < 4:
        ex[x['class_v1'] + '>' + x['class']].append(x)
table([(x['id'], x['stream_name'], x['fwa_feature_label'], f"{x['class_v1']} > {x['class']}", '; '.join(x['class_reason'])[:170])
       for v in ex.values() for x in v], ['wbk', 'stream', 'code', 'change', 'why'])

# new candidates
P('## New candidates\n')
new = [x for x in final if x['class_v1'] is None]
src = sorted({x['found_by'][0] for x in new})
ct = collections.Counter((x['found_by'][0], x['class']) for x in new)
table([[s] + [ct.get((s, c), 0) for c in cl] + [sum(ct.get((s, c), 0) for c in cl)] for s in src], ['found by'] + cl + ['n'])
regs = sorted({x['region'] or '?' for x in new}, key=lambda r: (r == '?', r))
ct = collections.Counter((x['class'], x['region'] or '?') for x in new)
table([[c] + [ct.get((c, r), 0) for r in regs] for c in cl], ['class \\ region'] + regs)
P('Strongest new ones (STREAM_BY_BOOK, then LIKELY_STREAM on a regulated river):\n')
top = [x for x in new if x['class'] == 'STREAM_BY_BOOK'] + \
      [x for x in new if x['class'] == 'LIKELY_STREAM' and x.get('river_regulated')]
table([(x['id'], x['found_by'][0], x['stream_name'], x['fwa_feature_label'] or x['fwa_feature_code'], x['region'], x['mu'],
        x['elongation'], x['mean_width_m'], f"{x['lat']},{x['lon']}", '; '.join(x['class_reason'])[:120]) for x in top[:60]],
      ['id', 'found by', 'stream', 'code', 'reg', 'mu', 'elong', 'width', 'lat,lon', 'why'])
aa = [x for x in new if x['found_by'][0] == 'v2:A-soft-lake-on-river']
P(f"Weight of the A pool: {len(aa)} unnamed Lake-Indefinite/Intermittent polygons with a named stream's main flow through; "
  f"{sum(1 for x in aa if x.get('river_regulated'))} sit on a blue line a named book entry regulates somewhere, "
  f"{sum(1 for x in aa if x.get('river_entry_at_piece'))} where that entry binds the river at the polygon, "
  f"{sum(1 for x in aa if x['region'] == '7b')} are in Region 7B. Folding the rest changes only which zone rules and colour "
  "the polygon gets, so review the regulated ones first.\n")
touch = [x for x in final if any(e.startswith('touches') for e in x['dam_or_reservoir']['evidence'])]
P(f"R2 evidence other than the polygon's own reservoir code (a touching manmade reservoir or a polygon named Reservoir): {len(touch)} "
  f"({', '.join(x['id'] + ' ' + (x['stream_name'] or '') for x in touch[:8])}).\n")
P(f'Not listed as candidates: {int((~ditch.stream_item & (ditch.name.fillna("") == "")).sum())} unnamed ditch/canal blue lines '
  'that are no stream item. They are already line sections drawn and ruled as streams; with no name there is no stream '
  'item to fold them into.\n')

# not fishing water
P('## Not fishing water (sewage basins, tailing ponds, dumps)\n')
nf = [x for x in final if x['class'] == 'NOT_FISHING_WATER']
P(f"{len(nf)} polygons. Drawn: {sum(1 for x in nf if x['drawn'])} (all as lakes in the lake tile layer). "
  f"Bound by any water or area entry: {sum(1 for x in nf if x['binds'] and x['binds']['water_entries'])}. "
  f"With a named stream's main flow through: {sum(1 for x in nf if x.get('stream_name'))}.\n")
nf_ct = collections.Counter((x['fwa_feature_label'], (x['drawn'] or {}).get('minzoom')) for x in nf)
table([(a, b, n) for (a, b), n in sorted(nf_ct.items(), key=lambda t: (t[0][0], t[0][1] or 99))], ['type', 'drawn from zoom', 'n'])
P('Those the app binds a rule to or that sit on a named creek:\n')
table([(x['id'], x['fwa_feature_label'], round((x['area_m2'] or 0) / 1e4, 2), x['stream_name'] or '', ', '.join(x['binds']['water_entries']) if x['binds'] else '',
        (x['drawn'] or {}).get('minzoom'), f"{x['lat']},{x['lon']}")
       for x in nf if x.get('stream_name') or (x['binds'] and x['binds']['water_entries'])],
      ['wbk', 'type', 'ha', 'creek through', 'binds', 'zoom', 'lat,lon'])
P('Full list: every record with class NOT_FISHING_WATER in candidates_v2.json.gz.\n')

# naming
P('## Naming suggestions\n')
nm = [x for x in final if x['suggested_name']]
P(f"{len(nm)} candidates carry a suggested name: the owner's note first, else the nearest FWA slough / canal / channel "
  "name on a line or polygon within 250 m (the polygon has no FWA name of its own). Fuller Slough is in no FWA or "
  "registry name, so it rests on the owner's note alone.\n")
table([(x['id'], x['stream_name'], x['class'], x['suggested_name']['name'], x['suggested_name']['source'],
        x['suggested_name'].get('distance_m', ''), ', '.join(x['suggested_name'].get('items') or []),
        '; '.join(f"{s['name']} {s['distance_m']} m" for s in x['suggested_name'].get('nearby', [])[1:3]))
       for x in sorted(nm, key=lambda x: (x['suggested_name']['source'] != 'owner note', x['class'] == 'LAKE', x['suggested_name'].get('distance_m', 0)))[:80]],
      ['wbk', 'stream (v1)', 'class', 'suggested', 'source', 'm', 'item', 'also near'])

P('## Part of a named lake (R1) and dam / reservoir evidence (R2)\n')
pl = [x for x in final if x['part_of_named_lake'] and 'R1' in x['rule_ids']]
table([(x['id'], x['stream_name'], x['part_of_named_lake']['name'], x['part_of_named_lake']['shares_blue_line'], x['class_v1'], x['class'],
        (x['owner_mark'] or {}).get('verdict', '')) for x in pl[:60]],
      ['wbk', 'stream', 'named lake', 'shares blue line', 'v1', 'v2', 'owner'])
dm = [x for x in final if x['dam_or_reservoir']['value'] and x['class'] != 'NOT_FISHING_WATER']
table([(x['id'], x['stream_name'], '; '.join(x['dam_or_reservoir']['evidence'])[:100], ', '.join(x['dam_or_reservoir']['book_names_as_river']),
        x['class_v1'], x['class'], (x['owner_mark'] or {}).get('verdict', '')) for x in dm[:80]],
      ['wbk', 'stream', 'evidence', 'book names as river', 'v1', 'v2', 'owner'])
# ---------------------------------------------------------------- Verification (adversarial pass, 2026-10-23)
P('## Verification\n')
P('An adversarial pass re-checked this sweep: leave-one-out against the marks, 50 pieces plotted against the FWA '
  'geometry (25 changed classes, 25 new candidates; verify/c*.png, verify/n*.png), the claimed book entries against '
  'the synopsis PDF, feature-code reads, owner-verdict compliance, and ids. Five defects were fixed in revise.py and '
  'the sweep re-run; every number above is the fixed run.\n')
P('**Fixes**\n')
P('- **V1, R1 over-reach.** R1 fired on a shared MAIN FLOW blue line, which only says the same creek also runs through '
  'the lake. Miller Creek 329070551 is a pond 140 m up an unnamed creek from Gooseneck Lake, not a bay of it. It also '
  'fired on river-shaped pieces: Obo River 329568770 has perimeter elongation 14 and is 71 m wide, a river reach '
  'between lakes. R1 now needs no 1200 line inside and perimeter elongation under 4. Sproat (3.05), Cruickshank (1.7) '
  'and Shuswap/Mabel (2.3, 1300 only) still fire. Miller goes back to UNSURE and Obo to LIKELY_STREAM, their '
  'first-sweep classes. Whatshan and Lemon were LAKE already.')
P('- **V2, R4 promoted round ponds.** R4 sent every soft-coded piece that was not both round AND over 5 ha to '
  'LIKELY_STREAM. That moved 375 round ponds (perimeter elongation under 2) to stream, for example a 3.4 ha pond on '
  'Tunkwa Creek (elongation 1.0, 92 m wide), and Stave 329292335 and July Creek 329316476 (2.2). The R4 marks are '
  'all channel-shaped (5.6 to 54) except the named Hatzis Slough. R4 now promotes only perimeter elongation 3 or more, '
  'or a slough/channel name. The rest keep their shape class: under 2 LAKE, 2 to 4 UNSURE. Oxbows such as Gataga '
  '(21) and Cameron (19) still promote. Their record `elongation` (the first sweep\'s rotated-rectangle ratio, 2.0 and '
  '1.75) under-reads a curved channel. The rules have always used the census perimeter elongation, now exported as '
  '`elongation_perimeter`.')
P('- **V3, multi-code wetlands misread.** A wetland\'s FEATURE_CODE is the code of its largest piece. Flooded Land '
  'inside a marsh/swamp wetland was invisible to pool F and R4. 71 wetlands carry Flooded Land only in their codes; '
  '3 have a named stream through them: Eightyseven Creek 328949530, Fredy Creek 328941785 and Maurice Creek '
  '328939798. Pool F and R4 now read all codes. New field `fwa_feature_codes_all`; `fwa_feature_label` now lists '
  'every code, where it used to show only the first.')
P('- **V4, STREAM_BY_BOOK on a reach the entry does not cover.** New-candidate STREAM_BY_BOOK used the census '
  'river_regulated, which is true when a named entry binds ANY section of the blue line. Several entries are reach-limited:')
P('  - Big Bar Creek: "downstream of Big Bar Lake" (p.29).')
P('  - Deadman River: "downstream of Mowich Lake" (p.29).')
P('  - Chimney Creek: "downstream of Brunson Lake" (p.44).')
P('  - Murray River: "Kinuseo Falls to signs about 2 km downstream" (p.65).')
P('  - Attichika Creek: "500 m ... of the Thorn Creek confluence" (p.59).')
P('  - Puntledge River.')
P('  Because of that, 11 new STREAM_BY_BOOK sat where the river\'s own sections carry no such entry. '
  'verify/local_entries.py now reads the entries on the river\'s sections at each polygon (its own main-flow '
  'sections plus one up- and downstream). STREAM_BY_BOOK needs one of them. New field `river_entry_at_piece`. The '
  'gap-pool headline "Big Bar Creek about 12 pieces, Deadman 4" overstated the book: 7 of its STREAM_BY_BOOK pieces (3 Big Bar, 4 Deadman) lie outside '
  'the entry\'s reach.')
P('- **V5, the rule text.** The R1 and R4 rule text and the A-pool caveat now say what the code does. The caveat '
  'counts the blue line regulated somewhere apart from the entry binding at the polygon.\n')
# leave-one-out
SUPPORT = {'R1': {'329016376'}, 'R4': {'329102746', '328966690', '329522585', '329292125', '329652477'}}
loo = 0
for x in marked:
    c = x['rule_class']
    if any(x['id'] in SUPPORT.get(rid.split('-')[0], ()) for rid in x['rule_ids']):
        c = x['class_v1'] or c          # the rule that fired was built from this very mark: drop it
    loo += as_verdict(c) == x['owner_mark']['verdict']
nom = sum(1 for x in marked if as_verdict(x['rule_class']) == x['owner_mark']['verdict'])
v1a = sum(1 for x in marked if as_verdict(x['class_v1']) == x['owner_mark']['verdict'])
fired_on_marks = collections.Counter(r.split('-')[0] for x in marked for r in x['rule_ids'])
P('**Leave-one-out (honest agreement).**\n')
P(f'- Rule class against the marks: {nom} of {len(marked)}.')
P(f'- Leave-one-out: {loo} of {len(marked)}.')
P(f'- First sweep: {v1a} of {len(marked)}.\n')
P(f'A v2 rule fired on a marked record only {sum(fired_on_marks.values())} times ({dict(fired_on_marks)}). '
  'Every other mark is scored on its first-sweep class.')
P('- The one gain, Sproat, comes from R1. R1 was written from Sproat\'s own note, with Cruickshank\'s note-only mark '
  'as the second case. Drop Sproat and only Cruickshank (24 m) is left to set the distance. The 150 m reach that '
  'catches Sproat at 59 m is a choice, not a measurement.')
P(f'- The honest out-of-sample figure is therefore {loo} of {len(marked)}, the first sweep\'s level. The v2 rules have '
  'not yet been tested on any mark they did not come from.')
P('- R4, R5 and R6 change 0 marked records. Their "support" counts only show that marks are consistent with them, '
  'because those records were stream classes already.')
P('- The 89 of 89 on stream marks is the first sweep\'s, and the sample is biased: the owner marked candidates that '
  'the first sweep showed.\n')
offv = [x['id'] for x in marked if as_verdict(x['class']) != x['owner_mark']['verdict']]
ids_ = [x['id'] for x in final]
P('**Invariants.**\n')
P(f'- Owner-marked records off their verdict: {len(offv)}.')
P(f'- Duplicate ids: {len(ids_) - len(set(ids_))}.')
P(f'- New ids colliding with first-sweep ids: {sum(1 for x in final if x["class_v1"] is None and x["id"].isdigit() and int(x["id"]) in v1ids)}.')
P('- First-sweep records all present.')
P('- The lakes-layer reservoir code GB24300000 is on 2 polygons, Williston and Dinosaur Lake. Both are lake items, '
  'so neither is a candidate, and R2 reads the reservoir code in any layer.\n')
P('**Spot checks.** 50 pieces were plotted.')
P('- Sound: the R5 and R6 promotions (side channels and narrow reaches), the R0 dumps and tailing ponds (Pukaist and '
  'Alkali Creek run through tailing ponds), the R2 reservoirs (tiny impoundments on creeks), Kitseguecla Flooded '
  'Land, and the Gataga and Cameron oxbows.')
P('- Wrong, now fixed: Miller and Obo under R1, and Stave, July Creek, Okanagan 329460279 and A-pool round ponds such '
  'as Indian Meadows, Black Angus, Gulch, Stronach and Hoffard under R4.')
P('- Book entries checked against the PDF, printed page = PDF page - 2 up to p.40 and - 6 after:')
P('  - Nelson Creek 2-8 "No Fishing" (p.25).')
P('  - Still Creek 2-8 "No Fishing" (p.26).')
P('  - Watch Creek 5-1 (p.47).')
P('  - Damdochax Creek 6-17 (p.50).')
P('  - Lachmach River 6-14 (p.52).')
P('  - Bowron River 7-7 (p.59). The Bowron oxbow 329059951 is in MU 5-16, but the app binds r7:bowron_river on '
  'the sections there, so it stays STREAM_BY_BOOK as the app has it. Whether a Region 7 entry should reach into '
  '5-16 is a question for the owner.\n')
try:
    old = json.load(gzip.open(OUT + 'verify/candidates_v2.before_verify.json.gz'))['candidates']
    oc = {x['id']: x['class'] for x in old}
    mv = collections.Counter((oc.get(x['id'], '(new)'), x['class']) for x in final if oc.get(x['id']) != x['class'])
    P('**Class moves made by the fixes** (before the verification pass, then after):\n')
    table([(a, b, n) for (a, b), n in mv.most_common()], ['before', 'after', 'n'])
except FileNotFoundError:
    pass
open(WOUT + 'sweep2.md', 'w').write('\n'.join(md))
sz = os.path.getsize(WOUT + 'candidates_v2.json.gz')
print('wrote candidates_v2.json.gz', sz, 'bytes; sweep2.md')

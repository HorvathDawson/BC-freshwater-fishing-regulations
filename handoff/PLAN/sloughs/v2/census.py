"""T55 v2 census: every polygon in FWA lakes / manmade / wetlands, keyed by WATERBODY_KEY.

Read-only on the repo. Run through scripts/heavy.sh. Stages cache to OUT so a rerun is cheap.
Writes OUT/census.parquet (one row per waterbody key) and OUT/census.md (tables A-F).
"""
import gzip, json, math, os, re, sqlite3, collections
import pandas as pd
from shapely import wkb
from pyproj import Transformer

REPO = '/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/'
OUT = '/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/t55v2/'
MARKS = '/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/t55marks/verdicts/'
GPKG = REPO + 'data/source/bc_fisheries_data.gpkg'
BUNDLE = REPO + 'data/generated/bundle/bundle.sqlite'
HANDLES = REPO + 'data/generated/atlas/full/section_handles.txt'
TILES = REPO + 'data/generated/tiles/layers/'
LAYERS = ('lakes', 'manmade', 'wetlands')
FC = {
    'GB15300000': 'Lake-Definite', 'GB15300130': 'Lake-Indefinite', 'GB15300140': 'Lake-Intermittent',
    'GB24300000': 'Reservoir-Definite', 'GB90100000': 'Reservoir-Indefinite', 'GB90100110': 'Reservoir-Intermittent',
    'EA26700110': 'Settling Basin-Sewage', 'AP90300100': 'Mine-Tailing Pond', 'AP09200000': 'Dump',
    'GA03950000': 'Canal', 'GC17100000': 'Marsh', 'GC30050000': 'Swamp', 'GB11350110': 'Flooded Land-Inundated',
    'WA24111110': 'Constr Main Flow', 'WA24111111': 'Lake Arm', 'WA24111120': 'Main Connector',
    'WA24111130': 'Secondary Flow', 'WA11410000': 'Flow Connector-Inferred', 'GA08800110': 'Ditch',
    'GA24850000': 'River/Stream-Def', 'GA24850140': 'River/Stream-Indef', 'GA24850150': 'River/Stream-Intermit',
}
os.makedirs(OUT, exist_ok=True)
gp = sqlite3.connect(f'file:{GPKG}?mode=ro', uri=True)
bd = sqlite3.connect(f'file:{BUNDLE}?mode=ro', uri=True)
to_ll = Transformer.from_crs(3005, 4326, always_xy=True)


def cached(name, fn):
    p = OUT + name
    if os.path.exists(p):
        return pd.read_parquet(p)
    df = fn()
    df.to_parquet(p)
    return df


# ---------------------------------------------------------------- 1. polygons (attrs + bbox centre)
def load_polys():
    parts = []
    for lyr in LAYERS:
        q = (f'select t.fid, t.WATERBODY_KEY wbk, t.FEATURE_CODE fc, t.GNIS_NAME_1 n1, t.GNIS_NAME_2 n2, '
             f't.GEOMETRY_Area a, t.GEOMETRY_Length p, t.BLUE_LINE_KEY pblk, '
             f'(r.minx+r.maxx)/2 cx, (r.miny+r.maxy)/2 cy '
             f'from {lyr} t left join rtree_{lyr}_geom r on r.id=t.fid')
        d = pd.read_sql(q, gp)
        d['layer'] = lyr
        parts.append(d)
    return pd.concat(parts, ignore_index=True)


polys = cached('polys_raw.parquet', load_polys)
print('polygon rows', len(polys), polys.wbk.isna().sum(), 'without wbk')


# ---------------------------------------------------------------- 2. lines inside (streams by WATERBODY_KEY)
def load_lines():
    q = ('select WATERBODY_KEY wbk, FEATURE_CODE fc, EDGE_TYPE et, LENGTH_METRE len, GNIS_NAME name, '
         'BLUE_LINE_KEY blk, STREAM_ORDER ord from streams where WATERBODY_KEY > 0')
    return pd.read_sql(q, gp)


def load_ditch_lines():
    q = ("select FEATURE_CODE fc, EDGE_TYPE et, LENGTH_METRE len, GNIS_NAME name, BLUE_LINE_KEY blk, "
         "WATERBODY_KEY wbk, DOWNSTREAM_ROUTE_MEASURE dm, fid from streams where FEATURE_CODE in ('GA08800110','GA03950000')")
    return pd.read_sql(q, gp)


lines = cached('lines_in.parquet', load_lines)
ditch = cached('ditch_lines.parquet', load_ditch_lines)
lines['wbk'] = lines.wbk.astype('int64')
print('lines with wbk', len(lines), 'ditch/canal lines', len(ditch))


# ---------------------------------------------------------------- 3. app side: handles, items, rules, gauges, tiles
def load_app():
    sid_of_lake = {}
    blk_sids = collections.defaultdict(list)
    with open(HANDLES) as f:
        for n, line in enumerate(f, 1):
            h = line.rstrip('\n')
            if h.startswith('lake:'):
                sid_of_lake[int(h[5:])] = n
            else:
                b, m = h.rsplit(':', 1)
                blk_sids[int(b)].append((int(float(m)), n))
    return sid_of_lake, blk_sids


sid_of_lake, blk_sids = load_app()
items = {o: (iid, name, kind) for o, iid, name, kind in bd.execute('select ord,item_id,name,kind from item')}
iid_ord = {v[0]: o for o, v in items.items()}
sec_item = {}
for o, s in bd.execute('select ord,sid from item_section'):
    sec_item.setdefault(s, o)
shape1 = {s for (s,) in bd.execute('select sid from section_span where shape=1')}
# a set is "water-ruled" when any of its rules comes from a water entry (r*), not a zone (z*)
set_water = collections.defaultdict(lambda: [False, False])  # [reach, trib]
# where a set's reach rules come from: the kind of the item its water entry names ('area' = a watershed /
# area entry with no water item, e.g. "Liard River watershed")
entry_kind = {}
_ik = {v[0]: v[2] for v in items.values()}
for eid, iid in bd.execute('select entry_id, item_id from entry'):
    entry_kind[eid] = (iid, _ik.get(iid, 'area') if iid else 'area')
set_src = collections.defaultdict(set)
for sid_, eid, via in bd.execute('select set_id, entry_id, via from ruleset'):
    if not eid.startswith('z'):
        set_water[sid_][0 if via == 'reach' else 1] = True
        if via == 'reach':
            set_src[sid_].add(eid)
sec_set = dict(bd.execute('select sid,set_id from section_ruleset'))
sec_gauge = dict(bd.execute('select sid,station from section_gauge'))
lake_gauge = collections.defaultdict(list)
for iid, st in bd.execute('select item_id, station from lake_gauge'):
    lake_gauge[iid].append(st)
entry_items = collections.defaultdict(list)
entry_names = {}
for eid, iid, name, matched in bd.execute('select entry_id,item_id,name,matched from entry'):
    entry_names[eid] = name
    ids = set(json.loads(matched or '[]'))
    if iid:
        ids.add(iid)
    for i in ids:
        entry_items[i].append(eid)


def tile_index():
    p = OUT + 'tiles_idx.json'
    if os.path.exists(p):
        return json.load(open(p))
    rx_s = re.compile(r'"section_id":\s*(\d+)'); rx_w = re.compile(r'"water":\s*"(\w+)"')
    rx_z = re.compile(r'"minzoom":\s*(\d+)')
    idx = {}
    for lyr in ('lake', 'wetland'):
        with open(TILES + f'{lyr}.geojsonl') as f:
            for line in f:
                tail = line[line.rfind('"properties"'):]
                m = rx_s.search(tail)
                if not m:
                    continue
                w = rx_w.search(tail); z = rx_z.search(tail)
                idx[m.group(1)] = [lyr, w.group(1) if w else None, int(z.group(1)) if z else None]
    json.dump(idx, open(p, 'w'))
    return idx


tiles = tile_index()
print('tile polygons', len(tiles), collections.Counter((v[0], v[1]) for v in tiles.values()))

# per blue line: which stream items it carries, and whether any of its sections has a water rule
blk_info = {}
for b, lst in blk_sids.items():
    its = set(); wr = False
    for _, s in lst:
        o = sec_item.get(s)
        if o is not None and items[o][2] == 'stream':
            its.add(o)
        st = sec_set.get(s)
        # regulated river = a reach rule from a named water entry (not a watershed / area entry)
        if st is not None and any(entry_kind.get(e, (None, 'area'))[1] != 'area' for e in set_src.get(st, ())):
            wr = True
    blk_info[b] = (its, wr)


# ---------------------------------------------------------------- 4. per-waterbody census
def build():
    P = polys.dropna(subset=['wbk']).copy()
    P['wbk'] = P.wbk.astype('int64')
    P = P.sort_values('a', ascending=False)
    g = P.groupby('wbk', sort=False)
    base = g.agg(layer=('layer', 'first'), fc=('fc', 'first'),
                 fcs=('fc', lambda s: ','.join(sorted(set(s)))), nrows=('fid', 'size'),
                 area_m2=('a', 'sum'), perim_m=('p', 'sum'), fid=('fid', 'first'),
                 cx=('cx', 'first'), cy=('cy', 'first'),
                 n1=('n1', lambda s: '|'.join(sorted({x for x in s if isinstance(x, str)}))),
                 n2=('n2', lambda s: '|'.join(sorted({x for x in s if isinstance(x, str)})))).reset_index()
    lon, lat = to_ll.transform(base.cx.values, base.cy.values)
    base['lat'] = lat.round(5); base['lon'] = lon.round(5)
    A, Pm = base.area_m2.values, base.perim_m.values
    el = []
    for a, p in zip(A, Pm):
        disc = (p / 4) ** 2 - a
        if disc > 0 and p / 4 - math.sqrt(disc) > 0:
            el.append((p / 4 + math.sqrt(disc)) / (p / 4 - math.sqrt(disc)))
        else:
            el.append(1.0)
    base['elong'] = [round(x, 2) for x in el]
    base['mean_width_m'] = (2 * base.area_m2 / base.perim_m).round(1)

    # lines inside
    L = lines[lines.wbk.isin(set(base.wbk))]
    agg = L.groupby(['wbk', 'fc', 'et']).agg(n=('len', 'size'), m=('len', 'sum')).reset_index()
    sig = collections.defaultdict(dict)
    for r in agg.itertuples():
        sig[r.wbk][f'{r.fc}/{r.et}'] = [int(r.n), round(float(r.m))]
    tot = L.groupby('wbk').len.sum()
    by_et = L.groupby(['wbk', 'et']).len.sum().unstack(fill_value=0)

    # named main flow (1200) through: longest named construction-main-flow blk
    mf = L[(L.et == 1200) & L.name.notna()]
    mfb = mf.groupby(['wbk', 'blk', 'name']).len.sum().reset_index().sort_values('len', ascending=False)
    mfb = mfb.drop_duplicates('wbk').set_index('wbk')
    # any 1200 at all, and the longest blk of any edge (for unnamed through-lines)
    rows = []
    for r in base.itertuples():
        w = r.wbk
        sid = sid_of_lake.get(w)
        o = sec_item.get(sid) if sid else None
        if o is None:                       # a split lake: the item keeps the wbk id, its sections are the split parts
            o = iid_ord.get(f'wbk:{w}')
        it = items.get(o) if o is not None else None
        st = sec_set.get(sid) if sid else None
        wr = set_water[st] if st is not None else [False, False]
        t = tiles.get(str(sid)) if sid else None
        mfn = mfb.loc[w] if w in mfb.index else None
        rblk = int(mfn.blk) if mfn is not None else None
        rits, rwr = blk_info.get(rblk, (set(), False)) if rblk else (set(), False)
        e = lambda k: float(by_et.at[w, k]) if (w in by_et.index and k in by_et.columns) else 0.0
        rows.append(dict(
            wbk=w, sid=sid, item=it[0] if it else None, item_name=it[1] if it else None,
            item_kind=it[2] if it else 'none', shape1=bool(sid in shape1),
            book_entries='|'.join(entry_items.get(it[0], [])) if it else '',
            water_rule_reach=wr[0], water_rule_trib=wr[1], has_ruleset=st is not None,
            rule_own=bool(it) and any(entry_kind.get(e, (None,))[0] == it[0] for e in set_src.get(st, ())),
            rule_stream=any(entry_kind.get(e, (None, ''))[1] == 'stream' and not (it and entry_kind[e][0] == it[0]) for e in set_src.get(st, ())),
            rule_lake=any(entry_kind.get(e, (None, ''))[1] in ('lake', 'wetland') and not (it and entry_kind[e][0] == it[0]) for e in set_src.get(st, ())),
            rule_area=any(entry_kind.get(e, (None, ''))[1] == 'area' for e in set_src.get(st, ())),
            rule_entries='|'.join(sorted(e for e in set_src.get(st, ()) if entry_kind.get(e, (None, ''))[1] != 'area')),
            gauge=sec_gauge.get(sid) or ('|'.join(lake_gauge.get(it[0], [])) if it else '') or '',
            tile_layer=t[0] if t else None, tile_water=t[1] if t else None, tile_minzoom=t[2] if t else None,
            lines_sig=json.dumps(sig.get(w, {})), lines_len=round(float(tot.get(w, 0.0))),
            len_1200=round(e(1200)), len_1250=round(e(1250)), len_1300=round(e(1300)), len_1475=round(e(1475)),
            len_1350=round(e(1350)), len_1400=round(e(1400)), len_1450=round(e(1450)), len_1410=round(e(1410)),
            len_1350_1450=round(e(1350) + e(1400) + e(1450) + e(1410) + e(1425)),
            len_1000_1100=round(e(1000) + e(1050) + e(1100) + e(1150)), len_2000s=round(e(2000) + e(2100) + e(2300)),
            main_flow_name=mfn['name'] if mfn is not None else None, main_flow_blk=rblk,
            main_flow_len=round(float(mfn.len)) if mfn is not None else 0,
            river_item=('|'.join(items[o2][1] for o2 in rits) if rits else None),
            river_through=bool(rits), river_regulated=bool(rits) and rwr))
    df = base.merge(pd.DataFrame(rows), on='wbk')
    df['fc_label'] = df.fc.map(FC).fillna(df.fc)
    df['through_ratio'] = (df.main_flow_len / (df.perim_m / 2)).round(2)
    df['drawn'] = df.tile_layer.notna()
    df['regulated'] = df.rule_own | df.rule_stream | df.rule_lake
    df['lineclass'] = [lineclass(r) for r in df.itertuples()]
    return df


def lineclass(r):
    named = isinstance(r.main_flow_name, str) and r.main_flow_name != ''
    if r.lines_len == 0:
        return 'no_lines'
    if r.len_1200 > 0 and r.river_through:
        return 'river_through'            # named main flow (1200) of a stream item's blue line passes through
    if named:
        return 'main_flow_named'          # named 1200, blue line is no stream item
    if r.len_1475 > 0:
        return 'lake_arm'                 # any 1475 (Lake Arm) inside
    if r.len_1300 + r.len_1350 > 0:
        return 'secondary_flow'           # 1300 / 1350 inside
    if r.len_1250 > 0:
        return 'main_connector'
    if r.len_1200 > 0:
        return 'main_flow_unnamed'
    if r.len_1000_1100 > 0:
        return 'blueline_1000s'           # wetlands: ordinary stream lines run through
    if r.len_1400 + r.len_1450 + r.len_1410 > 0:
        return 'only_1400_1450'
    if r.len_2000s > 0:
        return 'ditch_canal_lines'
    return 'other'


cen = cached('census.parquet', build)
print('census rows', len(cen))


# ---------------------------------------------------------------- 5. representative point for examples
def rep_point(layer, fid):
    b = gp.execute(f'select geom from {layer} where fid=?', (int(fid),)).fetchone()[0]
    flags = b[3]; env = (flags >> 1) & 7
    off = 8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[env]
    p = wkb.loads(bytes(b[off:])).representative_point()
    x, y = to_ll.transform(p.x, p.y)
    return round(y, 5), round(x, 5)


# ---------------------------------------------------------------- 6. candidates + marks
cand = json.load(gzip.open(REPO + 'handoff/PLAN/sloughs/candidates.json.gz'))['candidates']
cd = pd.DataFrame([{k: c.get(k) for k in ('id', 'poly_layer', 'fwa_feature_code', 'class', 'stream_name', 'current_item')} for c in cand])
marks = {}
for f in os.listdir(MARKS):
    if f.endswith('.json'):
        m = json.load(open(MARKS + f))
        marks[int(f[:-5])] = (m.get('verdict') or 'note-only', m.get('note') or '')
cd['verdict'] = cd.id.map(lambda i: marks.get(i, (None,))[0])
cd['note'] = cd.id.map(lambda i: marks.get(i, (None, ''))[1])
C = cd.merge(cen, left_on='id', right_on='wbk', how='left')


# ---------------------------------------------------------------- 7. report
md = []
def h(t): md.append(f'\n## {t}\n')
def to_md(df):
    df = df.reset_index(drop=True) if not isinstance(df.columns, pd.MultiIndex) else df
    cols = [str(c) for c in df.columns]
    f = lambda v: '' if v is None or (isinstance(v, float) and math.isnan(v)) else ((str(int(round(v))) if abs(v) >= 100 else f'{v:.2f}'.rstrip('0').rstrip('.')) if isinstance(v, float) else str(v).replace('|', '/'))
    out = ['| ' + ' | '.join(cols) + ' |', '|' + '---|' * len(cols)]
    out += ['| ' + ' | '.join(f(v) for v in row) + ' |' for row in df.itertuples(index=False)]
    return '\n'.join(out)
def tbl(df):
    md.append(to_md(df) if len(df) else '_(none)_')
    md.append('')
def ct(df, *cols):
    return df.groupby(list(cols), dropna=False).size().reset_index(name='n').sort_values('n', ascending=False)
EXC = ['wbk', 'fc_label', 'n1', 'item_name', 'item_kind', 'area_m2', 'elong', 'mean_width_m', 'lineclass',
       'main_flow_name', 'river_item', 'regulated', 'rule_entries', 'lat', 'lon']
def ex(df, n=10, cols=EXC, sort='area_m2'):
    d = df.sort_values(sort, ascending=False).head(n).copy()
    pts = [rep_point(r.layer, r.fid) for r in d.itertuples()]
    d['lat'] = [p[0] for p in pts]; d['lon'] = [p[1] for p in pts]
    tbl(d[[c for c in cols if c in d.columns]])

md.append('# T55 v2 census: FWA lake / manmade / wetland polygons\n')
md.append(f'Read-only census, one row per WATERBODY_KEY ({len(cen):,} waterbodies from {len(polys):,} polygon rows). '
          'Sources: bc_fisheries_data.gpkg, bundle.sqlite, section_handles.txt, tiles/layers lake+wetland. '
          '"water rule" = the polygon\'s own section carries a rule from a water entry (r*), by reach; zone rules (z*) '
          'do not count. "drawn" = the polygon is a feature in the lake or wetland tile layer. "river through" = a NAMED '
          'Construction-Main-Flow (edge 1200) line inside whose blue line carries a stream item. Lat/lon in examples are '
          'representative points inside the polygon.\n')
md.append('### Census by feature code\n')
tbl(cen.assign(item=cen.item_kind, rule=cen.water_rule_reach).groupby(['layer', 'fc_label']).agg(
    n=('wbk', 'size'), drawn=('drawn', 'sum'), lake_item=('item_kind', lambda s: (s == 'lake').sum()),
    wetland_item=('item_kind', lambda s: (s == 'wetland').sum()), stream_item=('item_kind', lambda s: (s == 'stream').sum()),
    rule_own=('rule_own', 'sum'), rule_from_stream=('rule_stream', 'sum'), rule_from_lake=('rule_lake', 'sum'),
    rule_area_only=('rule_area', lambda s: (s & ~cen.loc[s.index, 'regulated']).sum()),
    named_fwa=('n1', lambda s: (s != '').sum()), river_through=('river_through', 'sum')).reset_index().sort_values('n', ascending=False))
md.append('rule_own = a reach rule from a book entry for this polygon\'s own item; rule_from_stream / rule_from_lake = a reach '
          'rule from another water\'s entry (e.g. a river entry binding a lake polygon on its course); rule_area_only = only '
          'watershed / area entries (e.g. "Liard River watershed"), which bind every polygon inside.\n')
md.append('### Line class by feature code (lakes + manmade)\n')
lm = cen[cen.layer != 'wetlands']
tbl(pd.crosstab(lm.fc_label, lm.lineclass).reset_index())

# A
h('A. Lake-Indefinite and Lake-Intermittent that the app treats as lakes')
A = cen[cen.fc.isin(['GB15300130', 'GB15300140'])]
A_app = A[(A.item_kind == 'lake') | A.regulated | (A.n1 != '')]
md.append(f'All: {len(A):,} ({(A.fc=="GB15300130").sum():,} indefinite, {(A.fc=="GB15300140").sum():,} intermittent); '
          f'drawn {A.drawn.sum():,}; lake item {(A.item_kind=="lake").sum():,}; stream item {(A.item_kind=="stream").sum():,}; '
          f'FWA-named {(A.n1!="").sum():,}; regulated by a named water\'s rule {A.regulated.sum():,} (own {A.rule_own.sum()}, from a river entry {A.rule_stream.sum()}, from a lake entry {A.rule_lake.sum()}).\n')
md.append('Named, a lake item, or carrying a water rule, by line class:\n')
tbl(pd.crosstab([A_app.fc_label, A_app.item_kind], A_app.lineclass).reset_index())
md.append('All Lake-Indefinite/Intermittent by line class (drawn as lake polygons whatever their item):\n')
tbl(pd.crosstab(A.fc_label, A.lineclass).reset_index())
md.append('Main flow (1200) inside vs lake-arm (1475) only, among those the app names/regulates as a lake item:\n')
Al = A[A.item_kind == 'lake']
tbl(pd.DataFrame([dict(group=k, n=int(v)) for k, v in dict(
    lake_items=len(Al), main_flow_any=(Al.len_1200 > 0).sum(), named_main_flow=Al.main_flow_name.notna().sum(),
    river_through=Al.river_through.sum(), river_through_regulated=Al.river_regulated.sum(),
    lake_arm_any=(Al.len_1475 > 0).sum(), secondary_any=(Al.len_1300 + Al.len_1350 > 0).sum(),
    no_lines=(Al.lineclass == 'no_lines').sum()).items()]))
md.append('Examples: Lake-Indefinite/Intermittent lake items with a river through (largest):\n')
ex(Al[Al.river_through])
md.append('Examples: Lake-Indefinite/Intermittent lake items with no named line through (headwater / isolated basins, oxbows?):\n')
ex(Al[~Al.river_through & Al.main_flow_name.isna()])
md.append('Examples: Lake-Indefinite/Intermittent with any Lake Arm (1475) line:\n')
ex(A[(A.len_1475 > 0)])
md.append('Examples: Lake-Indefinite/Intermittent, NOT items, river through, elongated (>= 4), drawn as lake:\n')
ex(A[(A.item_kind == 'none') & A.river_through & (A.elong >= 4)])
md.append('Examples: Lake-Indefinite/Intermittent, not items, a RIVER ENTRY\'s rule binds the polygon (regulated as part of the river, drawn as lake):\n')
ex(A[(A.item_kind == 'none') & A.rule_stream])
md.append(f'Counts: not-an-item and a river entry binds it: {((A.item_kind=="none") & A.rule_stream).sum()}; of those river_through: {((A.item_kind=="none") & A.rule_stream & A.river_through).sum()}.\n')

# B
h('B. Sewage settling basins, tailing ponds, dumps')
Bd = cen[cen.fc.isin(['EA26700110', 'AP90300100', 'AP09200000'])]
tbl(Bd.groupby('fc_label').agg(n=('wbk', 'size'), drawn=('drawn', 'sum'), has_ruleset=('has_ruleset', 'sum'),
    any_item=('item', lambda s: s.notna().sum()), named=('n1', lambda s: (s != '').sum()),
    regulated=('regulated', 'sum'), rule_area=('rule_area', 'sum'), water_rule_trib=('water_rule_trib', 'sum'),
    gauge=('gauge', lambda s: (s != '').sum()), lines_inside=('lines_len', lambda s: (s > 0).sum()),
    river_through=('river_through', 'sum')).reset_index())
bad = Bd[Bd.item.notna() | Bd.regulated | Bd.rule_area | Bd.water_rule_trib | (Bd.gauge != '') | (Bd.n1 != '')]
md.append(f'{len(bad)} have an item, a name, a non-zone water rule (any source, reach or trib) or a gauge:\n')
ex(bad, n=40, cols=EXC[:5] + ['regulated', 'rule_area', 'rule_entries', 'water_rule_trib', 'gauge', 'tile_water', 'tile_minzoom', 'lat', 'lon'])
md.append('Examples: largest drawn ones:\n')
ex(Bd[Bd.drawn], cols=EXC + ['tile_minzoom'])

# C
h('C. Canals and ditches')
Cp = cen[cen.fc == 'GA03950000']
md.append(f'Manmade canal polygons: {len(Cp)}\n')
ex(Cp, n=20, cols=EXC[:5] + ['book_entries', 'lineclass', 'regulated', 'rule_entries', 'lat', 'lon'])
md.append('Canal-coded polygons in any layer (feature codes mixed per key):\n')
tbl(ct(cen[cen.fcs.str.contains('GA03950000')], 'layer', 'item_kind'))
# ditch/canal lines by blue line
dl = ditch.copy()
dl['blk'] = dl.blk.astype('int64')
db = dl.groupby('blk').agg(fc=('fc', lambda s: ','.join(sorted(set(s)))), n=('len', 'size'), m=('len', 'sum'),
                           name=('name', lambda s: next((x for x in s if isinstance(x, str)), '')),
                           edges=('et', lambda s: ','.join(str(x) for x in sorted(set(s)))),
                           fid=('fid', 'first')).reset_index()
def blk_status(b):
    lst = blk_sids.get(b, [])
    its = {sec_item.get(s) for _, s in lst} - {None}
    wr = any(entry_kind.get(e, (None, 'area'))[1] != 'area' for _, s in lst if s in sec_set for e in set_src.get(sec_set[s], ()))
    kinds = {items[o][2] for o in its}
    return pd.Series(dict(sections=len(lst), items='|'.join(items[o][1] for o in its), item_kinds=','.join(sorted(kinds)),
                          stream_item=('stream' in kinds), water_rule=wr,
                          book_entries='|'.join(sorted({e for o in its for e in entry_items.get(items[o][0], [])}))))
db = pd.concat([db, db.blk.apply(blk_status)], axis=1)
norm = lambda s: re.sub(r'[^a-z]', '', (s or '').lower())
entry_by_norm = collections.defaultdict(list)
for eid, n in entry_names.items():
    entry_by_norm[norm(n)].append(eid)
db['entry_by_name'] = db.name.map(lambda n: '|'.join(entry_by_norm.get(norm(n), [])) if n else '')
md.append(f'Ditch/canal LINES (streams layer GA08800110 / GA03950000): {len(dl):,} lines on {len(db):,} blue lines, '
          f'{db.m.sum()/1000:,.0f} km. Named blue lines: {(db.name!="").sum():,}.\n')
tbl(db.groupby(['fc', 'stream_item']).agg(blks=('blk', 'size'), km=('m', lambda s: round(s.sum() / 1000)),
    named=('name', lambda s: (s != '').sum()), with_sections=('sections', lambda s: (s > 0).sum()),
    water_rule=('water_rule', 'sum'), book_entry=('book_entries', lambda s: (s != '').sum()),
    entry_by_name=('entry_by_name', lambda s: (s != '').sum())).reset_index())
md.append('Named ditch/canal blue lines that are NOT a stream item (longest):\n')
nd = db[(db.name != '') & ~db.stream_item].sort_values('m', ascending=False).head(25)
def line_pt(fid):
    b = gp.execute('select geom from streams where fid=?', (int(fid),)).fetchone()[0]
    flags = b[3]; env = (flags >> 1) & 7
    g = wkb.loads(bytes(b[8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[env]:]))
    p = g.interpolate(0.5, normalized=True) if hasattr(g, 'interpolate') else g.representative_point()
    x, y = to_ll.transform(p.x, p.y); return round(y, 5), round(x, 5)
if len(nd):
    pts = [line_pt(f) for f in nd.fid]; nd = nd.assign(lat=[p[0] for p in pts], lon=[p[1] for p in pts])
tbl(nd[['blk', 'name', 'fc', 'edges', 'n', 'm', 'sections', 'items', 'item_kinds', 'entry_by_name', 'lat', 'lon']].round(0) if len(nd) else nd)
md.append('Ditch/canal blue lines a book entry names (via item or by name):\n')
ne = db[(db.book_entries != '') | (db.entry_by_name != '')].sort_values('m', ascending=False)
tbl(ne[['blk', 'name', 'fc', 'n', 'm', 'items', 'item_kinds', 'water_rule', 'book_entries', 'entry_by_name']].round(0).head(40))
db.to_parquet(OUT + 'ditch_blks.parquet')

# D
h('D. Reservoirs')
D = cen[cen.fc.isin(['GB24300000', 'GB90100000', 'GB90100110'])]
tbl(D.groupby(['fc_label', 'item_kind']).agg(n=('wbk', 'size'), drawn=('drawn', 'sum'), named=('n1', lambda s: (s != '').sum()),
    river_through=('river_through', 'sum'), river_regulated=('river_regulated', 'sum'),
    regulated=('regulated', 'sum'), rule_from_stream=('rule_stream', 'sum'), book_entry=('book_entries', lambda s: (s != '').sum())).reset_index())
tbl(pd.crosstab(D.fc_label, D.lineclass).reset_index())
md.append('Examples: reservoir lake items with a regulated river running through ("dammed area"):\n')
ex(D[(D.item_kind == 'lake') & D.river_regulated], n=15, cols=EXC + ['through_ratio', 'book_entries'])
md.append('Examples: reservoirs NOT items that a river entry\'s rule binds:\n')
ex(D[(D.item_kind == 'none') & D.rule_stream], n=15, cols=EXC + ['through_ratio'])
md.append('Examples: reservoirs with a river through and NO item (drawn as plain lake):\n')
ex(D[(D.item_kind == 'none') & D.river_through], cols=EXC + ['through_ratio'])
md.append('Examples: reservoirs already folded into a stream item:\n')
ex(D[D.item_kind == 'stream'], cols=EXC + ['through_ratio'])

# E
h('E. Lake-arm evidence across the 865 candidates and the owner\'s marks')
md.append(f'Candidates joined to the census: {C.wbk.notna().sum()} of {len(C)}.\n')
tbl(pd.crosstab(C['class'], C.lineclass.fillna('not_in_census'), margins=True).reset_index())
PR = dict(n=('id', 'size'), any_1200=('len_1200', lambda s: (s > 0).sum()), named_1200=('main_flow_name', lambda s: s.notna().sum()),
          river_through=('river_through', 'sum'), lake_arm_1475=('len_1475', lambda s: (s > 0).sum()),
          main_conn_1250=('len_1250', lambda s: (s > 0).sum()), secondary_1300=('len_1300', lambda s: (s > 0).sum()),
          side_1350=('len_1350', lambda s: (s > 0).sum()), e1400=('len_1400', lambda s: (s > 0).sum()),
          e1450=('len_1450', lambda s: (s > 0).sum()), bl_1000s=('len_1000_1100', lambda s: (s > 0).sum()),
          regulated=('regulated', 'sum'), rule_from_stream=('rule_stream', 'sum'))
md.append('Presence of each line code (count of polygons with any line of that edge type) by candidate class:\n')
tbl(C.groupby('class').agg(**PR).reset_index())
md.append('Same, by owner verdict:\n')
tbl(C[C.verdict.notna()].groupby('verdict').agg(**PR).reset_index())
md.append('Province-wide, for scale (lakes layer):\n')
LK = cen[cen.layer == 'lakes'].rename(columns={'wbk': 'id'})
tbl(LK.groupby('fc_label').agg(**PR).reset_index())
md.append('Owner marks (98) by line class:\n')
M = C[C.verdict.notna()]
tbl(pd.crosstab(M.verdict, M.lineclass.fillna('not_in_census'), margins=True).reset_index())
md.append('Owner marks by FWA feature code:\n')
tbl(pd.crosstab(M.verdict, M.fc_label.fillna('?'), margins=True).reset_index())
md.append('Median metrics by verdict:\n')
tbl(M.groupby('verdict').agg(n=('id', 'size'), elong=('elong', 'median'), width=('mean_width_m', 'median'),
    area=('area_m2', 'median'), through=('through_ratio', 'median'), river_through=('river_through', 'mean'),
    len1200=('len_1200', lambda s: (s > 0).mean()), len1475=('len_1475', lambda s: (s > 0).mean()),
    len1300=('len_1300', lambda s: (s > 0).mean())).round(2).reset_index())
md.append('Every non-stream / unsure / note mark:\n')
nm = M[(M.verdict != 'stream') | (M.note != '')]
tbl(nm[['id', 'verdict', 'note', 'stream_name', 'fc_label', 'item_kind', 'item_name', 'lineclass', 'elong',
        'mean_width_m', 'area_m2', 'main_flow_name', 'rule_entries', 'lines_sig', 'lat', 'lon']])
md.append('Median metrics by candidate class:\n')
tbl(C.groupby('class').agg(n=('id', 'size'), elong=('elong', 'median'), width=('mean_width_m', 'median'),
    through=('through_ratio', 'median'), river_through=('river_through', 'mean'),
    len1475=('len_1475', lambda s: (s > 0).mean())).round(2).reset_index())

# F
h('F. Flooded land and Lake-Intermittent on a river: stream candidates')
F = cen[(cen.fc == 'GB11350110') | ((cen.fc == 'GB15300140') & cen.river_through)]
tbl(F.groupby(['fc_label', 'item_kind', 'river_regulated']).agg(n=('wbk', 'size'), drawn=('drawn', 'sum'),
    in_candidates=('wbk', lambda s: s.isin(set(cd.id)).sum()), regulated=('regulated', 'sum'), rule_area=('rule_area', 'sum')).reset_index())
md.append('Flooded land by line class:\n')
Fl = cen[cen.fc == 'GB11350110']
tbl(ct(Fl, 'lineclass', 'item_kind'))
md.append('Examples: flooded land with a river through (largest):\n')
ex(Fl[Fl.river_through], cols=EXC + ['through_ratio'])
md.append('Examples: Lake-Intermittent with a regulated river through AND bound by that river entry rule, not yet a stream item (largest):\n')
ex(F[(F.fc == 'GB15300140') & F.river_regulated & (F.item_kind != 'stream') & F.rule_stream], cols=EXC + ['through_ratio'], sort='area_m2')
md.append('Not yet in the 865 candidates: Lake-Intermittent / Flooded land, regulated river through, not a stream item:\n')
nf = F[F.river_regulated & (F.item_kind != 'stream') & ~F.wbk.isin(set(cd.id))]
md.append(f'{len(nf)} waterbodies.\n')
ex(nf, n=15, cols=EXC + ['through_ratio'], sort='elong')

open(OUT + 'census.md', 'w').write('\n'.join(md))
C.to_parquet(OUT + 'candidates_census.parquet')
print('wrote', OUT + 'census.md')

"""T149: build the slough review page v2 (page.html) from candidates_v2 + the intermittent census.

    .venv/bin/python build_page.py          (run through scripts/heavy.sh; reads the gpkg read-only)

Inputs (read-only):  handoff/PLAN/sloughs/v2/candidates_v2.json.gz, data/source/bc_fisheries_data.gpkg,
                     scratchpad/t55v2/polys_raw.parquet (wbk -> layer/fid), t149/intermittent_groups.parquet
                     (made by intermittent.py), page_template.html (the page shell, CSS + JS).
Outputs:             t149/page.html, t149/complexes.json, t149/spot.json (what went into the page).

COMPLEXES. Candidates of class STREAM_BY_BOOK / LIKELY_STREAM / UNSURE (LAKE controls and NOT_FISHING_WATER
are left out) that share a stream item AND a region are joined when their FULL FWA outlines
  - touch (distance <= TOUCH_M = 1 m), or
  - lie within GAP_M = 150 m of each other AND the river's own line (FWA streams with that GNIS_ID)
    passes within GAP_M of the midpoint of the shortest gap between them.
Connected components with >= 2 pieces are complexes; id = "cx-" + the smallest piece id.
"""
import gzip, json, math, os, random, sqlite3, sys, collections
import pandas as pd
from shapely import wkb
from shapely.geometry import Point, Polygon, MultiPolygon, LineString, shape
from shapely.ops import nearest_points, unary_union, transform as shp_transform
from pyproj import Transformer

REPO = '/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/'
SP = '/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/'
OUT = SP + 't149/'
CAND = REPO + 'handoff/PLAN/sloughs/v2/candidates_v2.json.gz'
GPKG = REPO + 'data/source/bc_fisheries_data.gpkg'
POLYS = SP + 't55v2/polys_raw.parquet'
GROUPS = OUT + 'intermittent_groups.parquet'
TEMPLATE = OUT + 'page_template.html'
TOUCH_M, GAP_M = 1.0, 150.0
COMPLEX_CLASSES = {'STREAM_BY_BOOK', 'LIKELY_STREAM', 'UNSURE'}
SEED = 149

gp = sqlite3.connect(f'file:{GPKG}?mode=ro', uri=True)
to_ll = Transformer.from_crs(3005, 4326, always_xy=True)
to_al = Transformer.from_crs(4326, 3005, always_xy=True)
FC = {
    'GB15300000': 'Lake-Definite', 'GB15300130': 'Lake-Indefinite', 'GB15300140': 'Lake-Intermittent',
}
ET_WORDS = {1000: 'stream line', 1050: 'stream line (secondary)', 1100: 'stream line', 1150: 'stream line',
            1200: 'main-flow line', 1250: 'main-connector line', 1300: 'secondary-flow line', 1350: 'secondary connector',
            1400: 'construction line (1400)', 1410: 'construction line (1410)', 1450: 'construction line (1450)',
            1475: 'lake arm', 2000: 'ditch line', 2100: 'canal line'}


def geom(blob):
    flags = blob[3]; env = (flags >> 1) & 7
    return wkb.loads(bytes(blob[8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[env]:]))


def poly_geom(wbk, idx):
    parts = [geom(gp.execute(f'select geom from {lyr} where fid=?', (int(fid),)).fetchone()[0]) for lyr, fid in idx.get(int(wbk), [])]
    return unary_union(parts) if parts else None


def outline_ll(g, max_pts=60):
    """Largest exterior ring, simplified until <= max_pts, as [[lon, lat], ...] (5 dp) like the candidates."""
    if g is None or g.is_empty:
        return []
    p = max(getattr(g, 'geoms', [g]), key=lambda q: q.area)
    ring = LineString(p.exterior.coords)
    tol = 1.0
    while len(ring.coords) > max_pts:
        ring = LineString(p.exterior.coords).simplify(tol); tol *= 1.6
    return [[round(x, 5), round(y, 5)] for x, y in (to_ll.transform(*c[:2]) for c in ring.coords)]


def river_near(gnis, pt, r):
    x, y = pt.x, pt.y
    ids = [i for (i,) in gp.execute('select id from rtree_streams_geom where maxx>=? and minx<=? and maxy>=? and miny<=?',
                                    (x - r, x + r, y - r, y + r))]
    if not ids:
        return False
    for g, gid in gp.execute(f'select geom, GNIS_ID from streams where fid in ({",".join(map(str, ids))})'):
        if gid is not None and int(gid) == gnis and geom(g).distance(pt) <= r:
            return True
    return False


# ------------------------------------------------------------------ candidates -> slim records
INSIDE_WORDS = {'Secondary Flow': 'secondary-flow line inside', 'Lake Arm': 'lake arm', 'Main Connector': 'main-connector line inside',
                'River/Stream-Definite': 'stream line inside', 'River/Stream-Intermittent': 'intermittent stream line inside',
                'River/Stream-Indefinite': 'indefinite stream line inside', 'Ditch': 'ditch line', 'Canal': 'canal line',
                'Flow Connector-Inferred': 'inferred flow connector inside'}
ORDER = {'STREAM_BY_BOOK': 0, 'LIKELY_STREAM': 1, 'UNSURE': 2, 'LAKE': 3, 'NOT_FISHING_WATER': 4}


def inside_words(x):
    out = []
    for code, v in sorted(x['inside_line_codes'].items(), key=lambda kv: -(kv[1].get('m') or 0)):
        lab = v.get('label', code)
        if lab == 'Construction Line-Main Flow':
            w = f"river main flow through ({x['stream_name']})" if x.get('river_through') else 'main-flow line inside'
        elif lab.startswith('Construction Line ('):
            w = 'construction line ' + lab.split('(')[1].rstrip(')').join('()')
        else:
            w = INSIDE_WORDS.get(lab, lab)
        if v.get('m'):
            w += f" {v['m']} m"
        out.append(w)
    return out


def enc(o):
    """Outline [[lon, lat], ...] at 5 dp -> [lon0, lat0, dlon1, dlat1, ...] in 1e-5 degree integers (lossless
    for 5 dp; the page decodes it). About half the bytes of the nested float list."""
    out, px, py = [], 0, 0
    for lon, lat in o:
        x, y = round(lon * 1e5), round(lat * 1e5)
        out += [x - px, y - py]; px, py = x, y
    return out


def slim(x, E):
    nm = x['name']
    nm = ' / '.join(nm) if isinstance(nm, list) else (nm or '')
    for e in x['catalogue_entries']:
        E.setdefault(e['entry_id'], {'nm': e['name'], 'p': e.get('pages') or [], 'v': e.get('verbatim') or ''})
    dam = x['dam_or_reservoir'] or {}
    pl = x['part_of_named_lake']
    sn = x['suggested_name']
    drawn = x.get('drawn') or {}
    r = {'i': x['id'], 'n': nm, 'c': x['class'], 'g': x['region'], 'm': x['mu'], 'la': x['lat'], 'lo': x['lon'],
         'o': enc(x['outline']), 'a': x['area_m2'],
         'e': x['elongation_perimeter'] if x['elongation_perimeter'] is not None else x['elongation'],
         'w': x['mean_width_m'], 's': x['stream_name'] or '', 'si': x['stream_item'] or '', 'rt': bool(x['regs_reach_today']),
         'why': x['class_reason'], 'ev': [e if len(e) <= 300 else e[:297] + '...' for e in x['evidence']],
         'ce': [e['entry_id'] for e in x['catalogue_entries']], 'miss': x['entries_missing_on_polygon'] or [],
         'fc': x['fwa_feature_label'] or '', 'lc': inside_words(x)}
    if x.get('geom_kind') == 'line':
        r['k'] = 'line'
    if x['class_v1'] and x['class_v1'] != x['class']:
        r['c1'] = x['class_v1']
    if x.get('rule_ids'):
        r['ru'] = x['rule_ids']
    if dam.get('value'):
        r['dam'] = '; '.join(dam.get('evidence') or []) or 'dam or reservoir'
        if dam.get('book_names_as_river'):
            r['dam'] += ' (the book names it as part of the river)'
    if pl:
        r['pl'] = f"{pl['name']} ({pl['touch_m']} m away{', shares its blue line' if pl.get('shares_blue_line') else ''})"
    if sn:
        r['sn'] = f"{sn['name']} (from {sn['source']})"
    if x['class'] == 'NOT_FISHING_WATER':
        if drawn.get('as'):
            r['dr'] = f"The app draws it as a {drawn['as']} today (from zoom {drawn.get('minzoom')})."
    if x.get('river_entry_at_piece'):
        r['re'] = x['river_entry_at_piece']
    return r


def sort_key(x):
    return (ORDER[x['class']], 0 if x.get('river_entry_at_piece') else 1, -(x['elongation_perimeter'] or x['elongation'] or 0))


# ------------------------------------------------------------------ complexes
def build_complexes(cands, idx):
    groups = collections.defaultdict(list)
    for x in cands:
        if x['class'] in COMPLEX_CLASSES and x['stream_item'] and x.get('geom_kind') != 'line':
            groups[(x['stream_item'], x['region'])].append(x)
    out, stats = [], collections.Counter()
    for (item, reg), xs in groups.items():
        if len(xs) < 2:
            continue
        gnis = int(item.split(':')[1])
        G = {x['id']: poly_geom(x['id'], idx) for x in xs}
        ids = [x['id'] for x in xs if G[x['id']] is not None]
        par = {i: i for i in ids}

        def find(i):
            while par[i] != i:
                par[i] = par[par[i]]; i = par[i]
            return i
        for a_i, a in enumerate(ids):
            for b in ids[a_i + 1:]:
                d = G[a].distance(G[b])
                if d > GAP_M:
                    continue
                if d <= TOUCH_M:
                    ok, why = True, 'touch'
                else:
                    p, q = nearest_points(G[a], G[b])
                    ok, why = river_near(gnis, Point((p.x + q.x) / 2, (p.y + q.y) / 2), GAP_M), 'gap'
                if ok:
                    stats[why] += 1
                    par[find(a)] = find(b)
        comp = collections.defaultdict(list)
        for i in ids:
            comp[find(i)].append(i)
        byid = {x['id']: x for x in xs}
        for members in comp.values():
            if len(members) < 2:
                continue
            members.sort(key=lambda i: sort_key(byid[i]))
            mx = [byid[i] for i in members]
            named = [(' / '.join(x['name']) if isinstance(x['name'], list) else x['name']) for x in mx if x['name']]
            sugg = [x['suggested_name']['name'] for x in mx if x['suggested_name']]
            if named:
                name, basis = collections.Counter(named).most_common(1)[0][0], 'a named piece'
            elif sugg:
                name, basis = collections.Counter(sugg).most_common(1)[0][0], 'suggested name'
            else:
                name, basis = f"{mx[0]['stream_name']} complex", 'the river'
            u = unary_union([G[i] for i in members])
            cx = {'id': 'cx-' + min(members), 'n': name, 'nb': basis, 's': mx[0]['stream_name'], 'si': item, 'g': reg,
                  'mu': ', '.join(sorted({x['mu'] for x in mx})), 'p': members,
                  'a': round(sum(x['area_m2'] or 0 for x in mx)),
                  'cl': dict(collections.Counter(x['class'] for x in mx)),
                  'la': round(to_ll.transform(u.centroid.x, u.centroid.y)[1], 5),
                  'lo': round(to_ll.transform(u.centroid.x, u.centroid.y)[0], 5)}
            out.append(cx)
    out.sort(key=lambda c: (-len(c['p']), c['id']))
    return out, stats


# ------------------------------------------------------------------ intermittent spot check
def lines_words(sig):
    try:
        d = json.loads(sig or '{}')
    except Exception:
        return []
    out = []
    for k, (n, m) in sorted(d.items(), key=lambda kv: -kv[1][1]):
        et = int(k.split('/')[1])
        out.append(f"{ET_WORDS.get(et, 'line ' + str(et))} x{n}, {m} m")
    return out


def spot(cands, idx):
    im = pd.read_parquet(GROUPS)
    cand_ids = {x['id'] for x in cands}
    im['is_cand'] = im.wbk.astype(str).isin(cand_ids)
    groups = im.group.value_counts().to_dict()
    by_reg = {g: im[im.group == g].region.value_counts().to_dict() for g in ('a', 'b', 'c')}
    A = im[im.group == 'a']
    summary = {
        'total': int(len(im)), 'groups': {k: int(v) for k, v in groups.items()},
        'by_region': {g: {str(k): int(v) for k, v in d.items()} for g, d in by_reg.items()},
        'a_kind': {k: int(v) for k, v in A.a_kind.value_counts().to_dict().items()},
        'b_kind': {k: int(v) for k, v in im[im.group == 'b'].b_kind.value_counts().to_dict().items()},
        'a_cand': int(A.is_cand.sum()), 'a_named_out': int((A.out_names.fillna('') != '').sum()),
        'a_area_ha': round(float(A.area_m2.sum()) / 1e4), 'a_rule_entries': int((A.rule_entries.fillna('') != '').sum()),
        'a_median_m2': int(A.area_m2.median()),
    }
    rng = random.Random(SEED)
    pool = A[~A.is_cand & (A.region != '')].copy()
    t1, t2 = pool.area_m2.quantile([1 / 3, 2 / 3])
    pool['band'] = pool.area_m2.map(lambda a: 'small' if a < t1 else ('medium' if a < t2 else 'large'))
    summary['bands_m2'] = [int(t1), int(t2)]
    regs = sorted(pool.region.unique())
    N = len(pool)
    quota = {r: max(2, round(40 * (pool.region == r).sum() / N)) for r in regs}
    while sum(quota.values()) > 40:
        r = max(quota, key=quota.get); quota[r] -= 1
    named_regions = pool[pool.a_kind == 'named main flow through'].region.value_counts().index[:5].tolist()
    picks = []
    for r in regs:
        sub = pool[pool.region == r]
        bands = ['small', 'medium', 'large']
        for k in range(quota[r]):
            kind = 'named main flow through' if (k == 0 and r in named_regions) else 'stream in and out'
            band = bands[(k + regs.index(r)) % 3]
            c = sub[(sub.a_kind == kind) & (sub.band == band) & ~sub.wbk.isin([p[0] for p in picks])]
            if not len(c):
                c = sub[(sub.a_kind == kind) & ~sub.wbk.isin([p[0] for p in picks])]
            if not len(c):
                c = sub[~sub.wbk.isin([p[0] for p in picks])]
            row = c.iloc[rng.randrange(len(c))]
            picks.append((int(row.wbk), 'a', r, band if row.band == band else row.band))
    B = im[(im.group == 'b') & (im.region != '')].copy()
    B['band'] = B.area_m2.map(lambda a: 'small' if a < t1 else ('medium' if a < t2 else 'large'))
    breg = B.region.value_counts().index[:10].tolist()
    for k in range(10):
        r = breg[k % len(breg)]
        kind = 'b0' if k < 6 else 'b1'
        band = ['small', 'medium', 'large'][k % 3]
        c = B[(B.region == r) & (B.b_kind == kind) & (B.band == band)]
        if not len(c):
            c = B[(B.region == r) & (B.b_kind == kind)]
        row = c.iloc[rng.randrange(len(c))]
        picks.append((int(row.wbk), 'b', r, row.band))
    # group (a) samples into complexes with their (a) neighbours: touching, or within GAP_M with a stream line
    # passing near the gap (any stream line here: these rivers are mostly unnamed)
    Aidx = A.set_index('wbk')
    items, used = [], set()
    kmlat = 1 / 111.0
    for wbk, grp, reg, band in picks:
        if wbk in used:
            continue
        row = im[im.wbk == wbk].iloc[0]
        members = [wbk]
        if grp == 'a':
            frontier = [wbk]
            G = {wbk: poly_geom(wbk, idx)}
            while frontier and len(members) < 12:
                w = frontier.pop()
                r0 = Aidx.loc[w]
                near = A[(abs(A.lat - r0.lat) < 2 * kmlat) & (abs(A.lon - r0.lon) < 2 * kmlat / math.cos(math.radians(r0.lat)))
                         & ~A.wbk.isin(members) & ~A.wbk.isin(list(used))]
                for v in near.wbk:
                    gv = G.get(v) or poly_geom(v, idx); G[v] = gv
                    d = G[w].distance(gv)
                    if d <= TOUCH_M or (d <= GAP_M and any_line_near(nearest_mid(G[w], gv), GAP_M)):
                        members.append(int(v)); frontier.append(int(v))
                        if len(members) >= 12:
                            break
        used.update(members)
        pieces = []
        for m in members:
            q = im[im.wbk == m].iloc[0]
            g = poly_geom(m, idx)
            pieces.append({'i': str(m), 'o': enc(outline_ll(g)), 'a': round(q.area_m2), 'fc': 'Lake-Intermittent',
                           'lc': lines_words(q.lines_sig), 'la': round(q.lat, 5), 'lo': round(q.lon, 5),
                           'no': int(q.n_out or 0), 'on': q.out_names or '', 'mf': q.main_flow_name or '',
                           'ak': q.a_kind or q.b_kind, 'm': q.mu, 'e': round(float(q.elong), 2) if q.elong == q.elong else None})
        items.append({'id': f'sp-{wbk}', 'grp': grp, 'g': reg, 'band': band, 'p': pieces,
                      'a': sum(p['a'] for p in pieces)})
    fold = {'regions': summary['by_region']['a']}
    return summary, items


def nearest_mid(a, b):
    p, q = nearest_points(a, b)
    return Point((p.x + q.x) / 2, (p.y + q.y) / 2)


def any_line_near(pt, r):
    x, y = pt.x, pt.y
    ids = [i for (i,) in gp.execute('select id from rtree_streams_geom where maxx>=? and minx<=? and maxy>=? and miny<=?',
                                    (x - r, x + r, y - r, y + r))]
    if not ids:
        return False
    return any(geom(g).distance(pt) <= r for (g,) in gp.execute(f'select geom from streams where fid in ({",".join(map(str, ids))})'))


def main():
    cands = json.load(gzip.open(CAND))['candidates']
    cands.sort(key=sort_key)
    p = pd.read_parquet(POLYS, columns=['wbk', 'layer', 'fid'])
    idx = collections.defaultdict(list)
    for w, l, f in zip(p.wbk, p.layer, p.fid):
        idx[int(w)].append((l, int(f)))
    E = {}
    data = [slim(x, E) for x in cands]
    if os.path.exists(OUT + 'complexes.json') and '--recompute' not in sys.argv:
        cx = json.load(open(OUT + 'complexes.json'))
    else:
        cxl, stats = build_complexes(cands, idx)
        cx = {'rule': {'touch_m': TOUCH_M, 'gap_m': GAP_M, 'classes': sorted(COMPLEX_CLASSES)}, 'joins': dict(stats), 'complexes': cxl}
        json.dump(cx, open(OUT + 'complexes.json', 'w'))
    if os.path.exists(OUT + 'spot.json') and '--recompute' not in sys.argv:
        sp = json.load(open(OUT + 'spot.json'))
    else:
        summary, items = spot(cands, idx)
        sp = {'summary': summary, 'items': items}
        json.dump(sp, open(OUT + 'spot.json', 'w'))
    payload = {'rows': data, 'entries': E, 'complexes': cx['complexes'], 'cxrule': cx['rule'], 'spot': sp}
    js = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).replace('</', '<\\/')
    tpl = open(TEMPLATE).read()
    html = tpl.replace('/*__DATA__*/null', js)
    open(OUT + 'page.html', 'w').write(html)
    cl = cx['complexes']
    print('records', len(data), 'complexes', len(cl), 'pieces', sum(len(c['p']) for c in cl), 'joins', cx.get('joins'))
    print('biggest', [(c['n'], c['g'], len(c['p']), c['a']) for c in cl[:8]])
    print('spot items', len(sp['items']), collections.Counter(i['grp'] for i in sp['items']), 'pieces', sum(len(i['p']) for i in sp['items']))
    print('size', os.path.getsize(OUT + 'page.html'))


if __name__ == '__main__':
    main()

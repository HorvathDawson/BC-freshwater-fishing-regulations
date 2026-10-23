"""T154: match the provincial dams (BC Dams, WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW) to the FWA lines and polygons
they sit on, and list the dams at every T148 candidate polygon — the evidence revise.py's R2 reads with `--dams`.

Read-only on the repo. Inputs:
  --dams FILE     the dams as GeoJSON in EPSG:3005 (a WFS GetFeature page, e.g. the T154 scratch fetch), or omit it to
                  read the `dams` layer of the gpkg (data/fetch_data.py `--layers dams` puts it there)
  --gpkg FILE     bc_fisheries_data.gpkg (FWA streams / lakes / manmade / wetlands); default the repo's
  --polys FILE    the T148 census's polys_raw.parquet (fid, wbk, layer of every lake / manmade / wetland piece); omit it
                  to read the pieces from the gpkg by WATERBODY_KEY (the census scratch is not kept; same pieces)
  --candidates    candidates_v2.json.gz (the T148 records; their numeric ids are the polygons to look at)
  --out DIR       writes dams_fwa.json (every dam -> its FWA line and polygons) and dams_by_wbk.json (candidate
                  polygon -> the dams within NEAR_M of it)

    python -I handoff/PLAN/sloughs/v2/dams.py --dams <scratch>/t154/raw/dams_0.json --out <scratch>/t154/

A dam is a CREST LINE. Its FWA line is the nearest streams-layer edge within LINE_M (crossing it, or the nearest when
the crest sits beside the line); its polygons are every lakes / manmade / wetlands polygon within POLY_M. Which dams
count as impounding water is revise.py's decision (`STANDING`), not this file's: every dam is listed with its
DAM_OPERATION_CODE.
"""
import argparse, collections, gzip, json, os, sqlite3, sys

import pandas as pd
from shapely import wkb
from shapely.geometry import shape
from shapely.ops import unary_union

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
LINE_M = 50.0          # a dam's FWA line: the nearest stream edge this close (FWA 1:20k positional tolerance x2)
POLY_M = 50.0          # a dam's FWA polygons
NEAR_M = 100.0         # a candidate polygon lists every dam this close (revise.py applies its own, tighter, reach)
KEEP = ('WRIS_DP_SYSID', 'DAM_FILE_NUMBER', 'DAM_NAME', 'DAM_FUNCTION', 'DAM_OPERATION_CODE', 'DAM_REGULATED_CODE',
        'DAM_TYPE', 'DAM_HEIGHT', 'CREST_LENGTH', 'COMMISSIONED_YEAR', 'REGION')


def gpkg_geom(b):
    env = (b[3] >> 1) & 7
    return wkb.loads(bytes(b[8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[env]:]))


def load_dams(path, gp):
    if path:
        fc = json.load(open(path))
        crs = ((fc.get('crs') or {}).get('properties') or {}).get('name', '')
        if crs and '3005' not in crs:
            raise SystemExit(f'dams: {path} is in {crs}, not EPSG:3005')
        return [({k: f['properties'].get(k) for k in KEEP}, shape(f['geometry'])) for f in fc['features']
                if f.get('geometry')]
    cols = [r[1] for r in gp.execute('pragma table_info(dams)')]
    if not cols:
        raise SystemExit('dams: no --dams file and no `dams` layer in the gpkg — run data/fetch_data.py --layers dams')
    q = f"select {', '.join(KEEP)}, geom from dams"
    return [(dict(zip(KEEP, r[:-1])), gpkg_geom(r[-1])) for r in gp.execute(q) if r[-1] is not None]


def near(gp, layer, g, reach, cols):
    minx, miny, maxx, maxy = g.bounds
    q = (f"select {', '.join('t.' + c for c in cols)}, t.geom from {layer} t join rtree_{layer}_geom r on r.id=t.fid "
         'where r.maxx>=? and r.minx<=? and r.maxy>=? and r.miny<=?')
    for row in gp.execute(q, (minx - reach, maxx + reach, miny - reach, maxy + reach)):
        h = gpkg_geom(row[-1])
        d = g.distance(h)
        if d <= reach:
            yield dict(zip(cols, row[:-1])), h, d


#: a dam is at the polygon's OUTLET when its measure on the polygon's own blue line is within this of the polygon's
#: lowest measure on that line, at its INLET within this of the highest (the crest sits on the polygon's edge)
END_M = 30.0


def side_of(gp, w, poly, dam):
    """Where a dam sits on the polygon's water: 'outlet' (the polygon is the water it holds back), 'inlet' (the
    polygon is downstream of it), 'mid', or None (no blue line of the polygon passes the dam). Measured on the blue
    lines FWA draws INSIDE the polygon (WATERBODY_KEY = w): the dam's route measure (the nearest point of the line to the
    crest; FWA lines run downstream -> upstream, measure rising) against the polygon's measure range on that line."""
    from shapely.ops import linemerge, nearest_points
    edges = [(m, h) for m, h, _d in near(gp, 'streams', poly, 1.0, ('BLUE_LINE_KEY', 'WATERBODY_KEY',
                                                                     'DOWNSTREAM_ROUTE_MEASURE', 'UPSTREAM_ROUTE_MEASURE'))
             if m['WATERBODY_KEY'] == w]
    best = None
    for blk in sorted({m['BLUE_LINE_KEY'] for m, _h in edges}):
        mine = [(m, h) for m, h in edges if m['BLUE_LINE_KEY'] == blk]
        lo = min(m['DOWNSTREAM_ROUTE_MEASURE'] for m, _h in mine)
        hi = max(m['UPSTREAM_ROUTE_MEASURE'] for m, _h in mine)
        for m, h in mine:
            ln = linemerge(h) if h.geom_type == 'MultiLineString' else h
            if ln.geom_type != 'LineString':
                ln = max(ln.geoms, key=lambda x: x.length)
            d = ln.distance(dam)
            if d > LINE_M:
                continue
            p = nearest_points(ln, dam)[0]
            at = m['DOWNSTREAM_ROUTE_MEASURE'] + ln.project(p) * (
                (m['UPSTREAM_ROUTE_MEASURE'] - m['DOWNSTREAM_ROUTE_MEASURE']) / ln.length if ln.length else 0)
            if best is None or d < best[0]:
                best = (d, blk, at, lo, hi)
    if best is None:
        return None, None
    d, blk, at, lo, hi = best
    side = 'outlet' if at - lo <= END_M and at - lo <= hi - at else ('inlet' if hi - at <= END_M else 'mid')
    return side, dict(blk=blk, at=round(at, 1), lo=round(lo, 1), hi=round(hi, 1), d=round(d, 1))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--dams', default=None)
    ap.add_argument('--gpkg', default=os.path.join(REPO, 'data', 'source', 'bc_fisheries_data.gpkg'))
    ap.add_argument('--polys', default=None)
    ap.add_argument('--candidates', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'candidates_v2.json.gz'))
    ap.add_argument('--out', required=True)
    a = ap.parse_args(argv)
    gp = sqlite3.connect(f'file:{a.gpkg}?mode=ro', uri=True)
    dams = load_dams(a.dams, gp)
    print('dams', len(dams))

    # 1. every dam -> its FWA line (nearest stream edge within LINE_M) and its polygons (within POLY_M)
    out = []
    for p, g in sorted(dams, key=lambda t: t[0]['WRIS_DP_SYSID']):
        lines = list(near(gp, 'streams', g, LINE_M, ('BLUE_LINE_KEY', 'GNIS_NAME', 'FEATURE_CODE', 'EDGE_TYPE',
                                                     'WATERBODY_KEY', 'DOWNSTREAM_ROUTE_MEASURE')))
        line = None
        if lines:
            m, h, d = min(lines, key=lambda t: (t[2], t[0]['BLUE_LINE_KEY']))
            line = dict(blk=m['BLUE_LINE_KEY'], name=m['GNIS_NAME'], fc=m['FEATURE_CODE'], edge_type=m['EDGE_TYPE'],
                        wbk=m['WATERBODY_KEY'], d=round(d, 1), crosses=bool(g.intersects(h)))
        polys = []
        for lyr in ('lakes', 'manmade', 'wetlands'):
            for m, h, d in near(gp, lyr, g, POLY_M, ('WATERBODY_KEY', 'FEATURE_CODE', 'GNIS_NAME_1')):
                polys.append(dict(layer=lyr, wbk=m['WATERBODY_KEY'], fc=m['FEATURE_CODE'], name=m['GNIS_NAME_1'],
                                  d=round(d, 1)))
        polys.sort(key=lambda x: (x['d'], x['layer'], x['wbk'] or 0))
        c = g.centroid
        out.append(dict(p, x=round(c.x, 1), y=round(c.y, 1), line=line, polys=polys))
    with open(os.path.join(a.out, 'dams_fwa.json'), 'w') as f:
        json.dump(out, f, separators=(',', ':'))
    print('dams with an FWA line within', LINE_M, 'm:', sum(1 for d in out if d['line']),
          '(crossing:', sum(1 for d in out if d['line'] and d['line']['crosses']), ')',
          '| with a polygon within', POLY_M, 'm:', sum(1 for d in out if d['polys']),
          '| neither:', sum(1 for d in out if not d['line'] and not d['polys']))

    # 2. every candidate polygon -> the dams within NEAR_M of it (whole polygon: every piece of the wbk)
    cands = json.load(gzip.open(a.candidates))['candidates']
    want = sorted({int(x['id']) for x in cands if x['id'].isdigit()})
    if a.polys:
        raw = pd.read_parquet(a.polys, columns=['fid', 'wbk', 'layer'])
        raw = raw[raw.wbk.isin(want)]
    else:
        ws = ','.join(map(str, want))
        raw = pd.DataFrame([(fid, w, lyr) for lyr in ('lakes', 'manmade', 'wetlands') for fid, w in
                            gp.execute(f'select fid, WATERBODY_KEY from {lyr} where WATERBODY_KEY in ({ws})')],
                           columns=['fid', 'wbk', 'layer'])
    pieces = collections.defaultdict(list)
    for r in raw.itertuples():
        b = gp.execute(f'select geom from {r.layer} where fid=?', (int(r.fid),)).fetchone()[0]
        pieces[int(r.wbk)].append(gpkg_geom(b))
    from shapely import STRtree
    tree = STRtree([g for _p, g in dams])
    by = {}
    for w in want:
        if w not in pieces:
            continue
        g = unary_union(pieces[w]) if len(pieces[w]) > 1 else pieces[w][0]
        hits = []
        for i in tree.query(g.buffer(NEAR_M).envelope):
            p, dg = dams[int(i)]
            d = g.distance(dg)
            if d <= NEAR_M:
                side, on = side_of(gp, w, g, dg)
                hits.append(dict(sysid=p['WRIS_DP_SYSID'], name=p['DAM_NAME'], function=p['DAM_FUNCTION'],
                                 operation=p['DAM_OPERATION_CODE'], height_m=p['DAM_HEIGHT'], d=round(d, 1),
                                 side=side, on_line=on))
        if hits:
            by[str(w)] = sorted(hits, key=lambda h: (h['d'], h['sysid']))
    with open(os.path.join(a.out, 'dams_by_wbk.json'), 'w') as f:
        json.dump(by, f, indent=0, sort_keys=True)
    print('candidate polygons', len(want), 'with a dam within', NEAR_M, 'm:', len(by))
    return 0


if __name__ == '__main__':
    sys.exit(main())

"""T149 item 2: split every Lake-Intermittent (GB15300140) polygon into
(a) on a river's course, (b) isolated, (c) named lakes; draw the spot-check sample.

Read-only on the repo (gpkg via sqlite ?mode=ro). Writes only under scratchpad/t149/.
Run through scripts/heavy.sh. Caches the per-polygon touch test to intermittent_touch.parquet.

(c) named   : FWA GNIS name on the polygon, or a lake item in the registry.
(a) course  : unnamed, and either a named stream item's main flow (1200) runs through it
              (census river_through), or 2+ stream lines OUTSIDE the polygon end on its edge
              (a stream in and a stream out).
(b) isolated: unnamed, and 0 or 1 outside stream line ends on its edge (b0: none; b1: one, a
              headwater or sink pond).
"""
import json, os, random, sqlite3, sys
import pandas as pd
from shapely import wkb
from shapely.geometry import box
from pyproj import Transformer

REPO = '/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/'
SP = '/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/'
OUT = SP + 't149/'
CENSUS = SP + 't55v2/census.parquet'
GPKG = REPO + 'data/source/bc_fisheries_data.gpkg'
sys.path.insert(0, REPO)
gp = sqlite3.connect(f'file:{GPKG}?mode=ro', uri=True)
to_ll = Transformer.from_crs(3005, 4326, always_xy=True)
TOUCH_M = 2.0


def geom(blob):
    flags = blob[3]; env = (flags >> 1) & 7
    return wkb.loads(bytes(blob[8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[env]:]))


def touch_test(fid, wbk):
    poly = geom(gp.execute('select geom from lakes where fid=?', (int(fid),)).fetchone()[0])
    x0, y0, x1, y1 = poly.bounds
    ids = [r[0] for r in gp.execute(
        'select id from rtree_streams_geom where maxx>=? and minx<=? and maxy>=? and miny<=?',
        (x0 - TOUCH_M, x1 + TOUCH_M, y0 - TOUCH_M, y1 + TOUCH_M))]
    if not ids:
        return 0, 0, ''
    rows = gp.execute(f'select geom, WATERBODY_KEY, BLUE_LINE_KEY, GNIS_NAME, EDGE_TYPE from streams where fid in ({",".join(map(str, ids))})').fetchall()
    edge = poly.boundary
    n_out, blks, names = 0, set(), set()
    for g, wk, blk, nm, et in rows:
        if wk is not None and int(wk) == int(wbk):
            continue                      # the polygon's own construction lines
        ln = geom(g)
        if ln.distance(edge) > TOUCH_M:
            continue
        # it must END on the edge (a stream entering or leaving), not merely pass by
        ends = []
        for part in getattr(ln, 'geoms', [ln]):
            c = list(part.coords); ends += [c[0], c[-1]]
        from shapely.geometry import Point
        if any(Point(p[:2]).distance(edge) <= TOUCH_M for p in ends):
            n_out += 1; blks.add(int(blk))
            if nm: names.add(nm)
    return n_out, len(blks), '; '.join(sorted(names))


def main():
    cen = pd.read_parquet(CENSUS, columns=['wbk', 'layer', 'fc', 'fid', 'n1', 'item', 'item_kind', 'item_name', 'river_through',
                                           'lineclass', 'lines_sig', 'area_m2', 'lat', 'lon', 'elong', 'mean_width_m',
                                           'main_flow_name', 'drawn', 'tile_minzoom', 'rule_entries'])
    im = cen[(cen.fc == 'GB15300140') & (cen.layer == 'lakes')].copy()
    cache = OUT + 'intermittent_touch.parquet'
    if os.path.exists(cache):
        t = pd.read_parquet(cache)
    else:
        res = []
        for i, (fid, wbk) in enumerate(zip(im.fid, im.wbk)):
            res.append((wbk, *touch_test(fid, wbk)))
            if i % 2000 == 0:
                print('touch', i, len(im), flush=True)
        t = pd.DataFrame(res, columns=['wbk', 'n_out', 'n_out_blk', 'out_names'])
        t.to_parquet(cache)
    im = im.merge(t, on='wbk', how='left')
    # region from the MU the point falls in
    from pipeline.atlas.splits.area_splits import mu_region_table
    mreg = {k: v.lower() for k, v in mu_region_table().items()}
    mus = []
    for lat, lon in zip(im.lat, im.lon):
        x, y = to_3005(lon, lat)
        r = gp.execute('select id from rtree_wmu_geom where minx<=? and maxx>=? and miny<=? and maxy>=?', (x, x, y, y)).fetchall()
        mu = ''
        for (f,) in r:
            if wmu_geom(f).contains(Point(x, y)):
                mu = gp.execute('select WILDLIFE_MGMT_UNIT_ID from wmu where fid=?', (f,)).fetchone()[0]; break
        mus.append(mu)
    im['mu'] = mus
    im['region'] = im.mu.map(lambda m: mreg.get(m, ''))
    named = (im.n1.fillna('') != '') | (im.item_kind == 'lake')
    course = ~named & (im.river_through.fillna(False) | (im.n_out >= 2))
    im['group'] = 'b'
    im.loc[course, 'group'] = 'a'
    im.loc[named, 'group'] = 'c'
    im['b_kind'] = ''
    im.loc[im.group == 'b', 'b_kind'] = im.n_out.map(lambda n: 'b0' if n == 0 else 'b1')
    im['a_kind'] = ''
    im.loc[im.group == 'a', 'a_kind'] = im.river_through.map(lambda v: 'named main flow through' if v else 'stream in and out')
    im.to_parquet(OUT + 'intermittent_groups.parquet')
    print(im.group.value_counts().to_dict(), im[im.group == 'a'].a_kind.value_counts().to_dict(), im[im.group == 'b'].b_kind.value_counts().to_dict())
    print(pd.crosstab(im.group, im.region))


_t3005 = Transformer.from_crs(4326, 3005, always_xy=True)
from shapely.geometry import Point
from shapely.prepared import prep
_wmu = {}


def to_3005(lon, lat):
    return _t3005.transform(lon, lat)


def wmu_geom(f):
    if f not in _wmu:
        _wmu[f] = prep(geom(gp.execute('select geom from wmu where fid=?', (f,)).fetchone()[0]))
    return _wmu[f]


if __name__ == '__main__':
    main()

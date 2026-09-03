"""Shared FWA/OSM resolution helpers for the 24-entry needs-pin batch.
All geometry in EPSG:3005; outputs (lat,lon) rounded 5dp. Read-only against the gpkg.
"""
import geopandas as gpd, pandas as pd, numpy as np, urllib.request, json, time
from shapely.geometry import Point, LineString, MultiLineString, box as BOX
from shapely.ops import unary_union, linemerge, nearest_points, substring

GPKG = 'data/source/bc_fisheries_data.gpkg'
_cache = {}

def _streams():
    if 'st' not in _cache:
        st = gpd.read_file(GPKG, layer='streams')
        st['_n'] = st['GNIS_NAME'].astype(str).str.lower()
        _cache['st'] = st
    return _cache['st']

def _tidal():
    if 'tidal' not in _cache:
        _cache['tidal'] = gpd.read_file(GPKG, layer='tidal_boundary')
    return _cache['tidal']

def _fsr():
    if 'fsr' not in _cache:
        _cache['fsr'] = gpd.read_file(GPKG, layer='forest_service_roads')
    return _cache['fsr']

def _falls():
    if 'falls' not in _cache:
        _cache['falls'] = gpd.read_file(GPKG, layer='waterfalls')
    return _cache['falls']

def _wmu():
    if 'wmu' not in _cache:
        _cache['wmu'] = gpd.read_file(GPKG, layer='wmu')
    return _cache['wmu']

def to4326(geom3005):
    return gpd.GeoSeries([geom3005], crs=3005).to_crs(4326)[0]

def latlon(pt3005):
    p = to4326(pt3005)
    return (round(p.y, 5), round(p.x, 5))

def osm_link(lat, lon, z=16):
    return f'https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map={z}/{lat}/{lon}'

def mu_bbox(mus):
    """bbox (in 3005) union of the given WMU polygons; None if not found."""
    w = _wmu()
    sel = w[w['WILDLIFE_MGMT_UNIT_ID'].astype(str).isin([str(m) for m in mus])]
    if sel.empty:
        return None
    return sel.geometry.unary_union

def river_segments(name, mus=None, extra_box3005=None):
    """All FWA stream segments for `name`, optionally clipped to the MU union bbox
    (+buffer) or an explicit 3005 box. Returns GeoDataFrame in 3005."""
    st = _streams()
    g = st[st['_n'] == name.lower()].copy()
    if g.empty:
        return g
    clip = None
    if mus is not None:
        mb = mu_bbox(mus)
        if mb is not None:
            clip = mb.buffer(3000).envelope
    if extra_box3005 is not None:
        clip = extra_box3005 if clip is None else clip.intersection(extra_box3005)
    if clip is not None:
        g = g[g.geometry.intersects(clip)]
    return g

def mainstem(name, mus=None, extra_box3005=None):
    """Pick the dominant BLUE_LINE_KEY for `name` (longest total length in scope),
    return (ordered_line_mouth_to_source_3005, seg_gdf) using route measures.
    The line is ordered so that distance-from-start == metres upstream of mouth-of-this-blk."""
    g = river_segments(name, mus, extra_box3005)
    if g.empty:
        return None, g
    # dominant blue line key
    blk = g.groupby('BLUE_LINE_KEY')['LENGTH_METRE'].sum().idxmax()
    gg = g[g['BLUE_LINE_KEY'] == blk].copy()
    gg = gg.sort_values('DOWNSTREAM_ROUTE_MEASURE')
    # build a single ordered line by measure
    parts = []
    for _, r in gg.iterrows():
        geom = r.geometry
        lines = [geom] if geom.geom_type == 'LineString' else list(geom.geoms)
        for ln in lines:
            parts.append((r['DOWNSTREAM_ROUTE_MEASURE'], ln))
    parts.sort(key=lambda x: x[0])
    coords = []
    for _, ln in parts:
        cs = list(ln.coords)
        if coords and Point(coords[-1]).distance(Point(cs[0])) > Point(coords[-1]).distance(Point(cs[-1])):
            cs = cs[::-1]
        coords.extend(cs)
    line = LineString(coords)
    return line, gg

def river_any(name, mus=None, extra_box3005=None):
    """Merged river line (all blks) for coarse ops; ordered arbitrarily."""
    g = river_segments(name, mus, extra_box3005)
    if g.empty:
        return None
    m = linemerge(unary_union(g.geometry.values))
    if m.geom_type == 'LineString':
        return m
    return max(m.geoms, key=lambda x: x.length)

def tidal_point_on(line3005):
    """Where the river line crosses the tidal-boundary polygon edge = tidal boundary.
    Returns (Point3005, measure_along_line) or (None,None)."""
    if line3005 is None:
        return None, None
    tb = _tidal().geometry.unary_union
    inter = line3005.intersection(tb.boundary)
    if inter.is_empty:
        # river may start already outside tidal; use the mouth end (start) if it's near tidal
        p0 = Point(line3005.coords[0])
        if p0.distance(tb) < 1500:
            return p0, 0.0
        return None, None
    pts = [inter] if inter.geom_type == 'Point' else list(inter.geoms)
    # choose the one closest to the mouth end (start of ordered line)
    best = min(pts, key=lambda p: line3005.project(p))
    return best, line3005.project(best)

def walk_from(line3005, start_measure, km_up):
    """Point km_up kilometres upstream (increasing measure) of start_measure."""
    target = start_measure + km_up * 1000.0
    target = max(0, min(target, line3005.length))
    return line3005.interpolate(target)

def fsr_crossings(line3005, buf=40):
    """FSR crossings of the river; list of (Point3005, measure, road_name)."""
    fsr = _fsr()
    env = line3005.buffer(buf)
    cand = fsr[fsr.geometry.intersects(env)]
    out = []
    for _, r in cand.iterrows():
        it = line3005.intersection(r.geometry)
        if it.is_empty:
            continue
        p = it if it.geom_type == 'Point' else (list(it.geoms)[0] if hasattr(it, 'geoms') else it)
        if p.geom_type != 'Point':
            p = Point(p.coords[0])
        out.append((p, line3005.project(p), str(r.get('ROAD_SECTION_NAME') or r.get('MAP_LABEL') or '')))
    out.sort(key=lambda x: x[1])
    return out

def falls_near(line3005, buf=250):
    """Waterfalls within buf of the river; (Point3005, measure, name, height)."""
    fl = _falls()
    env = line3005.buffer(buf)
    cand = fl[fl.geometry.intersects(env)]
    out = []
    for _, r in cand.iterrows():
        p = r.geometry
        out.append((p, line3005.project(nearest_points(line3005, p)[0]), str(r.get('name') or ''), r.get('height')))
    out.sort(key=lambda x: x[1])
    return out

def confluence(main_line3005, trib_name, mus=None, box=None):
    """Nearest point on main_line to the named tributary; (Point3005, measure) or (None,None)."""
    tg = river_segments(trib_name, mus, box)
    if main_line3005 is None or tg.empty:
        return None, None
    tu = unary_union(tg.geometry.values)
    npr, _ = nearest_points(main_line3005, tu)
    if npr.distance(tu) > 600:
        return None, None
    return npr, main_line3005.project(npr)

# ---- OSM overpass (named highways/streets) ----
HDR = {'User-Agent': 'bc-fishing-reg-curation/1.0 (horvath.dawson@gmail.com)'}
_last = [0.0]
def overpass(q):
    if time.time() - _last[0] < 2.0:
        time.sleep(2.0 - (time.time() - _last[0]))
    for att in range(4):
        try:
            t = urllib.request.urlopen(urllib.request.Request(
                'https://overpass-api.de/api/interpreter',
                data=('data=' + q).encode(), headers=HDR), timeout=90).read().decode()
            _last[0] = time.time()
            if t.startswith('{'):
                return json.loads(t)
        except Exception:
            pass
        time.sleep(5 * (att + 1))
    _last[0] = time.time()
    return None

def osm_road_cross(line3005, box4326, tagfilter):
    """Find a named road/street crossing the river via Overpass; (lat,lon,label) or None.
    box4326=(w,s,e,n). tagfilter e.g. '[ref~\"6\"]' or '[~\"name\"~\"216|Fifth\",i]'."""
    w, s, e, n = box4326
    d = overpass(f'[out:json][timeout:85];way[highway]{tagfilter}({s},{w},{n},{e});out geom tags;')
    for el in (d or {}).get('elements', []):
        pts = [(p['lon'], p['lat']) for p in el.get('geometry', [])]
        if len(pts) < 2:
            continue
        ln = gpd.GeoSeries([LineString(pts)], crs=4326).to_crs(3005)[0]
        it = line3005.intersection(ln)
        if it.is_empty:
            continue
        p = it if it.geom_type == 'Point' else list(it.geoms)[0]
        la, lo = latlon(p)
        tg = el.get('tags', {})
        return (la, lo, f"OSM way {el['id']} {tg.get('name') or tg.get('ref') or ''}".strip())
    return None

"""FWA helpers: MU-clipped river geometry, tidal-boundary point, walk N km upstream,
nearby FSR bridges, nearby waterfalls. All coords returned as (lat, lon)."""
import geopandas as gpd
from shapely.geometry import Point, LineString, box as BOX
from shapely.ops import unary_union, linemerge, nearest_points, transform
import pyproj

GPKG = 'data/source/bc_fisheries_data.gpkg'
_cache = {}

def L(name):
    if name not in _cache:
        _cache[name] = gpd.read_file(GPKG, layer=name)
    return _cache[name]

def streams():
    if 'STL' not in _cache:
        s = L('streams'); s = s.assign(_n=s['GNIS_NAME'].astype(str).str.lower())
        _cache['STL'] = s
    return _cache['STL']

_to4326 = pyproj.Transformer.from_crs(3005, 4326, always_xy=True).transform
def ll(geom_3005):
    """3005 geom/point -> (lat, lon) rounded."""
    g = transform(_to4326, geom_3005)
    return (round(g.y, 5), round(g.x, 5))

def mu_bbox(mus, pad=3000):
    w = L('wmu')
    sel = w[w['WILDLIFE_MGMT_UNIT_ID'].isin(mus)]
    if sel.empty: return None
    b = sel.total_bounds
    return (b[0]-pad, b[1]-pad, b[2]+pad, b[3]+pad)

def river_line(name, mus=None, extra_box=None):
    """Merged mainstem LineString (EPSG:3005), MU-clipped. Returns (line, note)."""
    s = streams()
    g = s[s['_n'] == name.lower()]
    if g.empty: return None, f'no FWA GNIS "{name}"'
    note = f'{len(g)} FWA segs'
    if mus:
        bb = mu_bbox(mus)
        if bb:
            clip = BOX(*bb)
            g2 = g[g.geometry.intersects(clip)]
            if not g2.empty: g = g2; note += f' MU-clip[{",".join(mus)}]'
    if extra_box:
        clip = BOX(*extra_box)
        g2 = g[g.geometry.intersects(clip)]
        if not g2.empty: g = g2; note += ' box-clip'
    # pick dominant BLUE_LINE_KEY (longest total)
    if 'BLUE_LINE_KEY' in g.columns and g['BLUE_LINE_KEY'].nunique() > 1:
        tot = g.groupby('BLUE_LINE_KEY').geometry.apply(lambda x: x.length.sum())
        blk = tot.idxmax()
        g = g[g['BLUE_LINE_KEY'] == blk]
        note += f' blk={blk}'
    m = linemerge(unary_union(g.geometry.values))
    if m.geom_type != 'LineString':
        parts = list(m.geoms); m = max(parts, key=lambda x: x.length)
        note += f' ({len(parts)} parts, took longest)'
    return m, note

def tidal_point(line):
    """Intersection of river line with tidal boundary polygon edge -> (pt3005, dist_note).
    Orients: returns the fresh/salt interface point nearest the river's ocean end."""
    tb = unary_union(L('tidal_boundary').geometry.values)
    inter = line.intersection(tb.boundary)
    if inter.is_empty:
        # river doesn't reach tidal polygon; use endpoint nearest tidal polygon
        e0 = Point(line.coords[0]); e1 = Point(line.coords[-1])
        d0 = e0.distance(tb); d1 = e1.distance(tb)
        pt = e0 if d0 < d1 else e1
        return pt, f'no tidal intersection; used nearest end (d={min(d0,d1):.0f}m)'
    pts = [inter] if inter.geom_type == 'Point' else list(inter.geoms)
    # choose the interface point that is the mouth: the one with max projection-distance
    # from the source end. Simpler: the one nearest either physical endpoint of the line.
    e0 = Point(line.coords[0]); e1 = Point(line.coords[-1])
    mouth_end = e0 if e0.distance(tb) < e1.distance(tb) else e1
    pt = min(pts, key=lambda p: p.distance(mouth_end))
    return pt, f'{len(pts)} tidal-edge crossings'

def oriented_from(line, mouth_pt):
    """Return line coords ordered so index 0 is the mouth end."""
    e0 = Point(line.coords[0]); e1 = Point(line.coords[-1])
    if e0.distance(mouth_pt) <= e1.distance(mouth_pt):
        return line
    return LineString(list(line.coords)[::-1])

def walk_upstream(line, mouth_pt, km):
    """Walk km upstream from mouth_pt along line -> pt3005."""
    ol = oriented_from(line, mouth_pt)
    m0 = ol.project(mouth_pt)
    target = m0 + km*1000.0
    target = min(target, ol.length)
    return ol.interpolate(target), (ol.length/1000.0)

def nearby_fsr(pt3005, radius=1500):
    """FSR crossings within radius of pt -> list of (name, (lat,lon), dist_m)."""
    fsr = L('forest_service_roads')
    buf = pt3005.buffer(radius)
    hit = fsr[fsr.geometry.intersects(buf)]
    out = []
    for _, r in hit.iterrows():
        p, _2 = nearest_points(r.geometry, pt3005)
        nm = r.get('ROAD_SECTION_NAME') or r.get('MAP_LABEL') or r.get('FOREST_FILE_ID') or '?'
        out.append((str(nm), ll(p), round(p.distance(pt3005))))
    out.sort(key=lambda x: x[2])
    return out[:6]

def river_road_crossings(line, radius_along=None):
    """All FSR that actually cross the river line -> (name, (lat,lon), km_from_start)."""
    fsr = L('forest_service_roads')
    hit = fsr[fsr.geometry.intersects(line)]
    out = []
    for _, r in hit.iterrows():
        it = r.geometry.intersection(line)
        pts = [it] if it.geom_type == 'Point' else [g for g in getattr(it,'geoms',[]) if g.geom_type=='Point']
        for p in pts:
            nm = r.get('ROAD_SECTION_NAME') or r.get('MAP_LABEL') or r.get('FOREST_FILE_ID') or '?'
            out.append((str(nm), p))
    return out

def osm_link(lat, lon, z=15):
    return f'https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map={z}/{lat}/{lon}'

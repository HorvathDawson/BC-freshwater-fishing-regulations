"""Inline FWA resolver for the remaining todo rows — falls, confluences, lake outlets, weirs,
park boundaries, and lake-anchored offsets. Writes output/fwa_candidates.json. FWA only (no OSM)."""
import json, re, warnings
warnings.filterwarnings('ignore')
import geopandas as gpd
from shapely.ops import linemerge, unary_union, nearest_points
from shapely.geometry import Point
from pyproj import Transformer
from pipeline.oneoff.waterbody_splits import load_curation

T3 = Transformer.from_crs(4326, 3005, always_xy=True)
T4 = Transformer.from_crs(3005, 4326, always_xy=True)
GP = 'data/bc_fisheries_data.gpkg'
STREAMS = gpd.read_file(GP, layer='streams')
LAKES = gpd.read_file(GP, layer='lakes')
WMU = gpd.read_file(GP, layer='wmu').to_crs(3005)
WF = gpd.read_file(GP, layer='waterfalls').to_crs(3005)
PARKS = gpd.read_file(GP, layer='parks_bc').to_crs(3005)


def rr(x):
    return [round(x[0], 5), round(x[1], 5)]


def rivername(nv):
    return re.sub(r'[",]', '', re.sub(r'\([^)]*\)', '', nv)).strip().title()


def river_geom(nv, mus, clip=True):
    sel = STREAMS[STREAMS.GNIS_NAME.astype(str).str.lower() == rivername(nv).lower()].to_crs(3005)
    if sel.empty:
        return None, None
    if clip:
        mp = WMU[WMU.WILDLIFE_MGMT_UNIT_ID.isin(mus or [])]
        if not mp.empty:
            ins = sel[sel.intersects(mp.union_all())]
            if not ins.empty:
                sel = ins
    return sel.union_all(), sel


def merged_line(sel):
    m = linemerge(unary_union(list(sel.geometry)))
    return max(m.geoms, key=lambda g: g.length) if m.geom_type == 'MultiLineString' else m


def resolve():
    rows = [r for r in load_curation() if r['status'] == 'todo']
    out = {}
    stats = {}
    for r in rows:
        lt = r['locator_text']
        low = lt.lower()
        nv, mus = r['name_verbatim'], r.get('mus') or []
        kind = None
        try:
            # FALLS (not "Falls Creek" tributary)
            if (r.get('resolver_hint') == 'falls_obstacle' or re.search(r'\bfalls?\b|canyon', low)) \
                    and not re.search(r'falls (creek|river)', low):
                riv, sel = river_geom(nv, mus)
                if riv is None:
                    continue
                near = WF[WF.geometry.distance(riv) < 260]
                if near.empty:
                    continue
                cands = [rr(T4.transform(g.centroid.x, g.centroid.y)) for g in near.geometry]
                kind = 'falls'
            # CONFLUENCE (tributary named in locator)
            elif r['anchor_kind'] == 'confluence':
                m = re.sub(r'(?i)^(confluence with|from|to|the|upstream of|downstream of)\s+', '', lt).strip()
                m = re.sub(r'(?i)\s+confluence$', '', m).title()
                trib = STREAMS[STREAMS.GNIS_NAME == m].to_crs(3005)
                riv, sel = river_geom(nv, mus, clip=False)
                if trib.empty or riv is None:
                    continue
                trib = trib.sort_values('DOWNSTREAM_ROUTE_MEASURE')
                g = trib.iloc[0].geometry
                mouth = Point((g if g.geom_type == 'LineString' else list(g.geoms)[0]).coords[0])
                if mouth.distance(riv) > 250:
                    mouth = nearest_points(riv, trib.union_all())[0]
                cands = [rr(T4.transform(mouth.x, mouth.y))]
                out[r['id']] = {'coord': cands[0], 'kind': 'confluence', 'cands': cands,
                                'target': {'blk': int(trib.iloc[0]['BLUE_LINE_KEY']),
                                           'wsc': trib.iloc[0]['FWA_WATERSHED_CODE']}}
                stats['confluence'] = stats.get('confluence', 0) + 1
                continue
            # LAKE outlet / WEIR at outlet
            elif re.search(r'outlet of .*lake|weir.*(outlet|lake)|lake.*outlet', low):
                lm = re.search(r'([A-Z][A-Za-z ]+? Lake)', lt)
                riv, sel = river_geom(nv, mus)
                if not lm or riv is None:
                    continue
                L = LAKES[LAKES.GNIS_NAME_1 == lm.group(1)].to_crs(3005)
                if L.empty:
                    continue
                L = L.assign(d=L.geometry.distance(riv)).sort_values('d')
                p = nearest_points(riv, L.iloc[0].geometry)[0]
                cands = [rr(T4.transform(p.x, p.y))]
                kind = 'lake_outlet'
            # PARK boundary
            elif re.search(r'\bpark\b', low):
                pm = re.search(r'([A-Z][A-Za-z ]+? Park)', lt)
                riv, sel = river_geom(nv, mus)
                if not pm or riv is None:
                    continue
                col = next((c for c in PARKS.columns if 'NAME' in c.upper()), None)
                P = PARKS[PARKS[col].astype(str).str.contains(pm.group(1).split(' Park')[0], case=False, na=False)] if col else PARKS.iloc[:0]
                if P.empty:
                    continue
                x = P.union_all().boundary.intersection(riv)
                pts = [x] if x.geom_type == 'Point' else list(getattr(x, 'geoms', []))
                cands = [rr(T4.transform(p.x, p.y)) for p in pts if p.geom_type == 'Point']
                if not cands:
                    continue
                kind = 'park'
            else:
                continue
            out[r['id']] = {'coord': cands[0], 'kind': kind, 'cands': cands}
            stats[kind] = stats.get(kind, 0) + 1
        except Exception:
            continue
    json.dump(out, open('output/fwa_candidates.json', 'w'), indent=1)
    print('FWA resolved:', len(out), '|', stats)


if __name__ == '__main__':
    resolve()

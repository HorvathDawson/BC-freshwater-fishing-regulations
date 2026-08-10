"""Paced Overpass batch for road/street crossings + urban landmarks that FWA can't give.
Writes results to osm_batch.txt. BC highways are often NAMED not ref'd."""
import urllib.request, json, time, pathlib
import fwa_helper as F
import geopandas as gpd
from shapely.geometry import LineString, Point
from shapely.ops import unary_union

HDR = {'User-Agent': 'bc-fishing-reg-curation/1.0 (horvath.dawson@gmail.com)'}
ENDPOINTS = ['https://overpass.kumi.systems/api/interpreter',
             'https://overpass-api.de/api/interpreter',
             'https://overpass.private.coffee/api/interpreter']
_last = [0.0]
def fetch(q):
    if time.time()-_last[0] < 1.5: time.sleep(1.5-(time.time()-_last[0]))
    for att in range(3):
        ep = ENDPOINTS[att % len(ENDPOINTS)]
        try:
            t = urllib.request.urlopen(urllib.request.Request(
                ep, data=('data='+q).encode(), headers=HDR), timeout=35).read().decode()
            _last[0] = time.time()
            if t.startswith('{'): return json.loads(t)
        except Exception as e:
            print('   fetch err', ep.split('/')[2], type(e).__name__, flush=True)
        time.sleep(3)
    _last[0] = time.time(); return None

def osm_river(name, box):
    d = fetch(f'[out:json][timeout:80];way[waterway~"river|stream|tidal_channel"][name~"{name}",i]({box[1]},{box[0]},{box[3]},{box[2]});out geom;')
    segs = [e for e in (d or {}).get('elements', []) if len(e.get('geometry', [])) > 1]
    if not segs: return None
    u = unary_union(gpd.GeoSeries([LineString([(p['lon'],p['lat']) for p in e['geometry']]) for e in segs], crs=4326).to_crs(3005).values)
    return u if u.geom_type=='LineString' else max(u.geoms, key=lambda x:x.length)

def cross(line3005, box, filt):
    """Find highway/road matching filt crossing line -> (lat,lon,label)."""
    if line3005 is None: return None
    d = fetch(f'[out:json][timeout:85];way[highway]{filt}({box[1]},{box[0]},{box[3]},{box[2]});out geom tags;')
    best = None
    for e in (d or {}).get('elements', []):
        pts = [(p['lon'],p['lat']) for p in e.get('geometry', [])]
        if len(pts) < 2: continue
        ln = gpd.GeoSeries([LineString(pts)], crs=4326).to_crs(3005)[0]
        it = line3005.intersection(ln)
        if it.is_empty: continue
        pt = it if it.geom_type=='Point' else list(it.geoms)[0]
        ll = gpd.GeoSeries([pt], crs=3005).to_crs(4326)[0]
        nm = e.get('tags',{}).get('name') or e.get('tags',{}).get('ref') or e['id']
        return (round(ll.y,5), round(ll.x,5), f"OSM way {e['id']} ({nm})")
    return None

def find_node(box, filt, kind='nwr'):
    """Find a named feature (bridge, culvert, weir) -> (lat,lon,label)."""
    d = fetch(f'[out:json][timeout:80];{kind}{filt}({box[1]},{box[0]},{box[3]},{box[2]});out center tags;')
    for e in (d or {}).get('elements', []):
        c = e.get('center') or {'lat': e.get('lat'), 'lon': e.get('lon')}
        if not c.get('lat'): continue
        nm = e.get('tags',{}).get('name') or e.get('tags',{}).get('bridge') or e.get('tags',{}).get('waterway') or e['id']
        return (round(c['lat'],5), round(c['lon'],5), f"OSM {e['type']} {e['id']} ({nm})")
    return None

R = []
def rec(tag, res, note=''):
    R.append((tag, res, note))
    line = f'{tag}:: {res}   {note}'
    print(line, flush=True)

# --- North Alouette River x 216 St (Maple Ridge) ---
L = osm_river('Alouette', (-122.62,49.20,-122.52,49.28))
rec('north-alouette-216st', cross(L,(-122.62,49.20,-122.52,49.28),'[~"name|ref"~"216",i]'), 'N Alouette x 216 St')

# --- Burton Creek x Hwy 6 (Arrow Lakes, Burton BC ~49.99,-118.0) ---
L = osm_river('Burton Creek', (-118.10,49.95,-117.95,50.05))
rec('burton-hwy6', cross(L,(-118.10,49.95,-117.95,50.05),'[ref~"6"]'), 'Burton Ck x Hwy6 (Arrow Lk)')

# --- Trepanier Creek x Hwy 97C (Peachland) ---
L = osm_river('Trepanier', (-119.85,49.74,-119.70,49.80))
rec('trepanier-97c', cross(L,(-119.85,49.74,-119.70,49.80),'[~"name|ref"~"97C|Okanagan Connector",i]'), 'Trepanier x 97C')
rec('trepanier-anyroad', cross(L,(-119.85,49.74,-119.70,49.80),'[bridge]'), 'Trepanier any bridge')

# --- Kitimat River x Hwy 37 bridge ---
L = osm_river('Kitimat River', (-128.72,54.00,-128.60,54.10))
rec('kitimat-hwy37', cross(L,(-128.72,54.00,-128.60,54.10),'[ref~"37"]'), 'Kitimat x Hwy37')

# --- Chowade River x Horseshoe Road bridge ---
L = osm_river('Chowade', (-122.90,56.60,-122.60,56.80))
rec('chowade-horseshoe', cross(L,(-122.90,56.60,-122.60,56.80),'[~"name"~"Horseshoe",i]'), 'Chowade x Horseshoe Rd')
rec('chowade-anybridge', cross(L,(-122.90,56.60,-122.60,56.80),'[bridge]'), 'Chowade any bridge')

# --- Elk / Coal Creek MF&M Railway bridge, Fernie ---
L = osm_river('Coal Creek', (-115.10,49.47,-114.98,49.52))
rec('coal-mfm-rail', find_node((-115.10,49.47,-114.98,49.52),'["railway"="bridge"]','way'), 'Coal Ck MF&M rail br (Fernie)')
rec('coal-anybridge', cross(L,(-115.10,49.47,-114.98,49.52),'[bridge]'), 'Coal Ck road bridge')

# --- Capilano hatchery footbridge (~100m below fish fence, below Cleveland Dam) ---
rec('capilano-weir', find_node((-123.125,49.335,-123.105,49.355),'[man_made=weir]','nwr'), 'Capilano weir/fence')
rec('capilano-footbridge', find_node((-123.125,49.335,-123.108,49.352),'[bridge][~"foot|highway"~"yes|footway|path",i]','way'), 'Capilano footbridge')

# --- Hays Creek culvert near fish cannery, Prince Rupert ---
rec('hays-cannery', find_node((-130.34,54.30,-130.28,54.34),'[~"name"~"cannery|Hays",i]','nwr'), 'Hays Ck cannery/culvert PR')

# --- Ksi X'anmas (Kwinamass) lower bridge ---
L = osm_river('Kwinamass', (-130.10,54.62,-129.85,54.80)) or osm_river("Ksi X", (-130.10,54.62,-129.85,54.80))
rec('ksixanmas-bridge', cross(L,(-130.10,54.62,-129.85,54.80),'[bridge]') if L else None, "Ksi X'anmas lower bridge")

# --- Thompson River near Martel (1 km downstream of Martel) ---
rec('martel-place', find_node((-120.30,50.65,-120.05,50.80),'[place][~"name"~"Martel",i]','node'), 'Martel locality')

# --- Ptarmigan Creek Quarry Bridge (McBride area ~53.4,-120.8) ---
L = osm_river('Ptarmigan', (-120.95,53.35,-120.70,53.50))
rec('ptarmigan-quarry', cross(L,(-120.95,53.35,-120.70,53.50),'[~"name"~"Quarry",i]'), 'Ptarmigan x Quarry Br')
rec('ptarmigan-anybridge', cross(L,(-120.95,53.35,-120.70,53.50),'[bridge]'), 'Ptarmigan any bridge')

# --- White River salmon viewing pool (near Sayward, VanIsle 1-10) ---
L = osm_river('White River', (-126.20,50.05,-125.85,50.35))
rec('white-anybridge', cross(L,(-126.20,50.05,-125.85,50.35),'[~"name"~"Sayward",i]'), 'White R x Sayward Rd')

pathlib.Path('osm_batch.txt').write_text('\n'.join(f'{t}:: {r}   {n}' for t,r,n in R))
print('DONE', len(R))

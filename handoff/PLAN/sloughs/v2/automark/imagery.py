"""Imagery signal per slough candidate: sample ESRI World Imagery (the app's satellite layer) inside each outline.

Output: imagery.json {id: {z, px, water, veg, bare, bright, sat_std, ...}} — fractions of the polygon's pixels.
Tiles cached under $TILE_CACHE (default scratch). ~1 request per tile, 8 threads.
"""
import gzip, io, json, math, os, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from PIL import Image, ImageDraw

HERE = os.path.dirname(os.path.abspath(__file__))
CAND = os.path.join(HERE, "..", "candidates_v2.json.gz")
OUT = os.path.join(HERE, "imagery.json")
CACHE = os.environ.get("TILE_CACHE", "/tmp/esri_tiles")
URL = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}?blankTile=false"
os.makedirs(CACHE, exist_ok=True)


def px(lon, lat, z):
    n = 256 * 2 ** z
    x = (lon + 180) / 360 * n
    s = math.sin(math.radians(lat))
    y = (0.5 - math.log((1 + s) / (1 - s)) / (4 * math.pi)) * n
    return x, y


def tile(z, x, y):
    p = os.path.join(CACHE, f"{z}_{x}_{y}.jpg")
    if not os.path.exists(p):
        req = urllib.request.Request(URL.format(z=z, x=x, y=y), headers={"User-Agent": "bc-regs-audit/1"})
        try:
            data = urllib.request.urlopen(req, timeout=30).read()
        except Exception:
            return None
        open(p, "wb").write(data)
    try:
        return Image.open(p).convert("RGB")
    except Exception:
        return None


def one(c):
    o = c["outline"]
    if not o or len(o) < 3:
        return c["id"], None
    lons = [p[0] for p in o]; lats = [p[1] for p in o]
    # largest zoom (<=17) where the bbox spans at most 3x3 tiles
    for z in range(17, 9, -1):
        x0, y1 = px(min(lons), min(lats), z); x1, y0 = px(max(lons), max(lats), z)
        if int(x1 // 256) - int(x0 // 256) <= 2 and int(y1 // 256) - int(y0 // 256) <= 2:
            break
    tx0, ty0, tx1, ty1 = int(x0 // 256), int(y0 // 256), int(x1 // 256), int(y1 // 256)
    W, H = (tx1 - tx0 + 1) * 256, (ty1 - ty0 + 1) * 256
    mosaic = Image.new("RGB", (W, H))
    for tx in range(tx0, tx1 + 1):
        for ty in range(ty0, ty1 + 1):
            im = tile(z, tx, ty)
            if im is None:
                return c["id"], None
            mosaic.paste(im, ((tx - tx0) * 256, (ty - ty0) * 256))
    pts = [(px(lo, la, z)[0] - tx0 * 256, px(lo, la, z)[1] - ty0 * 256) for lo, la in o]
    mask = Image.new("L", (W, H), 0)
    ImageDraw.Draw(mask).polygon(pts, fill=1)
    m = np.array(mask).astype(bool)
    if m.sum() < 20:  # too thin at this zoom: buffer by drawing the outline wide
        ImageDraw.Draw(mask).line(pts + [pts[0]], fill=1, width=3)
        m = np.array(mask).astype(bool)
    a = np.array(mosaic).astype(float)[m]
    r, g, b = a[:, 0], a[:, 1], a[:, 2]
    br = (r + g + b) / 3
    mx = a.max(1); mn = a.min(1)
    sat = np.where(mx > 0, (mx - mn) / np.maximum(mx, 1), 0)
    exg = 2 * g - r - b  # excess green
    water = (br < 75) & (b >= r - 5) & (exg < 18)          # dark, not green
    veg = (exg >= 18) | ((g > r) & (g > b) & (br >= 50))   # green
    bare = (br >= 120) & (sat < 0.25)                      # pale bare ground / dry bed / ice
    blank = float(((a.std(1) < 2) & (br > 240)).mean())   # 'no imagery' tiles are flat
    return c["id"], {
        "z": z, "px": int(m.sum()),
        "water": round(float(water.mean()), 3), "veg": round(float(veg.mean()), 3),
        "bare": round(float(bare.mean()), 3), "bright": round(float(br.mean()), 1),
        "bright_std": round(float(br.std()), 1), "exg": round(float(exg.mean()), 1),
        "blank": round(blank, 3),
    }


def main():
    cands = json.load(gzip.open(CAND))["candidates"]
    out = json.load(open(OUT)) if os.path.exists(OUT) else {}
    todo = [c for c in cands if c["id"] not in out]
    print(f"{len(todo)} to sample", flush=True)
    with ThreadPoolExecutor(8) as ex:
        for i, (cid, r) in enumerate(ex.map(one, todo)):
            out[cid] = r
            if i % 200 == 0:
                print(i, flush=True); json.dump(out, open(OUT, "w"))
    json.dump(out, open(OUT, "w"), indent=0)
    print("done", sum(v is not None for v in out.values()), "of", len(out))


if __name__ == "__main__":
    main()

"""Overzoom: the real web app at z16-18 over a river and a lake (expo web dev build — `globalThis.__map` is the map).

  .venv/bin/python handoff/PLAN/app-data/overzoom/overzoom_check.py --url http://localhost:39561 \
      --out handoff/PLAN/app-data/overzoom

For each place: jump to z13 to find a real feature near the place (the tiles say where the water is, not this file),
then to z14, 16, 17, 18 centred on a vertex of it. At each zoom, after the map settles: the camera's zoom (did the
ceiling hold), how many water features are drawn and how many carry the profile colouring's `p` (or a `status`) feature-state (colours on), whether a
tap at the centre answers through the app's own 24 px tap box (the renderer's `press`), a screenshot, and the
deepest tile zoom any raster source asked for (none may exceed its source maxzoom). Then the camera is asked for z20
and must stop at 18.
"""
import argparse, json, re, time
from playwright.sync_api import sync_playwright

PLACES = {
    # Vedder River below Vedder Crossing; Cultus Lake's north shore
    "river": {"at": [-121.985, 49.098], "layers": ["stream"]},
    "lake": {"at": [-121.985, 49.07], "layers": ["lake", "lake_flowing"]},
}

# a camera move that changes nothing fires no `idle`: wait for it, or 8 s, whichever comes first
SETTLE = """const settle = (m, f) => Promise.race([new Promise(r => { m.once('idle', r); f(); }),
                                         new Promise(r => setTimeout(r, 8000))]);"""

FIND = """async ([lon, lat, layers]) => {
  const m = globalThis.__map;
  await settle(m, () => m.jumpTo({ center: [lon, lat], zoom: 13 }));
  const c = m.project([lon, lat]);
  let best = null;
  for (const f of m.queryRenderedFeatures({ layers: layers.filter(id => m.getLayer(id)) })) {
    const g = f.geometry; const walk = (a) => { if (typeof a[0] === 'number') { const p = m.project(a);
      const d = (p.x - c.x) ** 2 + (p.y - c.y) ** 2; if (!best || d < best.d) best = { d, at: a, id: f.id, layer: f.layer.id }; }
      else for (const k of a) walk(k); };
    walk(g.coordinates);
  }
  return best;
}"""

PROBE = """async ([lon, lat, z, layers]) => {
  const m = globalThis.__map;
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  await settle(m, () => m.jumpTo({ center: [lon, lat], zoom: z }));
  const t0 = performance.now();
  while (performance.now() - t0 < 15000 && !(m.areTilesLoaded() && !m.isMoving())) await sleep(50);
  await sleep(1200);
  const present = layers.filter(id => m.getLayer(id));
  const fs = m.queryRenderedFeatures({ layers: present });
  const centre = m.project([lon, lat]);
  const box = [[centre.x - 12, centre.y - 12], [centre.x + 12, centre.y + 12]];
  const under = m.queryRenderedFeatures(box, { layers: present });
  const src = m.getSource('atlas');
  return { zoom: +m.getZoom().toFixed(2), drawn: fs.length,
           coloured: fs.filter(f => f.state && (f.state.p !== undefined || f.state.status !== undefined)).length,
           underFinger: under.length, underId: under[0] ? under[0].id : null,
           underState: under[0] ? under[0].state : null,
           atlasMaxzoom: src ? src.maxzoom : null, basemapMaxzoom: m.getSource('basemap')?.maxzoom ?? null,
           imageryMaxzoom: m.getSource('imagery')?.maxzoom ?? null };
}"""


def SETTLE_IN(fn: str) -> str:
    """`fn` with `settle` in scope (an evaluated function cannot see a page global we did not install)."""
    return "(" + "(...a) => { " + SETTLE + " return (" + fn + ")(...a); }" + ")"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:39561")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    out = {"places": {}, "errors": [], "rasterZooms": []}
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, args=["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist"])
        pg = b.new_page(viewport={"width": 390, "height": 844}, device_scale_factor=2)
        pg.on("console", lambda msg: out["errors"].append(msg.text) if msg.type == "error" else None)
        pg.on("requestfailed", lambda r: out["errors"].append(f"failed {r.url}"))

        def on_resp(r):
            mm = re.search(r"/g/(\d+)/\d+/\d+\.jpg", r.url)
            if mm:
                out["rasterZooms"].append(int(mm.group(1)))
                if r.status >= 400:
                    out["errors"].append(f"{r.status} {r.url}")
        pg.on("response", on_resp)
        pg.goto(a.url)
        pg.wait_for_function("globalThis.__map && globalThis.__map.loaded()", timeout=180000, polling=500)
        time.sleep(4)
        for name, place in PLACES.items():
            hit = pg.evaluate(SETTLE_IN(FIND), [*place["at"], place["layers"]])
            rows = {}
            for z in (14, 16, 17, 18):
                r = pg.evaluate(SETTLE_IN(PROBE), [*hit["at"], z, place["layers"]])
                shot = f"{a.out}/{name}-z{z}.png"
                pg.screenshot(path=shot)
                rows[z] = r
            # THE APP'S OWN TAP at z18: a click at the centre, then the sheet the app opens
            box = pg.evaluate("() => { const r = globalThis.__map.getContainer().getBoundingClientRect(); "
                              "return [r.left + r.width / 2, r.top + r.height / 2]; }")
            pg.mouse.click(*box)
            time.sleep(2.5)
            pg.screenshot(path=f"{a.out}/{name}-z18-tap.png")
            rows["tapText"] = pg.evaluate("() => document.body.innerText.slice(0, 400)")
            # back to the map: the place page replaced it
            # the page's BACK link ("‹  Map"), not its own Map tab — that is a second, smaller map
            pg.get_by_text(re.compile(r"^\u2039\s+\w+$")).first.click()
            pg.wait_for_function("globalThis.__map && globalThis.__map.loaded()", timeout=60000, polling=500)
            time.sleep(2)
            out["places"][name] = {"found": hit, "rows": rows}
        # THE CEILING: ask for z20, get z18
        out["askedFor20"] = pg.evaluate(SETTLE_IN("""async () => { const m = globalThis.__map;
            await settle(m, () => m.jumpTo({ zoom: 12 })); await settle(m, () => m.jumpTo({ zoom: 20 }));
            return m.getZoom(); }"""))
        # SATELLITE at z18: the imagery overzooms from its own maxzoom, never asking past it
        pg.evaluate("""async () => { const m = globalThis.__map;
            if (m.getLayer('imagery')) m.setLayoutProperty('imagery', 'visibility', 'visible'); }""")
        lake = out["places"]["lake"]["found"]["at"]
        pg.evaluate(SETTLE_IN(PROBE), [*lake, 18, ["lake"]])
        time.sleep(3)
        pg.screenshot(path=f"{a.out}/lake-z18-imagery.png")
        b.close()
    out["maxRasterZoom"] = max(out["rasterZooms"], default=None)
    del out["rasterZooms"]
    with open(f"{a.out}/results.json", "w") as f:
        json.dump(out, f, indent=1)
    print(json.dumps(out, indent=1)[:6000])


main()

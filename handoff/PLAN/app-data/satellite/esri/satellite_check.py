"""Satellite = Esri World Imagery, online-only: the real web app (expo web dev build; `globalThis.__map` is the map).

  .venv/bin/python handoff/PLAN/app-data/satellite/esri/satellite_check.py --url http://localhost:39318 \
      --out handoff/PLAN/app-data/satellite/esri

Through the app's own controls: open Layers, choose Satellite. Over the Vedder River (below Vedder Crossing) at z12,
z16 and z18: the imagery layer's visibility, the imagery source's tile URL and maxzoom, the Esri tile requests (zoom,
status), the map's attribution text, and a screenshot. Then the browser goes OFFLINE (Playwright's network emulation,
which also sets navigator.onLine and fires `offline`): the drawn map, Layers says why, Satellite still chosen, no Esri
credit. Then back ONLINE: satellite returns by itself.
"""
import argparse, json, re, time
from playwright.sync_api import sync_playwright

VEDDER = [-121.985, 49.098]

SETTLE = """const settle = (m, f) => Promise.race([new Promise(r => { m.once('idle', r); f(); }),
                                         new Promise(r => setTimeout(r, 10000))]);"""

STATE = """() => { const m = globalThis.__map; const s = m.getSource('imagery');
  const attrib = document.querySelector('.maplibregl-ctrl-attrib-inner');
  return { zoom: +m.getZoom().toFixed(2),
           imagery: m.getLayer('imagery') ? m.getLayoutProperty('imagery', 'visibility') ?? 'visible' : 'absent',
           tiles: s ? s.tiles : null, maxzoom: s ? s.maxzoom : null,
           attribution: attrib ? attrib.textContent : null,
           onLine: navigator.onLine }; }"""

GOTO = """async ([lon, lat, z]) => {
  const m = globalThis.__map;
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  await settle(m, () => m.jumpTo({ center: [lon, lat], zoom: z }));
  const t0 = performance.now();
  while (performance.now() - t0 < 20000 && !(m.areTilesLoaded() && !m.isMoving())) await sleep(100);
  await sleep(1500);
}"""


def with_settle(fn: str) -> str:
    return "(" + "(...a) => { " + SETTLE + " return (" + fn + ")(...a); }" + ")"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:39318")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    out = {"steps": {}, "esri": [], "errors": []}
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, args=["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist"])
        ctx = b.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2)
        pg = ctx.new_page()
        pg.on("console", lambda m: out["errors"].append(m.text[:300]) if m.type == "error" else None)

        def on_resp(r):
            mm = re.search(r"World_Imagery/MapServer/tile/(\d+)/\d+/\d+", r.url)
            if mm:
                out["esri"].append({"z": int(mm.group(1)), "status": r.status})
        pg.on("response", on_resp)
        pg.goto(a.url)
        pg.wait_for_function("globalThis.__map && globalThis.__map.loaded()", timeout=240000, polling=500)
        time.sleep(4)
        out["steps"]["start"] = pg.evaluate(STATE)

        def layers_open():
            pg.get_by_label("Choose layers").first.click()
            time.sleep(1.2)

        def layers_close():
            pg.keyboard.press("Escape")
            time.sleep(1.2)

        # THE READER'S CHOICE: Layers -> Basemap -> Satellite
        layers_open()
        pg.get_by_role("radio", name="Satellite").click()
        time.sleep(1)
        layers_close()
        for z in (12, 16, 18):
            pg.evaluate(with_settle(GOTO), [*VEDDER, z])
            out["steps"][f"satellite-z{z}"] = pg.evaluate(STATE)
            pg.screenshot(path=f"{a.out}/vedder-satellite-z{z}.png")

        # OFFLINE: the drawn map, and Layers says why — Satellite still chosen
        pg.evaluate(with_settle(GOTO), [*VEDDER, 14])
        ctx.set_offline(True)
        time.sleep(2)
        out["steps"]["offline"] = pg.evaluate(STATE)
        pg.screenshot(path=f"{a.out}/vedder-offline-map.png")
        layers_open()
        out["steps"]["offline"]["note"] = pg.evaluate(
            "() => document.querySelector('[data-testid=basemap-note]')?.textContent ?? null")
        out["steps"]["offline"]["satelliteChecked"] = pg.get_by_role("radio", name="Satellite") \
            .get_attribute("aria-checked")
        pg.screenshot(path=f"{a.out}/vedder-offline-layers.png")
        layers_close()

        # BACK ONLINE: satellite returns by itself
        ctx.set_offline(False)
        time.sleep(1)
        pg.evaluate(with_settle(GOTO), [*VEDDER, 14])
        out["steps"]["online-again"] = pg.evaluate(STATE)
        pg.screenshot(path=f"{a.out}/vedder-online-again.png")
        layers_open()
        out["steps"]["online-again"]["note"] = pg.evaluate(
            "() => document.querySelector('[data-testid=basemap-note]')?.textContent ?? null")
        layers_close()
        b.close()
    zs = sorted({e["z"] for e in out["esri"]})
    out["esriSummary"] = {"requests": len(out["esri"]), "zooms": zs,
                          "statuses": sorted({e["status"] for e in out["esri"]})}
    del out["esri"]
    with open(f"{a.out}/results.json", "w") as f:
        json.dump(out, f, indent=1)
    print(json.dumps(out, indent=1)[:8000])


main()

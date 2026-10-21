"""The Layers sheet's lake modes and the basemap toggle, timed in the real app (Map tab): each switch from the click to
the last frame whose lake colours changed, the paint calls it made, long tasks; and the lake features' feature-state
keys after the switch (which values the mode reads, and whether any were ever set).

  .venv/bin/python handoff/PLAN/app-data/colour-latency/harness/lake_modes_measure.py --url http://localhost:39562 \
      --out <file.json> [--size 390x844 --net 4g --cpu 4]
"""
import argparse, json
from pathlib import Path
from playwright.sync_api import sync_playwright

INSTALL = """() => {
  const m = globalThis.__map; const T = () => performance.now();
  const A = window.L = { lt: [], idles: [], frames: [], paints: 0 };
  new PerformanceObserver(l => { for (const e of l.getEntries()) A.lt.push([e.startTime, e.duration]); }).observe({ type: 'longtask' });
  m.on('idle', () => A.idles.push(T()));
  const sp = m.setPaintProperty.bind(m);
  m.setPaintProperty = (...a) => { A.paints++; return sp(...a); };
  const sl = m.setLayoutProperty.bind(m);
  m.setLayoutProperty = (...a) => { A.paints++; return sl(...a); };
  m.on('render', () => {
    const fc = JSON.stringify(m.getPaintProperty('lake', 'fill-color')).length + ':' +
               m.getLayoutProperty('imagery', 'visibility') + ':' + m.getLayoutProperty('contour', 'visibility');
    A.frames.push([T(), fc]);
  });
}"""


def timed(pg, label, act):
    pg.evaluate("() => { const L = window.L; L.t0 = performance.now(); L.p0 = L.paints; L.frames.length = 0; }")
    act()
    pg.wait_for_timeout(3000)
    return pg.evaluate("""([label]) => { const L = window.L, m = globalThis.__map;
      let last = null, prev = null; for (const f of L.frames) { if (prev !== null && f[1] !== prev) last = f[0]; prev = f[1]; }
      const lakes = m.queryRenderedFeatures({ layers: ['lake'] });
      const keys = {}; for (const f of lakes) for (const k of Object.keys(f.state ?? {})) keys[k] = (keys[k] ?? 0) + 1;
      return { label, paintCalls: L.paints - L.p0, settledMs: last === null ? 0 : Math.round(last - L.t0),
               idleMs: Math.round((L.idles.find(t => t >= L.t0) ?? NaN) - L.t0),
               longtasks: Math.round(L.lt.filter(([s]) => s >= L.t0).reduce((a, [, d]) => a + d, 0)),
               lakeMode: JSON.stringify(m.getPaintProperty('lake', 'fill-color')).slice(0, 90),
               lakesRendered: lakes.length, lakeStateKeys: keys,
               imagery: m.getLayoutProperty('imagery', 'visibility'), contour: m.getLayoutProperty('contour', 'visibility') }; }""",
                       [label])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--size", default="1280x800")
    ap.add_argument("--net", default="none")
    ap.add_argument("--cpu", type=float, default=1)
    a = ap.parse_args()
    w, h = map(int, a.size.split("x"))
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, args=["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist"])
        pg = b.new_page(viewport={"width": w, "height": h}, device_scale_factor=2 if w < 500 else 1)
        cdp = pg.context.new_cdp_session(pg)
        if a.net == "4g":
            cdp.send("Network.enable")
            cdp.send("Network.emulateNetworkConditions", {"offline": False, "latency": 120,
                     "downloadThroughput": 9e6 / 8, "uploadThroughput": 3e6 / 8})
        pg.goto(a.url)
        pg.wait_for_function("globalThis.__map && globalThis.__map.loaded()", timeout=240000, polling=500)
        pg.evaluate("""async () => { const m = globalThis.__map; await new Promise(r => { m.once('moveend', r);
            m.jumpTo({ center: [-121.95, 49.35], zoom: 11.5 }); }); await new Promise(r => setTimeout(r, 6000)); }""")
        pg.evaluate(INSTALL)
        if a.cpu > 1:
            cdp.send("Emulation.setCPUThrottlingRate", {"rate": a.cpu})
        pg.get_by_role("button", name="Choose layers").click()
        pg.wait_for_timeout(800)
        steps = []
        for name in ["Stocked", "Depth", "Plain"]:
            steps.append(timed(pg, f"lakes {name}", lambda n=name: pg.get_by_role("radio", name=n, exact=True).first.click()))
        for name in ["Satellite", "Map"]:
            steps.append(timed(pg, f"basemap {name}", lambda n=name: pg.get_by_role("radio", name=n, exact=True).first.click()))
        b.close()
    print(json.dumps(steps, indent=1))
    Path(a.out).write_text(json.dumps({"size": a.size, "net": a.net, "cpu": a.cpu, "steps": steps}, indent=1))


main()

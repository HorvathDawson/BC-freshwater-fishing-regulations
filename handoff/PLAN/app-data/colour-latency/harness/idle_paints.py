"""Paint and feature-state calls the map makes while NOTHING happens — the Map tab and the Conditions tab, each left
alone for `--seconds` after settling. A look effect re-running on every Shell render shows up here as hundreds of no-op
`setPaintProperty` calls per render (the WIP feed-swap run recorded 71,940 over five idle minutes).

  .venv/bin/python handoff/PLAN/app-data/colour-latency/harness/idle_paints.py --url http://localhost:39642 [--seconds 60]
"""
import argparse, json
from playwright.sync_api import sync_playwright

INSTALL = """() => { const m = globalThis.__map; const A = window.I = { paint: {}, fs: 0, layout: 0 };
  const sp = m.setPaintProperty.bind(m);
  m.setPaintProperty = (l, p, v) => { const k = l + ' ' + p; A.paint[k] = (A.paint[k] ?? 0) + 1; return sp(l, p, v); };
  const fs = m.setFeatureState.bind(m); m.setFeatureState = (f, s) => { A.fs++; return fs(f, s); };
  const sl = m.setLayoutProperty.bind(m); m.setLayoutProperty = (...a) => { A.layout++; return sl(...a); }; }"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--seconds", type=int, default=60)
    a = ap.parse_args()
    out = {}
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, args=["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist"])
        pg = b.new_page(viewport={"width": 1280, "height": 800})
        pg.goto(a.url)
        pg.wait_for_function("globalThis.__map && globalThis.__map.loaded()", timeout=240000, polling=500)
        for tab in ["Map", "Conditions"]:
            pg.get_by_role("tab", name=tab).click()
            pg.wait_for_timeout(8000)
            pg.evaluate(INSTALL)
            pg.wait_for_timeout(a.seconds * 1000)
            I = pg.evaluate("() => window.I")
            out[tab] = {"paintCalls": sum(I["paint"].values()), "featureState": I["fs"], "layout": I["layout"],
                        "topPaints": sorted(I["paint"].items(), key=lambda x: -x[1])[:5]}
        b.close()
    print(json.dumps(out, indent=1))


main()

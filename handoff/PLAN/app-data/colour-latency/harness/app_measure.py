"""Colour latency in the REAL app (expo web dev build; `globalThis.__map` is the map) — before and after AP.

  .venv/bin/python handoff/PLAN/app-data/colour-latency/harness/app_measure.py --url http://localhost:39472 \
      --state p|status --out <file.json> [--size 390x844] [--net 4g --cpu 4] [--public] [--shots <dir>]

`--state` is the feature-state key that means "this section has its colour": `status` for the old idle path, `p` for
AP (the profile label). Every rendered frame is probed (`queryRenderedFeatures` over the status layers) for visible
sections WITHOUT that key — drawn in the layer's grey/plain fallback. Runs the study's 9-pan script at z8, z11 and z13
at (-121.0, 52.0), then a day change ("One month earlier" in the legend, x3) and a theme change (Layers -> Palette ->
Dark), recording per step: grey frames while moving / after the camera stops, ms from pan end to every visible section
coloured, colouring work (time inside setFeatureState + the map's own __colourCost record), long tasks, heap.
`--public` sets the dev flag that makes the map take the public querySourceFeatures route (the GL-internals fallback).
"""
import argparse, json, statistics as st, time
from pathlib import Path
from playwright.sync_api import sync_playwright

INSTALL = """(key) => {
  const m = globalThis.__map; const T = () => performance.now();
  const A = window.A = { key, sets: [], idles: [], frames: [], pans: [], lt: [], fs: { ms: 0, n: 0 }, mem: {},
                         paint: [] };
  new PerformanceObserver(l => { for (const e of l.getEntries()) A.lt.push([Math.round(e.startTime), Math.round(e.duration)]); })
    .observe({ type: 'longtask', buffered: false });
  const orig = m.setFeatureState.bind(m);
  m.setFeatureState = (f, s) => { const t = T(); const r = orig(f, s); A.fs.ms += T() - t; A.fs.n++; return r; };
  m.on('idle', () => A.idles.push(Math.round(T())));
  const layers = ['stream', 'lake', 'lake_flowing', 'wetland_flowing'].filter(id => m.getLayer(id));
  m.on('render', () => {
    let grey = 0, total = 0;
    for (const f of m.queryRenderedFeatures({ layers })) {
      if (f.id === undefined) continue; total++;
      if (f.state?.[key] === undefined) grey++;
    }
    A.frames.push([Math.round(T()), grey, total]);
  });
  A.mark = (k) => { A.mem[k] = performance.memory ? Math.round(performance.memory.usedJSHeapSize / 1048576) : null; };
  return layers;
}"""

PANS = """async ([lon, lat, z, label]) => {
  const m = globalThis.__map; const A = window.A; const T = () => performance.now();
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const move = f => new Promise(r => { m.once('moveend', r); f(); });
  const settle = async after => { const t0 = T(); while (T() - t0 < 20000) {
      if (A.idles.some(t => t >= after) && !m.isMoving() && m.areTilesLoaded()) return; await sleep(25); } };
  await move(() => m.jumpTo({ center: [lon, lat], zoom: z }));
  await settle(T()); await sleep(2500);
  const W = m.getContainer().clientWidth, H = m.getContainer().clientHeight;
  const dirs = [[1,0],[0,1],[-1,0],[-1,0],[0,-1],[0,-1],[1,0],[1,0],[0,1]];
  for (const [i, [dx, dy]] of dirs.entries()) {
    const c0 = (globalThis.__colourCost ?? []).length, f0 = { ...A.fs };
    const tStart = T();
    await move(() => m.panBy([dx * W, dy * H], { duration: 400, easing: t => t }));
    const tEnd = T();
    await settle(tEnd); await sleep(1500);
    const cost = (globalThis.__colourCost ?? []).slice(c0);
    A.pans.push({ label: `${label}/pan${i+1}`, tStart: Math.round(tStart), tEnd: Math.round(tEnd), tNext: Math.round(T()),
                  costMs: cost.reduce((s, c) => s + c.ms, 0), costCalls: cost.reduce((s, c) => s + c.calls, 0),
                  fsMs: A.fs.ms - f0.ms, fsN: A.fs.n - f0.n });
  }
  return true;
}"""


def paint_kind(pg):
    return pg.evaluate("""() => { const s = JSON.stringify(globalThis.__map.getPaintProperty('stream', 'line-color'));
        return s.includes('"feature-state","p"') ? 'profile' : s.includes('"feature-state","status"') ? 'status' : 'plain'; }""")


def timed_change(pg, label, act):
    """Do `act`, then follow frames until the map is idle again: sync ms, setFeatureState calls, idle after, grey."""
    t0 = pg.evaluate("() => { window.A.c0 = { ...window.A.fs, k: (globalThis.__colourCost ?? []).length }; "
                     "window.A.t0 = performance.now(); return window.A.t0; }")
    act()
    pg.wait_for_timeout(2500)
    return pg.evaluate("""([label]) => { const A = window.A; const after = A.frames.filter(f => f[0] >= A.t0);
        const idle = A.idles.find(t => t >= A.t0);
        const cost = (globalThis.__colourCost ?? []).slice(A.c0.k);
        return { label, fsCalls: A.fs.n - A.c0.n, fsMs: +(A.fs.ms - A.c0.ms).toFixed(1),
                 colourCost: cost.map(c => [c.kind, +c.ms.toFixed(2), c.calls]),
                 idleMs: idle === undefined ? null : Math.round(idle - A.t0),
                 greyFrames: after.filter(f => f[1] > 0).length, frames: after.length,
                 longtasks: A.lt.filter(([s]) => s >= A.t0).reduce((s, [, d]) => s + d, 0) }; }""", [label])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--state", choices=["p", "status"], required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--size", default="1280x800")
    ap.add_argument("--net", default="none")
    ap.add_argument("--cpu", type=float, default=1)
    ap.add_argument("--public", action="store_true")
    ap.add_argument("--shots")
    a = ap.parse_args()
    w, h = map(int, a.size.split("x"))
    args = ["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist", "--enable-precise-memory-info"]
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, args=args)
        pg = b.new_page(viewport={"width": w, "height": h}, device_scale_factor=2 if w < 500 else 1)
        cdp = pg.context.new_cdp_session(pg)
        if a.net == "4g":
            cdp.send("Network.enable")
            cdp.send("Network.emulateNetworkConditions", {"offline": False, "latency": 120,
                     "downloadThroughput": 9e6 / 8, "uploadThroughput": 3e6 / 8})
        pg.goto(a.url)
        pg.wait_for_function("globalThis.__map && globalThis.__map.loaded()", timeout=240000, polling=500)
        # the status must be ON before measuring: the profile (or status) paint installed on the rivers
        want = "profile" if a.state == "p" else "status"
        for _ in range(240):
            if paint_kind(pg) == want:
                break
            time.sleep(0.5)
        print("paint", paint_kind(pg))
        if a.public:
            pg.evaluate("() => { globalThis.__colourPublic = true; }")
        print("status layers", pg.evaluate(INSTALL, a.state))
        if a.cpu > 1:
            cdp.send("Emulation.setCPUThrottlingRate", {"rate": a.cpu})
        pg.evaluate("() => window.A.mark('start')")
        for z in (8, 11, 13):
            pg.evaluate(PANS, [-121.0, 52.0, z, f"z{z}"])
            if a.shots:
                Path(a.shots).mkdir(parents=True, exist_ok=True)
                pg.screenshot(path=f"{a.shots}/{a.state}{'-public' if a.public else ''}-{a.size}-z{z}.png")
        pg.evaluate("() => window.A.mark('afterPans')")
        # back to z11 for the day and theme changes
        pg.evaluate("""async () => { const m = globalThis.__map; await new Promise(r => { m.once('moveend', r);
            m.jumpTo({ center: [-121.0, 52.0], zoom: 11 }); }); await new Promise(r => setTimeout(r, 3000)); }""")
        changes = []
        for i in range(3):
            changes.append(timed_change(pg, f"day-{i+1}",
                                        lambda: pg.get_by_role("button", name="One month earlier").click()))
        pg.evaluate("() => window.A.mark('afterDay')")
        changes[-1]["paint"] = paint_kind(pg)
        pg.get_by_role("button", name="Choose layers").click()
        pg.wait_for_timeout(800)
        changes.append(timed_change(pg, "theme-dark", lambda: pg.get_by_role("radio", name="Dark").click()))
        changes[-1]["paint"] = paint_kind(pg)
        if a.shots:
            pg.keyboard.press("Escape")
            pg.wait_for_timeout(1200)
            pg.screenshot(path=f"{a.shots}/{a.state}{'-public' if a.public else ''}-{a.size}-dark.png")
        pg.evaluate("() => window.A.mark('end')")
        A = pg.evaluate("window.A")
        b.close()
    rows = []
    for pn in A["pans"]:
        fr = [f for f in A["frames"] if pn["tStart"] <= f[0] < pn["tNext"]]
        moving = sum(1 for f in fr if f[0] <= pn["tEnd"] and f[1] > 0)
        after = [f for f in fr if f[0] > pn["tEnd"]]
        wrong_after = [f for f in after if f[1] > 0]
        lat = 0
        if wrong_after:
            last = wrong_after[-1][0]
            nxt = [f[0] for f in after if f[0] > last and f[1] == 0]
            lat = nxt[0] - pn["tEnd"] if nxt else None
        rows.append({"label": pn["label"], "greyMoving": moving, "greyAfter": len(wrong_after), "colouredMs": lat,
                     "maxGreyFrac": round(max((f[1] / f[2] for f in fr if f[2]), default=0), 3),
                     "workMs": round(max(pn["costMs"], pn["fsMs"]), 2), "fsMs": round(pn["fsMs"], 2),
                     "fsCalls": pn["fsN"], "longtasks": sum(d for s, d in A["lt"] if pn["tStart"] <= s < pn["tNext"])})
    by_z = {}
    for z in ("z8", "z11", "z13"):
        rs = [r for r in rows if r["label"].startswith(z + "/")]
        lat = [r["colouredMs"] for r in rs if r["colouredMs"] is not None]
        by_z[z] = {"pansGreyMoving": sum(1 for r in rs if r["greyMoving"]),
                   "pansGreyAfter": sum(1 for r in rs if r["greyAfter"]),
                   "greyFramesAfter": sum(r["greyAfter"] for r in rs),
                   "colouredMs": {"median": st.median(lat) if lat else None, "max": max(lat) if lat else None},
                   "workMs": {"median": st.median([r["workMs"] for r in rs]), "max": max(r["workMs"] for r in rs)},
                   "fsCalls": {"median": st.median([r["fsCalls"] for r in rs]), "max": max(r["fsCalls"] for r in rs)},
                   "longtasksMs": sum(r["longtasks"] for r in rs)}
    summary = {"state": a.state, "public": a.public, "size": a.size, "net": a.net, "cpu": a.cpu, "byZoom": by_z,
               "changes": changes, "heapMB": A["mem"], "rows": rows}
    print(json.dumps({k: v for k, v in summary.items() if k != "rows"}, indent=1))
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    with open(a.out, "w") as f:
        json.dump(summary, f, indent=1)


main()

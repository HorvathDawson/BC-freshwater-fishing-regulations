"""Colour latency on the CONDITIONS tab of the real app (expo web dev build; `globalThis.__map` is the map) — the
pushed path (before) and the per-tile class path (after, the colouring engine's `standing` channel).

  .venv/bin/python handoff/PLAN/app-data/colour-latency/harness/conditions_measure.py --url http://localhost:39561 \
      --path pushed|state|classed --out <file.json> [--size 390x844] [--net 4g --cpu 4] [--shots <dir>] [--dump <file.json>]

`pushed` = the old idle path (before), `state` = the shipped engine (both read feature-state `standing`), `classed` = the
measured-not-shipped style-table variant (its app code was not kept). A rendered flow feature is
UNCOLOURED in a frame when it has no value yet but has one once the map settles: the `standing` feature-state, or the
class path's `c` label (its value is in the style from the moment the feed
answers, so a labelled section is coloured). Sections nothing speaks for have no value on either path at settle and are
never counted. z8 is the basin field (the rivers stand down below z9): the pushed path sets `standing` per basin after
`idle`; the class path reads the basin's own id in the style, so a basin is coloured whenever the values are installed.

Per run: the study's 9-pan script at z8, z11 and z13 at `--at` (default the app's HOME, (-121.85, 49.15): the Fraser Valley, densely gauged); then the horizon chips (+1d, +3d, Now) and the
quantity (Level, Flow), each timed from the click to the last frame whose colours changed; per step the feature-state
calls and time, the map's own `__colourCost`, long tasks. `--dump` writes every settled section's value (layer:id ->
ramp value or null) after each pan and change, for the old-vs-new correctness comparison (`conditions_compare.py`).
"""
import argparse, json, statistics as st, time
from pathlib import Path
from playwright.sync_api import sync_playwright

INSTALL = """(path) => {
  const m = globalThis.__map; const T = () => performance.now();
  const A = window.A = { path, idles: [], frames: [], pans: [], lt: [], fs: { ms: 0, n: 0 }, mem: {}, dumps: {} };
  new PerformanceObserver(l => { for (const e of l.getEntries()) A.lt.push([Math.round(e.startTime), Math.round(e.duration)]); })
    .observe({ type: 'longtask', buffered: false });
  const orig = m.setFeatureState.bind(m);
  m.setFeatureState = (f, s) => { const t = T(); const r = orig(f, s); A.fs.ms += T() - t; A.fs.n++; return r; };
  // the look's cost: every paint and layout property set (a horizon change should set none — engine polish)
  A.paint = { paint: 0, layout: 0 };
  const sp = m.setPaintProperty.bind(m), sl = m.setLayoutProperty.bind(m);
  m.setPaintProperty = (...x) => { A.paint.paint++; return sp(...x); };
  m.setLayoutProperty = (...x) => { A.paint.layout++; return sl(...x); };
  m.on('idle', () => A.idles.push(Math.round(T())));
  // the class path's tables, read out of the paint the map is drawing (the stream's, and the field's)
  const cache = {};
  const table = (layer) => {
    const e = m.getPaintProperty(layer, layer === 'basin' ? 'fill-color' : 'line-color') ?? null;
    if (cache[layer] && cache[layer][0] === e) return cache[layer][1];
    const t = parse(e); cache[layer] = [e, t]; return t;
  };
  // walked, not stringified: a 227 KB JSON round trip per paint change would be the probe's own long task
  const parse = (e) => {
    if (!Array.isArray(e)) return null;
    if (e[0] === 'literal') return e[1];
    for (const x of e) { const t = parse(x); if (t !== null) return t; }
    return null;
  };
  const parseOld = (e) => {
    const s = JSON.stringify(e);
    const i = s.indexOf('["literal",'); if (i < 0) return null;
    let d = 0, j = i;
    for (; j < s.length; j++) { if (s[j] === '[' || s[j] === '{') d++; else if (s[j] === ']' || s[j] === '}') { d--; if (d === 0) break; } }
    return JSON.parse(s.slice(i, j + 1))[1];
  };
  A.table = table;
  /** Every rendered flow feature's value now: the ramp value, or null (nothing to say / not yet). */
  A.values = () => {
    const out = {};
    const flow = ['stream', 'lake', 'lake_flowing', 'wetland_flowing'].filter(id => m.getLayer(id));
    const T1 = path === 'classed' ? table('stream') : null, TB = path === 'classed' ? table('basin') : null;
    for (const f of m.queryRenderedFeatures({ layers: [...flow, ...(m.getLayer('basin') ? ['basin'] : [])] })) {
      const layer = f.layer.id;
      if (layer === 'basin') {
        const id = String(f.properties.basin_id);
        const v = path !== 'classed' ? f.state?.standing : TB ? TB[id] : undefined;
        out['basin:' + id] = typeof v === 'number' ? v : null;
        continue;
      }
      if (f.id === undefined) continue;
      let v;
      if (path !== 'classed') v = f.state?.standing;
      else { const c = f.state?.c; v = typeof c === 'number' && T1 ? T1[c] : undefined; if (v === -1000) v = undefined; }
      out[f.sourceLayer + ':' + f.id] = typeof v === 'number' ? v : null;
    }
    return out;
  };
  /** Per frame: which rendered features have NO value yet (by key), and a hash of every value. */
  m.on('render', () => {
    const v = A.values(); let h = 0, n = 0; const none = [];
    for (const [k, x] of Object.entries(v)) {
      n++; if (x === null) none.push(k);
      const s = k + '=' + x; for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
    }
    A.frames.push([Math.round(T()), none, n, h]);
  });
  A.mark = (k) => { A.mem[k] = performance.memory ? Math.round(performance.memory.usedJSHeapSize / 1048576) : null; };
  return true;
}"""

PANS = """async ([lon, lat, z, label]) => {
  const m = globalThis.__map; const A = window.A; const T = () => performance.now();
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const move = f => new Promise(r => { m.once('moveend', r); f(); });
  const settle = async after => { const t0 = T(); while (T() - t0 < 20000) {
      if (A.idles.some(t => t >= after) && !m.isMoving() && m.areTilesLoaded()) return; await sleep(25); } };
  await move(() => m.jumpTo({ center: [lon, lat], zoom: z }));
  await settle(T()); await sleep(3000);
  const W = m.getContainer().clientWidth, H = m.getContainer().clientHeight;
  const dirs = [[1,0],[0,1],[-1,0],[-1,0],[0,-1],[0,-1],[1,0],[1,0],[0,1]];
  for (const [i, [dx, dy]] of dirs.entries()) {
    const c0 = (globalThis.__colourCost ?? []).length, f0 = { ...A.fs };
    const fi = A.frames.length;
    const tStart = T();
    await move(() => m.panBy([dx * W, dy * H], { duration: 400, easing: t => t }));
    const tEnd = T();
    await settle(tEnd); await sleep(1500);
    // THE SETTLED TRUTH: what has a value now. A frame is uncoloured when it drew one of these without its value.
    const settled = A.values();
    const has = new Set(Object.keys(settled).filter(k => settled[k] !== null));
    let movingGrey = 0, afterGrey = 0, lastGrey = null;
    const fr = A.frames.slice(fi);
    for (const [t, none] of fr) {
      const bad = none.some(k => has.has(k));
      if (!bad) continue;
      if (t <= tEnd) movingGrey++; else { afterGrey++; lastGrey = t; }
    }
    let colouredMs = 0;
    if (lastGrey !== null) { const nxt = fr.find(f => f[0] > lastGrey); colouredMs = nxt ? nxt[0] - tEnd : null; }
    const cost = (globalThis.__colourCost ?? []).slice(c0);
    const lab = `${label}/pan${i+1}`;
    A.dumps[lab] = settled;
    A.pans.push({ label: lab, tStart: Math.round(tStart), tEnd: Math.round(tEnd), tNext: Math.round(T()),
                  movingGrey, afterGrey, colouredMs, frames: fr.length, settledWithValue: has.size,
                  rendered: Object.keys(settled).length,
                  maxGreyFrac: Math.max(0, ...fr.map(f => f[2] ? f[1].filter(k => has.has(k)).length / f[2] : 0)),
                  costMs: cost.reduce((s, c) => s + c.ms, 0), costCalls: cost.reduce((s, c) => s + c.calls, 0),
                  fsMs: A.fs.ms - f0.ms, fsN: A.fs.n - f0.n,
                  longtasks: A.lt.filter(([s]) => s >= tStart).reduce((s, [, d]) => s + d, 0) });
    A.frames.length = 0;
  }
  return true;
}"""


def timed_change(pg, label, act):
    """Do `act`, then follow frames until every colour has settled: sync work, calls, ms to the last colour change."""
    pg.evaluate("() => { const A = window.A; A.c0 = { ...A.fs, k: (globalThis.__colourCost ?? []).length, "
                "p: A.paint.paint, l: A.paint.layout }; "
                "A.frames.length = 0; A.t0 = performance.now(); A.h0 = A.values(); }")
    act()
    pg.wait_for_timeout(4000)
    return pg.evaluate("""([label]) => { const A = window.A;
        const fr = A.frames.filter(f => f[0] >= A.t0);
        let last = null, prev = null;
        for (const f of fr) { if (prev !== null && f[3] !== prev) last = f[0]; prev = f[3]; }
        const idle = A.idles.find(t => t >= A.t0);
        const cost = (globalThis.__colourCost ?? []).slice(A.c0.k);
        const now = A.values(); A.dumps[label] = now;
        const changed = Object.keys(now).filter(k => now[k] !== A.h0[k]).length;
        return { label, fsCalls: A.fs.n - A.c0.n, fsMs: +(A.fs.ms - A.c0.ms).toFixed(1),
                 paintCalls: A.paint.paint - A.c0.p, layoutCalls: A.paint.layout - A.c0.l,
                 colourCost: cost.map(c => [c.kind, +c.ms.toFixed(2), c.calls]),
                 settledMs: last === null ? 0 : Math.round(last - A.t0),
                 idleMs: idle === undefined ? null : Math.round(idle - A.t0),
                 sectionsChanged: changed, rendered: Object.keys(now).length, frames: fr.length,
                 longtasks: A.lt.filter(([s]) => s >= A.t0).reduce((s, [, d]) => s + d, 0) }; }""", [label])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--path", choices=["pushed", "classed", "state"], required=True,
                    help="pushed: the old idle path; classed: the style-table variant (measured, not shipped); state: the engine (labels carry the value)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--size", default="1280x800")
    ap.add_argument("--net", default="none")
    ap.add_argument("--cpu", type=float, default=1)
    ap.add_argument("--shots")
    ap.add_argument("--dump")
    ap.add_argument("--at", default="-121.85,49.15", help="lon,lat of the pans (default: the app's HOME, the Fraser Valley)")
    ap.add_argument("--changes-only", action="store_true", help="skip the pans: only the horizon and quantity changes")
    a = ap.parse_args()
    w, h = map(int, a.size.split("x"))
    lon, lat = map(float, a.at.split(","))
    args = ["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist", "--enable-precise-memory-info"]
    tag = f"{a.path}-{a.size}"
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
        pg.get_by_role("tab", name="Conditions").click()
        # the colouring must be ON before measuring: a pushed standing, or the class table in the paint
        want = ("() => { const m = globalThis.__map; return m.queryRenderedFeatures({ layers: ['stream'] })"
                ".some(f => typeof f.state?.standing === 'number'); }") if a.path != "classed" else \
               ("() => JSON.stringify(globalThis.__map.getPaintProperty('stream', 'line-color')).includes('[\"literal\",')")
        pg.wait_for_function(want, timeout=240000, polling=500)
        print("colouring on:", a.path)
        pg.evaluate(INSTALL, a.path)
        if a.cpu > 1:
            cdp.send("Emulation.setCPUThrottlingRate", {"rate": a.cpu})
        pg.evaluate("() => window.A.mark('start')")
        for z in (() if a.changes_only else (8, 11, 13)):
            pg.evaluate(PANS, [lon, lat, z, f"z{z}"])
            if a.shots:
                Path(a.shots).mkdir(parents=True, exist_ok=True)
                pg.screenshot(path=f"{a.shots}/{tag}-z{z}.png")
        pg.evaluate("() => window.A.mark('afterPans')")
        pg.evaluate("""async () => { const m = globalThis.__map; await new Promise(r => { m.once('moveend', r);
            m.jumpTo({ center: [lon, lat], zoom: 11 }); }); await new Promise(r => setTimeout(r, 4000)); }""".replace("lon, lat", f"{lon}, {lat}"))
        changes = []
        for name in ["Forecast 1 day ahead", "Forecast 3 days ahead", "Conditions now"]:
            changes.append(timed_change(pg, name, lambda n=name: pg.get_by_role("button", name=n).first.click()))
            if a.shots and name == "Forecast 3 days ahead":
                pg.screenshot(path=f"{a.shots}/{tag}-z11-plus3.png")
        for name in ["Level", "Flow"]:
            changes.append(timed_change(pg, f"quantity {name}", lambda n=name: pg.get_by_role("radio", name=n).first.click()))
        if a.shots:
            pg.screenshot(path=f"{a.shots}/{tag}-z11-now.png")
        pg.evaluate("() => window.A.mark('end')")
        A = pg.evaluate("({ pans: window.A.pans, lt: window.A.lt, mem: window.A.mem, dumps: window.A.dumps })")
        b.close()
    by_z = {}
    for z in ("z8", "z11", "z13"):
        rs = [r for r in A["pans"] if r["label"].startswith(z + "/")]
        if not rs:
            continue
        lat = [r["colouredMs"] for r in rs if r["colouredMs"] is not None]
        by_z[z] = {"pansGreyMoving": sum(1 for r in rs if r["movingGrey"]),
                   "pansGreyAfter": sum(1 for r in rs if r["afterGrey"]),
                   "greyFramesAfter": sum(r["afterGrey"] for r in rs),
                   "colouredMs": {"median": st.median(lat) if lat else None, "max": max(lat) if lat else None},
                   "workMs": {"median": round(st.median([max(r["costMs"], r["fsMs"]) for r in rs]), 2),
                              "max": round(max(max(r["costMs"], r["fsMs"]) for r in rs), 2)},
                   "fsCalls": {"median": st.median([r["fsN"] for r in rs]), "max": max(r["fsN"] for r in rs)},
                   "settledWithValue": {"median": st.median([r["settledWithValue"] for r in rs])},
                   "longtasksMs": sum(r["longtasks"] for r in rs)}
    summary = {"path": a.path, "size": a.size, "net": a.net, "cpu": a.cpu, "byZoom": by_z, "changes": changes,
               "heapMB": A["mem"], "pans": A["pans"]}
    print(json.dumps({k: v for k, v in summary.items() if k != "pans"}, indent=1))
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(summary, indent=1))
    if a.dump:
        Path(a.dump).write_text(json.dumps(A["dumps"]))


main()

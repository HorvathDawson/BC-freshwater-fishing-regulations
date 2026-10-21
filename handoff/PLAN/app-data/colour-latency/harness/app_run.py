"""Colour-latency: TODAY's path measured in the REAL app (expo web, dev build — `globalThis.__map` is the map).

  .venv/bin/python handoff/PLAN/app-data/colour-latency/harness/app_run.py --url http://localhost:39303 --out <file> \
      [--size 390x844] [--net 4g --cpu 4] [--gpu gpu]

Wraps map.setFeatureState to timestamp the app's status pushes, probes every rendered frame for visible status-layer
features with NO `status` feature-state (= drawn in the default grey; today every visible section is pushed a value,
base included), and runs the harness's pan script at the harness's test place.
"""
import argparse, json, statistics as st, time
from playwright.sync_api import sync_playwright

INSTALL = """() => {
  const m = globalThis.__map; const T = () => performance.now();
  const A = window.A = { sets: [], idles: [], frames: [], pans: [], lt: [] };
  new PerformanceObserver(l => { for (const e of l.getEntries()) A.lt.push([Math.round(e.startTime), Math.round(e.duration)]); })
    .observe({ type: 'longtask', buffered: false });
  const orig = m.setFeatureState.bind(m);
  let burst = null;
  m.setFeatureState = (f, s) => {
    if ('status' in s) {
      const t = T();
      if (!burst || t - burst[1] > 30) { burst = [t, t, 0]; A.sets.push(burst); }
      burst[1] = t; burst[2]++;
    }
    return orig(f, s);
  };
  m.on('idle', () => A.idles.push(Math.round(T())));
  const layers = ['stream', 'lake', 'lake_flowing', 'wetland_flowing'].filter(id => m.getLayer(id));
  m.on('render', () => {
    let grey = 0, total = 0;
    for (const f of m.queryRenderedFeatures({ layers })) {
      if (f.id === undefined) continue; total++;
      if (f.state?.status === undefined) grey++;
    }
    A.frames.push([Math.round(T()), grey, total]);
  });
  return layers;
}"""

SCRIPT = """async ([lon, lat, z]) => {
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
    const tStart = T();
    await move(() => m.panBy([dx * W, dy * H], { duration: 400, easing: t => t }));
    const tEnd = T();
    await settle(tEnd); await sleep(1500);
    A.pans.push({ label: `pan${i+1}`, tStart: Math.round(tStart), tEnd: Math.round(tEnd), tNext: Math.round(T()) });
  }
  return true;
}"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:39303")
    ap.add_argument("--out", required=True)
    ap.add_argument("--size", default="390x844")
    ap.add_argument("--net", default="none")
    ap.add_argument("--cpu", type=float, default=1)
    ap.add_argument("--gpu", default="gpu")
    a = ap.parse_args()
    w, h = map(int, a.size.split("x"))
    args = ["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist"] if a.gpu == "gpu" else []
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, args=args)
        pg = b.new_page(viewport={"width": w, "height": h}, device_scale_factor=2 if w < 500 else 1)
        cdp = pg.context.new_cdp_session(pg)
        if a.net == "4g":
            cdp.send("Network.enable")
            cdp.send("Network.emulateNetworkConditions", {"offline": False, "latency": 120,
                     "downloadThroughput": 9e6 / 8, "uploadThroughput": 3e6 / 8})
        pg.goto(a.url)
        pg.wait_for_function("globalThis.__map && globalThis.__map.loaded()", timeout=180000, polling=500)
        time.sleep(4)
        print("status layers", pg.evaluate(INSTALL))
        if a.cpu > 1:
            cdp.send("Emulation.setCPUThrottlingRate", {"rate": a.cpu})
        pg.evaluate(SCRIPT, [-121.0, 52.0, 11])
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
        idle = next((t - pn["tEnd"] for t in A["idles"] if t >= pn["tEnd"]), None)
        push = next((s for s in A["sets"] if s[0] >= pn["tEnd"]), None)
        rows.append({"label": pn["label"], "wrongMoving": moving, "wrongAfter": len(wrong_after), "colouredMs": lat,
                     "idleMs": idle, "pushStartMs": round(push[0] - pn["tEnd"]) if push else None,
                     "pushMs": round(push[1] - push[0], 1) if push else None, "pushCalls": push[2] if push else None,
                     "maxGreyFrac": round(max((f[1] / f[2] for f in fr if f[2]), default=0), 3),
                     "longtasks": sum(d for s, d in A["lt"] if pn["tStart"] <= s < pn["tNext"])})
    for r in rows:
        print(json.dumps(r))
    lat = [r["colouredMs"] for r in rows if r["colouredMs"] is not None]
    summary = {"size": a.size, "net": a.net, "cpu": a.cpu, "colouredMs": {"median": st.median(lat), "max": max(lat)},
               "pushStartMs": st.median([r["pushStartMs"] for r in rows if r["pushStartMs"] is not None]),
               "pansWithWrongMoving": sum(1 for r in rows if r["wrongMoving"]),
               "pansWithWrongAfter": sum(1 for r in rows if r["wrongAfter"]), "rows": rows}
    print(json.dumps({k: v for k, v in summary.items() if k != "rows"}))
    with open(a.out, "w") as f:
        json.dump(summary, f)


main()

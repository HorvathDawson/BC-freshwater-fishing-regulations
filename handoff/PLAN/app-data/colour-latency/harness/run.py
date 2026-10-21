"""Colour-latency harness driver: headless Chromium (repo .venv Playwright), one fresh browser per run.

  .venv/bin/python handoff/PLAN/app-data/colour-latency/harness/run.py --port 39301 --out <dir> \
      --modes idle,A,Ds,Bm,Bs,E0,E1,E2,E1A --sizes 390x844,1280x800 --gpu default|swiftshader [--probe]

Writes <out>/<gpu>-<size>-<mode>-<probe|clean>.json (the page's raw record) and prints one summary line per run.
"""
import argparse, json, statistics as st, sys, time
from pathlib import Path
from playwright.sync_api import sync_playwright


def summarise(R):
    mk = R["marks"]
    fr = R["frames"]
    idles = R["idles"]
    out = {"mode": R["mode"], "probe": R["probe"], "errors": R["errors"][:3], "notes": R["notes"][:3]}

    def lt(a, b):
        xs = [d for s, d in R["longtasks"] if a <= s < b]
        return {"n": len(xs), "sum": sum(xs), "max": max(xs, default=0)}

    def latency(a, b, t_end):
        """frames in [a, b): wrong frames while moving / after the move ended, and pan-end -> all coloured."""
        win = [f for f in fr if a <= f[0] < b]
        moving = sum(1 for f in win if f[0] <= t_end and f[1] > 0)
        after = [f for f in win if f[0] > t_end]
        wrong_after = [f for f in after if f[1] > 0]
        lat = 0
        if wrong_after:
            last = wrong_after[-1][0]
            nxt = [f[0] for f in after if f[0] > last and f[1] == 0]
            lat = (nxt[0] - t_end) if nxt else None
        frac = max((f[1] / f[2] for f in win if f[2]), default=0)
        return {"wrongMoving": moving, "wrongAfter": len(wrong_after), "colouredMs": lat, "maxWrongFrac": round(frac, 3),
                "frames": len(win)}

    out["start"] = {
        "firstRender": mk.get("firstRender", 0) - mk["mapCreate"],
        "indexFetchMs": mk.get("indexFetchMs"), "indexDecodeMs": mk.get("indexDecodeMs"),
        "indexReady": mk.get("indexReady", 0) - mk["mapCreate"],
        "setupMs": mk.get("setupDone", 0) - mk.get("indexReady", 0),
        "longtasks": lt(mk["mapCreate"], mk.get("startDone", 1e12)),
    }
    if R["probe"]:
        out["start"]["latency"] = latency(mk["mapCreate"], mk.get("startDone", 1e12), mk.get("setupDone", 0))
    pans = []
    for p in R["pans"]:
        d = {"label": p["label"], "calls": p["calls"],
             "idleMs": next((t - p["tEnd"] for t in idles if t >= p["tEnd"]), None)}
        if R["probe"]:
            d.update(latency(p["tStart"], p["tNext"], p["tEnd"]))
        work = [c for c in R["colour"] if p["tStart"] <= c[0] < p["tNext"]]
        d["colourMs"] = round(sum(c[1] for c in work), 1)
        d["colourMaxMs"] = max((c[1] for c in work), default=0)
        d["longtasks"] = lt(p["tStart"], p["tNext"])
        pans.append(d)
    out["pans"] = pans

    def agg(key):
        xs = [p[key] for p in pans if p.get(key) is not None]
        return {"median": st.median(xs) if xs else None, "max": max(xs) if xs else None}

    out["panSummary"] = {k: agg(k) for k in ["idleMs", "colourMs", "colourMaxMs", "calls"]}
    if not R["probe"]:  # frame pacing while the camera moves (render-event gaps), clean runs only
        gaps = []
        for p in R["pans"]:
            ts = [f[0] for f in fr if p["tStart"] <= f[0] <= p["tEnd"]]
            gaps += [b - a for a, b in zip(ts, ts[1:])]
        gaps.sort()
        out["panSummary"]["frameGapP95"] = gaps[int(len(gaps) * 0.95)] if gaps else None
        out["panSummary"]["frameGapMax"] = gaps[-1] if gaps else None
    out["panSummary"]["longtaskSum"] = sum(p["longtasks"]["sum"] for p in pans)
    out["panSummary"]["longtaskMax"] = max(p["longtasks"]["max"] for p in pans) if pans else 0
    if R["probe"]:
        out["panSummary"]["colouredMs"] = agg("colouredMs")
        out["panSummary"]["pansWithWrongAfter"] = sum(1 for p in pans if p["wrongAfter"])
        out["panSummary"]["pansWithWrongMoving"] = sum(1 for p in pans if p["wrongMoving"])
        out["panSummary"]["wrongFrames"] = sum(p["wrongAfter"] + p["wrongMoving"] for p in pans)
    if "dayStart" in mk:
        dw = [c for c in R["colour"] if mk["dayStart"] <= c[0] < mk["dayDone"]]
        out["day"] = {"diff": R.get("dayDiff"), "calls": R.get("dayCalls"),
                      "syncMs": mk["dayApplied"] - mk["dayStart"],
                      "workMs": round(sum(c[1] for c in dw), 1),
                      "settleMs": next((t - mk["dayStart"] for t in idles if t >= mk["dayApplied"]), None),
                      "longtasks": lt(mk["dayStart"], mk["dayDone"])}
        if R["probe"]:
            out["day"]["latency"] = latency(mk["dayStart"], mk["dayDone"], mk["dayApplied"])
    out["setup"] = [c for c in R["colour"] if c[3] in ("Ds-setall", "Ds-flip", "Dfull-setall", "E-start")
                    or c[3].startswith("day")]
    out["expr"] = R["expr"]
    out["mem"] = R["mem"]
    out["totalCalls"] = R.get("totalCalls")
    out["missingCells"] = R.get("missingCells")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=39301)
    ap.add_argument("--out", required=True)
    ap.add_argument("--modes", default="none,idle,A,Ds,Bm,Bs,E0,E1,E2,E1A")
    ap.add_argument("--sizes", default="390x844,1280x800")
    ap.add_argument("--gpu", default="default")
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--extra", default="")
    ap.add_argument("--net", default="none")
    ap.add_argument("--cpu", type=float, default=1)
    ap.add_argument("--tag", default="")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    args = ["--enable-precise-memory-info", "--js-flags=--max-old-space-size=8192"]
    if a.gpu == "swiftshader":
        args += ["--use-angle=swiftshader", "--enable-unsafe-swiftshader"]
    elif a.gpu == "gpu":
        args += ["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist"]
    with sync_playwright() as p:
        for size in a.sizes.split(","):
            w, h = map(int, size.split("x"))
            for mode in a.modes.split(","):
                b = p.chromium.launch(headless=True, args=args)
                pg = b.new_page(viewport={"width": w, "height": h}, device_scale_factor=2 if w < 500 else 1)
                cdp = pg.context.new_cdp_session(pg)
                if a.net == "4g":  # a phone on LTE talking to R2: 120 ms RTT, 9 Mbit/s down
                    cdp.send("Network.enable")
                    cdp.send("Network.emulateNetworkConditions", {"offline": False, "latency": 120,
                             "downloadThroughput": 9e6 / 8, "uploadThroughput": 3e6 / 8})
                if a.cpu > 1:
                    cdp.send("Emulation.setCPUThrottlingRate", {"rate": a.cpu})
                url = f"http://127.0.0.1:{a.port}/?mode={mode}{'&probe=1' if a.probe else ''}{a.extra}"
                t0 = time.time()
                pg.goto(url)
                try:
                    pg.wait_for_function("window.DONE === true", timeout=420000, polling=500)
                except Exception as e:  # noqa: BLE001
                    print(f"{size} {mode}: TIMEOUT {e}", file=sys.stderr)
                try:
                    R = pg.evaluate("window.R")
                except Exception as e:  # noqa: BLE001  a renderer crash (out of memory) is a result too
                    print(f"{size} {mode}: CRASHED {str(e)[:120]}", flush=True)
                    b.close()
                    continue
                if mode == "none" or True:
                    R["renderer"] = pg.evaluate("""() => { const c = document.createElement('canvas').getContext('webgl2');
                        const d = c && c.getExtension('WEBGL_debug_renderer_info');
                        return d ? c.getParameter(d.UNMASKED_RENDERER_WEBGL) : null; }""")
                tag = f"{a.gpu}{a.tag}-{size}-{mode}-{'probe' if a.probe else 'clean'}"
                (out / f"{tag}.json").write_text(json.dumps(R))
                s = summarise(R)
                s["wall"] = round(time.time() - t0, 1)
                s["renderer"] = R["renderer"]
                (out / f"{tag}.summary.json").write_text(json.dumps(s, indent=1))
                ps = s["panSummary"]
                print(f"{tag}: idle {ps['idleMs']} colourMs {ps['colourMs']} lt {ps['longtaskSum']}/{ps['longtaskMax']}"
                      + (f" coloured {ps.get('colouredMs')} wrongPans {ps.get('pansWithWrongAfter')}"
                         f"/{ps.get('pansWithWrongMoving')} frames {ps.get('wrongFrames')}" if a.probe else "")
                      + f" day {s.get('day', {}).get('syncMs')}/{s.get('day', {}).get('settleMs')}"
                      + f" mem {s['mem']} expr { {k: (v.get('bytes'), v.get('setPaintMs')) for k, v in s['expr'].items()} }"
                      + f" err {s['errors'][:1]} wall {s['wall']}s", flush=True)
                b.close()


main()

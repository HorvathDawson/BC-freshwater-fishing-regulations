"""THE FEED ALONE recolours the Conditions map (owner 2026-10-21): same tiles, same release, a new gauge index only.

  .venv/bin/python handoff/PLAN/app-data/colour-latency/harness/conditions_feed_swap.py --url http://localhost:39562 \
      --data http://127.0.0.1:39560 --index <a frozen index.json> --out <file.json> [--shots <dir>]

Opens the Conditions tab on the frozen index, settles, then has the data server (`data_server.mjs`) serve a MODIFIED
copy of the index (every percentile p -> 1 - p, forecasts too) and waits for the app to re-ask the feed on its own
schedule (the feed's 5-minute cache, then `FEED_POLL_MS`) — nothing is republished, no tile or release file is touched.
Records: which rendered sections changed value, whether any tile or release byte was fetched after the swap (the data
server's request log), whether the stream's paint was replaced (it must not be: the values ride in feature-state), and
the colouring's own cost record for the change.
"""
import argparse, json, time
from pathlib import Path
from urllib.request import urlopen
from playwright.sync_api import sync_playwright


def flipped(src: Path, dst: Path) -> None:
    idx = json.loads(src.read_text())
    flip = lambda v: (1 - v) if isinstance(v, (int, float)) and 0 <= v <= 1 else v
    for row in idx["stations"].values():
        for k in ("percentile", "discharge", "level"):
            if k in row:
                row[k] = flip(row[k])
        for a in (row.get("ahead") or {}).values():
            for k in ("discharge", "level"):
                if k in a:
                    a[k] = flip(a[k])
    idx["fetchedAt"] = "2026-10-21T23:59:00Z"
    dst.write_text(json.dumps(idx))


VALUES = """() => { const m = globalThis.__map; const out = {};
  for (const f of m.queryRenderedFeatures({ layers: ['stream', 'lake', 'lake_flowing', 'wetland_flowing'] }))
    if (f.id !== undefined) out[f.sourceLayer + ':' + f.id] = typeof f.state?.standing === 'number' ? f.state.standing : null;
  return out; }"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--index", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--shots")
    a = ap.parse_args()
    frozen = Path(a.index)
    swapped = frozen.with_name("index-flipped.json")
    flipped(frozen, swapped)
    urlopen(f"{a.data}/__feed?index={frozen}").read()
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, args=["--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist"])
        pg = b.new_page(viewport={"width": 1280, "height": 800})
        pg.goto(a.url)
        pg.wait_for_function("globalThis.__map && globalThis.__map.loaded()", timeout=240000, polling=500)
        pg.get_by_role("tab", name="Conditions").click()
        pg.wait_for_function("globalThis.__map.queryRenderedFeatures({ layers: ['stream'] })"
                             ".some(f => typeof f.state?.standing === 'number')", timeout=240000, polling=500)
        pg.wait_for_timeout(5000)
        before = pg.evaluate(VALUES)
        pg.evaluate("() => { const m = globalThis.__map; window.__paint0 = JSON.stringify(m.getPaintProperty('stream', 'line-color')); "
                    "window.__cost0 = (globalThis.__colourCost ?? []).length; window.__paints = []; "
                    "const sp = m.setPaintProperty.bind(m); m.setPaintProperty = (l, p, v) => { window.__paints.push(l + ' ' + p); return sp(l, p, v); }; }")
        if a.shots:
            pg.screenshot(path=f"{a.shots}/feed-before.png")
        t_swap = time.time()
        log0 = len(json.loads(urlopen(f"{a.data}/__log").read()))
        urlopen(f"{a.data}/__feed?index={swapped}").read()
        # the app re-asks on its own schedule: wait for the colours to move (up to 12 min)
        changed_at = None
        for _ in range(720):
            pg.wait_for_timeout(1000)
            now = pg.evaluate(VALUES)
            if any(now.get(k) != v for k, v in before.items() if v is not None):
                changed_at = time.time()
                pg.wait_for_timeout(3000)
                break
        after = pg.evaluate(VALUES)
        info = pg.evaluate("() => ({ paintReplaced: JSON.stringify(globalThis.__map.getPaintProperty('stream', 'line-color')) "
                           "!== window.__paint0, paints: window.__paints, cost: (globalThis.__colourCost ?? []).slice(window.__cost0) "
                           ".filter(c => c.kind !== 'tile' || c.calls > 0) })")
        if a.shots:
            pg.screenshot(path=f"{a.shots}/feed-after.png")
        b.close()
    log = [r for r in json.loads(urlopen(f"{a.data}/__log").read())[log0:] if r[0] >= t_swap * 1000]
    urlopen(f"{a.data}/__feed?index={frozen}").read()
    keys = [k for k, v in before.items() if v is not None and k in after]
    flips = sum(1 for k in keys if after[k] is not None and abs(after[k] - (100 - before[k] if before[k] >= 0 else -1)) < 1e-6)
    summary = {
        "secondsToRecolour": None if changed_at is None else round(changed_at - t_swap),
        "renderedWithValue": len(keys),
        "changed": sum(1 for k in keys if after[k] != before[k]),
        "flippedExactly": flips,
        "requestsAfterSwap": sorted({r[1] for r in log}),
        "tileOrReleaseBytesAfterSwap": [r for r in log if not r[1].startswith("/feeds/")],
        "paintReplaced": info["paintReplaced"],
        "setPaintPropertyAfterSwap": info["paints"],
        "colourCost": [[c["kind"], round(c["ms"], 2), c["calls"]] for c in info["cost"]],
    }
    print(json.dumps(summary, indent=1))
    Path(a.out).write_text(json.dumps(summary, indent=1))


main()

"""Record every animation frame while switching gauges on the Chilliwack (Conditions tab).

argv: base_url out_dir tag
Per frame: is a spinner (role=progressbar) up, is a chart drawn, is the "Loading the record" note up, which gauge is
checked. A flicker is any frame after a click with no chart, or with a spinner.
"""
import json
import re
import sys
from collections import Counter
from playwright.sync_api import sync_playwright

BASE, OUT, TAG = sys.argv[1], sys.argv[2], sys.argv[3]
SPAN = sys.argv[4] if len(sys.argv) > 4 else "Last 72 hours"
SAMPLER = """
() => {
  window.__frames = [];
  const tick = () => {
    const charts = [...document.querySelectorAll('[aria-label]')]
      .filter(e => /^the (last 72 hours|whole year against its record)$/.test(e.getAttribute('aria-label')));
    const on = document.querySelector('[role=radio][aria-checked=true][aria-label^="Showing 08"]');
    window.__frames.push({
      t: performance.now(),
      spinner: document.querySelectorAll('[role=progressbar]').length,
      chart: charts.length,
      note: document.body.innerText.includes('Loading the record'),
      on: on ? on.getAttribute('aria-label').slice(8, 15) : null,
    });
    window.__raf = requestAnimationFrame(tick);
  };
  window.__raf = requestAnimationFrame(tick);
}
"""

with sync_playwright() as p:
    b = p.chromium.launch()
    pg = b.new_page(viewport={"width": 1440, "height": 900})
    reqs = []
    pg.on("request", lambda r: reqs.append(r.url))
    pg.goto(BASE, wait_until="load", timeout=240_000)
    pg.wait_for_timeout(8_000)
    pg.get_by_role("tab", name="Conditions").click()
    pg.wait_for_timeout(5_000)
    pg.mouse.click(530, 450)
    pg.get_by_role("radio", name=re.compile(r"^Showing 08MH001")).wait_for(timeout=60_000)
    if SPAN != "Last 72 hours":
        pg.get_by_role("radio", name=SPAN).click()
    pg.wait_for_timeout(1_500)          # the panel's open; first gauge charted
    opened = len(reqs)
    pg.evaluate(SAMPLER)
    report = []
    for st in ["08MH016", "08MH056", "08MH047", "08MH001", "08MH016"]:
        start = pg.evaluate("() => window.__frames.length")
        pg.get_by_role("radio", name=re.compile(rf"^Chart {st}")).click()
        pg.wait_for_timeout(700)
        frames = pg.evaluate(f"() => window.__frames.slice({start})")
        bad = [f for f in frames if f["spinner"] or not f["chart"]]
        note = [f for f in frames if f["note"]]
        first_on = next((i for i, f in enumerate(frames) if f["on"] == st), None)
        report.append({"to": st, "frames": len(frames), "no_chart_or_spinner": len(bad),
                       "note_frames": len(note), "checked_at_frame": first_on})
        pg.screenshot(path=f"{OUT}/{TAG}-{st}.png")
    feed = Counter(re.sub(r"\?.*", "", u).rsplit("/", 1)[-1] for u in reqs if re.search(r"/08[A-Z]{2}\d{3}\.json", u))
    print(json.dumps({"tag": TAG, "span": SPAN, "switches": report, "station_file_requests": feed,
                      "requests_after_open": len(reqs) - opened}, indent=1))
    b.close()

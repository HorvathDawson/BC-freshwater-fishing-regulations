"""Fetch the Pacific Salmon Explorer indicator pages, verbatim.

ONE REQUEST PER INDICATOR IS THE WHOLE DATASET. The site server-renders every conservation
unit into `all-regions/all-species/all-cus` — all 463 units, every daily curve, every
annual series and every data-quality badge are in the HTML of one page, with nothing
fetched later. So this is three GETs, not 463 x 3.

The pages are saved as they arrived. The flight payload inside them is a Next.js
implementation detail that will change shape when they upgrade; keeping the raw HTML means
a parser change never needs a re-fetch, and a re-fetch never silently rewrites history.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import urllib.request
from pathlib import Path

BASE = "https://www.salmonexplorer.ca/explore/data/{}/all-regions/all-species/all-cus/"
INDICATORS = ("run-timing", "catch-and-run-size", "biological-status")
UA = {"User-Agent": "Mozilla/5.0 (compatible; bc-fishing-regs/1.0)"}
#: (T199) WHAT `parse` CREDITS, written into every fetch's `fetched.json` (the first one was recorded by hand): the
#: dataset the Explorer's curves are, and the permission the app ships them under. The dataset's own `updated` date is
#: its landing page's (read 2026-10-24); a fetch cannot see it in the Explorer's pages, so a newer revision moves it here.
UPSTREAM = {
    "name": "Average Run Timing of Salmon and Steelhead Conservation Units",
    "datasetid": 90,
    "publisher": "Pacific Salmon Foundation",
    "landing": "https://data.salmonwatersheds.ca/result?datasetid=90",
    "updated": "2024-09-10",
    "source_data_from": ["LGL Ltd.", "Fisheries and Oceans Canada", "Pacific Salmon Commission"],
}
PERMISSION = {
    "granted": "2026-10-24",
    "what": "derived run-timing curves and CU-based reach assignments, shipped in the free app (T162)",
    "attribution": "Pacific Salmon Foundation (Salmon Watersheds Program)",
    "note": "Asked for by the owner and granted in writing. PSF asked that its reply not be shared: it is not stored.",
}


def fetch_page(slug: str, timeout: int = 120) -> str:
    req = urllib.request.Request(BASE.format(slug), headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def main(out: Path, indicators=INDICATORS) -> None:
    out.mkdir(parents=True, exist_ok=True)
    stamp = {}
    for slug in indicators:
        html = fetch_page(slug)
        # A page that came back without a flight payload is a redirect or an error page
        # wearing a 200. Refuse it rather than overwriting a good copy with a login screen.
        if "self.__next_f.push" not in html:
            raise SystemExit(f"{slug}: no flight payload in the response — refusing to write")
        (out / f"{slug}.html").write_text(html)
        stamp[slug] = {"bytes": len(html), "url": BASE.format(slug)}
        print(f"  {slug:22} {len(html)/1024:8.0f} KB")
    stamp = {"fetched_at": dt.datetime.now(dt.timezone.utc).date().isoformat(), "base": BASE,
             "pages": {k: {"bytes": v["bytes"]} for k, v in stamp.items()},
             "upstream_dataset": UPSTREAM, "permission": PERMISSION}
    (out / "fetched.json").write_text(json.dumps(stamp, indent=1))
    print(f"wrote {out}")


if __name__ == "__main__":
    from pipeline.common.curated import SOURCE
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=SOURCE / "runtiming")
    ap.add_argument("--only", nargs="+", choices=INDICATORS, default=list(INDICATORS),
                    help="the pages to fetch (parse reads run-timing alone)")
    a = ap.parse_args()
    main(a.out, tuple(a.only))

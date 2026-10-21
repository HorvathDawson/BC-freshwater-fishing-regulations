"""The stocking roster: every fish release the Freshwater Fisheries Society of BC reports, one request.

    python -m pipeline.stocking.fetch [--out FILE] [--end YYYY-MM-DD]

THE SOURCE. FFSBC's public stocking report (https://www.gofishbc.com/stocked-fish) reads its rows from
`https://www.gofishbc.com/wp-json/ffsbc-stocking/v1/releases`; its "export CSV" asks the same endpoint for
`per_page=all`, and so does this — ONE request for the whole roster (71,072 rows, 1901-01-01 to 2026-10-20 on
2026-10-21, ~18 MB), never a page-through. Each row names the water by FIDQ waterbody identifier (`wbid`, e.g.
`00330VICT`, the FWA's 50K group code) — `pipeline.deliver.lake_facts` binds it to a lake by that key, confirmed by a
name. A few hundred hatchery ponds carry FFSBC's own ids (`FFSBC3893`) and bind nowhere.

Why not the BC Data Catalogue: its "Provincial Fish Stocking Reports" (WHSE_FISH.FISH_RELEASES_PUBLIC_VW) is a
custom-download-only BCGW view (no WFS, no file), and FIDQ's stocking query is an interactive form.

Written to `data/source/stocking/ffsbc_releases.json` as `{url, fetched, generated_at, rows}`: `fetched` is the day
the roster was read (the lake facts' `asof` — "stocked this season" is counted from it), `generated_at` the
endpoint's own timestamp. The file is a fetch away, so it is not tracked (data/.gitignore).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional, Sequence

from pipeline.common.curated import SOURCE

ENDPOINT = "https://www.gofishbc.com/wp-json/ffsbc-stocking/v1/releases"
START = "1900-01-01"
USER_AGENT = "bc-fishing-regulations-pipeline (stocking roster; one request per build)"


def url_for(end: str) -> str:
    return ENDPOINT + "?" + urllib.parse.urlencode(
        {"start": START, "end": end, "sort": "date", "dir": "asc", "page": 1, "per_page": "all"})


def main(argv: Optional[Sequence[str]] = None) -> int:
    from pipeline.deliver.lake_facts import STOCKING
    ap = argparse.ArgumentParser(prog="pipeline.stocking.fetch", description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=SOURCE / STOCKING)
    ap.add_argument("--end", default=dt.date.today().isoformat())
    a = ap.parse_args(argv)
    url = url_for(a.end)
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=300) as fh:
        page = json.load(fh)
    rows = page.get("rows") or []
    if page.get("total") != len(rows):
        raise SystemExit(f"stocking: the endpoint reported {page.get('total')} rows and sent {len(rows)}")
    a.out.parent.mkdir(parents=True, exist_ok=True)
    out = {"url": url, "fetched": dt.date.today().isoformat(), "generated_at": page.get("generatedAt"),
           "start": page.get("start"), "end": page.get("end"), "rows": rows}
    a.out.write_text(json.dumps(out, separators=(",", ":"), sort_keys=True) + "\n", encoding="utf-8")
    print(f"stocking: {len(rows):,} releases, {rows[0]['date'] if rows else '-'} to {rows[-1]['date'] if rows else '-'}"
          f" -> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

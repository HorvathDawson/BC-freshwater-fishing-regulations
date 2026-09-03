"""What KIND of water each gauge sits on, as the Water Office itself states it.

    python -m pipeline.gauges.generate.waterbody_type            # fetch/resume, writes the artifact
    python -m pipeline.gauges.generate.waterbody_type --check    # what is missing, no requests

WHY THIS IS WORTH 2,300 REQUESTS. Every other signal the matcher has is either derived from
the candidate it is trying to choose, or is a guess:

    `wsc` / `wbk`     copied OFF the matched node. An output of the match; it cannot
                      referee the match.
    the station name  "TROUT CREEK AT OUTLET OF TROUT LAKE" contains both words. Parsing
                      which one it is ON is exactly the ambiguity.
    the node's kind   whether the thing we picked is a lake — again, a property of the
                      choice, not evidence about it.

`Type of water body` is ECCC's own statement about the station, made independently of this
pipeline. It is the only field that can contradict a match without begging the question.
Measured against the current match it disagrees in dozens of places, and every disagreement
found by hand so far was the match being wrong: STELLAKO RIVER AT GLENANNAN placed on
François Lake, TROUT CREEK AT OUTLET OF TROUT LAKE placed on Trout Lake.

WHY IT IS A COMMITTED ARTIFACT AND NOT A BUILD STEP. It changes when ECCC commissions a
station, which is a handful of times a year — `pipeline.gauges.roster` reports when that has
happened. Re-fetching it on every build would be thousands of requests against a government
site for an answer that has not moved since the last one. Fetch it when the roster gains a
station; otherwise read the file.

MANNERS. One request at a time, one second apart, resumable, and it never re-fetches a
station it already has. The Water Office puts its real-time pages behind a disclaimer
consent, so the cookie below is the same one a browser sets by clicking through it.

    THE FIELD IS ONLY ON THE REAL-TIME PAGE. A discontinued station has no real-time page
    and comes back `null` — recorded as such rather than retried forever, because "ECCC does
    not publish this" and "we have not asked yet" are different facts.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from pipeline.curated import CURATED, SOURCE

ROOT = Path(__file__).resolve().parents[2]

#: The roster this enriches. Kept SEPARATE from `bc_hydrometric_stations.json` rather than
#: merged into it: that file is what `data/fetch_data.py` writes, and a second writer would
#: make "re-fetch the roster" silently destroy this.
TYPES_FILE = CURATED.gauges.waterbody_type
STATIONS = SOURCE / "bc_hydrometric_stations.json"

_URL = "https://wateroffice.ec.gc.ca/report/real_time_e.html?stn={}"
_UA = "BC-FishRegs-Hydro/1.0 (freshwater fishing regulations; contact via repo)"
#: The consent the site's own disclaimer page sets. Not a bypass — the same value a browser
#: gets from clicking Agree, sent so a script does not have to render the interstitial.
_COOKIE = "disclaimer=agree"

# The value is NOT adjacent to its label. The page puts the label in one div and the value
# in a sibling that points back at it:
#
#     <div id="water-body"><b>Type of water body:</b></div>
#     <div aria-labelledby="water-body" class="col-xs-6">River</div>
#
# So the accessibility association is the only thing tying them together, and it is the
# stable hook — reading forward from the label text matches the label's own closing tag.
_FIELD = re.compile(r'aria-labelledby="water-body"[^>]*>\s*([^<]+?)\s*<', re.I)
#: Fallback for a future template that inlines it after all.
_FIELD_PLAIN = re.compile(r"Type of water body:\s*(?:</b>\s*)?([A-Za-z ]+?)\s*<", re.I)

#: One second between requests. The whole roster is ~40 minutes; that is the correct speed
#: for a once-a-year job against somebody else's public server.
DELAY_S = 1.0


def _fetch_one(station: str, timeout: float = 30.0) -> str | None:
    """The stated water-body type, or None if the page does not carry one."""
    req = urllib.request.Request(_URL.format(station),
                                 headers={"User-Agent": _UA, "Cookie": _COOKIE})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as fh:
            html = fh.read().decode("utf-8", "replace")
    except (urllib.error.URLError, OSError, TimeoutError):
        return None
    for rx in (_FIELD, _FIELD_PLAIN):
        m = rx.search(html)
        if m:
            got = " ".join(m.group(1).split()).title()
            return got or None
    return None


def load_types(path: Path | None = None) -> dict[str, str | None]:
    """`{station: "River" | "Lake" | ... | None}`, or empty if never fetched."""
    path = path or TYPES_FILE
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8")).get("stations", {})


def _save(rows: dict[str, str | None], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "_about": "ECCC's own `Type of water body` for each station, scraped once from the "
                  "Water Office real-time page. INDEPENDENT of anything this pipeline "
                  "derives, which is the whole reason it exists — `wsc`, `wbk` and the "
                  "matched node's kind are all outputs of the match and cannot referee it. "
                  "null means the page carried no such field (usually a discontinued "
                  "station, which has no real-time page). Regenerate with "
                  "`python -m pipeline.gauges.generate.waterbody_type` when the roster gains a "
                  "station; it resumes and never re-fetches what it already has.",
        "stations": dict(sorted(rows.items())),
    }, indent=1) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stations", type=Path, default=STATIONS)
    ap.add_argument("--out", type=Path, default=TYPES_FILE)
    ap.add_argument("--check", action="store_true",
                    help="report what is missing and exit; makes no requests")
    ap.add_argument("--only", nargs="*", help="specific station ids")
    ap.add_argument("--limit", type=int, default=0, help="stop after N fetches")
    ap.add_argument("--delay", type=float, default=DELAY_S)
    a = ap.parse_args()

    roster = [s["station"] for s in json.loads(a.stations.read_text(encoding="utf-8"))]
    have = load_types(a.out)
    want = a.only or [s for s in roster if s not in have]

    if a.check:
        print(f"{len(have)} of {len(roster)} stations have a stated water-body type; "
              f"{len(want)} to fetch")
        if have:
            import collections
            for k, n in collections.Counter(have.values()).most_common():
                print(f"  {n:5d}  {k}")
        return 0

    if a.limit:
        want = want[: a.limit]
    print(f"fetching {len(want)} of {len(roster)} ({len(have)} already on disk), "
          f"{a.delay}s apart -> {a.out.name}", flush=True)

    for n, station in enumerate(want, 1):
        have[station] = _fetch_one(station)
        if n % 25 == 0 or n == len(want):
            _save(have, a.out)
            done = sum(1 for v in have.values() if v)
            print(f"  {n}/{len(want)}  {station} -> {have[station]}  "
                  f"({done} typed so far)", flush=True)
        time.sleep(a.delay)

    _save(have, a.out)
    import collections
    print("done. " + ", ".join(f"{n} {k}"
                               for k, n in collections.Counter(have.values()).most_common()))
    return 0


if __name__ == "__main__":
    sys.exit(main())

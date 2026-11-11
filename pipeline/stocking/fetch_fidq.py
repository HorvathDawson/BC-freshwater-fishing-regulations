"""Fetch the Province's lake stocking records (FIDQ, the public front end of `WHSE_FISH.FISH_RELEASES_PUBLIC_VW`,
"Provincial Fish Stocking Reports") to a folder OUTSIDE git (T634 third pass, PROTOTYPE).

LICENCE: the catalogue record says "Access Only"; the owner says OGL (2026-11-11, link owed). Every file written here
carries `licence: "owner says OGL, catalogue says Access Only, unconfirmed"`. Confirm before any production publish.

Method (the v1 chain's bulk Preview, `git show a40989161^:archive/pipeline/recurring/anglerinfo/fetch_stocking.py`):
the watershed-group list from `viewMultipleWaterbodies.do`, then one Preview POST per group (lakes only,
`waterbodyType=L`); a group at the 4,000-row cap is re-fetched in four date chunks. Sequential, 1 s apart, resumable
(one JSON per group), stops on 403/429/5xx. A Preview with no data block is recorded as `no_block` (the v1 code
treated it as empty silently; here it is counted and listed).

    python -m pipeline.stocking.fetch_fidq --out ~/data-offgit/bc-stocking-fidq
"""
from __future__ import annotations

import argparse
import html
import http.cookiejar
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

LICENCE = "owner says OGL, catalogue says Access Only, unconfirmed"
BASE = "https://a100.gov.bc.ca/pub/fidq"
CAP = 4000
CHUNKS = [("01-Jan-1900", "31-Dec-1979"), ("01-Jan-1980", "31-Dec-1999"), ("01-Jan-2000", "31-Dec-2009"),
          ("01-Jan-2010", "31-Dec-2030")]
UA = "canifishthis.ca research (T634; contact via the repository owner)"

_JAR = http.cookiejar.CookieJar()
_OPEN = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_JAR))


def _req(url: str, data: dict | None = None) -> str:
    body = urllib.parse.urlencode(data).encode() if data is not None else None
    r = urllib.request.Request(url, data=body, headers={"User-Agent": UA})
    try:
        with _OPEN.open(r, timeout=120) as resp:
            return resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{url}: HTTP {e.code} — stopping (polite fetch)")
    finally:
        time.sleep(1.0)


def groups() -> list[str]:
    t = _req(f"{BASE}/viewMultipleWaterbodies.do")
    g = re.findall(r'<option value="([A-Z]{4})">[A-Z]{4} - ', t)
    if not g:
        raise SystemExit("no watershed groups on viewMultipleWaterbodies.do: the page changed")
    return sorted(set(g))


def _preview(group: str, frm: str | None = None, to: str | None = None):
    payload = {"numSearchResults": "-1", "searchCriteria.action": "PREVIEW", "searchCriteria.queryType": "DET_SI",
               "searchCriteria.watershedCode": "", "searchCriteria.waterbodyType": "L",
               "searchCriteria.watershedGroup": group, "searchCriteria.emailAddress": ""}
    if frm:
        payload["searchCriteria.fromDate"], payload["searchCriteria.toDate"] = frm, to
    t = _req(f"{BASE}/searchMultipleWaterbodies.do", payload)
    m = re.search(r"var\s+dataStocking\s*=\s*(\{.*?\});", t, re.S)
    if not m:
        return None
    recs = json.loads(m.group(1).replace("\\&", "&")).get("records") or []
    # entities (BOBB&#039;S) are decoded per value, after parsing: decoding first can put a quote inside a string
    return [{k: html.unescape(v) if isinstance(v, str) else v for k, v in r.items()} for r in recs]


def fetch_group(group: str) -> dict:
    recs = _preview(group)
    if recs is None:
        return {"group": group, "state": "no_block", "records": []}
    if len(recs) < CAP:
        return {"group": group, "state": "ok", "records": recs}
    out = []
    for a, b in CHUNKS:
        part = _preview(group, a, b)
        if part is None or len(part) >= CAP:
            return {"group": group, "state": "chunk_failed", "records": []}
        out += part
    return {"group": group, "state": "ok_chunked", "records": out}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="FIDQ lake stocking records -> outside git (T634 prototype)")
    ap.add_argument("--out", type=Path, default=Path.home() / "data-offgit" / "bc-stocking-fidq")
    a = ap.parse_args(argv)
    out = a.out.expanduser()
    (out / "groups").mkdir(parents=True, exist_ok=True)
    _req(f"{BASE}/")
    gs = groups()
    states = {}
    for k, g in enumerate(gs):
        dst = out / "groups" / f"{g}.json"
        if dst.exists():
            states[g] = json.loads(dst.read_text())["state"]
            continue
        res = fetch_group(g)
        res["licence"] = LICENCE
        tmp = dst.with_suffix(".tmp")
        tmp.write_text(json.dumps(res, sort_keys=True))
        os.replace(tmp, dst)
        states[g] = res["state"]
        print(f"{k + 1}/{len(gs)} {g} {res['state']} {len(res['records'])}", flush=True)
    summary = {"licence": LICENCE, "groups": len(gs), "states": {s: sorted(g for g in gs if states[g] == s)
                                                                  for s in sorted(set(states.values()))}}
    (out / "fetch.json").write_text(json.dumps(summary, indent=1, sort_keys=True))
    print({s: len(v) for s, v in summary["states"].items()})
    return 0 if not summary["states"].get("chunk_failed") else 1


if __name__ == "__main__":
    raise SystemExit(main())

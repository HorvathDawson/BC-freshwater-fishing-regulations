"""Fetch the Province's 2001 vector depth files (zips of ArcInfo `.e00`) to a directory OUTSIDE git (T634 item 3).

WHAT IS FETCHED. Every sheet the sheet layer (`WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW`, our gpkg layer
`bathymetry_polygons`) flags `VECTORIZED_FLAG = Y`. Two names are tried per sheet: the layer's own `VECTOR_FILE_URL`
(it names sheet 1's zip for every sheet of a multi-sheet lake: 697 flagged sheets, 513 distinct URLs) and the zip named
after the sheet's own image (`MAP_IMAGE_FILENAME`, 684 distinct). Both live on the same FIDQ address as the open PDFs.

POLITE BY CONSTRUCTION: one request at a time, a pause between, a generic client name, resume (a file already on disk
is never fetched again; `status.json` records every answer), and it STOPS at the first refusal (HTTP 403 / 429 / 5xx)
rather than retrying round it. A 200 that is not a zip is the server's "File not found" page: recorded as `missing`.

Nothing here writes into the repo: `--out` is required and has no default.

    python -m pipeline.bathymetry.fetch_vectors --out ~/data-offgit/bc-bathymetry-vectors
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = "https://a100.gov.bc.ca/pub/fidq/downloadBathymetricMap.do?filename="
AGENT = "Mozilla/5.0 (compatible)"
PAUSE_S = 0.5
REFUSED = {403, 429}


def sheets(gpkg: Path) -> list[dict]:
    """Every flagged sheet, with both candidate zip names. Sorted, so a rerun walks the same order."""
    import fiona
    out = []
    with fiona.open(gpkg, layer="bathymetry_polygons") as src:
        for f in src:
            p = f["properties"]
            if p["VECTORIZED_FLAG"] != "Y":
                continue
            url_zip = (p["VECTOR_FILE_URL"] or "").split("filename=")[-1].lower()
            img_zip = (p["MAP_IMAGE_FILENAME"] or "").rsplit(".", 1)[0].lower() + ".zip"
            out.append({"wbid": p["WATERBODY_IDENTIFIER"], "title": p["MAP_TITLE"], "sheet": p["SHEET_NO"],
                        "n_sheets": p["NUMBER_OF_MAP_SHEETS"], "image": p["MAP_IMAGE_FILENAME"],
                        "pdf": (p["PDF_FILE_URL"] or "").split("filename=")[-1], "url_zip": url_zip,
                        "image_zip": img_zip, "gazetted": p["GAZETTED_NAME"], "lake_id": p["LAKE_ID"]})
    return sorted(out, key=lambda r: (r["image"], r["wbid"]))


def _write_json(path: Path, obj) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True))
    os.replace(tmp, path)


def fetch(out: Path, names: list[str]) -> dict:
    zips = out / "zips"
    zips.mkdir(parents=True, exist_ok=True)
    status_path = out / "status.json"
    status = json.loads(status_path.read_text()) if status_path.exists() else {}
    for i, name in enumerate(names):
        dest = zips / name
        if dest.exists() or status.get(name, {}).get("state") == "missing":
            continue
        req = urllib.request.Request(BASE + name, headers={"User-Agent": AGENT})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                body, ctype = r.read(), r.headers.get("Content-Type", "")
        except urllib.error.HTTPError as e:
            status[name] = {"state": "http_error", "code": e.code}
            _write_json(status_path, status)
            if e.code in REFUSED or e.code >= 500:
                print(f"STOP: server answered {e.code} for {name}; {i} of {len(names)} walked", file=sys.stderr)
                raise SystemExit(2)
            continue
        if body[:2] == b"PK":
            tmp = dest.with_suffix(".part")
            tmp.write_bytes(body)
            os.replace(tmp, dest)
            status[name] = {"state": "ok", "bytes": len(body)}
        else:
            status[name] = {"state": "missing", "content_type": ctype, "bytes": len(body),
                            "body": body[:120].decode("utf-8", "replace")}
        _write_json(status_path, status)
        if i % 25 == 0:
            print(f"{i}/{len(names)} {name} {status[name]['state']}", flush=True)
        time.sleep(PAUSE_S)
    return status


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", required=True, type=Path, help="a directory outside git")
    ap.add_argument("--gpkg", type=Path, help="defaults to config fetch.output_gpkg")
    a = ap.parse_args(argv)
    out = a.out.expanduser().resolve()
    repo = Path(__file__).resolve().parents[2]
    if repo in out.parents or out == repo:
        raise SystemExit(f"--out {out} is inside the repo; the vector files stay outside git")
    if a.gpkg is None:
        from project_config import get_config
        a.gpkg = Path(get_config().get_path("data", "fetch", "output_gpkg"))
    rows = sheets(a.gpkg)
    out.mkdir(parents=True, exist_ok=True)
    _write_json(out / "sheets.json", rows)
    names = sorted({r["url_zip"] for r in rows} | {r["image_zip"] for r in rows})
    status = fetch(out, names)
    ok = sum(1 for v in status.values() if v["state"] == "ok")
    print(f"done: {len(names)} names, {ok} zips, {sum(1 for v in status.values() if v['state'] == 'missing')} missing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

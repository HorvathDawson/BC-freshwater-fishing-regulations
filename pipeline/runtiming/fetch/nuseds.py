"""DFO's NuSEDS (the New Salmon Escapement Database System) -> data/source/runtiming/dfo/.

THE SECOND RUN-TIMING SOURCE (T162, owner 2026-10-24: "best source per species: DFO for the salmon runs where it is
better, PSF for steelhead and where DFO has none"). NuSEDS is DFO's spawner-survey record: per stream and year, when
the fish ARRIVED in the stream and when spawning started, peaked and ended. That is measured IN the stream, where the
Pacific Salmon Explorer's curves are dated at RIVER ENTRY — so on a surveyed stream it is the better answer. It has no
steelhead (steelhead are the Province's), so steelhead stays PSF-only.

Licence: Open Government Licence – Canada (the open.canada.ca record's `license_id` is `ca-ogl-lgo`); the app credits
"Fisheries and Oceans Canada". The record is read through the open.canada.ca CKAN API so a new yearly file is found by
NAME, not by a URL typed here: the "All Areas NuSEDS_<date>.zip" resource (one CSV, every area) and the "Conservation
Unit Census Sites_<date>.csv" resource (each site's location, for binding a big river's site to its own reach).

    python -m pipeline.runtiming.fetch.nuseds          # ~13 MB down, ~230 MB on disk; owner-run, about once a year

Writes `nuseds.csv`, `census_sites.csv` and `fetched.json` (the resources' names, sizes, the record's licence and when)
under `data/source/runtiming/dfo/` (gitignored: only the parsed timings are tracked). Then:
`python -m pipeline.runtiming.parse`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import re
import urllib.request
import zipfile
from pathlib import Path

RECORD = "c48669a3-045b-400d-b730-48aafe8c5ee6"
API = f"https://open.canada.ca/data/api/action/package_show?id={RECORD}"
LANDING = f"https://open.canada.ca/data/en/dataset/{RECORD}"
#: the two resources, by the start of their (dated) names
WANT = {"nuseds": re.compile(r"^All Areas NuSEDS_(\d{8})\.zip$"),
        "census_sites": re.compile(r"^Conservation Unit Census Sites_(\d{8})\.csv$")}


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "canifishthis.ca run-timing fetch"})
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read()


def pick(resources: list[dict]) -> dict[str, dict]:
    """{nuseds | census_sites: the newest resource of that name} — refused when either is missing, so a renamed file
    stops the fetch instead of leaving last year's beside a half-updated record."""
    out: dict[str, tuple[str, dict]] = {}
    for r in resources:
        name = r.get("name")
        name = name.get("en") if isinstance(name, dict) else name
        for k, pat in WANT.items():
            m = pat.match(name or "")
            if m and (k not in out or m.group(1) > out[k][0]):
                out[k] = (m.group(1), {**r, "name": name, "date": m.group(1)})
    missing = sorted(set(WANT) - set(out))
    if missing:
        raise SystemExit(f"nuseds: the open.canada.ca record names no {missing} resource — check {LANDING}")
    return {k: v for k, (_, v) in out.items()}


def main(out: Path) -> None:
    rec = json.loads(_get(API))["result"]
    res = pick(rec["resources"])
    out.mkdir(parents=True, exist_ok=True)
    z = zipfile.ZipFile(io.BytesIO(_get(res["nuseds"]["url"])))
    csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if len(csvs) != 1:
        raise SystemExit(f"nuseds: expected one CSV in the zip, found {csvs}")
    (out / "nuseds.csv").write_bytes(z.read(csvs[0]))
    (out / "census_sites.csv").write_bytes(_get(res["census_sites"]["url"]))
    (out / "fetched.json").write_text(json.dumps({
        "fetched_at": dt.date.today().isoformat(),
        "record": LANDING, "title": "NuSEDS-New Salmon Escapement Database System",
        "publisher": "Fisheries and Oceans Canada", "licence": rec.get("license_id"),
        "metadata_modified": rec.get("metadata_modified"),
        "files": {k: {"name": v["name"], "date": v["date"], "url": v["url"]} for k, v in sorted(res.items())},
    }, indent=1, sort_keys=True) + "\n")
    print(f"nuseds: {res['nuseds']['name']} + {res['census_sites']['name']} -> {out}")


if __name__ == "__main__":
    from pipeline.common.curated import SOURCE
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=SOURCE / "runtiming" / "dfo")
    main(ap.parse_args().out)

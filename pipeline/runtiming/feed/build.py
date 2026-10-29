"""THE APP'S RUN-TIMING FILES, from the parsed timings + the CU polygons + a binding base. No atlas, no network.

    python -m pipeline.runtiming.feed.build --timing FILE --polygons FILE --base FILE --out DIR

Writes `<out>/runtiming/`: `runs-<sha12>.json.gz`, `paint-<sha12>.json.gz`, `waters-<sha12>.json.gz` and `index.json`
(the formats: `pipeline/runtiming/feed/__init__.py`), plus `<out>/report.json`. Every file is read back and checked
before it is written (`check`): a profile naming a run the runs file lacks, a section listed twice, a file whose
sha256 is not the index's — refused here, never in the app.

IN THE WATER, per month and per pentad: a PSF curve at `PRESENT["psf"]` % of its own peak or more (its tails below that
are a trickle the 5th–95th percentile window already brackets); a DFO curve where at least `PRESENT["dfo"]` % of the
surveyed years saw fish in the stream.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Sequence, Set, Tuple

from pipeline.common import calendar_spec as SPEC
from pipeline.runtiming.feed import base as BASE
from pipeline.runtiming.feed.bind import PASSAGE, RELATIONS, SIMPLIFY, atlas_of, bind, km_to_sea
from pipeline.runtiming.timing import CREDIT, PENTADS, SLOTS, TimedRun, read_timing

INDEX_FORMAT = "runtiming-index/1"
RUNS_FORMAT = "runtiming-runs/1"
PAINT_FORMAT = "runtiming-paint/1"
WATERS_FORMAT = "runtiming-waters/1"
#: Q144 (owner 2026-10-28): the species the mode opens on
DEFAULT_SLOT = "Steelhead"
PRESENT = {"psf": 10, "dfo": 25}
#: what every surface showing a timing credits, and on what terms (T162: PSF's reply is never stored or quoted)
TERMS = {"psf": {"terms": "Used with the written permission of the Pacific Salmon Foundation.",
                 "url": "https://www.salmonexplorer.ca"},
         "dfo": {"terms": "Contains information licensed under the Open Government Licence – Canada.",
                 "url": "https://open.canada.ca/en/open-government-licence-canada"}}
#: the words a reader is told about each relation (the sheet's line; the app reads them from here)
RELATION_WORDS = {"spawning": "Spawns in this watershed", "passage": "Passes through here",
                  "observed": "Seen in this stream (DFO spawner surveys)"}


# ---- the run's calendar --------------------------------------------------------------------------------------------

def _month_pentads() -> List[List[int]]:
    """Per month, the pentads (0..72, 5-day steps from Jan 1 on the common year) that touch it."""
    out = []
    for m in range(12):
        a = SPEC.DAYS_BEFORE[m] - (1 if m > 1 else 0)
        b = a + SPEC.COMMON_YEAR_LAST_DAY[m]
        out.append(sorted({min(PENTADS - 1, d // 5) for d in range(a, b)}))
    return out


MONTH_PENTADS = _month_pentads()


def present(r: TimedRun) -> List[bool]:
    t = PRESENT[r.source]
    return [v >= t for v in r.curve]


def months_of(r: TimedRun) -> int:
    """12 bits, January = bit 0: the months the run is in the water at all."""
    p = present(r)
    return sum(1 << m for m, ps in enumerate(MONTH_PENTADS) if any(p[i] for i in ps))


def pentads_of(r: TimedRun) -> str:
    """73 bits, pentad 0 = bit 0, as hex (19 digits)."""
    return format(sum(1 << i for i, on in enumerate(present(r)) if on), "019x")


# ---- the files -----------------------------------------------------------------------------------------------------

def run_record(r: TimedRun) -> dict:
    d = {"id": r.key, "src": r.source, "sp": r.species, "slot": SLOTS.index(r.slot), "name": r.name,
         "months": months_of(r), "pentads": pentads_of(r)}
    for k, v in (("label", r.label), ("peak", r.peak), ("window", list(r.window) if r.window else None),
                 ("q", r.quality), ("years", [r.years, r.first_year, r.last_year] if r.years else None)):
        if v is not None:
            d[k] = v
    return d


def sources_of(sources: dict) -> dict:
    out = {}
    for k in ("psf", "dfo"):
        s = sources[k]
        out[k] = {"credit": CREDIT[k], "title": s["title"], "dataset": s["dataset"], "updated": s.get("updated"),
                  "fetched": s.get("fetched"), **TERMS[k], **({"licence": s["licence"]} if s.get("licence") else {})}
    return out


def profiles_of(per: Dict[int, Set[Tuple[int, int]]], runs: Sequence[TimedRun], remap: Dict[int, int]):
    """(profiles, {sid: profile}): each distinct set of (run, relation), with its species bitmask and, per slot, the
    months a LOCAL run (spawning or observed) and a PASSING run is in the water."""
    keyed: Dict[tuple, int] = {}
    rows: Dict[int, int] = {}
    for s in sorted(per):
        key = tuple(sorted((remap[i], rel) for i, rel in per[s]))
        rows[s] = keyed.setdefault(key, len(keyed))
    out = []
    for key, _ in sorted(keyed.items(), key=lambda kv: kv[1]):
        local: Dict[int, int] = defaultdict(int)
        passing: Dict[int, int] = defaultdict(int)
        for i, rel in key:
            r = runs[i]
            slot = SLOTS.index(r.slot)
            (passing if rel == PASSAGE else local)[slot] |= months_of(r)
        slots = sorted(set(local) | set(passing))
        out.append({"r": [i * 3 + rel for i, rel in key], "s": sum(1 << sl for sl in slots),
                    "m": [[sl, local.get(sl, 0), passing.get(sl, 0)] for sl in slots]})
    return out, rows


def reaches(rows: Dict[int, int], a) -> dict:
    """THE BINDING WITH NO SECTION IDS (`waters`): every painted section as a REACH — a stream's FWA blue line and a
    measure range (touching pieces of one profile merged), or a lake's waterbody key — with its water's `item_id` (null
    for an unnamed stream) and its profile (the paint index's, so a reach's runs are `profiles[p]`). Columnar, strings
    in tables. Durable across atlas rebuilds: blue lines, measures and item ids outlive section ids."""
    items: Dict[str, int] = {}
    lines: Dict[str, int] = {}
    ix = lambda t, v: t.setdefault(v, len(t))                                       # noqa: E731
    stream: Dict[Tuple[str, int], List[Tuple[int, int, str]]] = defaultdict(list)
    lake: List[Tuple[str, str, int]] = []
    for s, p in rows.items():
        it = a.item.get(s, "")
        if s in a.lake:
            lake.append((a.wbk.get(s, ""), it, p))
        else:
            stream[(a.blk.get(s, ""), p)].append((a.m0[s], a.m1[s], it))
    cols = {"it": [], "blk": [], "m0": [], "m1": [], "p": []}
    for (blk, p), rs in sorted(stream.items()):
        merged: List[List] = []
        for lo, hi, it in sorted(rs):
            if merged and lo <= merged[-1][1] + 1 and merged[-1][2] == it:
                merged[-1][1] = max(merged[-1][1], hi)
            else:
                merged.append([lo, hi, it])
        for lo, hi, it in merged:
            cols["it"].append(ix(items, it) if it else -1)
            cols["blk"].append(ix(lines, blk))
            cols["m0"].append(lo)
            cols["m1"].append(hi)
            cols["p"].append(p)
    lakes = {"it": [], "wbk": [], "p": []}
    for wbk, it, p in sorted(set(lake)):
        lakes["it"].append(ix(items, it) if it else -1)
        lakes["wbk"].append(wbk)
        lakes["p"].append(p)
    return {"items": list(items), "blks": list(lines), "streams": cols, "lakes": lakes}


def _bytes(obj: dict) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def build(timing_path: Path, polygons_path: Path, base_path: Path) -> Tuple[Dict[str, bytes], dict, dict]:
    """({name: raw bytes} for runs / paint / waters, the index (urls + digests), a report)."""
    from shapely.geometry import shape
    timing = read_timing(timing_path)
    if not polygons_path.is_file():
        raise SystemExit(f"build: {polygons_path} not found — make it: python -m pipeline.runtiming.parse")
    fc = json.loads(polygons_path.read_text())
    polygons = {int(f["id"]): shape(f["geometry"]).simplify(SIMPLIFY, preserve_topology=False).buffer(0)
                for f in fc["features"]}
    a = atlas_of(BASE.read(base_path))
    psf, dfo = list(timing.psf), list(timing.dfo)
    per, report = bind(a, psf, dfo, polygons)
    all_runs = psf + dfo
    bound = sorted({i for v in per.values() for i, _ in v})
    remap = {old: new for new, old in enumerate(bound)}
    runs = [all_runs[i] for i in bound]
    profiles, rows = profiles_of(per, all_runs, remap)
    km = km_to_sea(a)
    sids = sorted(rows)
    psf_here = {s for s in sids if any(all_runs[i].source == "psf" for i, _ in per[s])}
    delta, prev = [], 0
    for s in sids:
        delta.append(s - prev)
        prev = s
    sources = sources_of(timing.sources)
    runs_doc = {"format": RUNS_FORMAT, "slots": list(SLOTS), "default": SLOTS.index(DEFAULT_SLOT),
                "present": PRESENT, "relations": list(RELATIONS), "relation_words": RELATION_WORDS,
                "sources": sources, "runs": [run_record(r) for r in runs]}
    runs_raw = _bytes(runs_doc)
    runs_sha = hashlib.sha256(runs_raw).hexdigest()
    paint_doc = {"format": PAINT_FORMAT, "section_handles": a.handles, "tile_version": a.tile_version,
                 "runs_sha256": runs_sha, "profiles": profiles, "sid": delta, "p": [rows[s] for s in sids],
                 "km": [int(round(km[s] / 10)) + 1 if s in psf_here and s in km else 0 for s in sids]}
    waters_doc = {"format": WATERS_FORMAT, "runs_sha256": runs_sha, "relations": list(RELATIONS),
                  "run_ids": [r.key for r in runs], "profiles": [pr["r"] for pr in profiles], **reaches(rows, a)}
    files = {"runs": runs_raw, "paint": _bytes(paint_doc), "waters": _bytes(waters_doc)}
    built_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    index = {"format": INDEX_FORMAT, "built_at": built_at, "section_handles": a.handles,
             "tile_version": a.tile_version, "sources": {k: {"credit": v["credit"], "updated": v["updated"],
                                                             "fetched": v["fetched"]} for k, v in sources.items()},
             "files": {}}
    for name, raw in files.items():
        sha = hashlib.sha256(raw).hexdigest()
        index["files"][name] = {"url": f"{name}-{sha[:12]}.json.gz", "bytes": len(raw), "sha256": sha}
    report.update({"runs": len(runs), "runs_unbound": len(all_runs) - len(runs), "profiles": len(profiles),
                   "painted_sections": len(sids), "base_sections": len(a.sids),
                   "raw_bytes": {k: len(v) for k, v in files.items()},
                   "gz_bytes": {k: len(gzip.compress(v, mtime=0)) for k, v in files.items()}})
    check(files, index)
    return files, index, report


def check(files: Dict[str, bytes], index: dict) -> None:
    """Every file read back against the index and each other, or refused."""
    for name, f in index["files"].items():
        raw = files[name]
        if len(raw) != f["bytes"] or hashlib.sha256(raw).hexdigest() != f["sha256"]:
            raise SystemExit(f"check: {name} is not the index's")
    runs = json.loads(files["runs"])
    paint = json.loads(files["paint"])
    waters = json.loads(files["waters"])
    n = len(runs["runs"])
    if paint["runs_sha256"] != index["files"]["runs"]["sha256"] or waters["runs_sha256"] != paint["runs_sha256"]:
        raise SystemExit("check: paint / waters are not of this runs file")
    if not (len(paint["sid"]) == len(paint["p"]) == len(paint["km"])):
        raise SystemExit("check: paint columns differ in length")
    if any(d <= 0 for d in paint["sid"]):
        raise SystemExit("check: a section is listed twice or out of order")
    if any(p < 0 or p >= len(paint["profiles"]) for p in paint["p"]):
        raise SystemExit("check: a section names a profile the file lacks")
    for pr in paint["profiles"]:
        if not pr["r"] or any(x // 3 >= n for x in pr["r"]):
            raise SystemExit("check: a profile names a run the runs file lacks")
    if waters["run_ids"] != [r["id"] for r in runs["runs"]] or waters["profiles"] != [pr["r"] for pr in paint["profiles"]]:
        raise SystemExit("check: waters and runs / paint list different runs or profiles")


def write(out: Path, files: Dict[str, bytes], index: dict, report: dict) -> Path:
    d = out / "runtiming"
    d.mkdir(parents=True, exist_ok=True)
    for name, raw in files.items():
        (d / index["files"][name]["url"]).write_bytes(gzip.compress(raw, mtime=0))
    (d / "index.json").write_text(json.dumps(index, indent=1, sort_keys=True) + "\n")
    (out / "report.json").write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
    return d


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.runtiming.feed.build", description=__doc__.split("\n\n")[0])
    ap.add_argument("--timing", type=Path, required=True)
    ap.add_argument("--polygons", type=Path, required=True)
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args(argv)
    files, index, report = build(a.timing, a.polygons, a.base)
    d = write(a.out, files, index, report)
    print(f"build: {json.dumps(report, sort_keys=True)} -> {d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

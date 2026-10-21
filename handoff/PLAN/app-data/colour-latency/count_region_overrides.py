"""Colour-latency, option C: if a tile carried each section's region set (the regs store's `section_place`, 23 places;
geometry-like, it moves only when a region boundary does), the day's colouring could be
match(place -> that place's commonest status) + match(id -> the sections that differ from it). How many ids is that
per day, across the year? Read-only on the release.

  .venv/bin/python handoff/PLAN/app-data/colour-latency/count_region_overrides.py <paint.bin.gz> <regs-sid.sqlite> [out.json]

(<regs-sid.sqlite> is the release's regs-sid-*.sqlite.gz gunzipped to a scratch file.)
"""
import gzip, json, sqlite3, statistics as st, struct, sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import numpy as np

N = 1957202
spec = spec_from_file_location("co", Path(__file__).with_name("count_overrides.py"))
src = Path(__file__).with_name("count_overrides.py").read_text().replace("\nmain()\n", "\n")
co = module_from_spec(spec)
exec(compile(src, "count_overrides.py", "exec"), co.__dict__)


def main():
    raw = gzip.open(sys.argv[1]).read()
    n = struct.unpack_from("<I", raw, 5)[0]
    days, runs = co.decode(raw[9:9 + n])
    days = np.array(days, dtype=np.uint8)                      # profile x 366
    prof = np.zeros(N, dtype=np.int32)                           # 0 = not listed (base all year)
    for s, c, p in runs:
        prof[s:s + c] = p + 1
    db = sqlite3.connect(sys.argv[2])
    place = np.zeros(N, dtype=np.uint8)
    for blk, b in db.execute("SELECT block, b FROM section_place"):
        a = np.frombuffer(b, dtype=np.uint8)
        lo = blk * len(a)
        place[lo:min(N, lo + len(a))] = a[:max(0, min(len(a), N - lo))]
    nplaces = int(place.max()) + 1
    per_day = []
    for d in range(366):
        if d == 59:
            continue
        code = np.where(prof > 0, np.concatenate([[0], days[:, d]])[prof], 0).astype(np.uint8)
        # each place's commonest code that day = its base colour; overrides = the rest
        counts = np.zeros((nplaces, 8), dtype=np.int64)
        np.add.at(counts, (place, code), 1)
        modal = counts.argmax(axis=1)
        over = int((code != modal[place]).sum())
        nonbase = int((code != 0).sum())
        per_day.append({"day": d + 1, "nonbase": nonbase, "region_overrides": over,
                        "places_not_base": int((modal[1:] != 0).sum())})

    def summ(k):
        xs = [r[k] for r in per_day]
        return {"min": min(xs), "median": int(st.median(xs)), "max": max(xs)}

    out = {"sections": N, "places": nplaces - 1, "nonbase": summ("nonbase"),
           "region_overrides": summ("region_overrides"), "places_not_base": summ("places_not_base"),
           "days_region_helps_10pct": sum(1 for r in per_day if r["region_overrides"] < 0.9 * r["nonbase"])}
    print(json.dumps(out, indent=1))
    if len(sys.argv) > 3:
        json.dump({"summary": out, "per_day": per_day}, open(sys.argv[3], "w"))


main()

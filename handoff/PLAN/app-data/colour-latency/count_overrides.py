"""Colour-latency study: how many sections are NOT plain `base` on each day, and how many
contiguous handle runs each day's colouring needs. Reads the release paint (read-only).

  .venv/bin/python handoff/PLAN/app-data/colour-latency/count_overrides.py <paint.bin.gz> [out.json]
"""
import gzip, json, struct, sys, statistics as st

CODES = ["base", "own", "closed", "tidal", "outside"]
DAYS = 366


def decode(b):
    pos = 0

    def byte():
        nonlocal pos
        pos += 1
        return b[pos - 1]

    def v():
        n = 0
        mul = 1
        while True:
            c = byte()
            n += (c & 0x7f) * mul
            mul *= 128
            if not c & 0x80:
                return n

    assert bytes(b[:4]) == b"BCSI"
    pos = 4
    assert byte() == 2
    pos += 16
    nprof = v()
    days = []
    for _ in range(nprof):
        row = []
        for _ in range(v()):
            x = v()
            row += [x & 7] * (x >> 3)
        assert len(row) == DAYS
        days.append(row)
    nruns = v()
    gaps = [v() for _ in range(nruns)]
    counts = [v() for _ in range(nruns)]
    runs = []
    end = 0
    for i in range(nruns):
        p = v()
        s = end + gaps[i]
        runs.append((s, counts[i], p))
        end = s + counts[i]
    return days, runs


def main():
    raw = gzip.open(sys.argv[1]).read()
    assert raw[:4] == b"BCPT"
    n = struct.unpack_from("<I", raw, 5)[0]
    days, runs = decode(raw[9:9 + n])
    total = 1957202
    listed = sum(c for _, c, _ in runs)
    per_day = []
    for d in range(DAYS):
        if d == 59:
            continue  # Feb 29 slot is never read
        by = {c: 0 for c in CODES}
        segs = []  # this day's non-base handle ranges, adjacent same-code runs merged
        for s, c, p in runs:
            code = days[p][d]
            by[CODES[code]] += c
            if code == 0:
                continue
            if segs and segs[-1][2] == code and segs[-1][1] == s:
                segs[-1][1] = s + c
            else:
                segs.append([s, s + c, code])
        by["base"] += total - listed
        per_day.append({"day": d + 1, **by, "nonbase": total - by["base"], "runs": len(segs)})

    def summ(k):
        xs = [r[k] for r in per_day]
        return {"min": min(xs), "median": int(st.median(xs)), "max": max(xs)}

    out = {"sections": total, "listed": listed, "index_runs": len(runs), "profiles": len(days),
           **{k: summ(k) for k in ["own", "closed", "tidal", "outside", "nonbase", "runs"]}}
    churn = []
    for a in range(DAYS - 1):
        b = a + 1
        if a == 59 or b == 59:
            continue
        churn.append(sum(c for _, c, p in runs if days[p][a] != days[p][b]))
    out["daily_change"] = {"min": min(churn), "median": int(st.median(churn)), "max": max(churn),
                           "days_with_any": sum(1 for x in churn if x)}
    print(json.dumps(out, indent=1))
    if len(sys.argv) > 2:
        json.dump({"summary": out, "per_day": per_day}, open(sys.argv[2], "w"))


main()

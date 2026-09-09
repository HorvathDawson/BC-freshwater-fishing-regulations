#!/usr/bin/env python3
"""Extract the other Pacific Salmon Explorer indicators that are server-rendered per CU.

Same trick as run timing: the `all-regions/all-species/all-cus` page carries every CU's
series in its RSC flight payload. Here each CU is a block that opens with `"cu":{"id":N`,
so blocks are split on that anchor and the JSON inside each one is decoded in place.
"""
import json, re, urllib.request

DEC = json.JSONDecoder()

#: Read from the SAVED page, never the network. `fetch/explorer.py` owns downloading; a
#: generator that could re-fetch is a generator that can silently disagree with the copy a
#: build was made from.
SRC = None

def flight(slug):
    html = (SRC / f"{slug}.html").read_text()
    return "".join(json.loads(c) for c in
                   re.findall(r'self\.__next_f\.push\(\[1,\s*(".*?")\]\)</script>', html, re.S))

def cu_blocks(raw):
    """Yield (cuid, text) for each CU's slice of the payload, in document order."""
    anchors = [(m.start(), int(m.group(1)))
               for m in re.finditer(r'"cu":\{"id":(\d+)', raw)]
    for i, (pos, cid) in enumerate(anchors):
        end = anchors[i+1][0] if i+1 < len(anchors) else len(raw)
        yield cid, raw[pos:end]

def clean(v):
    """React renders a missing value as the string "$undefined"; that is a gap, not a zero."""
    if isinstance(v, str) and v.startswith("$undefined"):
        return None
    if isinstance(v, list):
        return [clean(x) for x in v]
    if isinstance(v, dict):
        return {k: clean(x) for k, x in v.items()}
    return v


def objs_at(text, pattern):
    """Decode every JSON value that `pattern` points at inside this block."""
    out = []
    for m in re.finditer(pattern, text):
        try:
            out.append(clean(DEC.raw_decode(text, m.start(1))[0]))
        except ValueError:
            pass
    return out

def badges(raw):
    m = re.search(r'"dataQualityBadges":\{(.*?),"metrics":(\[.*?\])', raw, re.S)
    if not m:
        return {}, []
    scores = json.loads("{"+m.group(1)+"}")
    metrics = json.loads(m.group(2))
    clean = {}
    for k, v in scores.items():
        clean[int(k)] = [None if not isinstance(x, int) else x for x in v][:len(metrics)]
    return clean, metrics

def catch_and_run_size():
    raw = flight("catch-and-run-size")
    scores, metrics = badges(raw)
    out = {}
    for cid, txt in cu_blocks(raw):
        series = {}
        for arr in objs_at(txt, r'"data":(\[\{"year")'):
            for p in arr:
                series.setdefault(p["type"], {})[p["year"]] = p["value"]
        rec = {"quality": dict(zip(metrics, scores.get(cid, []))), "series": series}
        out[cid] = rec
    return out, metrics

def biological_status():
    raw = flight("biological-status")
    scores, metrics = badges(raw)
    out = {}
    for cid, txt in cu_blocks(raw):
        rec = {"quality": dict(zip(metrics, scores.get(cid, [])))}
        ab = objs_at(txt, r'"data":(\[\{"year")')
        if ab:
            rec["abundance"] = {p["year"]: {"smoothed": p.get("smoothedAbundance"),
                                            "estimated": p.get("estimatedCount")}
                                for p in ab[0]}
        for key, pat in [("psf", r'"psf":(\{)'), ("cosewic", r'"cosewic":(\{)'),
                         ("sara", r'"sara":(\{)')]:
            v = objs_at(txt, pat)
            if v:
                rec[key] = v[0]
        for k in ("gen_length", "status_type"):
            m = re.search(rf'"{k}":("?[^,}}]+)', txt)
            if m:
                rec[k] = json.loads(m.group(1)) if m.group(1)[0] == '"' else json.loads(m.group(1))
        for bm in ("sr", "percentile"):
            v = objs_at(txt, rf'"{bm}":(\{{"max")')
            if v:
                rec["benchmarks"] = {"type": bm, **v[0]}
        out[cid] = rec
    return out, metrics

if __name__ == "__main__":
    import argparse
    from pathlib import Path
    from pipeline.common.curated import GENERATED, SOURCE
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--src", type=Path, default=SOURCE / "runtiming")
    ap.add_argument("--out", type=Path, default=GENERATED.runtiming)
    a = ap.parse_args()
    SRC = a.src
    crs, m1 = catch_and_run_size()
    bio, m2 = biological_status()
    json.dump({"catch_and_run_size": {str(k): v for k, v in crs.items()},
               "biological_status": {str(k): v for k, v in bio.items()},
               "quality_metrics": {"catch_and_run_size": m1, "biological_status": m2}},
              open(a.out / "indicators.json", "w"))
    print("catch CUs with series:", sum(1 for v in crs.values() if v["series"]))
    print("status CUs with abundance:", sum(1 for v in bio.values() if v.get("abundance")))
    print("status CUs with PSF status:", sum(1 for v in bio.values() if v.get("psf")))
    print("catch metrics:", m1)
    print("status metrics:", m2)

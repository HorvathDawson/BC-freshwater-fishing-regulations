"""Fetched pages -> one `runs.json`: every conservation unit, its curve and its label.

The whole indicator is server-rendered into the page's Next.js flight payload, so parsing
is string work over a saved file and needs no network.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

from pipeline.runtiming.runs import Run, derive_form, derive_label, write_runs

RATINGS = ["Not Applicable", "Low", "Medium-Low", "Medium", "Medium-High", "High"]
_DEC = json.JSONDecoder()


def flight(html: str) -> str:
    """Reassemble the RSC payload from the `self.__next_f.push` chunks."""
    return "".join(json.loads(c) for c in
                   re.findall(r'self\.__next_f\.push\(\[1,\s*(".*?")\]\)</script>', html, re.S))


def _rows(raw: str) -> dict[str, object]:
    """Flight rows are `<hexid>:<json>`; values elsewhere reference them as "$<hexid>"."""
    out, pos = {}, 0
    while pos < len(raw):
        m = re.compile(r"([0-9a-f]+):").match(raw, pos)
        if not m:
            nl = raw.find("\n", pos)
            if nl < 0:
                break
            pos = nl + 1
            continue
        p = m.end()
        if p < len(raw) and raw[p] in '[{"-0123456789tfn':
            try:
                val, end = _DEC.raw_decode(raw, p)
                out[m.group(1)] = val
                nl = raw.find("\n", end)
                pos = nl + 1 if nl >= 0 else len(raw)
                continue
            except ValueError:
                pass
        nl = raw.find("\n", p)
        pos = nl + 1 if nl >= 0 else len(raw)
    return out


def _deref(v, rows, depth=0):
    if depth > 40:
        return v
    if isinstance(v, str):
        return _deref(rows[v[1:]], rows, depth + 1) if re.fullmatch(r"\$[0-9a-f]+", v) \
               and v[1:] in rows else v
    if isinstance(v, list):
        return [_deref(x, rows, depth + 1) for x in v]
    if isinstance(v, dict):
        return {k: _deref(x, rows, depth + 1) for k, x in v.items()}
    return v


def _array_around(raw: str, idx: int):
    depth = 0
    for i in range(idx, -1, -1):
        if raw[i] == "]":
            depth += 1
        elif raw[i] == "[":
            if depth == 0:
                try:
                    return _DEC.raw_decode(raw, i)[0]
                except ValueError:
                    return None
            depth -= 1
    return None


def _pentads(daily: list[float]) -> list[float]:
    """365 daily shares -> 73 pentads. gauge_clim's own axis, so the hydrograph's
    interpolation between pentads works unchanged."""
    return [round(sum(daily[i * 5:(i + 1) * 5]), 5) for i in range(73)]


def _dates(daily: list[float]) -> tuple[str, tuple[str, str]]:
    """Peak (plateau midpoint) and the 5th-95th span, both on the day-of-year CIRCLE.

    Circular because winter steelhead run December into April: a straight calendar mean
    puts them in July, and a straight percentile walk reports a span of 300 days.
    """
    md = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    mn = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def fmt(d0):
        d = int(round(d0)) % 365
        for m, n in enumerate(md):
            if d < n:
                return f"{mn[m]} {d + 1}"
            d -= n
        return "Dec 31"

    tot = sum(daily)
    ang = [2 * math.pi * i / 365 for i in range(365)]
    C = sum(w * math.cos(a) for w, a in zip(daily, ang)) / tot
    S = sum(w * math.sin(a) for w, a in zip(daily, ang)) / tot
    mean = (math.atan2(S, C) % (2 * math.pi)) * 365 / (2 * math.pi)

    # the mode as a PLATEAU: 188 of 329 curves are flat-topped, where argmax picks a day
    # at random from the top
    mx = max(daily)
    hits = [i for i, v in enumerate(daily) if v >= mx * 0.999]
    if hits[0] == 0 and hits[-1] == 364:
        gap = max(range(1, len(hits)), key=lambda k: hits[k] - hits[k - 1])
        hits = hits[gap:] + [h + 365 for h in hits[:gap]]
    peak = ((hits[0] + hits[-1]) / 2) % 365

    start = int(round(mean)) - 182
    run, out, qi = 0.0, {}, 0
    qs = [0.05, 0.95]
    for k in range(365):
        run += daily[(start + k) % 365]
        while qi < len(qs) and run / tot >= qs[qi]:
            out[qs[qi]] = (start + k) % 365
            qi += 1
    return fmt(peak), (fmt(out.get(0.05, 0)), fmt(out.get(0.95, 364)))


def main(src: Path, out: Path, review: Path | None) -> None:
    html = (src / "run-timing.html").read_text()
    raw = flight(html)
    rows = _rows(raw)

    regions = {r["id"]: r for r in
               _array_around(raw, re.search(r'\{"id":1,"name":"Skeena","slug":"skeena"', raw).start())}
    species = {s["id"]: s for s in
               _array_around(raw, re.search(r'"name":"Chinook"', raw).start())}

    cus = {}
    for m in re.finditer(r'\{"id":\d+,"name":"(?:[^"\\]|\\.)*","slug":"[^"]*-\d+"', raw):
        try:
            o, _ = _DEC.raw_decode(raw, m.start())
        except ValueError:
            continue
        if {"regionId", "speciesId"} <= set(o):
            cus[o["id"]] = _deref(o, rows)

    curves = {int(c): json.loads(a) for c, a in
              re.findall(r'\{"cuid":(\d+),"percent":(\[[^\]]*\])\}', raw)}
    bm = re.search(r'"dataQualityBadges":\{(.*?),"metrics":', raw, re.S)
    badges = {int(k): v for k, v in json.loads("{" + bm.group(1) + "}").items()}

    overrides = {}
    if review and Path(review).is_file():
        overrides = {int(k): v for k, v in
                     json.loads(Path(review).read_text()).get("run_labels", {}).items()}

    runs = {}
    for cid, cu in sorted(cus.items()):
        daily = curves.get(cid)
        label, src_ = derive_label(cu["name"]), "name"
        if cid in overrides:
            label, src_ = overrides[cid], "review"
        elif label is None:
            src_ = "none"
        q = badges.get(cid, [0])[0] or 0
        peak, span = _dates(daily) if daily else (None, None)
        runs[cid] = Run(
            cuid=cid, name=cu["name"],
            species=species[cu["speciesId"]]["name"],
            region=regions[cu["regionId"]]["name"],
            label=label, form=derive_form(cu["name"]), label_source=src_,
            pentads=tuple(_pentads(daily)) if daily else (),
            quality=q, quality_word=RATINGS[q],
            peak_date=peak, span=span,
        )

    write_runs(out / "runs.json", runs, {
        "source": "https://www.salmonexplorer.ca/explore/data/run-timing/",
        "units": len(runs),
        "with_curve": sum(1 for r in runs.values() if r.has_curve),
        "labelled": sum(1 for r in runs.values() if r.label),
        "labelled_by_review": sum(1 for r in runs.values() if r.label_source == "review"),
    })
    print(f"runs {len(runs)}  with curve {sum(1 for r in runs.values() if r.has_curve)}  "
          f"labelled {sum(1 for r in runs.values() if r.label)}")


if __name__ == "__main__":
    from pipeline.common.curated import CURATED, GENERATED, SOURCE
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--src", type=Path, default=SOURCE / "runtiming")
    ap.add_argument("--out", type=Path, default=GENERATED.runtiming)
    ap.add_argument("--review", type=Path,
                    default=CURATED.runtiming.review)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    main(a.src, a.out, a.review)

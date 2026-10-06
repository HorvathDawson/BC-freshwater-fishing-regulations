"""Compare the pipeline's answers with what the consumer page v35 shows, field by field.

The golden side is `golden.js`'s output (JSON lines). The pipeline side does not exist yet (its
format is for the answers work to settle), so this module fixes the MEETING POINT instead: five
streams of `(key, value)` records, each value a plain JSON projection. `golden_view()` projects the
page's golden records onto it; the answers layer supplies the same projection of its own output
(an adapter, or files `<stream>.jsonl[.gz]` of `{"key": [...], "value": {...}}`), and
`compare()` diffs them per key and per field.

  stream    key                                         value
  ladder    (water, part, seg_from, fish, origin)       {rule: [state, partly, by]}            Stage 4
  answer    (water, part, date, fish, origin)           status, daily, winner, lines, roles     Stage 5.2
  row       (water, part, date, kind, pool|win)         title, kind, pool, members, daily, real_daily,
                                                         sources (rule, role, by), decided       Stage 5.3-6
  gear      (water, part, date)                         counts, elems, tags, tiles               Stage 7.1-7.6
  licence   (water, part, date, profile)                buy, none, tile, requirements mine       Stage 7.7

`water` is the item id, `part` the part key `set|lset|sw|steelhead|sr0|province_except|home_region`
(golden.js `partKey`: what a part's answers can depend on; a rule set id alone repeats across the
parts of one water), `date` "MM-DD", `origin` hatchery | wild, `profile`
"residency/age/guidance/status".

  python -m pipeline.deliver.answers.reference.compare GOLDEN_DIR ANSWERS_DIR
"""
from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from typing import Iterable, Iterator

STREAMS = ("ladder", "answer", "row", "gear", "licence")
ORIGINS = ("hatchery", "wild")


def _lines(path: Path) -> Iterator[dict]:
    op = gzip.open if path.suffix == ".gz" else open
    with op(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def _files(d: Path, stem: str) -> list[Path]:
    return sorted(d.glob(f"{stem}.jsonl.gz")) + sorted(d.glob(f"{stem}.*.jsonl.gz")) \
        + sorted(d.glob(f"{stem}.jsonl")) + sorted(d.glob(f"{stem}.*.jsonl"))


# ------------------------------------------------------------------------------------------------
# the page's golden records -> the meeting point
# ------------------------------------------------------------------------------------------------

def _answer(res: dict | None) -> dict | None:
    if res is None:
        return None
    return {"status": res["status"], "daily": res.get("daily"), "winner": res["win"],
            "narrow": res.get("narrow"),
            "lines": [{k: v for k, v in l.items() if k in ("t", "r", "a", "b", "take", "o", "status", "daily", "outer", "only")}
                      for l in res.get("lines", [])],
            "roles": {k: [v["role"], v.get("by")] for k, v in res.get("roles", [])}}


def golden_view(golden: Path) -> dict[str, dict[tuple, dict]]:
    """Every golden record, projected and keyed. Licence answer ids are resolved."""
    out: dict[str, dict[tuple, dict]] = {s: {} for s in STREAMS}
    for rec in (r for f in _files(golden, "ladder") for r in _lines(f)):
        out["ladder"][(rec["w"], rec["pk"], rec["seg_from"], rec["fish"], rec["origin"])] = rec["states"]
    for rec in (r for f in _files(golden, "card") for r in _lines(f)):
        w, s, d = rec["w"], rec["pk"], rec["date"]
        for fish, a in rec["answers"].items():
            for o in ORIGINS:
                out["answer"][(w, s, d, fish, o)] = _answer(a[o])
        for row in rec["rows"]:
            out["row"][(w, s, d, row["kind"], row.get("pool") or row.get("win"))] = {
                "title": row["title"], "kind": row["kind"], "pool": row.get("pool"), "win": row.get("win"),
                "members": row.get("members"), "daily": row.get("daily"), "real_daily": row.get("real_daily"),
                "sources": [[x["rule"], x["role"], x["by"]] for x in row["sources"]],
                "decided": row["decided"]}
    for rec in (r for f in _files(golden, "gear") for r in _lines(f)):
        key = (rec["w"], rec["pk"], rec["date"])
        out["gear"][key] = {"closed": True} if rec["closed"] else {
            "closed": False,
            "counts": {k: [l[0]["rule"], l[0]["c"]] for k, l in rec["counts"].items() if l},
            "elems": {k: [e["rule"], e["s"]] for k, e in rec["elems"].items()},
            "tags": rec["tags"], "tiles": rec["tiles"]}
    answers = {r["id"]: r for f in _files(golden, "licence_answers") for r in _lines(f)}
    for rec in (r for f in _files(golden, "licence") for r in _lines(f)):
        s = rec["pk"]
        for prof, aid in rec["profiles"].items():
            a = answers[aid]
            out["licence"][(rec["w"], s, rec["date"], prof)] = {"closed": True} if a["closed"] else {
                "closed": False, "buy": a["buy"], "none": a["none"], "tile": a["tile"],
                "mine": sorted(x["rule"] for x in a["settle"]["reqs"] if x["mine"])}
    return out


def answers_view(d: Path) -> dict[str, dict[tuple, dict]]:
    """The answers layer's records, already in the meeting-point shape (`{"key": [...], "value": …}`)."""
    out: dict[str, dict[tuple, dict]] = {s: {} for s in STREAMS}
    for s in STREAMS:
        for f in _files(d, s):
            for r in _lines(f):
                out[s][tuple(r["key"])] = r["value"]
    return out


# ------------------------------------------------------------------------------------------------
# the diff
# ------------------------------------------------------------------------------------------------

def diff_values(want, got, path: tuple = ()) -> list[tuple[tuple, object, object]]:
    """Field-by-field differences: [(path, page value, pipeline value)]. Dicts are compared per key,
    lists per position (order is part of what the page shows)."""
    if isinstance(want, dict) and isinstance(got, dict):
        out = []
        for k in sorted(set(want) | set(got), key=str):
            out += diff_values(want.get(k, _MISSING), got.get(k, _MISSING), path + (k,))
        return out
    if isinstance(want, list) and isinstance(got, list):
        if len(want) != len(got):
            return [(path + ("len",), len(want), len(got))] + [
                d for i, (a, b) in enumerate(zip(want, got)) for d in diff_values(a, b, path + (i,))]
        return [d for i, (a, b) in enumerate(zip(want, got)) for d in diff_values(a, b, path + (i,))]
    return [] if want == got else [(path, want, got)]


class _Missing:
    def __repr__(self):
        return "<missing>"

    def __eq__(self, other):
        return isinstance(other, _Missing)


_MISSING = _Missing()


def compare(want: dict[str, dict], got: dict[str, dict], streams: Iterable[str] = STREAMS) -> dict:
    """Per stream: keys only the page has, keys only the pipeline has, and per shared key its field diffs."""
    rep = {}
    for s in streams:
        W, G = want.get(s, {}), got.get(s, {})
        diffs = {k: d for k in W.keys() & G.keys() if (d := diff_values(W[k], G[k]))}
        rep[s] = {"page": len(W), "pipeline": len(G), "only_page": sorted(W.keys() - G.keys())[:50],
                  "n_only_page": len(W.keys() - G.keys()), "n_only_pipeline": len(G.keys() - W.keys()),
                  "n_differ": len(diffs), "diffs": dict(sorted(diffs.items())[:50])}
    return rep


def summary(rep: dict) -> str:
    lines = []
    for s, r in rep.items():
        lines.append(f"{s}: page {r['page']}, pipeline {r['pipeline']}, missing {r['n_only_page']}, "
                     f"extra {r['n_only_pipeline']}, differ {r['n_differ']}")
        for k, ds in list(r["diffs"].items())[:5]:
            for p, a, b in ds[:4]:
                lines.append(f"   {k} {'/'.join(map(str, p))}: page {a!r} pipeline {b!r}")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("golden", type=Path)
    ap.add_argument("answers", type=Path)
    a = ap.parse_args(argv)
    print(summary(compare(golden_view(a.golden), answers_view(a.answers))))


if __name__ == "__main__":
    main()

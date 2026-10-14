"""Every part whose WORDS changed between two deliveries, parts paired by IDENTITY, not by index.

    python -m pipeline.tools.part_label_diff OLD_DIR NEW_DIR [--hint]

Each directory holds `ui-rules-export.json` and `ui-rules-answers.json` (a delivery's regs dir or a
side build). A water's parts are re-ordered whenever its rule sets change, so pairing part i with
part i (the first label diff did) reports reorderings as changes. Here a part is identified by WHERE
it runs: its runs' km ranges on the water (`_shape`). Same shape -> the same part; else the old part
it overlaps most (a part that grew or shrank: "reshaped"); none -> "new" / "gone". A pair is listed
when its `short`, `label` or `runs` words differ (`--hint`: also its hint line).

A directory without its own guide (an archived `before/`) borrows the other's: the guide carries no
part data, only the field dictionary, and the bundle digest it is checked against is the data's own.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.tools.export_codec import expand

WORDS = ("short", "label", "runs")


def _load(d: Path, other: Path) -> tuple[dict, dict]:
    data = json.loads((d / "ui-rules-export.json").read_text(encoding="utf-8"))
    g = d / "ui-rules-guide.json"
    guide = json.loads((g if g.exists() else other / "ui-rules-guide.json").read_text(encoding="utf-8"))
    guide = {**guide, "about": {**guide["about"], "bundle": data["about"]["bundle"]}}
    ans = json.loads((d / "ui-rules-answers.json").read_text(encoding="utf-8"))
    return expand(data, guide), ans["sections"]["display"]["waters"]


def _ranges(part: dict) -> list[tuple[float, float]]:
    return sorted((round(min(r["km_to"], r["km_from"]), 2), round(max(r["km_to"], r["km_from"]), 2))
                  for r in part.get("runs") or []
                  if r.get("km_to") is not None and r.get("km_from") is not None and not r.get("branch"))


def _shape(part: dict) -> tuple:
    """Where a part runs: its main-stem km ranges, then its other runs' ends (side channels, lakes)."""
    rest = sorted(json.dumps({k: r.get(k) for k in ("branch", "polygon", "km_to", "km_from")}, sort_keys=True)
                  for r in part.get("runs") or [] if r.get("branch") or r.get("km_to") is None)
    return tuple(_ranges(part)), tuple(rest)


def _overlap(a: dict, b: dict) -> float:
    return sum(max(0.0, min(h1, h2) - max(l1, l2)) for l1, h1 in _ranges(a) for l2, h2 in _ranges(b))


def pair(old: list[dict], new: list[dict]) -> list[tuple[int | None, int | None, str]]:
    """[(old_ix, new_ix, how)]: how is "same" (identical shape), "reshaped", "new" or "gone"."""
    out, left = [], set(range(len(old)))
    by_shape: dict[tuple, list[int]] = {}
    for i, p in enumerate(old):
        by_shape.setdefault(_shape(p), []).append(i)
    rest = []
    for j, q in enumerate(new):
        hit = [i for i in by_shape.get(_shape(q), []) if i in left]
        if hit:
            left.discard(hit[0])
            out.append((hit[0], j, "same"))
        else:
            rest.append(j)
    for j in rest:
        best = max(sorted(left), key=lambda i: _overlap(old[i], new[j]), default=None)
        if best is not None and _overlap(old[best], new[j]) > 0:
            left.discard(best)
            out.append((best, j, "reshaped"))
        else:
            out.append((None, j, "new"))
    out += [(i, None, "gone") for i in sorted(left)]
    return out


def diff(old_dir: Path, new_dir: Path, hint: bool = False) -> list[dict]:
    XA, DA = _load(old_dir, new_dir)
    XB, DB = _load(new_dir, old_dir)
    fields = WORDS + (("hint",) if hint else ())
    rows = []
    for wid in sorted(set(XA["waters"]) | set(XB["waters"])):
        pa = (XA["waters"].get(wid) or {}).get("parts") or []
        pb = (XB["waters"].get(wid) or {}).get("parts") or []
        # the answers' display parts are positional with the export's (`order` is the display order)
        da = dict(enumerate((DA.get(wid) or {}).get("parts") or []))
        db = dict(enumerate((DB.get(wid) or {}).get("parts") or []))
        name = (XB["waters"].get(wid) or XA["waters"].get(wid) or {}).get("name") or ""
        for i, j, how in pair(pa, pb):
            a = (da.get(i) or {}) if i is not None else {}
            b = (db.get(j) or {}) if j is not None else {}
            changed = [f for f in fields if a.get(f) != b.get(f)]
            if changed:
                rows.append({"water": wid, "name": name, "old": i, "new": j, "how": how, "fields": changed,
                             **{f"{f}_old": a.get(f) for f in fields}, **{f"{f}_new": b.get(f) for f in fields}})
    return rows


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("old", type=Path)
    ap.add_argument("new", type=Path)
    ap.add_argument("--hint", action="store_true", help="also report parts whose hint line changed")
    a = ap.parse_args(argv)
    rows = diff(a.old, a.new, a.hint)
    hows = {}
    for r in rows:
        hows[r["how"]] = hows.get(r["how"], 0) + 1
    print(f"{len(rows)} parts with changed words on {len({r['water'] for r in rows})} waters; "
          + ", ".join(f"{k} {v}" for k, v in sorted(hows.items())))
    for r in rows:
        print(f"{r['water']}\t{r['name']}\t{r['how']} {r['old']}->{r['new']}\t"
              f"{r['short_old']!r} -> {r['short_new']!r}\t| {r['label_old']!r} -> {r['label_new']!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

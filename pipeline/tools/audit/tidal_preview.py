"""T166: WHAT THE OWNER'S TIDAL BOUNDARIES WOULD TAKE, before any of them lands (read only, no graph).

    python -m pipeline.tools.audit.tidal_preview [--reach data/generated/reaches/full]
                                                 [--build data/generated/atlas/full] [--json OUT]

The tidal proposals the marks make NOW (`proposals.build`, incl. the owner's earlier calls) — each new
tidal cut at its (blk, measure), each flagged existing cut at the atlas's resolved measure(s) — against
the LIVE reach run's rule -> section rows. A section on a cut's blue line ending at or below the cut is
TIDAL (it leaves every binding); one spanning the cut STRADDLES it (the cut splits it; the rule keeps
the upper half). Lists:

  lose all   rules every one of whose sections is tidal: unresolved `tidal` once the cuts land
             (kitimat_river.r1). LOUD: each needs a look at the cut or the rule before applying.
  lose some  rules that keep water (count of tidal sections, and straddled ones).

Approximate on purpose (the side atlas + `binding_diff` is the real answer, AGENTS 63): only the cut's
OWN blue line is read — braid channels and river polygons below a cut are not (T166 step 2 cuts those
at build).
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import defaultdict
from pathlib import Path


#: a rule binding this many sections or more is a zone / province-wide rule: counted, not listed
WIDE = 5000


def _ends(handles: Path, blks: set[str]) -> dict[str, list[float]]:
    """Per blk on a cut, the start measures of its sections, sorted (the next start is a section's end)."""
    out: dict[str, list[float]] = defaultdict(list)
    with open(handles, encoding="utf-8") as f:
        for ln in f:
            sid = ln.strip()
            blk, _, m = sid.rpartition(":")
            if blk in blks:
                out[blk].append(float(m))
    return {b: sorted(v) for b, v in out.items()}


def cuts_now(build: Path) -> list[dict]:
    """The tidal cuts the marks in force make: {proposal, kind, split, blk, measure}."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import marks as M
    from pipeline.regs.review import proposals as P
    props = P.build(M.load(CURATED.regulations.review.marks), P.Sources.live(build))
    resolved = json.loads((Path(build) / "splits.resolved.json").read_text(encoding="utf-8"))
    out = []
    for p in P.tidal_props(props):
        if p["kind"] == "tidal_flag":
            rows = [r for r in resolved if r["split_id"] == p["split_id"]]
            out += [{"proposal": p["id"], "kind": "flag", "split": p["split_id"], "blk": r["blk"],
                     "measure": float(r["route_measure"])} for r in rows]
            if not rows:
                out.append({"proposal": p["id"], "kind": "flag", "split": p["split_id"], "blk": None,
                            "measure": None})
        else:
            t = p.get("tidal") or {}
            out.append({"proposal": p["id"], "kind": "new", "split": p["split_id"],
                        "blk": t.get("blk"), "measure": t.get("measure")})
    return out


def preview(cuts: list[dict], rule_section: Path, handles: Path) -> dict:
    by_blk: dict[str, list[dict]] = defaultdict(list)
    for c in cuts:
        if c["blk"] and c["measure"] is not None:
            by_blk[str(c["blk"])].append(c)
    ends = _ends(handles, set(by_blk))

    def tidal_of(sid: str):
        """(cut, 'below' | 'straddles') for a section on a cut's line, else None."""
        blk, _, m = sid.rpartition(":")
        if blk not in by_blk:
            return None
        lo = float(m)
        starts = ends.get(blk) or []
        hi = next((x for x in starts if x > lo), float("inf"))
        for c in by_blk[blk]:
            if hi <= c["measure"] + 0.5:
                return c, "below"
            if lo < c["measure"] < hi:
                return c, "straddles"
        return None

    lose_all, lose_some = [], []
    opener = gzip.open if str(rule_section).endswith(".gz") else open
    with opener(rule_section, "rt", encoding="utf-8") as f:
        for ln in f:
            if not any(b in ln for b in by_blk):
                continue
            r = json.loads(ln)
            secs = list(r.get("sections") or ())
            hit = {s: tidal_of(s) for s in secs}
            below = [s for s, h in hit.items() if h and h[1] == "below"]
            strad = [s for s, h in hit.items() if h and h[1] == "straddles"]
            if not below and not strad:
                continue
            row = {"entry_id": r["entry_id"], "rule_id": r["rule_id"], "sections": len(secs),
                   "tidal": len(below), "straddled": len(strad),
                   "splits": sorted({hit[s][0]["split"] for s in below + strad})}
            (lose_all if below and len(below) == len(secs) else lose_some).append(row)
    return {"cuts": cuts, "lose_all": lose_all, "lose_some": lose_some,
            "unlocated": [c for c in cuts if not c["blk"] or c["measure"] is None]}


def main(argv=None) -> int:
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(prog="python -m pipeline.tools.audit.tidal_preview",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("--build", type=Path, default=GENERATED.build())
    ap.add_argument("--reach", type=Path, default=None)
    ap.add_argument("--json", type=Path, help="write the result here")
    a = ap.parse_args(argv)
    reach = a.reach or (GENERATED.reaches / Path(a.build).name)
    got = preview(cuts_now(a.build), reach / "rule_section.jsonl.gz",
                  Path(a.build) / "section_handles.txt")
    print(f"{len(got['cuts'])} tidal cut point(s) from the marks; {len(got['unlocated'])} unlocated")
    for c in got["unlocated"]:
        print(f"  ?? {c['proposal']}: no blk/measure (a flag on a cut the atlas did not resolve?)")
    print(f"LOSE ALL ({len(got['lose_all'])}) — unresolved `tidal` once the cuts land:")
    for r in got["lose_all"]:
        print(f"  !! {r['entry_id']} {r['rule_id']}: {r['sections']} section(s) <- {', '.join(r['splits'])}")
    wide = [r for r in got["lose_some"] if r["sections"] >= WIDE]
    print(f"lose some ({len(got['lose_some'])}; {len(wide)} zone / province-wide rule(s) of "
          f">= {WIDE:,} sections not listed — they lose only the tidal pieces):")
    for r in got["lose_some"]:
        if r["sections"] >= WIDE:
            continue
        print(f"     {r['entry_id']} {r['rule_id']}: -{r['tidal']} of {r['sections']}"
              + (f", {r['straddled']} straddled" if r["straddled"] else "")
              + f" <- {', '.join(r['splits'])}")
    if a.json:
        a.json.write_text(json.dumps(got, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())

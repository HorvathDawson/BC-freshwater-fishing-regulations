"""T226 / T520: the curated in-season matches as the file the app reads beside the feed — FORMATTING ONLY, no atlas.

    python -m pipeline.regs.inseason.publish [--out DIR] [--matches FILE] [--release DIR] [--feed index.json]

writes `<out>/matches.json` (default `GENERATED.inseason / "feed"`, which the local tile server serves as
`/feeds/inseason/`; staging / production: R2 `feeds/inseason/matches.json` through `pipeline.deliver.sync_inputs`).
`--feed` copies a captured feed `index.json` there too (dev).

FORMAT `inseason-links/2` (T520). Where a notice applies was resolved where the atlas is (`pipeline.regs.inseason.resolve`:
the review app on save, the release promote) and committed in `notice_waters.json`; this only copies it out:

    {"format": "inseason-links/2", "release": "r-…", "digest": <sha of the curated file>,
     "notices": {"is-…": {"from"?: "YYYY-MM-DD", "to"?: "YYYY-MM-DD",
                          "waters": {"gnis:…": {"all": true}
                                     | {"runs": [[blk, m0, m1], …], "lakes"?: [i, …], "ends": "From A to B"}}}}}

No part index, label, extent or section id (AGENTS 5). The app (`@app/core` `inSeasonCover`) reads `runs` / `lakes` only
on the release named; `all` is a water's whole and holds on any release.

THE CHECKS LIVE HERE, ONCE (the feed has its own): a non-zero exit, nothing written, on
    - an unresolved match (`unresolved`, or no `waters`) — re-match it in the review app
    - empty coverage (a water with no range)
    - a range not landing on the release it names: when that release is here (`data/release/<release>`), every run must
      start on a section of its water there and every `lakes` place exist; a release that is NOT here (GitHub's
      runner) is SAID on stderr, never silently passed
    - the file resolved on another release than `data/release/LATEST` when LATEST is here (re-resolve:
      `python -m pipeline.regs.inseason.resolve`)

Deterministic: no clock in the output.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

from pipeline.regs.inseason import matches as M

FORMAT = "inseason-links/2"


class Refused(SystemExit):
    pass


def compile_links(doc: M.Matches, digest: str) -> dict:
    """The links, or Refused with every reason (unresolved / empty)."""
    bad: list[str] = []
    if not doc.release:
        bad.append("the file names no release: resolve it (python -m pipeline.regs.inseason.resolve)")
    notices: dict = {}
    for rid in sorted(doc.matches):
        m = doc.matches[rid]
        if m.unresolved or not m.waters:
            bad.append(f"{rid} ({m.water}): unresolved — {m.unresolved or 'no waters'}")
            continue
        waters = {}
        for item in sorted(m.waters):
            w = m.waters[item]
            if w.all:
                waters[item] = {"all": True}
            elif not (w.runs or w.lakes):
                bad.append(f"{rid} ({m.water}): {item} covers nothing")
            else:
                waters[item] = {"runs": [list(r) for r in w.runs], **({"lakes": list(w.lakes)} if w.lakes else {}),
                                "ends": w.ends}
        row: dict = {}
        if m.effective and m.effective.from_:
            row["from"] = m.effective.from_
        if m.effective and m.effective.to:
            row["to"] = m.effective.to
        row["waters"] = waters
        notices[rid] = row
    if bad:
        raise Refused("inseason.publish: refused —\n  " + "\n  ".join(bad))
    return {"format": FORMAT, "release": doc.release, "digest": digest, "notices": notices}


def check_on_release(links: dict, rel) -> list[str]:
    """Every range lands on its water in the release it names (`resolve.Release`); every lake place exists.
    -> [problem]."""
    from pipeline.regs.inseason import resolve as R
    bad: list[str] = []
    if rel.name != links["release"]:
        return [f"the release here is {rel.name}, not {links['release']}"]
    for rid, n in links["notices"].items():
        for item, w in n["waters"].items():
            members = rel.item_sections(item)
            if not members:
                bad.append(f"{rid}: {item} is not a water of {rel.name}")
                continue
            for run in w.get("runs", ()):
                if not R.covered_by_runs([run], members, rel.starts):
                    bad.append(f"{rid}: {item} range {run} lands on no section of it in {rel.name}")
            n_lakes = len(rel.lake_pieces(item))
            for i in w.get("lakes", ()):
                if not 0 <= i < n_lakes:
                    bad.append(f"{rid}: {item} lake place {i} — it has {n_lakes} lake piece(s) in {rel.name}")
    return bad


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--matches", type=Path, default=None, help="the curated file (default CURATED.inseason.notice_waters)")
    ap.add_argument("--feed", type=Path, default=None, help="a captured feed index.json to copy beside it (dev)")
    ap.add_argument("--release", type=Path, default=None,
                    help="the release directory to check the ranges on (default data/release/<the file's release>)")
    a = ap.parse_args(argv)
    src = a.matches or M.default_path()
    text = Path(src).read_text(encoding="utf-8")
    doc = M.load(src)
    links = compile_links(doc, hashlib.sha256(text.encode("utf-8")).hexdigest()[:16])
    root = Path(GENERATED.release)
    latest = root / "LATEST"
    if a.release is None and latest.exists():
        now = latest.read_text(encoding="utf-8").strip()
        if now != links["release"]:
            raise Refused(f"inseason.publish: refused — the matches are resolved on {links['release']}, LATEST is {now}: "
                          "re-resolve (python -m pipeline.regs.inseason.resolve)")
    rel_dir = a.release or root / links["release"]
    if (rel_dir / "manifest.json").exists():
        from pipeline.regs.inseason import resolve as R
        bad = check_on_release(links, R.read_release(rel_dir))
        if bad:
            raise Refused("inseason.publish: refused —\n  " + "\n  ".join(bad))
    else:
        print(f"inseason.publish: release {links['release']} is not here ({rel_dir}): its ranges were proved when they "
              "were resolved, not again now", file=sys.stderr)
    out = a.out or (GENERATED.inseason / "feed")
    out.mkdir(parents=True, exist_ok=True)
    (out / "matches.json").write_text(json.dumps(links, ensure_ascii=False, separators=(",", ":")) + "\n",
                                      encoding="utf-8")
    if a.feed:
        shutil.copyfile(a.feed, out / "index.json")
    print(f"in-season links: {len(links['notices'])} notice(s) on {links['release']} -> {out / 'matches.json'}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

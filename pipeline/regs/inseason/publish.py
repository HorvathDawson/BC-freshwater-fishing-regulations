"""T226 / T520: the curated in-season matches as the file the app reads beside the feed — FORMATTING ONLY, no atlas.

    python -m pipeline.regs.inseason.publish [--out DIR] [--matches FILE] [--release DIR] [--feed index.json]

writes `<out>/matches.json` (default `GENERATED.inseason / "feed"`, which the local tile server serves as
`/feeds/inseason/`; staging / production: R2 `feeds/inseason/matches.json` through `pipeline.deliver.sync_inputs`).
`--feed` copies a captured feed `index.json` there too (dev).

FORMAT `inseason-links/3` (T520; /3, T517 review finding 1: no `words` — the release's `display.ends` says every end). Where a notice applies was resolved where the atlas is (`pipeline.regs.inseason.resolve`:
the review app on save, its Re-resolve after a promote) and committed in `notice_waters.json`; this only copies it out:

    {"format": "inseason-links/3", "release": "r-…", "digest": <sha of the curated file>,
     "notices": {"is-…": {"from"?: "YYYY-MM-DD", "to"?: "YYYY-MM-DD",
                          "waters": {"gnis:…": {"all": true}
                                     | {"runs": [[blk, m0, m1], …], "lakes"?: [i, …], "ends": [[low, high], …]}}}}}

No part index, extent or section id (AGENTS 5); `ends` are TOKENS, the sentence is core's (`inSeasonWhere`). The app
(`@app/core` `inSeasonCover`) reads `runs` / `lakes` only on the release named; `all` holds on any release.

THE CHECKS LIVE HERE, ONCE (the feed has its own): a non-zero exit, nothing written, on
    - an unresolved match (`unresolved`, or no `waters`) — re-match it in the review app
    - a match whose `stamp` no longer fits its extents, the release and its waters (a hand edit not re-resolved)
    - an end token the release's `display.ends` holds no words for (the release build puts every curated notice's
      ends in it, `matches.end_tokens`: a token missing here is a notice changed since that release — rebuild it)
    - the file resolved on another release than `data/release/LATEST` (re-resolve in the review app, then commit)
    - that release not on disk, or a range not landing on its water there
`--side-release DIR` checks against a SIDE build instead of LATEST (a release not promoted yet) — never by default.

Deterministic: no clock in the output.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

from pipeline.common.feed_formats import FEED_FORMATS
from pipeline.regs.inseason import matches as M

FORMAT = FEED_FORMATS["inseason_links"]    # (T556) typed once, in pipeline/common/feed_formats.py
#: a water's own ends: core says them by the sentence's shape ("Upstream of …"), never by words
NATURAL = frozenset({"mouth", "source"})


class Refused(SystemExit):
    pass


def compile_links(doc: M.Matches, digest: str) -> dict:
    """The links, or Refused with every reason (unresolved / stale stamp / an end with no words)."""
    bad: list[str] = []
    if not doc.release:
        bad.append("the file names no release: re-resolve it (the review app's Re-resolve)")
    notices: dict = {}
    for rid in sorted(doc.matches):
        m = doc.matches[rid]
        if m.unresolved or not m.waters:
            bad.append(f"{rid} ({m.water}): unresolved — {m.unresolved or 'no waters'}")
            continue
        if m.stamp != M.stamp_of(m, doc.release):
            bad.append(f"{rid} ({m.water}): its extents or ranges changed since they were resolved on {doc.release} "
                       "(a hand edit?) — re-resolve it")
            continue
        waters = {}
        for item in sorted(m.waters):
            w = m.waters[item]
            if w.all:
                waters[item] = {"all": True}
                continue
            waters[item] = {"runs": [list(r) for r in w.runs], **({"lakes": list(w.lakes)} if w.lakes else {}),
                            "ends": [list(p) for p in w.ends]}
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


def ends_problems(links: dict, ends: dict, release: str) -> list[str]:
    """(T517 review finding 1) every end token the links name that the release's `display.ends` holds no words for."""
    return [f"{rid}: {item} end {t!r} has no words in {release}'s display.ends" for rid, n in links["notices"].items()
            for item, w in n["waters"].items() for p in w.get("ends", ()) for t in p if t not in NATURAL and t not in ends]


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
    ap.add_argument("--side-release", type=Path, default=None,
                    help="check against this SIDE build's release instead of data/release/LATEST (never by default)")
    a = ap.parse_args(argv)
    src = a.matches or M.default_path()
    text = Path(src).read_text(encoding="utf-8")
    doc = M.load(src)
    links = compile_links(doc, hashlib.sha256(text.encode("utf-8")).hexdigest()[:16])
    root = Path(GENERATED.release)
    if a.side_release is None:
        now = (root / "LATEST").read_text(encoding="utf-8").strip()      # a missing LATEST raises: no release here
        if now != links["release"]:
            raise Refused(f"inseason.publish: refused — the matches are resolved on {links['release']}, LATEST is "
                          f"{now}: re-resolve in the review app (Re-resolve), then commit")
        rel_dir = root / now
    else:
        rel_dir = a.side_release
    if not (rel_dir / "manifest.json").exists():
        raise Refused(f"inseason.publish: refused — release {links['release']} is not on disk at {rel_dir}: its "
                      "ranges cannot be checked")
    from pipeline.regs.inseason import resolve as R
    bad = check_on_release(links, R.read_release(rel_dir))
    # (T517 review finding 1) THE GUARD: every end the links name has words in that release's `display.ends`
    from pipeline.deliver.answers.display.name_check import release_waters
    bad += ends_problems(links, release_waters(rel_dir)[1], rel_dir.name)
    if bad:
        raise Refused("inseason.publish: refused —\n  " + "\n  ".join(bad))
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

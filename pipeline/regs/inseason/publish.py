"""T226 / T520: the curated in-season matches as the file the app reads beside the feed — FORMATTING ONLY, no atlas.

    python -m pipeline.regs.inseason.publish [--out DIR] [--matches FILE] [--release DIR] [--feed index.json]

writes `<out>/matches.json` (default `GENERATED.inseason / "feed"`, which the local tile server serves as
`/feeds/inseason/`; staging / production: R2 `feeds/inseason/matches.json` through `pipeline.deliver.sync_inputs`).
`--feed` copies a captured feed `index.json` there too (dev).

FORMAT `inseason-links/4` (T673, owner 2026-11-13: ONE file, release-independent — "Why not just have a single file
that does both?"; /3 named the one release its ranges were read on). Where a notice applies was resolved where the
atlas is (`pipeline.regs.inseason.resolve`: the review app on save, its Re-resolve after a promote) and committed in
`notice_waters.json`; this only copies it out:

    {"format": "inseason-links/4", "digest": <sha of the curated file>,
     "notices": {"is-…": {"from"?: "YYYY-MM-DD", "to"?: "YYYY-MM-DD",
                          "waters": {"gnis:…": {"all": true}
                                     | {"runs": [[blk, m0, m1], …], "lakes"?: true, "ends": [[low, high], …]}}}}}

Keyed by the water's durable item id (AGENTS 5); a stretch is the FWA stream key + metres along it — no release, part
index, extent or section id. ANY release places it (`@app/core` `inSeasonCover`, `resolve.touched`): every section of
the water whose line the range touches, every lake piece of the river when `lakes`; `all` is every section. `ends` are
TOKENS, said by the reading release's `display.ends` (core's `inSeasonWhere`).

THE CHECKS LIVE HERE, ONCE (the feed has its own): a non-zero exit, nothing written, on
    - an unresolved match (`unresolved`, or no `waters`) — re-match it in the review app
    - a match whose `stamp` no longer fits its extents, the release it was resolved on and its waters (a hand edit
      not re-resolved)
    - on `data/release/LATEST` (what every new install reads): a water it does not hold, a range touching no section
      of its water, and — when the file was resolved on LATEST — a range whose ends are not its water's section
      boundaries there (a shifted measure; `exact_problems`)
    - an end token LATEST's `display.ends` holds no words for (the release build puts every curated notice's ends in
      it, `matches.end_tokens`: a token missing here is a notice changed since that release — rebuild it)
(T673: the old "resolved on another release than LATEST" refusal is gone — the file is not bound to a release; an
older release on a phone reads it by the same rule.)
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
            waters[item] = {"runs": [list(r) for r in w.runs], **({"lakes": True} if w.lakes else {}),
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
    return {"format": FORMAT, "digest": digest, "notices": notices}


def ends_problems(links: dict, ends: dict, release: str) -> list[str]:
    """(T517 review finding 1) every end token the links name that the release's `display.ends` holds no words for."""
    return [f"{rid}: {item} end {t!r} has no words in {release}'s display.ends" for rid, n in links["notices"].items()
            for item, w in n["waters"].items() for p in w.get("ends", ()) for t in p if t not in NATURAL and t not in ends]


def check_on_release(links: dict, rel) -> list[str]:
    """(T673) Every water is one `rel` holds and every range TOUCHES a section of its water there (`resolve.touched`,
    the device's rule); a water saying `lakes` has a lake piece there. -> [problem]. Any release: the file names none."""
    from pipeline.regs.inseason import resolve as R
    bad: list[str] = []
    for rid, n in links["notices"].items():
        for item, w in n["waters"].items():
            members = rel.item_sections(item)
            if not members:
                bad.append(f"{rid}: {item} is not a water of {rel.name}")
                continue
            for run in w.get("runs", ()):
                if not R.touched([run], members, rel.lines):
                    bad.append(f"{rid}: {item} range {run} touches no section of it in {rel.name}")
            if w.get("lakes") and not rel.lake_pieces(item):
                bad.append(f"{rid}: {item} says its lake pieces are covered, but it has none in {rel.name}")
    return bad


def exact_problems(links: dict, rel) -> list[str]:
    """(T673) ON THE RELEASE THE FILE WAS RESOLVED ON, every range is EXACT: it starts where a section of its water
    starts and ends where one ends (its spans are the resolved sections', `resolve.make_runs`) — a measure shifted by
    hand, or a range read on another release's boundaries, is refused. An open-ended section (`lines.OPEN`: a stream's
    one section) ends wherever the atlas measured it; a section of no length ends one metre past its start."""
    from pipeline.deliver.release.lines import OPEN
    bad: list[str] = []
    for rid, n in links["notices"].items():
        for item, w in n["waters"].items():
            mine = [rel.lines[h] for h in rel.item_sections(item) if h in rel.lines]
            for blk, m0, m1 in w.get("runs", ()):
                on = [(lo, hi) for b, lo, hi in mine if b == blk]
                if not any(lo == m0 for lo, _ in on) or not any(
                        hi == m1 or (hi == OPEN and m1 > lo) or (hi == lo and m1 == lo + 1) for lo, hi in on):
                    bad.append(f"{rid}: {item} range {[blk, m0, m1]} does not start and end on its sections' "
                               f"boundaries in {rel.name}, the release it was resolved on (a shifted measure?) — "
                               "re-resolve it")
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
        from pipeline.deliver.release.manifest import live
        rel_dir = live(root)       # (T560 item 6) no LATEST, or one naming a folder with no manifest, raises
    else:
        rel_dir = a.side_release
    if not (rel_dir / "manifest.json").exists():
        raise Refused(f"inseason.publish: refused — release {rel_dir.name} is not on disk at {rel_dir}: the ranges "
                      "cannot be checked")
    from pipeline.regs.inseason import resolve as R
    rel = R.read_release(rel_dir)
    bad = check_on_release(links, rel)
    if rel.name == doc.release:
        bad += exact_problems(links, rel)
    else:
        print(f"inseason.publish: the matches were resolved on {doc.release}, this release is {rel.name}: the ranges "
              "are checked to touch their waters here (any release reads them), not to sit on its boundaries",
              file=sys.stderr)
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
    print(f"in-season links ({FORMAT}): {len(links['notices'])} notice(s), checked on {rel.name} -> "
          f"{out / 'matches.json'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

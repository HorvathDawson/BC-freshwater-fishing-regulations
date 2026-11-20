"""(T668 option 2, owner 2026-11-12) THE STRADDLER PAIR TABLE: a live salmon answer on a stretch in two DFO regions
resolves on its own.

On a section in two regions (a release chain with `s`) each region's places answer apart and the most strict per fish
speaks (`deliver.salmon.strictest`, RULINGS Z4a). Two OPENINGS that differ are ranked by the freshwater Z4 ladder
(`deliver.salmon.stricter_opening`): one region's opening, or both regions' words with no number. The release does that
in Python; under the live feed the app composes the chain itself (core `salmonFeed.ts` `strictestIx`) and holds no copy
of the ladder (the cascade is resolved in Python), so it showed both regions' words where the release kept one.

This table is the Python's answer for every such pair, computed here and looked up there: for every two-region chain of
the live release, every species and day where both regions' places (read from the CURRENT typed feed and the curated
places, as the release reads them) open with different verdicts, `[a, b, stricter_opening(a, b)]` in the chain's fold
order (regions by number). A pair is keyed by the two verdicts' whole content, so a limit DFO changes in season is a
pair the table does not hold, and the app keeps both regions' words for it (never looser) until the next sync.

Staged by `pipeline.deliver.sync_inputs` (`dfo-pairs` -> R2 `private/feeds/dfo/pairs.json`); the feeds worker carries
it into the feed (`feeds/dfo/index.json` `pairs`); the app reads it there.

    python -m pipeline.deliver.salmon_pairs [--release DIR]      # the table on stdout, then what it holds on stderr
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.common.feed_formats import FEED_FORMATS
from pipeline.deliver import salmon as S

FORMAT = FEED_FORMATS["dfo_pairs"]


def _key(v: dict) -> str:
    return json.dumps(v, sort_keys=True, ensure_ascii=False)


def release_salmon(rel: Path) -> dict:
    """The release's salmon payload (head + shards, `deliver.salmon.decode_files`)."""
    man = json.loads((rel / "manifest.json").read_text(encoding="utf-8"))
    files = {}
    for k, f in man["files"].items():
        if k == "salmon" or k.startswith("salmon-"):
            b = (rel.parent / f["url"]).read_bytes()
            files[k] = gzip.decompress(b) if f["url"].endswith(".gz") else b
    if "salmon" not in files:
        raise S.SalmonError(f"salmon_pairs: release {rel} carries no salmon file")
    return S.decode_files(files)[1]


def place_rules(entry_files: dict, feeds: dict, headers: dict) -> Dict[str, List[dict]]:
    """Every curated place's rules from the typed feed, as the release reads them (`deliver.salmon.location_rules`)."""
    from pipeline.regs.dfo_salmon import quota as Q
    out: Dict[str, List[dict]] = {}
    for slug, ef in sorted(entry_files.items()):
        feed = feeds.get(slug) or {}
        by_fp = {l["fingerprint"]: l["rules"] for l in feed.get("locators") or []}
        closure = Q.header_closure(list(headers.get(slug) or []), slug)
        for loc in ef.locations:
            out[loc.location_id] = S.location_rules(loc, by_fp, closure)
    return out


def pairs(chains: List[dict], rules_of: Dict[str, List[dict]], static: Optional[Dict[str, dict]] = None,
          verdicts: Optional[List[dict]] = None) -> Tuple[List[list], List[str]]:
    """([[a, b, answer]] sorted by content, the place ids a two-region chain names that no curated place is). Only
    chains with `s` (two regions); per species and day each group's first place with an answer (else `NOT_LISTED`),
    exactly as `live.compose` folds them; a pair is two OPENINGS that differ. (T796 A5) A release-side place (payload
    `static[id]`, a rule-book closure `book:...`: the live feed never carries it, the app composes it from the release)
    answers from its own year, its verdict indices into the payload's `verdicts`."""
    from pipeline.regs.dfo_salmon import live as L
    V, G = S._Interner(), S._Interner()
    V(S.NOT_LISTED)
    static = static or {}
    if static and verdicts is None:
        raise S.SalmonError("salmon_pairs: static places need the payload's verdicts")
    cal: Dict[str, dict] = {}
    for lid, year in static.items():
        cal[lid] = {"sp": [[x if i % 2 == 0 else (V(verdicts[x]) if x >= 0 else -1) for i, x in enumerate(runs)]
                           for runs in year["sp"]]}
    got: Dict[Tuple[str, str], list] = {}
    missing: set = set()
    for c in chains:
        groups = c.get("s")
        if not groups:
            continue
        lids = c["l"]
        gone = [l for l in lids if l not in rules_of and l not in cal]
        if gone:
            missing.update(gone)
            continue
        for lid in lids:
            if lid not in cal:
                cal[lid] = L.location_calendar(rules_of[lid], V, G)
        for k in range(len(S.SPECIES)):
            for d in range(len(S.DAYS)):
                ans = [V.items[next((x for x in (L._at(cal[lids[j]]["sp"][k], d) for j in g) if x >= 0), 0)]
                       for g in groups]
                labels = [S.region_label(lids[j] for j in g) for g in groups]
                best, rb = ans[0], labels[0]
                for v, r in zip(ans[1:], labels[1:]):
                    if v["s"] == best["s"] == "open" and v != best:
                        key = (_key(best), _key(v))
                        said = S.stricter_opening(best, v, rb, r)
                        if key not in got:
                            got[key] = [best, v, said]
                        elif got[key][2] != said:
                            # (T669 item 3) the same two openings in another region pair name other regions: the
                            # table keys by content, so it holds the numbers without names (never a wrong name)
                            got[key] = [best, v, S.stricter_opening(best, v)]
                    nxt = S.strictest([best, v], [rb, r])
                    best, rb = nxt, (rb if nxt is best else r if nxt is v else "")
    return [got[k] for k in sorted(got)], sorted(missing)


def table(rel: Path, *, entries_dir: Optional[Path] = None, feed_dir: Optional[Path] = None) -> dict:
    """The pair table for release `rel` from the current typed feed and curated places. A place a two-region chain
    names that no curated place is STOPS it (the release and the places are not of one vintage)."""
    from pipeline.common.curated import GENERATED
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    payload = release_salmon(rel)
    feed_dir = Path(feed_dir or Path(GENERATED.regs.dfo_salmon) / "typed")
    slugs = [s for s in ALL_SLUGS if not PAGES[s].is_stub]
    files = {s: E.load(s, entries_dir or E.ENTRIES_DIR) for s in slugs}
    feeds = {}
    for s in slugs:
        p = feed_dir / f"region-{s}.json"
        if not p.exists():
            raise S.SalmonError(f"salmon_pairs: no typed DFO feed {p} — write it:\n    python -m pipeline.regs.dfo_salmon.feed")
        feeds[s] = json.loads(p.read_text(encoding="utf-8"))
    rows, missing = pairs(payload["chains"], place_rules(files, feeds, S.load_headers(feeds)),
                          payload.get("static") or {}, payload["verdicts"])
    if missing:
        raise S.SalmonError(f"salmon_pairs: release {rel.name} chains name places no curated file holds: {missing}")
    return {"format": FORMAT, "release": rel.name, "pairs": rows}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--release", type=Path, default=None, help="a release directory (default data/release/LATEST)")
    a = ap.parse_args(argv)
    from pipeline.regs.inseason.resolve import release_dir
    t = table(release_dir(a.release))
    sys.stdout.write(json.dumps(t, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    one = sum(1 for a, b, r in t["pairs"] if r == a or r == b)
    words = sum(1 for _, _, r in t["pairs"] if r.get("u"))
    sys.stderr.write(f"salmon_pairs: {t['release']}: {len(t['pairs'])} pairs, {one} resolve to one region's opening, "
                     f"{len(t['pairs']) - one - words} to one quota of both regions' caps (T669 item 3), "
                     f"{words} to both regions' words\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

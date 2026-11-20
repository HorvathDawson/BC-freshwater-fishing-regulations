"""THE SALMON-OPEN, GEAR-HIDDEN WARNING (T936, owner 2026-11-20: "Maybe add a build warning if there is a waterbody like
that").

    python -m pipeline.deliver.release.salmon_gear [--release DIR] [--bundle FILE] [--out DIR] [--rows N]

A day the game-fish strip reads CLOSED draws no gear marks (`answers.display.strip.strip_code`: closed alone; owner
2026-11-20 00:58 on T934, "closed is only closed"), so the salmon year bar shows none there either (T934 reads the
strip's gear bits). That is right when the water is closed to all fishing. The leftover case this lists: the game fish
are closed that day by SPECIES closures that together close every game fish (no single full closure: a full closure
closes salmon too, RULINGS L25 / T796 A5, so the salmon verdict is closed there), DFO has some salmon OPEN or RELEASE
that day, and the B.C. gear answer at the same part key and segment holds a bait ban, fly fishing only or artificial
flies only — a salmon angler is fishing under a B.C. gear rule the bar does not draw.

ALL THREE, PER SECTION AND DAY, nothing decided here (every fact is read):
  * game fish closed: every moment's display frame `status` is closed (the status index's CLOSED: a night closure never
    closes the day), and NO rule closes every game fish at every moment (the frame's `closing`, the verdicts'
    `project.closing` — the same test `salmon.book_closures` uses for a book full closure);
  * salmon fishable: the section's salmon chain (AFTER the book closures, `salmon.with_book_closures`) answers `open` or
    `release` for some species that day;
  * gear: the gear frame of some moment of that day carries `bait_ban` or `fly`.

IT NEVER FAILS A BUILD. `release.build` prints `say(...)` (a count and the first rows, or "none") and, given a reports
folder, writes the whole list beside the release's other reports (`<reports>/<release id>/salmon_gear_hidden.json` +
`.txt`; `pipeline.release`'s checks write `gone_ids` / `live_counts` there). The CLI says it for a shipped release.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pipeline.common import calendar_spec

#: the salmon verdicts a salmon angler may fish under (`salmon.verdict_on`): keep, or catch and release
FISHABLE = ("open", "release")
#: the gear answer's words (`answers.gear`: `fly`, `bait_ban`), as the strip's legend says them
GEAR_WORDS = {"fly_fishing_only": "fly fishing only", "artificial_fly_only": "artificial flies only",
              "bait_ban": "bait ban"}
REPORT = "salmon_gear_hidden"
FIRST_ROWS = 10


def day_gear(display: Sequence[dict], gear: Sequence[dict], game: Iterable[str]) -> Optional[Tuple[str, ...]]:
    """One day's answer from its moments' display and gear frames (one pair per moment): the B.C. gear words held at
    some moment when the day is closed to game fish by species closures alone; None otherwise (some moment open, a tidal
    frame, a rule closing every game fish at every moment, or no gear rule)."""
    if not display or any(d.get("status") != "closed" for d in display):
        return None
    game = frozenset(game)
    full = [{r for r, fish in d.get("closing") or () if game <= set(fish)} for d in display]
    if set.intersection(*full):
        return None                                  # a full closure: salmon close too (RULINGS L25)
    words = set()
    for g in gear:
        if g.get("fly") in GEAR_WORDS:
            words.add(GEAR_WORDS[g["fly"]])
        if g.get("bait_ban"):
            words.add(GEAR_WORDS["bait_ban"])
    return tuple(sorted(words)) or None


def key_days(store) -> Dict[int, Dict[int, Tuple[str, ...]]]:
    """{akey: {day index 0..365: gear words}} for every akey of a regs store (`decode.Store`) with such a day."""
    from pipeline.deliver import types as T
    disp = {i: store.val("display_frame", b) for i, b in store.con.execute("SELECT id, j FROM display_frame")}
    gear = {i: store.val("gear_frame", b) for i, b in store.con.execute("SELECT id, j FROM gear_frame")}
    segs = {s: json.loads(t) for s, t in store.con.execute("SELECT seg, starts FROM seglist")}
    cells: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    for a, s, d, g in store.con.execute("SELECT akey, s, display, gear FROM cell ORDER BY akey, s"):
        cells[a].append((d, g))
    out: Dict[int, Dict[int, Tuple[str, ...]]] = {}
    for a, seg in store.con.execute("SELECT akey, seg FROM akey ORDER BY akey"):
        starts, cs = segs[seg], cells[a]
        if len(starts) != len(cs):
            raise ValueError(f"salmon_gear: akey {a} has {len(cs)} cells for {len(starts)} segment starts")
        distinct = sorted(set(starts))
        days: Dict[int, Tuple[str, ...]] = {}
        for i, start in enumerate(distinct):
            here = [cs[k] for k, x in enumerate(starts) if x == start]
            got = day_gear([disp[d] for d, _ in here], [gear[g] for _, g in here], T.GAME_FISH)
            if got:
                end = distinct[i + 1] if i + 1 < len(distinct) else calendar_spec.DAYS + 1
                days.update({day - 1: got for day in range(start, end)})
        if days:
            out[a] = days
    return out


def chain_days(payload: Mapping) -> Dict[int, Dict[int, Tuple[str, ...]]]:
    """{chain: {day index 0..365: the salmon species (`salmon.SPECIES`) open or release}} for every chain with such a
    day (the chain's `sp`: per species runs of [start day, verdict index, …])."""
    from pipeline.deliver.salmon import SPECIES
    from pipeline.deliver.salmon_who import _run_at
    ok = {i for i, v in enumerate(payload["verdicts"]) if v.get("s") in FISHABLE}
    out: Dict[int, Dict[int, Tuple[str, ...]]] = {}
    for c, ch in enumerate(payload["chains"]):
        if not any(ch["sp"][k][j + 1] in ok for k in range(len(SPECIES)) for j in range(0, len(ch["sp"][k]), 2)):
            continue
        days = {}
        for d in range(calendar_spec.DAYS):
            sp = tuple(s for k, s in enumerate(SPECIES) if _run_at(ch["sp"][k], d) in ok)
            if sp:
                days[d] = sp
        if days:
            out[c] = days
    return out


def spans(days: Iterable[int]) -> str:
    """Day indexes 0..365 as the book's ranges: "Jan 1-Mar 31, Nov 1-Dec 31"."""
    ds = sorted(days)
    out, i = [], 0
    while i < len(ds):
        j = i
        while j + 1 < len(ds) and ds[j + 1] == ds[j] + 1:
            j += 1
        a, b = (calendar_spec.month_day(d + 1) for d in (ds[i], ds[j]))
        txt = f"{calendar_spec.MONTHS[a[0] - 1]} {a[1]}"
        out.append(txt if i == j else f"{txt}-{calendar_spec.MONTHS[b[0] - 1]} {b[1]}")
        i = j + 1
    return ", ".join(out)


def rows(store, payload: Mapping, sections: Sequence[Tuple[int, int]],
         parts: Callable[[], Mapping[int, Tuple[int, int]]]) -> List[dict]:
    """Every water part (or unnamed stretch) and the days all three hold, with the salmon species fishable then and the
    B.C. gear words: [{water, item_id, part, label, sections, example_sid, days, species, gear}], most sections first.
    `sections`: (sid, salmon chain); `parts()`: sid -> (item ord, part_ix) (the bundle's `part_section`, `bundle_parts`),
    asked only when some section qualifies."""
    keys = key_days(store)
    if not keys:
        return []
    chains = chain_days(payload)
    akeys = store.section_akeys()
    part_of: Mapping[int, Tuple[int, int]] = {}
    groups: Dict[tuple, List[int]] = defaultdict(list)
    for sid, c in sections:
        a = akeys.get(sid)
        if a not in keys or c not in chains:
            continue
        kd, cd = keys[a], chains[c]
        both = sorted(set(kd) & set(cd))
        if not both:
            continue
        species = tuple(sorted({s for d in both for s in cd[d]}))
        gear = tuple(sorted({w for d in both for w in kd[d]}))
        if not part_of:
            part_of = parts()
        o, pix = part_of.get(sid, (None, None))
        where = (o, pix) if o is not None else ("unnamed", store.unnamed_name(sid))
        groups[(where, tuple(both), species, gear)].append(sid)
    item = {o: (store.s(i), n) for o, i, n in store.con.execute("SELECT item_ord, item_id, name FROM item")}
    label = {(o, p): store.s(s) for o, p, s in store.con.execute("SELECT item_ord, part_ix, label_s FROM part")}
    out = []
    for (where, days, species, gear), sids in groups.items():
        if where[0] == "unnamed":
            wid, name, pix, lab = None, where[1], None, None
        else:
            (wid, name), pix, lab = item[where[0]], where[1], label.get(where)
        out.append({"water": name, "item_id": wid, "part": pix, "label": lab, "sections": len(sids),
                    "example_sid": min(sids), "days": spans(days), "n_days": len(days),
                    "species": list(species), "gear": list(gear)})
    out.sort(key=lambda r: (-r["sections"], r["water"] or "", r["part"] if r["part"] is not None else -1, r["days"]))
    return out


def line(r: Mapping) -> str:
    part = f" part {r['part']}" + (f" ({r['label']})" if r.get("label") else "") if r.get("part") is not None else ""
    return (f"{r['water']}{part} [{r['item_id'] or 'unnamed'}]: {r['days']}; salmon {'/'.join(r['species'])} open or "
            f"release; B.C. {', '.join(r['gear'])}; {r['sections']} section(s), e.g. {r['example_sid']}")


def say(found: Sequence[Mapping], n: int = FIRST_ROWS) -> str:
    """The build's words: a WARNING with the count and the first `n` rows, or "none"."""
    if not found:
        return "T936 salmon-open, gear-hidden days: none"
    head = (f"WARNING (T936): {len(found)} stretch(es) where game fish are closed by species closures (no full "
            f"closure), DFO has salmon open or release, and a B.C. bait ban / fly rule holds — the salmon bar draws no "
            f"gear mark on those days ({sum(r['sections'] for r in found)} sections). First {min(n, len(found))}:")
    return "\n".join([head, *(f"  {line(r)}" for r in found[:n])])


def write(found: Sequence[Mapping], out_dir: Path) -> Tuple[Path, Path]:
    """The whole list: `<out_dir>/salmon_gear_hidden.json` (rows) and `.txt` (one line each)."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    js, txt = out_dir / f"{REPORT}.json", out_dir / f"{REPORT}.txt"
    js.write_text(json.dumps({"ticket": "T936", "count": len(found), "rows": list(found)}, indent=1,
                             ensure_ascii=False) + "\n", encoding="utf-8")
    txt.write_text(say(found, len(found)) + "\n", encoding="utf-8")
    return js, txt


def bundle_parts(b: sqlite3.Connection) -> Dict[int, Tuple[int, int]]:
    """sid -> (item ord, part_ix) from the bundle's `part_section`."""
    return {sid: (o, p) for o, sid, p in b.execute("SELECT ord, sid, part_ix FROM part_section")}


def main(argv=None) -> int:
    from pipeline.common.curated import GENERATED
    from pipeline.deliver import salmon as SALMON
    from pipeline.deliver.store.release import province_store
    from pipeline.deliver.store.release import release_dir as _release_dir
    ap = argparse.ArgumentParser(prog="pipeline.deliver.release.salmon_gear", description=__doc__.split("\n\n")[0])
    ap.add_argument("--release", type=Path, default=None, help="a release folder (default: the one LATEST names)")
    ap.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite",
                    help="the bundle the release was cut from (its part_section names the parts)")
    ap.add_argument("--out", type=Path, default=None, help="also write the whole list into this folder")
    ap.add_argument("--rows", type=int, default=FIRST_ROWS)
    a = ap.parse_args(argv)
    rel = _release_dir(a.release)
    st = province_store(rel, verify=False)
    b = sqlite3.connect(f"file:{a.bundle}?mode=ro", uri=True)
    try:
        bh = dict(b.execute("SELECT k, v FROM meta")).get("section_handles")
        if bh != st.meta.get("section_handles"):
            raise SystemExit(f"salmon_gear: {a.bundle} is not {rel.name}'s bundle (section_handles differ)")
        files = {}
        man = json.loads((rel / "manifest.json").read_text(encoding="utf-8"))["files"]
        for k, f in man.items():
            if k == "salmon" or k.startswith("salmon-"):
                blob = (rel.parent / f["url"]).read_bytes()
                files[k] = gzip.decompress(blob) if f["url"].endswith(".gz") else blob
        _h, payload, secs = SALMON.decode_files(files)
        found = rows(st, payload, secs, lambda: bundle_parts(b))
    finally:
        b.close()
    print(say(found, a.rows))
    if a.out:
        for p in write(found, a.out):
            print(f"wrote {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

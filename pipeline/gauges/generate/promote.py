"""Promote generated matches into the curated store, with a trust level on every row.

    python -m pipeline.gauges.generate.promote --dry-run              # what would change, every demotion
    python -m pipeline.gauges.generate.promote --only 08KG001 08MH166 # splice just these stations in
    python -m pipeline.gauges.generate.promote                        # all of it; refused if anything demotes
    python -m pipeline.gauges.generate.promote --accept-demotions     # ...once a person has read the list

It reads the matcher's RAW output (`--matches`, default `GENERATED.gauges.match`) and writes the curated
`matches.json` + `candidates.json`. That curated pair is also the BASELINE: what is in it now is what a
promote can demote.

A FULL PROMOTE IS NOT A NO-OP (T94). The size signal reads node magnitudes from the CURRENT bundle and
matched lines move between builds: on 2026-10-10 a full match + promote demoted 25 stations and rewrote
1,042 rows when the owner wanted two reviewed stations added. So:

    --only S ...    changes exactly those stations' rows (promoted or queued); every other row of
                    both files is left as it was, byte for byte.
    a full promote  lists each demotion (station, trust before -> after) and REFUSES (exit 2,
                    nothing written) unless `--accept-demotions`. `--only` obeys the same rule for
                    the stations it names.

WHAT PROMOTION IS. The matcher produces guesses. Most are obviously right, a few are
obviously wrong, and a person has looked at the ones in between. Promotion is the step that
writes down WHICH, so that everything downstream — and every future regenerate — can tell
the three apart.

    bound       a person stated the water outright. A regenerate must never touch it.
    reviewed    a person checked the guess and agreed. A regenerate may FLAG a drift but
                must never silently replace it.
    auto        every independent check agreed and nobody looked. Free to replace.
    wrong       the guess is bad and the right water is not known yet. A worklist.
    none        genuinely no such water.

AUTO-PROMOTION NEEDS FOUR INDEPENDENT SIGNALS TO AGREE, and independence is the whole point
— every failure found in review came from signals that were secretly the same signal:

    name        the FWA name appears in the station name on a word boundary
    distance    within the radius the match itself accepted
    size        ECCC's surveyed DRAINAGE_AREA_GROSS agrees with the node's stream magnitude
                within one decade. THE ONLY SIGNAL ECCC SUPPLIES ITSELF — `wsc` is copied
                off the node being judged and cannot referee it
    kind        the Water Office's stated water-body type is consistent with lake vs channel

Any disagreement and the row stays a candidate: it is written to `data/generated/` with its
evidence, and appears in the review queue. It is never promoted on a majority vote.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path

from pipeline.common.curated import CURATED, GENERATED, SOURCE
from pipeline.gauges import review as _review
from pipeline.gauges.generate.match import (
    AREA_TOLERANCE_DECADES, KM2_PER_MAGNITUDE, _contains_word, waterbody_name,
)
from pipeline.gauges.generate.waterbody_type import load_types
from pipeline.gauges.matches import MATCH_FILE, read_match
from pipeline.deliver.tiles.names import normalise

#: Highest first. A promote may only ever RAISE a row's trust, never lower it.
ORDER = ("bound", "reviewed", "wrong", "none", "auto", "candidate")


def _area_fits(area: float | None, mag: int | None) -> bool | None:
    """True / False / None where either side has nothing to say."""
    if not area or not mag or mag <= 0:
        return None
    return abs(math.log10(area / (mag * KM2_PER_MAGNITUDE))) <= AREA_TOLERANCE_DECADES


def _kind_fits(stated: str | None, on_lake: bool) -> bool | None:
    """Does ECCC's stated water-body type agree with where we put the station?

    `None` for anything but the two words we understand. ECCC types a slough as "Lake",
    which is coarse rather than wrong, so an unknown value must not read as disagreement.
    """
    if stated == "Lake":
        return on_lake
    if stated == "River":
        return not on_lake
    return None


def evidence(m, station: dict, mag: int | None, stated: str | None) -> dict:
    """Every independent signal, and whether it agrees. `None` means "no opinion"."""
    want = waterbody_name(station.get("name", ""))
    got = normalise(m.name or "")
    return {
        "name_agrees": bool(got) and _contains_word(want, got),
        "name_is_exact": got == want,
        "distance_m": m.distance_m,
        "area_km2": station.get("area_km2"),
        "mag": mag,
        "area_agrees": _area_fits(station.get("area_km2"), mag),
        "stated_kind": stated,
        "kind_agrees": _kind_fits(stated, bool(m.wbk)),
    }


def promotable(ev: dict) -> bool:
    """Auto-promote only when nothing disagrees.

    `None` is not agreement and not disagreement — a station with no published drainage
    area cannot confirm or deny anything, and refusing those would leave most of the roster
    permanently in the queue. What blocks a promotion is an explicit False.
    """
    return ev["name_agrees"] and False not in (ev["area_agrees"], ev["kind_agrees"])


def build(matches, stations, types, mags, rev=None) -> tuple[list[dict], list[dict]]:
    """`(promoted, candidates)` — every station, sorted, with its trust and evidence."""
    rev = _review.load().stations if rev is None else rev
    by_id = {s["station"]: s for s in stations}
    promoted, candidates = [], []
    for m in sorted(matches, key=lambda m: m.station):
        d = rev.get(m.station)
        st = by_id.get(m.station, {})
        ev = evidence(m, st, mags.get(m.station), types.get(m.station))
        # EVERY FIELD OF THE MATCH SURVIVES. A promoted row is the StationMatch plus a
        # trust and its evidence — not a summary of it. Dropping `lon`/`lat` here would
        # take the coordinate away from `nodes_for` and the gauge cuts, which is the one
        # fact ECCC publishes and the only thing that survives a re-sectioning.
        row = {"station": m.station, "station_name": st.get("name", ""),
               "status": m.status, "resolved_by": m.resolved_by,
               "distance_m": m.distance_m, "reason": m.reason,
               "lon": m.lon, "lat": m.lat,
               "wbk": [m.wbk] if m.wbk else [], "wsc": [m.wsc] if m.wsc else [],
               "also": list(m.also), "name": m.name, "blk": m.blk,
               "evidence": ev}
        if d is not None and d.verdict in ("bound", "bind"):
            row |= {"trust": "bound", "wbk": list(d.wbk), "wsc": list(d.wsc), "note": d.note}
        elif d is not None and d.verdict == "confirmed":
            row |= {"trust": "reviewed", "note": d.note, "reviewed": d.reviewed}
        elif d is not None and d.verdict == "wrong":
            row |= {"trust": "wrong", "wbk": [], "wsc": [], "note": d.note}
        elif d is not None and d.verdict == "none":
            row |= {"trust": "none", "wbk": [], "wsc": [], "note": d.note}
        elif m.status != "matched":
            candidates.append(row | {"trust": "candidate", "why": m.reason or m.status})
            continue
        elif promotable(ev):
            row |= {"trust": "auto"}
        else:
            why = [k for k in ("name_agrees", "area_agrees", "kind_agrees") if ev[k] is False]
            candidates.append(row | {"trust": "candidate", "why": ", ".join(why) or "unmatched"})
            continue
        promoted.append(row)
    return promoted, candidates



# ------------------------------------------------------------------ what a promote writes (T94)

#: Below every trust: the station is in neither file after the promote.
ABSENT = "absent"


def _rank(trust: str) -> int:
    """Position in ORDER (0 = highest); a station in neither file ranks below `candidate`."""
    return ORDER.index(trust) if trust in ORDER else len(ORDER)


def trust_after(station: str, promoted: dict, candidates: dict) -> str:
    """The trust a station holds in a `(promoted, candidates)` pair of station maps."""
    if station in promoted:
        return promoted[station]["trust"]
    return "candidate" if station in candidates else ABSENT


def demotions(before: dict, promoted: dict, candidates: dict) -> list[tuple[str, str, str]]:
    """`(station, was, now)` for every row of the baseline PROMOTED map that would end up lower.

    THE ONE DEFINITION of a demotion; the full promote and `--only` both ask it. A row of the
    baseline's review queue is not in it — a candidate has no trust to lose.
    """
    out = []
    for sid in sorted(before):
        was, now = before[sid].get("trust", "candidate"), trust_after(sid, promoted, candidates)
        if _rank(now) > _rank(was):
            out.append((sid, was, now))
    return out


def merge(before: dict, before_cand: dict, promoted: list[dict], candidates: list[dict],
          only: list[str] | None = None) -> tuple[dict, dict]:
    """The `(promoted, candidates)` station maps to write, sorted by station.

    Full (`only` None): the fresh result, whole. `--only`: the baseline, with exactly the named
    stations replaced by their fresh row in whichever file it now belongs; every other row is the
    baseline's own. A named station the fresh match does not carry is an error, never a skip.
    """
    fresh_p = {r["station"]: r for r in promoted}
    fresh_c = {c["station"]: c for c in candidates}
    if only is None:
        return dict(sorted(fresh_p.items())), dict(sorted(fresh_c.items()))
    missing = [s for s in only if s not in fresh_p and s not in fresh_c]
    if missing:
        raise SystemExit(f"--only: not in the fresh match: {', '.join(missing)}")
    new_p, new_c = dict(before), dict(before_cand)
    for sid in only:
        new_p.pop(sid, None)
        new_c.pop(sid, None)
        if sid in fresh_p:
            new_p[sid] = fresh_p[sid]
        else:
            new_c[sid] = fresh_c[sid]
    return dict(sorted(new_p.items())), dict(sorted(new_c.items()))


def _stations(path: Path) -> dict | None:
    """A promoted-shape file's `stations` map. None when the file is absent, or is a bare match
    LIST — what the matcher used to write over the curated file; that baseline has no trust left."""
    if not path.exists():
        return None
    blob = json.loads(path.read_text("utf-8")).get("stations")
    return blob if isinstance(blob, dict) else None


MATCHES_ABOUT = (
    "Gauge matches, PROMOTED. Every row carries a `trust`: bound (a person "
    "stated the water), reviewed (a person checked the guess), auto (four "
    "independent checks agreed, nobody looked), wrong (bad, right answer not "
    "known yet — a worklist), none (no such water). Generated by "
    "`python -m pipeline.gauges.generate.promote`; the decisions it reads are "
    "in the curated review beside this file, which no program writes.")
CANDIDATES_ABOUT = (
    "Generated matches NOT promoted: at least one independent check "
    "disagreed. Evidence is attached so a reviewer can see which. "
    "Regenerable — never edit by hand; record decisions in the curated "
    "review instead.")


def dump(about: str, rows: dict) -> str:
    """The on-disk shape both files share: `indent=1`, ASCII escapes, a trailing newline. A
    committed file read and dumped back is byte-identical, which is what lets `--only` leave
    every row it was not asked about exactly as it was."""
    return json.dumps({"_about": about, "stations": rows}, indent=1) + "\n"


def load_inputs(matches_path: Path):
    """`(matches, stations, types, mags, review)` — everything `build` reads, from disk."""
    matches = read_match(matches_path)
    if not matches:
        raise SystemExit(f"no matches at {matches_path} — run pipeline.gauges.generate.match")
    stations = json.loads((SOURCE / "bc_hydrometric_stations.json").read_text("utf-8"))
    types = load_types()
    # Magnitudes come from the last bundle: the promote decides TRUST, not placement, and
    # re-deriving 2,097 node magnitudes would mean loading the atlas for a bookkeeping pass.
    mags: dict[str, int] = {}
    bundle = GENERATED.bundle / "bundle.sqlite"
    if bundle.exists():
        import sqlite3
        mags = {s: m for s, m in sqlite3.connect(bundle).execute(
            "SELECT station, mag FROM gauge WHERE mag IS NOT NULL")}
    return matches, stations, types, mags, _review.load().stations


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="report (demotions too), write nothing")
    ap.add_argument("--matches", type=Path, default=GENERATED.gauges.match,
                    help="the matcher's raw output to promote from")
    ap.add_argument("--out", type=Path, default=MATCH_FILE,
                    help="the promoted file (also the baseline)")
    ap.add_argument("--candidates", type=Path, default=CURATED.gauges.candidates,
                    help="the review queue (also the baseline)")
    ap.add_argument("--only", nargs="+", metavar="STATION",
                    help="change only these stations' rows; every other row is kept as it is")
    ap.add_argument("--accept-demotions", action="store_true",
                    help="write although rows would lose trust (once a person has read the list)")
    a = ap.parse_args(argv)

    matches, stations, types, mags, rev = load_inputs(a.matches)
    promoted, candidates = build(matches, stations, types, mags, rev)
    live = {s["station"] for s in stations if s.get("realtime")}
    counts = Counter(r["trust"] for r in promoted)
    print(f"{len(promoted):,} promoted, {len(candidates):,} left as candidates")
    for k in ORDER:
        if counts.get(k):
            hot = sum(1 for r in promoted if r["trust"] == k and r["station"] in live)
            print(f"  {counts[k]:5,}  {k:9s} ({hot} transmitting)")
    if candidates:
        why = Counter(c["why"] for c in candidates)
        print("  candidates, by what disagreed:")
        for w, n in why.most_common(6):
            print(f"    {n:5,}  {w}")

    before, before_cand = _stations(a.out), _stations(a.candidates)
    if before is None:
        if a.only:
            raise SystemExit(f"--only needs a promoted baseline at {a.out} and there is none (absent, "
                             f"or a bare match list): restore it with  git checkout -- {a.out}")
        if not a.accept_demotions:
            print(f"\nREFUSED, nothing written: {a.out} is absent or not a promoted file, so what this "
                  f"would demote is unknown. Restore it (git checkout -- {a.out}), or pass "
                  f"--accept-demotions for a first promote.")
            return 2
        before = {}
    if a.only and before_cand is None:
        raise SystemExit(f"--only needs the review queue at {a.candidates} and there is none")
    new_p, new_c = merge(before, before_cand or {}, promoted, candidates, a.only)

    changed = sorted(s for s in set(before) | set(new_p) if before.get(s) != new_p.get(s))
    head = ", ".join(changed[:8]) + (" ..." if len(changed) > 8 else "")
    print(f"\n{len(changed):,} promoted row(s) would change" + (f": {head}" if changed else ""))
    down = demotions(before, new_p, new_c)
    if down:
        print(f"{len(down)} DEMOTION(S), rows that would lose trust:")
        for sid, was, now in down:
            print(f"  {sid}  {was} -> {now}")
    if a.dry_run:
        print("\n--dry-run: nothing written")
        return 0
    if down and not a.accept_demotions:
        print("\nREFUSED, nothing written. Splice in what you meant with --only <station ...>, or rerun "
              "with --accept-demotions once a person has reviewed every line above.")
        return 2

    a.candidates.write_text(dump(CANDIDATES_ABOUT, new_c), "utf-8")
    a.out.write_text(dump(MATCHES_ABOUT, new_p), "utf-8")
    print(f"\nwrote {a.out}")
    print(f"      {a.candidates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

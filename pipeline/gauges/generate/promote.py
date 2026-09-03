"""Promote generated matches into the curated store, with a trust level on every row.

    python -m pipeline.gauges.generate.promote --dry-run
    python -m pipeline.gauges.generate.promote

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

from pipeline.common.curated import GENERATED, SOURCE, generated
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


def build(matches, stations, types, mags) -> tuple[list[dict], list[dict]]:
    """`(promoted, candidates)` — every station, sorted, with its trust and evidence."""
    rev = _review.load().stations
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    ap.add_argument("--out", type=Path, default=MATCH_FILE)
    a = ap.parse_args()

    matches = read_match()
    if not matches:
        raise SystemExit(f"no matches at {MATCH_FILE} — run pipeline.gauges.generate.match")
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

    promoted, candidates = build(matches, stations, types, mags)
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
    if a.dry_run:
        print("\n--dry-run: nothing written")
        return 0

    cand_path = generated("gauges", "candidates.json")
    cand_path.write_text(json.dumps({
        "_about": "Generated matches NOT promoted: at least one independent check "
                  "disagreed. Evidence is attached so a reviewer can see which. "
                  "Regenerable — never edit by hand; record decisions in the curated "
                  "review instead.",
        "stations": {c["station"]: c for c in candidates}}, indent=1) + "\n", "utf-8")

    a.out.write_text(json.dumps({
        "_about": "Gauge matches, PROMOTED. Every row carries a `trust`: bound (a person "
                  "stated the water), reviewed (a person checked the guess), auto (four "
                  "independent checks agreed, nobody looked), wrong (bad, right answer not "
                  "known yet — a worklist), none (no such water). Generated by "
                  "`python -m pipeline.gauges.generate.promote`; the decisions it reads are "
                  "in the curated review beside this file, which no program writes.",
        "stations": {r["station"]: r for r in promoted}}, indent=1) + "\n", "utf-8")
    print(f"\nwrote {a.out}")
    print(f"      {cand_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

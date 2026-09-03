"""Is any frozen match artifact out of date with the roster it was built from?

    python -m pipeline.tools.check_curated            # human report
    python -m pipeline.tools.check_curated --ci       # exit 1 if anything needs regenerating

THE PROBLEM THIS SOLVES. Three artifacts in this repo are GENERATED but COMMITTED: a slow,
expensive pass produces them, a human reviews them, and from then on everything downstream
just reads the file. `pipeline/gauge_match.json` is the built one; stocking and bathymetry
want the same shape.

That is the right design — matching a station to a river needs 2.8 GB of pickled geometry
and a completed build, and BC commissions a handful of gauges a year, so re-deriving it on
every build would be enormous cost for an answer that has not moved. But it has a failure
mode, and it is a quiet one: **the roster moves and the artifact does not**. A new station
appears in the fetched roster, nothing regenerates the match, and the station simply never
gets a gauge. Nothing errors. Nothing is empty. The river is just ungauged forever.

So this is the tripwire. It is CHEAP — it reads two JSON files and compares id sets, no
geometry, no graph — which is what lets it run on every CI job while the regeneration it
guards runs a few times a year on a machine that can hold the atlas.

WHAT IT DOES NOT DO. It never regenerates anything. Regeneration needs a completed build and
a human to review the diff, and a tool that silently rewrote a curated file when a roster
changed would be exactly the thing this repo keeps getting bitten by. It tells you the
command; you run it.

READ THE THREE VERDICTS AS THREE DIFFERENT FACTS:

    fresh       every id in the roster has a decision in the artifact.
    STALE       the roster has ids the artifact has never seen. Regenerate.
    dropped     the artifact has ids the roster no longer lists. NOT an error on its own —
                ECCC does decommission stations — but it is how a curated decision quietly
                stops applying to anything, so it is reported rather than ignored.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

from pipeline.curated import ROOT, load

#: The reviewed artifacts come from `config.yaml`, so there is ONE place naming them and a
#: typo fails at load with the key rather than here with a confusing "not built". The
#: ROSTERS are fetched data, not curated, so they stay literal — losing one costs a refetch.
_C = load()


@dataclass(frozen=True)
class Artifact:
    """One generated-and-committed artifact, and the roster that decides its freshness."""

    name: str
    roster: Path            # the fetched list of things that need a decision
    frozen: Path            # the committed answers
    regenerate: str         # the command a human runs
    roster_ids: str         # see `ids` for the three spellings
    frozen_ids: str

    def ids(self, path: Path, spec: str) -> set[str] | None:
        """Ids out of a file, or None if the file is not there at all.

        THREE SPELLINGS, no inference. An earlier version guessed the shape from the JSON
        and counted the `_about` key of every artifact as a station id, which made a
        perfectly fresh file report two phantom drops. The shape is a property of the file
        and belongs in the declaration, not in a heuristic:

            "station"             a top-level LIST of objects; take this field
            "stations.station"    `blob["stations"]` is a LIST; take this field
            "stations.*"          `blob["stations"]` is a MAP; the ids are its keys
        """
        if not path.exists():
            return None
        blob = json.loads(path.read_text(encoding="utf-8"))
        container, _, field = spec.partition(".")
        rows = blob if not field else blob.get(container)
        if rows is None:
            raise KeyError(f"{path.name} has no {container!r} — check the Artifact spec")
        if field == "*":
            if not isinstance(rows, dict):
                raise TypeError(f"{path.name}: {container!r} is not a map")
            return set(rows)
        key = field or container
        if not isinstance(rows, list):
            raise TypeError(f"{path.name}: expected a list for {spec!r}")
        return {r[key] for r in rows if isinstance(r, dict) and key in r}


#: Every artifact that is generated once, reviewed by a human, and then only read.
#:
#: `gauge_match.json` is live. The other two are DECLARED BUT NOT BUILT — they are named here
#: on purpose, so the day somebody wires the FIDQ or bathymetry fetch the freshness gate is
#: already waiting for them rather than being remembered afterwards. A missing file reports
#: as `not built` and never fails CI.
ARTIFACTS = [
    Artifact(
        name="gauge match",
        roster=ROOT / "data" / "bc_hydrometric_stations.json",
        frozen=_C.matches.gauge or ROOT / "pipeline" / "gauge_match.json",
        regenerate="python -m pipeline.hydro.match --build output/v2/full",
        roster_ids="station",
        frozen_ids="stations.station",
    ),
    Artifact(
        name="gauge water-body type",
        roster=ROOT / "data" / "bc_hydrometric_stations.json",
        frozen=(_C.matches.waterbody_type
                or ROOT / "data" / "bc_station_waterbody_type.json"),
        regenerate="python -m pipeline.hydro.waterbody_type",
        roster_ids="station",
        frozen_ids="stations.*",
    ),
    Artifact(
        name="stocking waterbody match",
        roster=ROOT / "data" / "bc_stocked_waterbodies.json",
        frozen=_C.matches.stocking or ROOT / "pipeline" / "stock_match.json",
        regenerate="python -m pipeline.stocking.match --build output/v2/full  (not wired yet)",
        roster_ids="waterbody_id",
        frozen_ids="waterbodies.waterbody_id",
    ),
    Artifact(
        name="bathymetry sheet match",
        roster=ROOT / "data" / "bc_bathymetry_sheets.json",
        frozen=_C.matches.charts or ROOT / "pipeline" / "chart_match.json",
        regenerate="python -m pipeline.stocking.identifiers --build output/v2/full"
                   "  (not wired yet)",
        roster_ids="waterbody_identifier",
        frozen_ids="sheets.waterbody_identifier",
    ),
]


def _short(p: Path) -> str:
    """Repo-relative when it can be — a test's tmp_path is not, and must not crash."""
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def check(a: Artifact) -> tuple[str, str]:
    """`(verdict, detail)` for one artifact. Verdict is fresh | STALE | absent."""
    roster = a.ids(a.roster, a.roster_ids)
    if roster is None:
        return "absent", f"no roster at {_short(a.roster)} — nothing to check yet"
    frozen = a.ids(a.frozen, a.frozen_ids)
    if frozen is None:
        return "absent", (f"not built: {_short(a.frozen)} does not exist "
                          f"({len(roster):,} in the roster)")

    new, gone = sorted(roster - frozen), sorted(frozen - roster)
    if not new:
        tail = f"; {len(gone)} dropped from the roster" if gone else ""
        return "fresh", f"{len(frozen & roster):,} of {len(roster):,} decided{tail}"
    show = ", ".join(new[:8]) + (f" … +{len(new) - 8}" if len(new) > 8 else "")
    return "STALE", (f"{len(new)} in the roster with no decision: {show}"
                     + (f"; {len(gone)} dropped" if gone else ""))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ci", action="store_true",
                    help="exit 1 if any artifact is stale (absent is never a failure)")
    a = ap.parse_args()

    stale = []
    for art in ARTIFACTS:
        verdict, detail = check(art)
        mark = {"fresh": "  ok  ", "STALE": " STALE", "absent": "  --  "}[verdict]
        print(f"{mark}  {art.name:26s} {detail}")
        if verdict == "STALE":
            stale.append(art)

    if stale:
        print("\nRegenerate, review the diff, and commit:")
        for art in stale:
            print(f"  {art.regenerate}")
        print("\nThese need a completed build and are not CI's job — that is the point of "
              "committing the answers.")
    return 1 if (stale and a.ci) else 0


if __name__ == "__main__":
    sys.exit(main())

"""Is any frozen match artifact out of date with the roster it was built from? Are the hand-edited waters files sound?

    python -m pipeline.tools.audit.check_curated            # human report
    python -m pipeline.tools.audit.check_curated --ci       # exit 1 if anything needs regenerating
    python -m pipeline.tools.audit.check_curated --waters   # (T324) the FAST CHECK of the hand-edited waters files:
                                                            #   schema + references, seconds; exit 1 on any error

THE WATERS CHECK (T324, owner 2026-11-05: "keep hand edits; add a fast check"). `name_variants`, `area_closures`,
`added_areas` / `added_lakes` / `added_streams`, `continuations` and `unmapped_waters` are edited by hand and rarely,
and before this a bad edit surfaced only after the hour-long atlas build. `--waters` reads each file and checks its
SHAPE (the fields its loader needs, unique ids, coordinates inside BC) and its REFERENCES (an area def's `file` names
a declared curated file; an added stream's name variant names an added stream; an added lake's `part_of` lake is in
the atlas registry; a continuation is never listed twice, or both reviewed and refused). It is the build's FIRST step (`pipeline.build.preflight`) and a CI step (pipeline-ci.yml).
Ids a name variant targets that the current registry does not hold are WARNINGS, not errors: a new variant reaches
the registry only with the next atlas, so "not yet built" and "a typo" look alike until then — the list is printed.
With no atlas registry on the machine (CI) the registry checks are skipped and the output says so.

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
from typing import Callable, Mapping

from pipeline.common.curated import GENERATED, ROOT, SOURCE, load
from pipeline.gauges.liveness import may_speak

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
    regenerate: str         # the command a human runs; `{stations}` is filled by `fix_command`
    roster_ids: str         # see `ids` for the three spellings
    frozen_ids: str
    #: A generated file whose ids also count as "known". A candidate is a decision IN
    #: PROGRESS — the matcher produced it and something disagreed, so it is in the review
    #: queue rather than promoted. Counting those as undecided would leave the gate red
    #: forever and teach everyone to ignore it; only a station in NEITHER file is stale.
    candidates: Path | None = None
    candidate_ids: str = "stations.*"
    #: Only roster rows this predicate holds for NEED a decision (user, 2026-10-09: an inactive
    #: gauge publishes no reading, so an unmatched one leaves no water without its gauge). The
    #: rest are reported, never STALE. For the gauges it is `liveness.may_speak` (T122 review:
    #: this read the `active` field through its name, a reader the liveness pin could not see).
    roster_where: Callable[[Mapping], bool] | None = None
    #: What the excused rows are not, for the report ("none active").
    roster_where_word: str = ""

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
        if path is None or not path.exists():          # None: not declared in config.yaml yet (T435: no made-up path)
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
        roster=SOURCE / "bc_hydrometric_stations.json",
        frozen=_C.gauges.matches,
        # SURGICAL (T94): match into the generated file, then promote ONLY the stations the gate
        # found. A full promote rewrote 1,042 rows and demoted 25 on 2026-10-10.
        regenerate=("python -m pipeline.gauges.generate.match --build data/generated/atlas/full"
                    " && python -m pipeline.gauges.generate.promote --only {stations}"),
        roster_ids="station",
        frozen_ids="stations.*",
        candidates=GENERATED.gauges.candidates,
        roster_where=may_speak,
        roster_where_word="active",
    ),
    Artifact(
        name="gauge water-body type",
        roster=SOURCE / "bc_hydrometric_stations.json",
        # T331: scraped from ECCC, so SOURCE data (`waterbody_type.TYPES_FILE`), still frozen against the roster
        frozen=SOURCE / "bc_station_waterbody_type.json",
        regenerate="python -m pipeline.gauges.generate.waterbody_type",
        roster_ids="station",
        frozen_ids="stations.*",
    ),
    Artifact(
        name="stocking waterbody match",
        roster=SOURCE / "bc_stocked_waterbodies.json",
        frozen=_C.stocking.matches,
        regenerate="python -m pipeline.stocking.generate.match  (not wired yet)",
        roster_ids="waterbody_id",
        frozen_ids="waterbodies.waterbody_id",
    ),
    Artifact(
        name="bathymetry sheet match",
        roster=SOURCE / "bc_bathymetry_sheets.json",
        frozen=_C.bathymetry.matches,
        regenerate="no matcher yet: write pipeline/bathymetry/generate/match.py (pipeline/bathymetry/README.md)",
        roster_ids="waterbody_identifier",
        frozen_ids="sheets.waterbody_identifier",
    ),
]


def _short(p: Path) -> str:
    """Repo-relative when it can be — a test's tmp_path is not, and must not crash."""
    if p is None:
        return "(not declared in config.yaml)"
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def undecided(a: Artifact, roster: set[str], frozen: set[str]) -> tuple[list[str], list[str], set[str]]:
    """`(new, idle, queued)`: roster ids needing a decision and in neither file (the STALE ones),
    ids in neither file that `roster_where` excuses, and the review queue's ids."""
    queued = (a.ids(a.candidates, a.candidate_ids) if a.candidates else None) or set()
    new = sorted(roster - frozen - queued)
    idle: list[str] = []
    if a.roster_where:
        rows = json.loads(a.roster.read_text(encoding="utf-8"))
        need = {r[a.roster_ids] for r in rows if a.roster_where(r)}
        idle = [s for s in new if s not in need]
        new = [s for s in new if s in need]
    return new, idle, queued


def fix_command(a: Artifact) -> str:
    """The command to print for a STALE artifact, `{stations}` filled with exactly the stale ids —
    so the gauge fix promotes those stations and nothing else (T94)."""
    if "{stations}" not in a.regenerate:
        return a.regenerate
    roster, frozen = a.ids(a.roster, a.roster_ids), a.ids(a.frozen, a.frozen_ids)
    new = undecided(a, roster or set(), frozen or set())[0]
    return a.regenerate.replace("{stations}", " ".join(new) or "<station ...>")


def check(a: Artifact) -> tuple[str, str]:
    """`(verdict, detail)` for one artifact. Verdict is fresh | STALE | absent."""
    roster = a.ids(a.roster, a.roster_ids)
    if roster is None:
        return "absent", f"no roster at {_short(a.roster)} — nothing to check yet"
    frozen = a.ids(a.frozen, a.frozen_ids)
    if frozen is None:
        return "absent", (f"not built: {_short(a.frozen)} does not exist "
                          f"({len(roster):,} in the roster)")

    new, idle, queued = undecided(a, roster, frozen)
    gone = sorted(frozen - roster)
    if not new:
        bits = []
        if idle: bits.append(f"{len(idle)} without a decision, none {a.roster_where_word}")
        if queued: bits.append(f"{len(queued & roster)} in the review queue")
        if gone: bits.append(f"{len(gone)} dropped from the roster")
        tail = ("; " + "; ".join(bits)) if bits else ""
        return "fresh", f"{len(frozen & roster):,} of {len(roster):,} promoted{tail}"
    show = ", ".join(new[:8]) + (f" … +{len(new) - 8}" if len(new) > 8 else "")
    return "STALE", (f"{len(new)} in the roster with no decision: {show}"
                     + (f"; {len(gone)} dropped" if gone else ""))


# ======================================================================================
# --waters: the fast check of the hand-edited waters files (T324)
# ======================================================================================

#: BC's bounding box with a margin (lon, lat): a coordinate outside it is a swapped or mistyped point
BC_BOX = (-140.0, 47.5, -113.0, 60.5)
ITEM_PREFIXES = ("wbk:", "gnis:", "blk:", "wsc:")


def _in_bc(lon, lat) -> bool:
    return (isinstance(lon, (int, float)) and isinstance(lat, (int, float))
            and BC_BOX[0] <= lon <= BC_BOX[2] and BC_BOX[1] <= lat <= BC_BOX[3])


def _ring_points(geom: Mapping):
    t, c = geom.get("type"), geom.get("coordinates")
    if t == "Polygon":
        return [pt for ring in c for pt in ring]
    if t == "MultiPolygon":
        return [pt for poly in c for ring in poly for pt in ring]
    return None


def _registry_refs(registry: Path | None):
    """(item ids, ref ids) of the atlas registry, or None when there is none (CI)."""
    if registry is None or not registry.is_file():
        return None
    items = json.loads(registry.read_text(encoding="utf-8"))["items"]
    return {it["id"] for it in items}, {r for it in items for r in it.get("ref_ids") or ()}


def check_waters(waters=None, registry: Path | None = None) -> tuple[list[str], list[str], list[str]]:
    """`(errors, warnings, notes)` for the hand-edited waters files. `waters` is the `CURATED.waters` model (tests pass
    a stand-in); `registry` the atlas registry.json (None or missing: the registry checks are skipped, said in notes)."""
    w = waters if waters is not None else _C.waters
    err: list[str] = []
    warn: list[str] = []
    notes: list[str] = []

    def load(name: str):
        p = Path(getattr(w, name))
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except FileNotFoundError:
            err.append(f"{name}: {_short(p)} is missing")
        except json.JSONDecodeError as e:
            err.append(f"{name}: {_short(p)} is not valid JSON ({e})")
        return None

    reg = _registry_refs(registry)
    if reg is None:
        notes.append(f"registry checks skipped: no atlas registry at {_short(registry) if registry else '(none)'}")
    ref_ids = reg[1] if reg else set()

    # name_variants: a bare list of {target: {wbks|gnis_ids|wscs|blks…}, names: [{name, source…}]}
    nv = load("name_variants")
    if nv is not None:
        if not isinstance(nv, list):
            err.append("name_variants: the file must be a JSON list")
            nv = []
        unbuilt = []
        for i, r in enumerate(nv):
            where = f"name_variants[{i}]"
            tgt, names = (r or {}).get("target"), (r or {}).get("names")
            refs = []
            if not isinstance(tgt, dict):
                err.append(f"{where}: no `target` object")
            else:
                for k, prefix in (("wbks", "wbk"), ("wbk", "wbk"), ("gnis_ids", "gnis"), ("gnis_id", "gnis"),
                                  ("wscs", "wsc"), ("wsc", "wsc"), ("blks", "blk"), ("blk", "blk")):
                    v = tgt.get(k)
                    if v is None:
                        continue
                    for x in (v if isinstance(v, list) else [v]):
                        if not isinstance(x, (str, int)) or not str(x).strip():
                            err.append(f"{where}: target {k} holds {x!r}")
                        else:
                            refs.append(f"{prefix}:{x}")
                if not refs:
                    err.append(f"{where}: `target` names no water (wbks / gnis_ids / wscs / blks)")
            if not isinstance(names, list) or not names:
                err.append(f"{where}: `names` must be a non-empty list")
            else:
                for j, n in enumerate(names):
                    if not isinstance(n, dict) or not str(n.get("name") or "").strip():
                        err.append(f"{where}.names[{j}]: no `name`")
            if reg is not None and refs and not any(x in ref_ids for x in refs):
                unbuilt.append(f"{where} {refs[0]} ({(names or [{}])[0].get('name') if names else '?'})")
        if unbuilt:
            warn.append(f"name_variants: {len(unbuilt)} target(s) not in the current atlas registry (new since the "
                        "last atlas, or a typo): " + "; ".join(unbuilt[:12]) + (" …" if len(unbuilt) > 12 else ""))

    # area_closures: {areas: [{id, cut, tile_layer, layer | file …}]}
    ac = load("areas")
    if ac is not None:
        areas = ac.get("areas") if isinstance(ac, dict) else None
        if not isinstance(areas, list) or not areas:
            err.append("area_closures: no `areas` list")
            areas = []
        seen: set = set()
        for i, a in enumerate(areas):
            aid = (a or {}).get("id")
            where = f"area_closures[{aid or i}]"
            if not aid:
                err.append(f"area_closures[{i}]: no `id`")
            elif aid in seen:
                err.append(f"{where}: duplicate id")
            seen.add(aid)
            if bool(a.get("layer")) == bool(a.get("file")):
                err.append(f"{where}: needs exactly one of `layer` (a fetched gpkg layer) or `file` (a curated file)")
            if a.get("file") and getattr(w, a["file"], None) is None and not Path(a["file"]).is_file():
                err.append(f"{where}: `file` {a['file']!r} is no declared curated file (CURATED.waters) and no path")

    # added_areas / added_lakes: GeoJSON polygons with unique ids, inside BC
    for name in ("added_areas", "added_lakes"):
        g = load(name)
        if g is None:
            continue
        feats = g.get("features") if isinstance(g, dict) and g.get("type") == "FeatureCollection" else None
        if not isinstance(feats, list):
            err.append(f"{name}: not a GeoJSON FeatureCollection")
            continue
        seen = set()
        for i, f in enumerate(feats):
            props = (f or {}).get("properties") or {}
            fid = props.get("id")
            where = f"{name}[{fid if fid is not None else i}]"
            if fid is None:
                err.append(f"{name}[{i}]: no `properties.id`")
            elif fid in seen:
                err.append(f"{where}: duplicate id")
            seen.add(fid)
            if not str(props.get("name") or "").strip():
                err.append(f"{where}: no `name`")
            pts = _ring_points((f or {}).get("geometry") or {})
            if pts is None:
                err.append(f"{where}: geometry must be a Polygon or MultiPolygon")
            elif not pts or not all(_in_bc(*pt[:2]) for pt in pts):
                err.append(f"{where}: a vertex lies outside BC (lon/lat swapped or mistyped?)")
            if name == "added_lakes" and isinstance(props.get("part_of"), dict):
                wbk = props["part_of"].get("wbk")
                if not wbk:
                    err.append(f"{where}: `part_of` names no wbk")
                elif reg is not None and f"wbk:{wbk}" not in ref_ids:
                    err.append(f"{where}: `part_of` wbk {wbk} is in no item of the current atlas registry")

    # added_streams: unique negative blks; its own name variants name one of them (or an FWA blk)
    st = load("added_streams")
    if st is not None:
        streams = st.get("streams") if isinstance(st, dict) else None
        if not isinstance(streams, list) or not streams:
            err.append("added_streams: no `streams` list")
            streams = []
        blks: set[str] = set()
        for i, r in enumerate(streams):
            b = str((r or {}).get("blk") or "")
            where = f"added_streams.streams[{b or i}]"
            if not b.lstrip("-").isdigit():
                err.append(f"{where}: `blk` must be an integer id (minted streams are negative)")
            blks.add(b)
            for f in ("wsc", "segments"):
                if not r.get(f):
                    err.append(f"{where}: no `{f}`")
        for i, v in enumerate(st.get("name_variants") or []):
            tb = str((v or {}).get("target_blk") or "")
            if not str(v.get("name") or "").strip():
                err.append(f"added_streams.name_variants[{i}]: no `name`")
            if tb.startswith("-") and tb not in blks:
                err.append(f"added_streams.name_variants[{i}]: target_blk {tb} is no added stream")

    # continuations: {reviewed: [{from, to}], refused: [{from, to}]} over registry item ids
    co = load("continuations")
    if co is not None:
        pairs: dict[tuple, str] = {}
        for bucket in ("reviewed", "refused"):
            rows = co.get(bucket) if isinstance(co, dict) else None
            if not isinstance(rows, list):
                err.append(f"continuations: no `{bucket}` list")
                continue
            for i, r in enumerate(rows):
                a, b = (r or {}).get("from"), (r or {}).get("to")
                where = f"continuations.{bucket}[{i}]"
                for end in (a, b):
                    # shape only: an unnamed stream is an item of the BUNDLE, not of the registry, and a reviewed link
                    # the topology no longer proposes is already listed `stale` by `bundle.continuation.gate`
                    if not isinstance(end, str) or not end.startswith(ITEM_PREFIXES):
                        err.append(f"{where}: {end!r} is no item id (wbk: / gnis: / blk: / wsc:)")
                if a == b:
                    err.append(f"{where}: a water continues as itself")
                if (a, b) in pairs:
                    err.append(f"{where}: {a} -> {b} is already in `{pairs[(a, b)]}`")
                pairs[(a, b)] = bucket

    # unmapped_waters: {ungazetted: [{id: "ungaz:…", name, region, location: [lon, lat]}]}
    uw = load("unmapped_waters")
    if uw is not None:
        rows = uw.get("ungazetted") if isinstance(uw, dict) else None
        if not isinstance(rows, list):
            err.append("unmapped_waters: no `ungazetted` list")
            rows = []
        seen = set()
        for i, r in enumerate(rows):
            uid = (r or {}).get("id")
            where = f"unmapped_waters[{uid or i}]"
            if not isinstance(uid, str) or not uid.startswith("ungaz:"):
                err.append(f"{where}: `id` must start with 'ungaz:'")
            elif uid in seen:
                err.append(f"{where}: duplicate id")
            seen.add(uid)
            if not str(r.get("name") or "").strip() or not r.get("region"):
                err.append(f"{where}: needs `name` and `region`")
            loc = r.get("location")
            if not (isinstance(loc, list) and len(loc) == 2 and _in_bc(*loc)):
                err.append(f"{where}: `location` must be [lon, lat] inside BC")
    return err, warn, notes


def waters_main(registry: Path | None) -> int:
    import time
    t = time.time()
    err, warn, notes = check_waters(registry=registry)
    for n in notes:
        print(f"  --    {n}")
    for x in warn:
        print(f"  warn  {x}")
    for x in err:
        print(f" ERROR  {x}")
    print(f"waters check: {len(err)} error(s), {len(warn)} warning(s) in {time.time() - t:.1f}s "
          "(name_variants, area_closures, added_areas, added_lakes, added_streams, continuations, unmapped_waters)")
    return 1 if err else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ci", action="store_true",
                    help="exit 1 if any artifact is stale (absent is never a failure)")
    ap.add_argument("--waters", action="store_true",
                    help="(T324) the fast check of the hand-edited waters files instead; exit 1 on any error")
    ap.add_argument("--registry", type=Path, default=None,
                    help="with --waters: the atlas registry.json for the reference checks (default: the promoted "
                         "atlas's, when there is one)")
    a = ap.parse_args()
    if a.waters:
        reg = a.registry
        if reg is None:
            from pipeline.common.curated import GENERATED
            reg = GENERATED.build() / "registry.json"
        return waters_main(reg)

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
            print(f"  {fix_command(art)}")
        print("\nThese need a completed build and are not CI's job — that is the point of "
              "committing the answers.")
    return 1 if (stale and a.ci) else 0


if __name__ == "__main__":
    sys.exit(main())

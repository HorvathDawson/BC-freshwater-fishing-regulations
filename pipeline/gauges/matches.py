"""The frozen gauge match: the record, and reading and writing it.

WHY THIS IS NOT IN `generate/`. The build and the bundler READ this file; only the matcher
writes it. If the reader lived beside the matcher, every consumer would import the module
that needs 2.8 GB of pickled geometry and an STRtree over two million sections just to
learn where a station is — and "the build must not re-run matching" would be a convention
rather than a fact about the import graph.

So: the record and its IO live here, `generate/match.py` produces it, and
`consume/` reads it and cannot reach the matcher at all.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from pathlib import Path

from pipeline.common.curated import CURATED

@dataclass(frozen=True)
class StationMatch:
    """One station's outcome, addressed by keys the FWA owns rather than by a node id.

    NODE IDS ARE BUILD OUTPUT. `{blk}:{down_m}` is assigned by the sectionizer, so it moves
    the moment a river is cut differently — which the gauge splits do, deliberately, on the
    very next build. A match frozen as node ids is therefore stale by construction: it names
    sections the graph it is being read against does not have.

    THE ADDRESS IS A PLACE AND A WATER, and nothing derived.

        streams   the station's published coordinate + `wsc`, the FWA watershed code
        lakes     the station's published coordinate + `wbk`, the FWA waterbody key

    A route measure would work too — FWA measures survive re-sectioning — but it is a number
    WE compute from the coordinate, and freezing a derived value means every consumer
    inherits whatever we believed about projection on the day the file was written. The
    coordinate is the primary fact: ECCC publishes it, it does not move, and each consumer
    projects it itself against the graph in front of it.

        the build   a `gauge` point anchor at the coordinate, scoped by `wsc`
        the bundle  the nearest section on that water, taking the UPSTREAM one where the
                    gauge sits on a boundary — that is the water it has just measured

    One frozen fact, no cached derivative that can disagree with it.
    """
    station: str
    status: str                  # matched | no_match | unresolved
    resolved_by: str | None      # name+radius | alias | override
    distance_m: float | None
    reason: str | None = None
    # --- the address (empty when unmatched) ---
    lon: float | None = None     # the station's own published position
    lat: float | None = None
    wsc: str = ""                # FWA watershed code: the river and its side channels
    wbk: str = ""                # FWA waterbody key, when the station is on a lake
    # --- review aids, never an input to anything ---
    name: str = ""               # the water the atlas calls this, at the time of matching
    #: The name the matcher actually matched on (normalised), when that is NOT the display name:
    #: a node displayed "Lower Arrow Lake" also answers to "Arrow Reservoir", which is what
    #: ECCC calls it. Empty when the display name matched. promote's name check reads it (T159).
    matched_name: str = ""
    blk: str = ""                # the blue line it matched. Diagnostic; see the note above.
    node_id: str | None = None   # the node it matched IN THAT BUILD. Diagnostic only.
    #: Additional FWA keys a curated `bind` named, beyond the one in `wsc`/`wbk`. A lake
    #: outlet describes both the lake and the stream leaving it. RECORDED, NOT YET CONSUMED
    #: — widening the match to several waters reaches split_defs, nodes_for and the shed
    #: walk, so the curation is captured now and acted on when those are ready.
    also: tuple[str, ...] = ()


#: THE ONE FROZEN FACT about where BC's gauges are, committed and reviewable.
#:
#: Beside `splits.json` and `added_streams.build.json`, and generated the same way: run it
#: against a completed build, commit the answer, and the NEXT build reads it. It has to be
#: two-pass because matching compares a station's name against every name a node carries,
#: and those include `name_variants.json`, which is applied during the build.
MATCH_FILE = CURATED.gauges.matches


#: How the committed `MATCH_FILE` is (re)made, for every error that names it (T94): the matcher
#: writes the RAW match to `GENERATED.gauges.match` and only `promote` writes the curated pair.
REGENERATE = ("python -m pipeline.gauges.generate.match --build <a completed build> && "
              "python -m pipeline.gauges.generate.promote --only <station ...>")


#: The two files only `promote` writes; the raw match may land anywhere else.
CURATED_FILES = (MATCH_FILE, CURATED.gauges.candidates)


def refuse_curated(path: Path) -> None:
    """Stop when `path` is one of the two curated gauge files (T94). THE one check, asked by the
    matcher before it loads a build and by `write_match` before it writes: the raw match written
    over `matches.json` drops every trust a person gave, and over `candidates.json` the queue."""
    path = Path(path)
    for curated in CURATED_FILES:
        same = path.resolve() == curated.resolve() or (
            path.exists() and curated.exists() and path.samefile(curated))
        if same:
            raise SystemExit(f"refusing to write the raw match over the curated {curated} — it would "
                             "drop every trust a person gave. Write it elsewhere and promote from "
                             "there (T94).")


def write_match(matches: list[StationMatch], path: Path) -> Path:
    """Write the RAW match (never a curated file: `refuse_curated`). Sorted by station, so a diff
    reads as a list of gauges. No default path: the old default was the curated file (T94)."""
    refuse_curated(path)
    path.write_text(json.dumps({
        "_about": "Where each hydrometric station sits: its own published coordinate plus "
                  "the FWA key for the water it is on — `wsc` for a stream, `wbk` for a "
                  "lake. NOT node ids, which are build output and move whenever a river is "
                  "re-sectioned, and NOT a route measure, which is a number we derive from "
                  "the coordinate rather than a fact anyone published. RAW, GENERATED by "
                  "`python -m pipeline.gauges.generate.match --build <a completed build>`; do not "
                  "hand-edit. Read by `python -m pipeline.gauges.generate.promote`, which writes "
                  "the curated matches.json the atlas and the bundle read (T94).",
        "stations": [asdict(m) for m in sorted(matches, key=lambda m: m.station)],
    }, indent=1) + "\n", encoding="utf-8")
    return path


def read_match(path: Path | None = None) -> list[StationMatch]:
    """The frozen match. A missing file is an ERROR (P2, no missing-file fallbacks): read as empty,
    it built a bundle with no gauge tables that looked healthy.

    TWO SHAPES, one meaning. A promoted file keys rows by station and carries `trust` and
    `evidence` alongside; the pre-promotion file was a bare list. Both parse to the same
    `StationMatch`, because everything downstream wants the ADDRESS — a coordinate and an
    FWA key — and does not care how confident anyone was about it.

    `wsc`/`wbk` arrive as lists in the promoted shape (a lake outlet legitimately describes
    both the lake and the stream below it). The first is the match; the rest go to `also`,
    which nothing consumes yet.
    """
    path = path or MATCH_FILE
    if not path.exists():
        committed = any(path.resolve() == c.resolve() for c in CURATED_FILES)
        raise FileNotFoundError(
            f"no gauge matches at {path} — "
            + (f"restore the committed file (git checkout -- {path}) or regenerate it: {REGENERATE}"
               if committed else
               "make it: python -m pipeline.gauges.generate.match --build <a completed build>"))
    blob = json.loads(path.read_text(encoding="utf-8")).get("stations", [])
    rows = list(blob.values()) if isinstance(blob, dict) else blob
    known = {f.name for f in fields(StationMatch)}
    out = []
    for r in rows:
        r = dict(r)
        extra = []
        for key in ("wsc", "wbk"):
            v = r.get(key)
            if isinstance(v, list):
                r[key] = v[0] if v else ""
                extra += v[1:]
        r["also"] = tuple(r.get("also") or ()) + tuple(extra)
        # A promoted row records a decision as `trust`; the reader keeps the status
        # vocabulary the rest of the pipeline speaks.
        trust = r.get("trust")
        if trust:
            r["status"] = {"wrong": "unresolved", "none": "no_match",
                           "candidate": "unresolved"}.get(trust, "matched")
        out.append(StationMatch(**{k: v for k, v in r.items() if k in known}))
    return out


# ---------------------------------------------------------------- the atlas's record (N8, audit F28)

#: THE MATCHES AN ATLAS CUT AT, written into its build directory: `{"matches_sha256", "cut",
#: "stamped_after_build"}`. The atlas cuts rivers at their gauges from MATCH_FILE; the bundle places
#: every station from MATCH_FILE again, later. Two reads, so two versions could meet — a river cut
#: at a gauge the bundle no longer puts there — and nothing noticed. The bundle compares this record
#: with the file it is about to read (`check_record`) and refuses a difference.
RECORD = "gauge_matches.json"


def matches_digest(path: Path | None = None) -> str:
    """sha256 of the matches file's bytes (16 hex), the identity the atlas records."""
    import hashlib
    return hashlib.sha256(Path(path or MATCH_FILE).read_bytes()).hexdigest()[:16]


#: The fields of a gauge cut that shape the graph: which boundary, where, scoped how. `label` and
#: `_name` are review text the sectionizer never reads.
_CUT_KEYS = ("id", "station", "anchor", "wsc", "proximity_m")


def cuts_digest_of(rows: list[dict]) -> str:
    """sha256 (16 hex) of the gauge CUT SET — `consume.cuts.split_defs` rows, the only thing the atlas
    takes from the matches file. A station that is not a cut (a lake gauge, one not transmitting, an
    unresolved row) can change without moving any boundary, so it must not demand an atlas rebuild."""
    import hashlib
    canon = sorted(({k: r.get(k) for k in _CUT_KEYS} for r in rows), key=lambda r: r["id"])
    return hashlib.sha256(json.dumps(canon, sort_keys=True).encode()).hexdigest()[:16]


def cuts_digest(path: Path | None = None, stations_path: Path | None = None) -> str:
    """`cuts_digest_of` the cuts the matches file at `path` makes, over the station roster the atlas
    reads (`SOURCE/bc_hydrometric_stations.json`, whose liveness picks which stations cut)."""
    from pipeline.common.curated import SOURCE
    from pipeline.gauges.consume.cuts import split_defs
    from pipeline.gauges.consume.shed import load_stations
    return cuts_digest_of(split_defs(read_match(path), load_stations(
        Path(stations_path or SOURCE / "bc_hydrometric_stations.json"))))


def write_record(build_dir: Path, *, cut: bool, sha: str | None, cuts_sha: str | None = None,
                 stamped_after_build: bool = False) -> Path:
    """Write RECORD into an atlas build directory. `cut` False (`--no-gauge-splits`): no gauge cut
    is in the graph, so no version of the file can disagree with it. `cuts_sha` is the cut set's
    digest (`cuts_digest_of`): a later matches file that changes no cut is the same graph."""
    p = Path(build_dir) / RECORD
    p.write_text(json.dumps({"matches_sha256": sha if cut else None, "cut": bool(cut),
                             "cuts_sha256": cuts_sha if cut else None,
                             "stamped_after_build": bool(stamped_after_build)},
                            indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return p


def check_record(build_dir: Path, path: Path | None = None, stations_path: Path | None = None) -> None:
    """Refuse (SystemExit) unless the atlas at `build_dir` cut at the matches file now at `path` — the
    same bytes, or (T159) a file whose CUT SET is the one the atlas recorded: rows that are no cut
    (lake gauges, stations not transmitting) are the bundle's alone and cannot disagree with the graph."""
    path = Path(path or MATCH_FILE)
    rec_path = Path(build_dir) / RECORD
    if not rec_path.exists():
        raise SystemExit(
            f"gauge matches: {build_dir} records no {RECORD} — the atlas predates the record (N8), so "
            f"which matches.json it cut its rivers at is unknown.\n"
            f"  If {path} has not changed since that atlas was built, stamp it (refused otherwise):\n"
            f"    python -m pipeline.gauges.matches --stamp-atlas {build_dir}\n"
            f"  else rebuild the atlas: python -m pipeline build --atlas --promote")
    rec = json.loads(rec_path.read_text(encoding="utf-8"))
    if not rec.get("cut"):
        return
    now = matches_digest(path)
    if rec.get("matches_sha256") == now:
        return
    cuts_rec = rec.get("cuts_sha256")
    cuts_now = cuts_digest(path, stations_path)
    if cuts_rec and cuts_rec == cuts_now:
        print(f"gauge matches: {path} is {now}, not the {rec.get('matches_sha256')} the atlas read, "
              f"but its cut set is the atlas's ({cuts_now}) — no boundary moves")
        return
    hint = ("" if cuts_rec else
            f"\n  The record carries no cut digest: if only non-cut rows changed, record the cut set "
            f"from the file the atlas read (its bytes must hash to {rec.get('matches_sha256')}):\n"
            f"    python -m pipeline.gauges.matches --record-cuts {build_dir} --from <that file>")
    raise SystemExit(
        f"gauge matches: the atlas at {build_dir} cut its rivers at matches.json "
        f"{rec.get('matches_sha256')} (cuts {cuts_rec}), but {path} is now {now} (cuts {cuts_now}) — "
        f"the bundle would place stations where the atlas did not cut. Rebuild the atlas "
        f"(python -m pipeline.atlas.build, via `python -m pipeline build --atlas --promote`), or "
        f"restore the matches it was built with.{hint}")


def record_cuts(build_dir: Path, source: Path, stations_path: Path | None = None) -> Path:
    """Add `cuts_sha256` to an atlas record written before it existed, from `source` — the matches
    file the atlas read, proven by its bytes hashing to the recorded `matches_sha256` (e.g.
    `git show <commit>:data/curated/gauges/matches.json`). The roster must predate the build, or
    the liveness that picked the cuts may since have changed. Refused otherwise."""
    from pipeline.common.curated import SOURCE
    build_dir = Path(build_dir)
    rec_path = build_dir / RECORD
    if not rec_path.exists():
        raise SystemExit(f"gauge matches: {build_dir} records no {RECORD}; stamp it first (--stamp-atlas)")
    rec = json.loads(rec_path.read_text(encoding="utf-8"))
    if not rec.get("cut"):
        raise SystemExit(f"gauge matches: the atlas at {build_dir} cut at no gauge; nothing to record")
    if matches_digest(source) != rec.get("matches_sha256"):
        raise SystemExit(f"gauge matches: {source} is {matches_digest(source)}, not the "
                         f"{rec.get('matches_sha256')} the atlas at {build_dir} read")
    roster = Path(stations_path or SOURCE / "bc_hydrometric_stations.json")
    meta = build_dir / "graph.pkl.meta.json"
    built = (float(json.loads(meta.read_text(encoding="utf-8"))["build_ts"]) if meta.exists()
             else (build_dir / "graph.pkl").stat().st_mtime)
    if roster.stat().st_mtime > built:
        raise SystemExit(f"gauge matches: {roster} is newer than the atlas at {build_dir} — the cuts it "
                         f"made may have been picked by an earlier roster; rebuild the atlas instead")
    return write_record(build_dir, cut=True, sha=rec["matches_sha256"],
                        cuts_sha=cuts_digest(source, roster),
                        stamped_after_build=bool(rec.get("stamped_after_build")))


def stamp_atlas(build_dir: Path, path: Path | None = None) -> Path:
    """Write RECORD for an atlas built BEFORE the record existed, from today's matches file — only when
    that file is older than the atlas build (its `graph.pkl.meta.json` build_ts, else graph.pkl's
    mtime): then the atlas cannot have cut at a later version. Refused otherwise."""
    path = Path(path or MATCH_FILE)
    build_dir = Path(build_dir)
    meta = build_dir / "graph.pkl.meta.json"
    if meta.exists():
        built = float(json.loads(meta.read_text(encoding="utf-8"))["build_ts"])
    elif (build_dir / "graph.pkl").exists():
        built = (build_dir / "graph.pkl").stat().st_mtime
    else:
        raise SystemExit(f"gauge matches: {build_dir} is not an atlas build (no graph.pkl)")
    if path.stat().st_mtime > built:
        raise SystemExit(f"gauge matches: {path} is newer than the atlas at {build_dir} — it may have cut "
                         f"at an earlier version; rebuild the atlas instead of stamping it")
    return write_record(build_dir, cut=True, sha=matches_digest(path), cuts_sha=cuts_digest(path),
                        stamped_after_build=True)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="the atlas's gauge-match record (N8)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--stamp-atlas", type=Path,
                   help="an atlas build that predates the record; stamped only if matches.json is older")
    g.add_argument("--record-cuts", type=Path,
                   help="an atlas build whose record lacks cuts_sha256; needs --from")
    ap.add_argument("--from", dest="source", type=Path,
                    help="the matches file that atlas read (must hash to its matches_sha256)")
    a = ap.parse_args()
    if a.stamp_atlas:
        print(f"wrote {stamp_atlas(a.stamp_atlas)}")
    else:
        if not a.source:
            ap.error("--record-cuts needs --from <the matches file the atlas read>")
        print(f"wrote {record_cuts(a.record_cuts, a.source)}")

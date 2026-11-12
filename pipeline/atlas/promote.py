"""Promote a side atlas build to the one the pipeline serves.

    python -m pipeline.atlas.promote data/generated/atlas/full_next            # -> full, full -> full.prev
    python -m pipeline.atlas.promote data/generated/atlas/full_next --dry-run  # parity report only

A PROMOTED ATLAS IS IMMUTABLE (`pipeline.common.vintage.promoted_atlas`): the bundle, the status
index, the export and the tiles all key sections by its `section_handles.txt` digest, so nothing may build
INTO the promoted directory: a build from NEWER inputs there rewrites the handle table under every shipped
derivative. (The build itself is deterministic for the same inputs — T65 / N12, 2026-11-04: two builds differ only
in timings, `build_ts` and pickled set order; the immutability rests on the digest, not on that.) A new atlas is built
beside it (`<name>_next`) and takes its place here, by rename — after `pipeline.tools.audit.build_parity`
has said what changed between the two registries. The previous atlas is kept as `<name>.prev` until
the next promotion, so the derivatives that still carry its digest can be rebuilt or compared.

Renames, never copies: an atlas is ~9 GB. Refuses a candidate with no handle table, a candidate
that IS the current build, and a `.prev` that a shipped artifact still carries (it would be
deleted).

(T547 §3) Refuses, too, when a boundary id a CURATED RULE names (an extent's `splits`, in the synopsis corpus or
the DFO places) keeps its id but now means another place (`ref`, e.g. `lake:<wbk>`): a lake boundary's clash suffix
(`_2`, `_3`) follows node order, so a rebuild can swap two lakes under one rule with every check passing.
`--accept-moved-boundaries` goes on after reading the list. An id that is gone is listed, not refused: the reach run
reports its rule unresolved.

(T547 §4) A candidate must be FINISHED: the atlas build writes `COMPLETE.json` last (`atlas.inputs.COMPLETE`) and
removes it when it starts, so a build that died, or an older run's files in a reused `_next`, is refused
(`--no-marker REASON` for a candidate made another way, e.g. a `sidecars --out` farm). A NET BOUNDARY LOSS in the
parity report refuses too (`--accept-boundary-loss` after reading it). If the second rename fails the first is undone.

(T631 item 3, the id dry run) Refuses, too, a candidate that would ORPHAN A CURATED REFERENCE: an item id a curated
file names (any string in any curated `*.json` outside `review/` that is an item of the current atlas) which the
candidate no longer has (`dead`), or has only as an absorbed alias (`absorbed`: the curated files name survivors only,
AGENTS 55). `--dry-run` lists them by file; resync the files first (`regs.parsing.backfill_matched`, the reference
patches of the rebuild's runbook), or `--accept-dead-ids` after reading the list. The lost boundaries are listed by
family (`lost_boundaries`), so an expected loss (a replaced shore rule) reads apart from a curated cut that vanished.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from pipeline.common.curated import GENERATED
from pipeline.common.section_handles import FILENAME as HANDLES


def _boundary_refs(build: Path) -> dict[str, set[str]]:
    """{boundary id: every `ref` it has} in an atlas's registry.json (an id may sit on several items)."""
    import json
    out: dict[str, set[str]] = {}
    for it in json.loads((Path(build) / "registry.json").read_text(encoding="utf-8")).get("items", []):
        for b in it.get("boundaries", []):
            out.setdefault(b["id"], set()).add(b.get("ref", ""))
    return out


def curated_boundary_files() -> list[Path]:
    """The curated files whose extents name boundary ids: the synopsis corpus, the DFO places and the DFO scope
    places (T679 #2: the page defaults and cascade scopes, once code in `regs.dfo_salmon.entries`)."""
    from pipeline.common.curated import CURATED
    files = [f for d in (CURATED.regulations.synopsis, CURATED.dfo_salmon.places)
             for f in sorted(Path(d).glob("*.json"))]
    return files + [Path(CURATED.dfo_salmon.scope_places)]


def split_ids_in(files) -> set[str]:
    """Every string in an extent's `splits`, anywhere in these JSON files (read-only)."""
    import json
    out: set[str] = set()

    def walk(v):
        if isinstance(v, dict):
            for k, x in v.items():
                if k == "splits" and isinstance(x, list):
                    out.update(s for s in x if isinstance(s, str))
                else:
                    walk(x)
        elif isinstance(v, list):
            for x in v:
                walk(x)
    for f in files:
        walk(json.loads(Path(f).read_text(encoding="utf-8")))
    return out


def curated_boundary_ids() -> set[str]:
    """Every boundary id a curated rule names: each string in an extent's `splits`, in `curated_boundary_files`."""
    return split_ids_in(curated_boundary_files())


def moved_boundaries(target: Path, candidate: Path, named: set[str]) -> tuple[list[str], list[str]]:
    """(moved, gone): the `named` boundary ids whose `ref` differs between the two registries, and those the
    candidate no longer has, each as one readable line."""
    old, new = _boundary_refs(target), _boundary_refs(candidate)
    moved = [f"{i}: {', '.join(sorted(old[i]))} -> {', '.join(sorted(new[i]))}"
             for i in sorted(named & old.keys() & new.keys()) if old[i] != new[i]]
    gone = [f"{i}: {', '.join(sorted(old[i]))}" for i in sorted((named & old.keys()) - new.keys())]
    return moved, gone


def _registry_ids(build: Path) -> tuple[set[str], dict[str, str]]:
    """(item ids, {absorbed alias -> its survivor}) of an atlas's registry.json."""
    import json
    ids: set[str] = set()
    alias: dict[str, str] = {}
    for it in json.loads((Path(build) / "registry.json").read_text(encoding="utf-8")).get("items", []):
        ids.add(it["id"])
        for a in it.get("aliases") or ():
            alias[str(a)] = it["id"]
    return ids, alias


#: curated folders the id dry run does not read: the review log and its queues (a mark names what was looked at then)
ORPHAN_SCAN_SKIPS = ("review",)
#: keys whose values are not live references: an append-only `history` (what a match named when it was set), and
#: `one_water_takes` (the instruction that folds the listed items into one water: naming them is its job)
ORPHAN_SKIP_KEYS = frozenset({"history", "one_water_takes"})


def curated_item_refs(known: set[str], root: Path | None = None) -> dict[str, dict[str, int]]:
    """{item id: {curated file (relative): times named}} — every string (key or value) in every curated `*.json`
    outside `ORPHAN_SCAN_SKIPS`, and not under an `ORPHAN_SKIP_KEYS` key, that is one of `known` (the current atlas's item
    ids). Read-only."""
    import json
    if root is None:
        from pipeline.common import curated as _c
        root = Path(_c.load().base)
    root = Path(root)
    out: dict[str, dict[str, int]] = {}

    def walk(v, f):
        if isinstance(v, dict):
            for k, x in v.items():
                if k in ORPHAN_SKIP_KEYS:
                    continue
                if k in known:
                    out.setdefault(k, {})[f] = out.get(k, {}).get(f, 0) + 1
                walk(x, f)
        elif isinstance(v, list):
            for x in v:
                walk(x, f)
        elif isinstance(v, str) and v in known:
            out.setdefault(v, {})[f] = out.get(v, {}).get(f, 0) + 1
    for p in sorted(root.rglob("*.json")):
        rel = p.relative_to(root)
        if rel.parts[0] in ORPHAN_SCAN_SKIPS:
            continue
        walk(json.loads(p.read_text(encoding="utf-8")), str(rel))
    return out


def orphaned_ids(target: Path, candidate: Path, root: Path | None = None) -> tuple[list[str], list[str]]:
    """(dead, absorbed): the item ids curated files name that are items of `target` and that `candidate` no longer has
    at all, or has only as an alias of another item — each one readable line: id, the survivor, the files."""
    old, _ = _registry_ids(target)
    new, alias = _registry_ids(candidate)
    refs = curated_item_refs(old - new, root)

    def line(i):
        files = ", ".join(f"{f} x{n}" for f, n in sorted(refs[i].items()))
        return f"{i}" + (f" -> {alias[i]}" if i in alias else "") + f": {files}"
    dead = [line(i) for i in sorted(refs) if i not in alias]
    absorbed = [line(i) for i in sorted(refs) if i in alias]
    return dead, absorbed


def lost_boundaries(target: Path, candidate: Path) -> dict[str, list[str]]:
    """{family: boundary ids} the candidate's registry no longer has, by the id's family (`shore`, `length`, `area`,
    `border`, `lake` for a lake edge, else `curated`). A family is the id's prefix before its first `:`."""
    old, new = _boundary_refs(target), _boundary_refs(candidate)
    out: dict[str, list[str]] = {}
    for i in sorted(old.keys() - new.keys()):
        head = i.split(":", 1)[0] if ":" in i else ""
        fam = head if head in ("shore", "length", "area", "border", "gauge") else \
            ("lake" if any(str(r).startswith("lake:") for r in old[i]) else "curated")
        out.setdefault(fam, []).append(i)
    return out


def promote(candidate: Path, name: str | None = None, *, dry_run: bool = False,
            log=print, target: Path | None = None, tiles: Path | None = None,
            bundle: Path | None = None, accept_moved: bool = False, accept_loss: bool = False,
            no_marker: str | None = None, accept_dead_ids: bool = False) -> dict:
    """Make `candidate` the build called `name` (default: the configured default build) — or the
    build directory `target` (T136: a scratch atlas a review rehearsal serves). `tiles` / `bundle`
    are the shipped artifacts whose atlas the `.prev` may still be (default: the live ones). Returns
    {"from", "to", "prev", "parity"}; `parity` is build_parity's report text."""
    if target is not None and name is not None:
        raise SystemExit("promote: name a build (--to) or a directory (--target), not both")
    candidate = Path(candidate).resolve()
    target = Path(target).resolve() if target is not None else GENERATED.build(name).resolve()
    prev = target.with_name(target.name + ".prev")
    if not (candidate / HANDLES).exists() or not (candidate / "registry.json").exists():
        raise SystemExit(f"promote: {candidate} is not a finished atlas (no {HANDLES} / "
                         f"registry.json)")
    from pipeline.atlas.inputs import complete_problems
    unfinished = complete_problems(candidate)
    if unfinished and no_marker is None:
        raise SystemExit(f"promote: {candidate} is not a finished atlas: {'; '.join(unfinished)} — rebuild it "
                         "(python -m pipeline build --atlas), or --no-marker REASON for a candidate made another way")
    if unfinished:
        log(f"promote: {candidate.name} has no finished-build marker, promoted anyway (--no-marker): {no_marker}")
    if candidate == target:
        raise SystemExit(f"promote: {candidate} already is the promoted build")
    parity = ""
    if target.is_dir():
        got = subprocess.run([sys.executable, "-m", "pipeline.tools.audit.build_parity",
                              str(target), str(candidate)],
                             capture_output=True, text=True, check=False)
        parity = (got.stdout + got.stderr).strip()
        log(parity)
    moved: list[str] = []
    if target.is_dir() and (target / "registry.json").exists():
        moved, gone = moved_boundaries(target, candidate, curated_boundary_ids())
        if gone:
            log(f"promote: {len(gone)} boundary id(s) a curated rule names are gone (their rules will be unresolved):\n  "
                + "\n  ".join(gone))
        if moved:
            log(f"promote: {len(moved)} boundary id(s) a curated rule names now mean ANOTHER place (T547 §3):\n  "
                + "\n  ".join(moved))
    dead: list[str] = []
    absorbed: list[str] = []
    if target.is_dir() and (target / "registry.json").exists():
        lostb = lost_boundaries(target, candidate)
        if lostb:
            log("promote: boundary ids the candidate no longer has, by family: "
                + ", ".join(f"{k} {len(v)}" for k, v in sorted(lostb.items())))
            for k, v in sorted(lostb.items()):
                if k != "shore":
                    log(f"  {k}: " + ", ".join(v[:40]) + (f" … {len(v) - 40} more" if len(v) > 40 else ""))
        dead, absorbed = orphaned_ids(target, candidate)
        if dead:
            log(f"promote: {len(dead)} item id(s) curated files name are GONE from the candidate (T631):\n  "
                + "\n  ".join(dead))
        if absorbed:
            log(f"promote: {len(absorbed)} item id(s) curated files name were ABSORBED into another item (name the "
                f"survivor; AGENTS 55):\n  " + "\n  ".join(absorbed))
    if dry_run:
        return {"from": str(candidate), "to": str(target), "prev": str(prev), "parity": parity,
                "moved": moved, "dead": dead, "absorbed": absorbed, "dry_run": True}
    if (dead or absorbed) and not accept_dead_ids:
        raise SystemExit(f"promote: refused — {len(dead)} curated item id(s) are gone and {len(absorbed)} absorbed in the "
                         "candidate (listed above). Resync the curated files first (regs.parsing.backfill_matched, the "
                         "rebuild's reference patches), or promote with --accept-dead-ids after reading the list")
    lost = boundary_loss(parity)
    if lost and not accept_loss:
        raise SystemExit(f"promote: refused — the candidate has {lost:,} fewer boundaries than {target.name} (the parity "
                         "report above lists the items). A lost cut is a rule that no longer binds; promote with "
                         "--accept-boundary-loss after reading it")
    if moved and not accept_moved:
        raise SystemExit(f"promote: refused — {len(moved)} boundary id(s) a curated rule names moved to another place "
                         "(listed above). Fix the rules (or the ids) first, or promote with --accept-moved-boundaries "
                         "after reading the list")
    if prev.exists():
        from pipeline.common.vintage import promoted_atlas
        carrier = promoted_atlas(prev, Path(tiles) if tiles else GENERATED.tiles,
                                 Path(bundle) if bundle else GENERATED.bundle / "bundle.sqlite")
        # the current build carries the SAME sid space (a promote that moved only the registry): it becomes the new
        # `.prev` and still is what the shipped artifacts were cut from, so the old `.prev` holds nothing they need
        if carrier and _same_handles(prev, target):
            carrier = None
        if carrier:
            raise SystemExit(f"promote: {prev} is still what {carrier} was cut from — rebuild the "
                             f"shipped artifacts from {target} before promoting again")
        shutil.rmtree(prev)
    if target.is_dir():
        target.rename(prev)
        log(f"  {target.name} -> {prev.name}")
    try:
        candidate.rename(target)
    except OSError:
        if prev.is_dir() and not target.exists():
            prev.rename(target)                     # undo the first rename: the old atlas stays served
            log(f"  rename failed: {prev.name} -> {target.name} restored")
        raise
    log(f"  {candidate.name} -> {target.name}")
    return {"from": str(candidate), "to": str(target), "prev": str(prev), "parity": parity, "moved": moved}


def boundary_loss(parity: str) -> int:
    """The net number of boundaries the candidate lost, from build_parity's `boundaries  before N   after M` line
    (0 when it gained or held, or when there was no current atlas to compare)."""
    import re
    m = re.search(r"^boundaries\s+before ([\d,]+)\s+after ([\d,]+)", parity, re.M)
    if not m:
        return 0
    before, after = (int(x.replace(",", "")) for x in m.groups())
    return max(0, before - after)


def _same_handles(a: Path, b: Path) -> bool:
    """Whether two atlas directories have one handle table (`section_handles.txt` digest)."""
    from pipeline.common.section_handles import digest_for
    try:
        return (a / HANDLES).exists() and (b / HANDLES).exists() and digest_for(a) == digest_for(b)
    except Exception:            # noqa: BLE001 — unreadable: not provably the same
        return False


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.atlas.promote",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("candidate", type=Path, help="the finished side build (e.g. .../full_next)")
    ap.add_argument("--to", default=None, help="the build name to become (default: the "
                                                 "configured default build)")
    ap.add_argument("--target", type=Path, default=None,
                    help="the build DIRECTORY to become (a scratch atlas), instead of a name under the live builds")
    ap.add_argument("--tiles", type=Path, default=None,
                    help="the tile set the .prev may still be the atlas of (default: the live tiles)")
    ap.add_argument("--bundle", type=Path, default=None,
                    help="the bundle the .prev may still be the atlas of (default: the live bundle)")
    ap.add_argument("--dry-run", action="store_true", help="print the parity report and stop")
    ap.add_argument("--accept-moved-boundaries", action="store_true",
                    help="(T547) promote although a boundary id a curated rule names now means another place")
    ap.add_argument("--accept-boundary-loss", action="store_true",
                    help="(T547) promote although the candidate has fewer boundaries than the current atlas")
    ap.add_argument("--accept-dead-ids", action="store_true",
                    help="(T631) promote although curated files name item ids the candidate lost or absorbed")
    ap.add_argument("--no-marker", default=None, metavar="REASON",
                    help="(T547) promote a candidate with no COMPLETE.json (not made by the atlas build); printed")
    a = ap.parse_args(argv)
    if a.no_marker is not None and not a.no_marker.strip():
        ap.error("--no-marker needs a reason, which is printed")
    promote(a.candidate, a.to, dry_run=a.dry_run, target=a.target, tiles=a.tiles, bundle=a.bundle,
            accept_moved=a.accept_moved_boundaries, accept_loss=a.accept_boundary_loss, no_marker=a.no_marker,
            accept_dead_ids=a.accept_dead_ids)
    return 0


if __name__ == "__main__":
    sys.exit(main())

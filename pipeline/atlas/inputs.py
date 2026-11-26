"""THE CURATED INPUTS AN ATLAS WAS BUILT FROM (T547, T599 step 1 "curated guard").

    python -m pipeline.atlas.inputs                     # is the promoted atlas behind its curated inputs? (rc 1 = yes)
    python -m pipeline.atlas.inputs --build DIR         # ... of another atlas
    python -m pipeline.atlas.inputs --backfill          # record them for an atlas built before T547 (proof required)
    python -m pipeline.atlas.inputs --backfill-code     # record the CODE of an atlas built before T1171 (proof required)

The atlas build reads curated files (cuts, names, added waters, areas, tidal limits, slough calls) and bakes them into
`graph.pkl` / `registry.json`. Nothing downstream re-reads them, so a file edited after the build ships on the OLD
atlas and every later check passes. The build therefore records one digest per input in `graph.pkl.meta.json`
(`input_hashes`, `serialize.write_artifact`), taken when the build STARTS (an edit made during the build is not
recorded as built), and `pipeline release` refuses an atlas whose record differs from the tree now, naming each file
(`behind`). `pipeline build` prints the same list as a warning.

KEYS ARE `CURATED` KEYS (`curated:waters.cuts`), never paths: a move or rename of a curated file changes no digest
(as `build.curated_digest`, T324), and a `CURATED_ROOT` copy (T422) compares equal to the tree it was copied from.

VALUES: `sha256:<hex>` of the file's bytes, except
  - `curated:waters.cuts` is `anchors:<hex>`, the digest of every cut's ANCHOR (`splits.anchor_digests`, the record
    `split_anchors.json` already keeps, T133): a label, note or confidence edit moves no cut and reaches the bundle
    through `splits.resolved.json` / the sidecars, so it is no reason to refuse;
  - `source:fwa_gpkg` is `stamp:<size>:<mtime>` (9.5 GB; `pipeline.build.stamp` identifies it the same way);
  - `not read (<flag>)` when the build was told to skip the file (`--no-splits`, `--no-added-lakes`, …): an atlas
    built without a curated input is behind it.

NOT INPUTS (`NOT_ATLAS_INPUTS`, each with its reason) are curated files the atlas build opens that do not shape it, or
that a record of their own already guards. `test_atlas_inputs.py` fails when a `CURATED.` key named in the atlas
build's modules is in neither list.

BACKFILL (an atlas built before this record existed): a digest is written only for a file PROVEN unchanged since the
build — tracked by git, clean, and last committed before the atlas's `build_ts`; the cuts also by `split_anchors.json`
equal to the anchors now; the gpkg by an mtime before the build. A file that cannot be proven is left out and named, so
the release refuses it until the atlas is rebuilt. Writes the meta file atomically; never touches anything else.
An atlas whose record predates an input added to `ATLAS_INPUTS` (T678's coast inputs, 2026-11-12) is backfilled for
the MISSING keys only, by the same proof; every digest it already records stays as built.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from pipeline.common.curated import CURATED, GENERATED, REPO_ROOT

#: {input key: what in the atlas build reads it}. Order is the order problems are listed in.
ATLAS_INPUTS: dict[str, str] = {
    "curated:waters.cuts": "gauge_cuts → sectionize (every curated cut; anchors only)",
    "curated:waters.tidal": "gauge_cuts → tidal cuts and mouth aliases (`splits.tidal.load`)",
    "curated:waters.name_variants": "build_graph / add_waters (`graph.names.load_name_variants`)",
    "curated:waters.found_names": "add_waters → registry labels (`registry.mined`)",
    "curated:waters.added_lakes": "load_fwa (`waters.added_lakes.ingest.merge`)",
    "curated:waters.added_streams": "load_fwa (`waters.added_streams.build_dataset.to_graph_inputs`)",
    "curated:waters.areas": "cut_areas (`splits.area_splits.load_area_split_defs`) + outside_items",
    "curated:waters.added_areas": "cut_areas (the polygons an `areas.json` def names by `file`)",
    "curated:waters.slough_calls": "build_registry (`registry.flowing.fold_confirmed_sloughs`)",
    # owner 2026-11-13 (Gagnon Creek): ponds the owner keeps as lakes, left out of the widening fold
    "curated:waters.pond_calls": "build_registry (`registry.flowing.fold_widening_ponds` kept_lakes)",
    "source:fwa_gpkg": "every FWA layer (`--gpkg`)",
    # T678 (recorded from the promotion of the first coast-cut atlas, full_t678b, 2026-11-12, by `--backfill`)
    "curated:waters.tidal_split_authority": "cut_shores (`splits.coast.load_authority`: which splits are DFO-written)",
    "curated:waters.shore_held": "cut_shores (`splits.coast_held.load`: the owner's rulings on single lines)",
    "source:fwa_coast": "cut_shores (`splits.coast.CoastGeometry`: FWA river polygons + land mass)",
}

#: curated keys named in the atlas build's modules that are NOT recorded, and why
NOT_ATLAS_INPUTS: dict[str, str] = {
    "curated:gauges.matches": "guarded by its own record, `gauge_matches.json` (cut-set digest, T159): a row that is "
                              "no cut does not change the graph (`gauges.matches.check_record`)",
    "curated:gauges.match_calls": "read only for the printed trust mix of the gauge cuts; shapes nothing",
    "curated:waters.added_stream_calls": "(T679 #13) read only by the added-streams GENERATOR (`added_streams.clean."
                                         "calls`), never by the atlas build: its reviewed output, added_streams.json, "
                                         "is the atlas input",
}

#: what catches a stale input up. Every input needs a rebuild (T678: there is no post-build tidal rewrite).
REBUILD = "python -m pipeline build --atlas --promote"
META = "graph.pkl.meta.json"


def curated_path(key: str) -> Path:
    """The file a `curated:a.b` key names in the tree now (sandbox-aware: `CURATED_ROOT`)."""
    v = CURATED
    for part in key.split(":", 1)[1].split("."):
        v = getattr(v, part)
    return Path(v)


def default_path(key: str) -> Path:
    if key == "source:fwa_gpkg":
        from project_config import get_config
        return Path(get_config().fwa_data_gpkg)
    if key == "source:fwa_coast":
        from project_config import get_config
        return Path(get_config().fwa_coast_gpkg)
    return curated_path(key)


def digest(key: str, path: Path) -> str:
    """One input's digest (module docstring, VALUES). A missing file raises (AGENTS 37)."""
    path = Path(path)
    if key in ("source:fwa_gpkg", "source:fwa_coast"):
        st = path.stat()
        return f"stamp:{st.st_size}:{int(st.st_mtime)}"
    data = path.read_bytes()
    if key == "curated:waters.cuts":
        from pipeline.atlas.splits.splits import anchor_digests
        blob = json.dumps(anchor_digests(json.loads(data)), sort_keys=True, separators=(",", ":"))
        return "anchors:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()
    return "sha256:" + hashlib.sha256(data).hexdigest()


def not_read(flag: str) -> str:
    return f"not read ({flag})"


def digests(paths: dict[str, Path | str | None] | None = None) -> dict[str, str]:
    """{key: digest} for every `ATLAS_INPUTS` key. `paths` overrides a key's file (a `--splits` / `--gpkg` flag), or
    names the flag that skipped it (`not_read`, as a string starting "not read")."""
    paths = paths or {}
    out = {}
    for key in ATLAS_INPUTS:
        p = paths.get(key)
        if isinstance(p, str) and p.startswith("not read"):
            out[key] = p
        else:
            out[key] = digest(key, Path(p) if p is not None else default_path(key))
    return out


def build_paths(args) -> dict[str, Path | str | None]:
    """The files `pipeline.atlas.build` reads under `args` (its own flags), for `digests`."""
    from pipeline.atlas.registry import flowing
    # both FWA files as the build was told (`--gpkg`, `--coast-gpkg`: T667 slice 8; each defaults to the config's)
    out: dict[str, Path | str | None] = {"source:fwa_gpkg": Path(args.gpkg), "source:fwa_coast": Path(args.coast_gpkg)}
    if args.no_splits:
        out["curated:waters.cuts"] = not_read("--no-splits")
        out["curated:waters.tidal"] = not_read("--no-splits")
    elif args.splits:
        out["curated:waters.cuts"] = Path(args.splits)
    if args.name_variants:
        out["curated:waters.name_variants"] = Path(args.name_variants)
    if args.no_added_lakes:
        out["curated:waters.added_lakes"] = not_read("--no-added-lakes")
    elif args.added_lakes:
        out["curated:waters.added_lakes"] = Path(args.added_lakes)
    if args.no_added_streams:
        out["curated:waters.added_streams"] = not_read("--no-added-streams")
    elif args.added_streams:
        out["curated:waters.added_streams"] = Path(args.added_streams)
    if not flowing.FOLD_CONFIRMED:
        out["curated:waters.slough_calls"] = not_read("flowing.FOLD_CONFIRMED off")
    return out


# ------------------------------------------------------------------------------------------------------------ record

def read_meta(build_dir: Path) -> dict:
    p = Path(build_dir) / META
    if not p.is_file():
        raise FileNotFoundError(f"{p} is missing — no atlas there, or one written before serialize.write_artifact")
    return json.loads(p.read_text(encoding="utf-8"))


def recorded(build_dir: Path) -> dict[str, str]:
    return dict(read_meta(build_dir).get("input_hashes") or {})


def write_meta(build_dir: Path, meta: dict) -> Path:
    """Replace the meta file atomically (a temp file + `os.replace`: also replaces a symlink, never its target)."""
    p = Path(build_dir) / META
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, p)
    return p


def behind(build_dir: Path, now: dict[str, str] | None = None) -> list[str]:
    """Every curated (or source) input the atlas at `build_dir` is behind, one line each naming the file and what
    catches it up. Empty = current. `now`: the tree's digests (default: computed)."""
    build_dir = Path(build_dir)
    rec = recorded(build_dir)
    if not rec:
        return [f"{build_dir.name}/{META} records no inputs (an atlas built before T547): prove them with "
                f"`python -m pipeline.atlas.inputs --backfill`, or rebuild: {REBUILD}"]
    now = digests() if now is None else now
    out = []
    for key in ATLAS_INPUTS:
        have, want = rec.get(key), now[key]
        if have == want:
            continue
        name = default_path(key).name
        how = REBUILD
        if have is None:
            out.append(f"{name} ({key}): the atlas records no digest of it — rebuild: {how}")
        elif have.startswith("not read"):
            out.append(f"{name} ({key}): the atlas was built without it, {have} — rebuild: {how}")
        else:
            out.append(f"{name} ({key}): changed since the atlas was built — rebuild: {how}")
    return out + code_behind(build_dir)


# -------------------------------------------------------------------------------------------------------------- code

#: (T1171) THE CODE AN ATLAS WAS BUILT FROM, beside its inputs in the meta (`CODE`):
#:     {"modules": {file: sha256 as imported}, "reads": {code file it read (`stepcache.is_code_read`): sha256},
#:      "packages": {distribution: version} (only those owning a package it imported), "config": {config.yaml key it
#:      read: digest}, "python": sys.version, "sqlite": the SQLite library}
#: or {"unrecorded": why} when the build could not record it honestly. The build-record machinery (`_trace`,
#: `stepcache`, this module: `RECORD_MACHINERY = True`) is left out: it decides what is recorded, never what the atlas
#: holds. The atlas is never rebuilt by a key (a person starts it), so a moved part is a line of `behind`: `pipeline
#: build` warns and `pipeline release` refuses, as for a curated input.
CODE = "code"
#: (owner pick B, 2026-11-26) the one command that proves the code of an atlas built before T1171 (`backfill_code`)
BACKFILL_CODE = "python -m pipeline.atlas.inputs --backfill-code"
#: the module the atlas build runs (`python -m pipeline.atlas.build`): the root of the backfill's import closure
ATLAS_ENTRY = "pipeline/atlas/build.py"
RECORD_MACHINERY = True


def code_stamp(trace_dir: Path | None) -> dict:
    """The `CODE` record of a build whose recorder wrote to `trace_dir` (`stepcache.traced_code`); None: it ran with no
    recorder (its `main` was called in-process), so its code cannot be told."""
    from pipeline.common import stepcache
    if trace_dir is None:
        return {"unrecorded": "the build ran without its recorder (main() called in-process, not "
                              "`python -m pipeline.atlas.build`)"}
    try:
        code = stepcache.traced_code(stepcache.Trace(Path(trace_dir)), "the atlas build")
    except stepcache.Untraceable as e:
        return {"unrecorded": str(e)}
    if code["moved"]:
        return {"unrecorded": f"{code['moved'][0]} changed while the atlas was built"}
    modules = {k: v for k, v in code["modules"].items() if not stepcache.is_machinery(k)}
    if not modules:
        return {"unrecorded": "the recorder saw no module of this repo"}
    return {"modules": modules, "reads": code["reads"], "packages": stepcache.package_versions(code["packages"]),
            "config": code["config"], **stepcache.stage_env()}


def record_code(build_dir: Path, trace_dir: Path | None) -> dict:
    """Write the build's `CODE` record into its meta (atomically, beside `input_hashes`); returns it."""
    meta = read_meta(build_dir)
    meta[CODE] = code_stamp(trace_dir)
    write_meta(build_dir, meta)
    return meta[CODE]


def code_behind(build_dir: Path) -> list[str]:
    """The atlas's code lines of `behind`: none when every module and code file it recorded has the same bytes now,
    every package the same version, every config value read the same value, and the same Python and SQLite; else one
    line per part that moved (the first item named, the rest counted). An atlas built before T1171 records no code: the
    line names `BACKFILL_CODE` (owner pick B) and the rebuild."""
    from pipeline.common import stepcache
    build_dir = Path(build_dir)
    code = read_meta(build_dir).get(CODE)
    if code is None:
        return [f"{build_dir.name}/{META} ({CODE}): records no code (an atlas built before T1171) — prove it once with "
                f"`{BACKFILL_CODE}`, or rebuild: {REBUILD}"]
    if "unrecorded" in code:
        return [f"{build_dir.name}/{META} ({CODE}): its code was not recorded ({code['unrecorded']}) — rebuild: {REBUILD}"]
    # (second review of #286) an empty package or config set is a recorder miss, never "none": refused, not current
    if gone := stepcache.empty_parts(code.get("packages"), code.get("config")):
        return [f"{build_dir.name}/{META} ({CODE}): its code record names {' and '.join(gone)} "
                f"({stepcache.EMPTY_IS_A_MISS}) — rebuild: {REBUILD}"]
    out = []

    def line(moved: list[str], what: str) -> None:
        if moved:
            more = f" (+{len(moved) - 1} more)" if len(moved) > 1 else ""
            out.append(f"{moved[0]}{more} ({CODE}{what}): changed since the atlas was built — rebuild: {REBUILD}")
    now = {**stepcache.code_now(code["modules"]), **stepcache.code_reads_now(code["reads"])}
    was = {**code["modules"], **code["reads"]}
    line(sorted(k for k in was if now[k] != was[k]), "")
    pk = stepcache.package_versions(code["packages"])
    line([f"{d} {code['packages'][d]} -> {pk[d]}" for d in sorted(pk) if pk[d] != code["packages"][d]], ": packages")
    cf = stepcache.config_now(code["config"])
    line([f"config.yaml {k}" for k in sorted(cf) if cf[k] != code["config"][k]], ": config")
    env = stepcache.stage_env()
    line([k for k in sorted(env) if code.get(k) != env[k]], ": env")
    return out


def import_closure(entry: str, root: Path | None = None) -> tuple[list[str], list[str]]:
    """(backfill) Every repo module `entry` can import, by reading the source (every `import` / `from … import`, at any
    depth: a function's own imports too), with each package's `__init__.py`; and the third-party top-level names it
    imports. A STATIC closure: it over-includes (a branch never taken) and cannot see `importlib.import_module`."""
    root = Path(root or REPO_ROOT)
    import ast

    def module_file(dotted: str) -> Path | None:
        base = root.joinpath(*dotted.split("."))
        for p in (base.with_suffix(".py"), base / "__init__.py"):
            if p.is_file():
                return p
        return None

    seen: set[Path] = set()
    tops: set[str] = set()
    todo = [root / entry]
    while todo:
        f = todo.pop()
        if f in seen:
            continue
        seen.add(f)
        rel = f.relative_to(root)
        pkg = list(rel.parent.parts) if f.name != "__init__.py" else list(rel.parent.parts)
        names: list[str] = []
        for node in ast.walk(ast.parse(f.read_text(encoding="utf-8"), str(f))):
            if isinstance(node, ast.Import):
                names += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                if node.level:
                    up = pkg[:len(pkg) - (node.level - 1)] if node.level > 1 else pkg
                    base = ".".join([*up, *([base] if base else [])])
                names += [base] + [f"{base}.{a.name}" for a in node.names if a.name != "*"]
        for dotted in names:
            if not dotted:
                continue
            parts = dotted.split(".")
            if module_file(parts[0]) is None:
                if parts[0] not in sys.stdlib_module_names and parts[0] not in sys.builtin_module_names:
                    tops.add(parts[0])
                continue
            for i in range(1, len(parts) + 1):         # the module and every package above it
                p = module_file(".".join(parts[:i]))
                if p is not None and p not in seen:
                    todo.append(p)
    return sorted(str(p.relative_to(root)) for p in seen), sorted(tops)


def backfill_code(build_dir: Path, *, entry: str = ATLAS_ENTRY, log=print) -> dict:
    """(owner pick B, 2026-11-26: "one-time backfill") Record the code of an atlas built before T1171, PROVEN the way
    `backfill` proves a curated input: every file of the atlas build's import set (`import_closure` of `entry`, the
    machinery left out), the tracked non-module files beside them (a schema, a JSON contract) and config.yaml are tracked
    by git, clean, and last committed before the atlas's `build_ts` (`git_proof`). One file git cannot prove refuses the
    whole backfill, naming each — nothing is written (then rebuild, or `pipeline release --ship-stale-atlas REASON`).
    The installed packages, Python and SQLite cannot be proven: today's are recorded as ADOPTED, said loudly. The record
    names the commit it was checked against. A person runs it once; nothing runs it by itself."""
    from pipeline.common import stepcache
    build_dir = Path(build_dir)
    meta = read_meta(build_dir)
    if CODE in meta:
        raise SystemExit(f"inputs: {build_dir / META} already records its code — nothing to backfill")
    ts = float(meta["build_ts"])
    modules, tops = import_closure(entry)
    modules = [m for m in modules if not stepcache.is_machinery(m)]
    dirs = sorted({str(Path(m).parent) for m in modules})
    reads = sorted(f for d in dirs for f in _git("ls-files", "--", d).split()
                   if str(Path(f).parent) == d and not f.endswith((".py", ".md")) and stepcache.is_code_read(f))
    files = [*modules, *reads, stepcache.CONFIG_FILE]
    unproven = [f for f in files if git_proof(REPO_ROOT / f, ts) is None]
    if unproven:
        raise SystemExit(f"inputs: --backfill-code refused: git cannot prove {len(unproven)} file(s) of the atlas's code "
                         f"unchanged since its build ({meta['build_ts']}) — rebuild ({REBUILD}), or ship it with "
                         "`pipeline release --ship-stale-atlas REASON`:\n  " + "\n  ".join(unproven))
    import importlib.util
    try:
        dists = stepcache.distributions_of([t for t in tops if importlib.util.find_spec(t) is not None],
                                           "the atlas build")
    except stepcache.Untraceable as e:
        raise SystemExit(f"inputs: --backfill-code refused: {e}") from e
    head = _git("rev-parse", "HEAD").strip()
    at = head[:12]                                  # the commit, as the messages print it
    import yaml
    whole = yaml.safe_load(stepcache.config_path().read_text(encoding="utf-8")) or {}
    from pipeline import _trace
    code = {"modules": stepcache.code_now(modules), "reads": stepcache.code_reads_now(reads),
            "packages": stepcache.package_versions(dists), "config": {"*": _trace.config_digest(whole)},
            **stepcache.stage_env(),
            "backfilled": {"at": time.strftime("%Y-%m-%dT%H:%M:%S"), "commit": head,
                           "proof": f"git: {len(files)} files tracked, clean, last committed before build_ts "
                                    f"{meta['build_ts']} (checked at {at})",
                           "closure": f"static import closure of {entry}",
                           "adopted": ["packages", "python", "sqlite"]}}
    meta[CODE] = code
    write_meta(build_dir, meta)
    log(f"inputs: PROVEN by git at {at}: {len(modules)} modules, {len(reads)} code files and config.yaml "
        f"unchanged since the atlas was built ({meta['build_ts']})")
    log(f"inputs: ADOPTED, NOT PROVEN — git cannot say what was installed when it was built; today's are recorded as "
        f"the atlas's: {len(code['packages'])} packages ({', '.join(f'{d}=={v}' for d, v in code['packages'].items())}), "
        f"Python {sys.version.split()[0]}, SQLite {code['sqlite']}")
    log(f"inputs: wrote the code record into {build_dir / META}")
    return code


# ---------------------------------------------------------------------------------------------------------- finished

#: (T547 §4) THE LAST FILE AN ATLAS BUILD WRITES: its input digests and every file it wrote. Removed when a build starts
#: into a directory, so a build that died leaves none; `pipeline.atlas.promote` refuses a candidate without it.
COMPLETE = "COMPLETE.json"


def clear_complete(out: Path) -> None:
    """A build starting into `out`: whatever finished there before is no longer this build."""
    (Path(out) / COMPLETE).unlink(missing_ok=True)


def write_complete(out: Path, input_hashes: dict[str, str]) -> Path:
    out = Path(out)
    files = sorted(p.name for p in out.iterdir() if p.name != COMPLETE and not p.name.endswith(".tmp"))
    p = out / COMPLETE
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps({"input_hashes": input_hashes, "files": files,
                               "at": time.strftime("%Y-%m-%dT%H:%M:%S")}, indent=1) + "\n", encoding="utf-8")
    os.replace(tmp, p)
    return p


def complete_problems(build: Path) -> list[str]:
    """Why `build` is not a finished atlas: no marker, or a file the marker lists is missing. [] = finished."""
    p = Path(build) / COMPLETE
    if not p.is_file():
        return [f"no {COMPLETE} — the build did not finish (or predates T547)"]
    files = json.loads(p.read_text(encoding="utf-8")).get("files") or []
    return [f"{f} is listed in {COMPLETE} and missing" for f in files if not (Path(build) / f).exists()]


# ---------------------------------------------------------------------------------------------------------- backfill

def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout


def git_proof(path: Path, build_ts: float) -> str | None:
    """Why `path` cannot have changed since `build_ts` by git, or None when it is not proven: tracked, clean, and its
    last commit before the build. Refused under a `CURATED_ROOT` copy: git proves the live files only (T777: the refusal
    sits here, on the step that needs the live tree, so a test that stubs the proof runs the backfill on its sandbox)."""
    if os.environ.get("CURATED_ROOT"):
        raise SystemExit("inputs: --backfill proves files by git: run it on the live tree (unset CURATED_ROOT)")
    try:
        rel = str(Path(path).resolve().relative_to(REPO_ROOT))
    except ValueError:
        return None
    if not _git("ls-files", "--", rel).strip() or _git("status", "--porcelain", "--", rel).strip():
        return None
    got = _git("log", "-1", "--format=%ct %h", "--", rel).split()
    if not got or int(got[0]) > build_ts:
        return None
    return f"git: clean, last commit {got[1]} at {time.strftime('%Y-%m-%d %H:%M', time.localtime(int(got[0])))}"


def backfill(build_dir: Path, *, log=print) -> tuple[dict[str, str], list[str]]:
    """Record the digests an old atlas can be PROVEN to have read (module docstring). Returns (written, unproven)."""
    build_dir = Path(build_dir)
    meta = read_meta(build_dir)
    have = dict(meta.get("input_hashes") or {})
    missing = [k for k in ATLAS_INPUTS if k not in have]
    if not missing:
        raise SystemExit(f"inputs: {build_dir / META} already records its inputs — nothing to backfill")
    ts = float(meta["build_ts"])
    from pipeline.atlas.splits.splits import read_anchor_record, anchor_digests
    now = digests()
    got, proof, unproven = {}, {}, []
    # a record that exists keeps every digest it holds: only an input added to ATLAS_INPUTS after the build (T678's
    # coast inputs on the first coast-cut atlas) is proven and added
    for key in missing:
        path = default_path(key)
        if key in ("source:fwa_gpkg", "source:fwa_coast"):
            why = f"mtime {time.strftime('%Y-%m-%d %H:%M', time.localtime(path.stat().st_mtime))} before the build" \
                if path.stat().st_mtime <= ts else None
        else:
            why = git_proof(path, ts)
            if why and key == "curated:waters.cuts":
                anchors = read_anchor_record(build_dir)
                cur = anchor_digests(json.loads(path.read_bytes()))
                why = (why + f"; split_anchors.json = the {len(cur)} anchors now") if anchors == cur else None
        if why is None:
            unproven.append(f"{path.name} ({key})")
            continue
        got[key], proof[key] = now[key], why
    for key, why in proof.items():
        log(f"  {key}: {why}")
    for u in unproven:
        log(f"  NOT PROVEN (left out; the release refuses the atlas until it is rebuilt): {u}")
    meta["input_hashes"] = {k: (have.get(k) or got[k]) for k in ATLAS_INPUTS if k in have or k in got}
    prior = (meta.get("input_hashes_backfilled") or {}).get("proof") or {}
    meta["input_hashes_backfilled"] = {"at": time.strftime("%Y-%m-%dT%H:%M:%S"), "proof": {**prior, **proof}}
    write_meta(build_dir, meta)
    log(f"inputs: wrote {len(got)} digest(s) into {build_dir / META}")
    return got, unproven


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.atlas.inputs", description=__doc__.split("\n")[0])
    ap.add_argument("--build", type=Path, default=None, help="the atlas (default: the promoted one)")
    ap.add_argument("--backfill", action="store_true",
                    help="record the inputs of an atlas built before T547 (only those proven unchanged)")
    ap.add_argument("--backfill-code", action="store_true",
                    help="(T1171) record the code of an atlas built before T1171: every file proven unchanged by git, "
                         "the packages adopted; refuses when one cannot be proven")
    a = ap.parse_args(argv)
    build = a.build or GENERATED.require_build()
    if a.backfill_code:
        backfill_code(build)
        return 0
    if a.backfill:
        _got, unproven = backfill(build)
        return 1 if unproven else 0
    bad = behind(build)
    if bad:
        print(f"inputs: the atlas {build} is behind {len(bad)} input(s):")
        for b in bad:
            print("  " + b)
        return 1
    print(f"inputs: the atlas {build} is current with its {len(ATLAS_INPUTS)} inputs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""ONE REBUILD COMMAND (P2): curated + source data -> atlas -> reach -> tiles -> deliver.

    python -m pipeline build                    # preflight, then every stage that is out of date
    python -m pipeline build --dry-run          # the plan: each stage, its key, run or up to date
    python -m pipeline build --atlas            # also build a side atlas (<build>_next) and print
                                                #   its parity report; stops there
    python -m pipeline build --atlas --promote  # ... and promote it, then run the rest
    python -m pipeline build --force reach      # rerun a stage (and everything after it)

Every stage runs as its own process, one after another (peak ~12 GB; workers <= 4: memory
`heavy-jobs-one-at-a-time`). A stage is UP TO DATE when its key — a digest of everything it reads:
the atlas's handles and registry, the corpus, the curated tree, the source stamps, its own code —
equals the key of its last successful run in the manifest and its outputs exist. Any stage that
runs makes every later stage run. The atlas is never rebuilt to check it (it is not
deterministic, memory `atlas-build-not-deterministic`): only `--atlas` builds one, and only
`--promote` makes it the atlas everything else reads.

The manifest (`data/generated/build-manifest.json`) records, per run, each stage's command, key,
status and seconds; per stage, its last successful key. Durations are never part of a key.
The last stage is the strict vintage check: tiles and bundle from one atlas and one registry.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

from pipeline.common.curated import CURATED, GENERATED, REPO_ROOT, SOURCE

MANIFEST = GENERATED.bundle.parent / "build-manifest.json"
# TILES BEFORE DELIVER: tiles read only the atlas, and `pipeline.deliver all` ends with the vintage
# check against the shipped tiles — after an atlas promote, deliver-then-tiles failed that check every time.
STAGES = ("reach", "tiles", "deliver")
WORKERS = 4

# What each stage's CODE is: a change to any .py file under these (tests excluded) reruns it.
CODE = {
    "reach": ("pipeline/atlas/reach", "pipeline/atlas/registry", "pipeline/regs", "pipeline/common"),
    "deliver": ("pipeline",),
    "tiles": ("pipeline/deliver/tiles", "pipeline/common", "pipeline/atlas/registry"),
}


def _sha(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"),
                                     default=str).encode("utf-8")).hexdigest()[:16]


def code_digest(roots) -> str:
    """The stage's code, by content (uncommitted edits count): every .py file under `roots`,
    tests and caches excluded."""
    h = hashlib.sha256()
    for root in roots:
        for p in sorted((REPO_ROOT / root).rglob("*.py")):
            if "tests" in p.parts or "__pycache__" in p.parts:
                continue
            h.update(str(p.relative_to(REPO_ROOT)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()[:16]


def curated_digest() -> str:
    """Every curated input, by content: each path the `CURATED` model names (a file, or every
    file under a directory). A path the model names that is missing raises (P2)."""
    paths: set[Path] = set()

    def walk(v):
        if isinstance(v, dict):
            for x in v.values():
                walk(x)
        elif isinstance(v, (list, tuple)):
            for x in v:
                walk(x)
        elif isinstance(v, Path):
            paths.add(v)
    walk(CURATED.model_dump())
    h = hashlib.sha256()
    for root in sorted(paths):
        files = [root] if root.is_file() else sorted(q for q in root.rglob("*") if q.is_file())
        if not root.exists():
            raise FileNotFoundError(f"curated path {root} is missing")
        for q in files:
            if q.name != ".DS_Store":
                h.update(str(q.relative_to(REPO_ROOT)).encode())
                h.update(q.read_bytes())
    return h.hexdigest()[:16]


def stamp(path: Path) -> list:
    """A big source file's identity without reading it: size and mtime. A missing one raises."""
    st = Path(path).stat()
    return [str(path), st.st_size, int(st.st_mtime)]


def sources() -> dict:
    from project_config import get_config
    return {"gpkg": Path(get_config().fwa_data_gpkg),
            "stations": SOURCE / "bc_hydrometric_stations.json",
            "places": SOURCE / "bc_places.json",
            "hydat": SOURCE / "hydat.sqlite3"}


def keys(build_dir: Path) -> dict[str, dict]:
    """Every stage's inputs, as digests (the key is `_sha` of these)."""
    from pipeline.atlas.registry import load_registry
    from pipeline.atlas.reach.steelhead import fingerprint, load_list
    from pipeline.common.section_handles import digest_for, registry_digest_for
    from pipeline.regs.parsing.io import corpus_digest, read_all_entries

    atlas = {"handles": digest_for(build_dir), "registry": registry_digest_for(build_dir)}
    corpus = corpus_digest(read_all_entries(registry=load_registry(str(build_dir / "registry.json"))))
    src = {k: stamp(p) for k, p in sources().items()}
    return {
        "reach": dict(atlas, corpus=corpus, steelhead=fingerprint(load_list()),
                      code=code_digest(CODE["reach"])),
        "deliver": dict(atlas, corpus=corpus, curated=curated_digest(),
                        sources={k: v for k, v in src.items() if k != "gpkg"},
                        code=code_digest(CODE["deliver"])),
        "tiles": dict(atlas, gpkg=src["gpkg"], places=src["places"],
                      code=code_digest(CODE["tiles"])),
    }


def outputs(build_dir: Path) -> dict[str, list[Path]]:
    reach = GENERATED.reaches / build_dir.name
    return {"reach": [reach / "report.json", reach / "rule_section.jsonl"],
            "deliver": [GENERATED.bundle / "bundle.sqlite", GENERATED.bundle / "verdicts.sqlite",
                        GENERATED.bundle / "status_index.bin"],
            "tiles": [GENERATED.tiles / "atlas.pmtiles", GENERATED.tiles / "atlas.meta.json"]}


def commands(build_dir: Path) -> dict[str, list[str]]:
    py = [sys.executable, "-m"]
    return {
        "reach": py + ["pipeline.atlas.reach.cli", "--build", str(build_dir),
                       "--out", str(GENERATED.reaches / build_dir.name)],
        "deliver": py + ["pipeline.deliver", "all", "--build", str(build_dir)],
        "tiles": py + ["pipeline.deliver.tiles", "--build", str(build_dir),
                       "--out", str(GENERATED.tiles)],
    }


def load_manifest() -> dict:
    if not MANIFEST.exists():               # the first run: every stage is out of date
        return {"stages": {}, "runs": []}
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def save_manifest(m: dict) -> None:
    m["runs"] = m["runs"][-20:]
    MANIFEST.write_text(json.dumps(m, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def preflight(want_tiles: bool) -> None:
    """Refuse to start on a stale curated artifact or a missing source file — never regenerate."""
    missing = [f"{k}: {p}" for k, p in sources().items() if not Path(p).exists()]
    if missing:
        raise SystemExit("build: source data missing — python data/fetch_data.py\n  "
                         + "\n  ".join(missing))
    rc = subprocess.run([sys.executable, "-m", "pipeline.tools.check_curated", "--ci"]).returncode
    if rc:
        raise SystemExit("build: a curated artifact is stale (check_curated --ci above); "
                         "regenerate it with the command it prints, then rebuild")
    if want_tiles:
        from pipeline.deliver.tiles import tippe
        tippe.check()


def plan(build_dir: Path, force: set[str]) -> list[tuple[str, str, bool]]:
    """[(stage, key, run?)] in order; a stage that runs makes every later one run."""
    m, k, out = load_manifest(), keys(build_dir), outputs(build_dir)
    rows, dirty = [], False
    for s in STAGES:
        key = _sha(k[s])
        last = (m["stages"].get(s) or {}).get("key")
        dirty = dirty or s in force or key != last or not all(p.exists() for p in out[s])
        rows.append((s, key, dirty))
    return rows


def run_stage(stage: str, cmd: list[str], key: str, run: dict, m: dict) -> None:
    print(f"\n=== {stage}: {' '.join(cmd)}", flush=True)
    t0 = time.time()
    rc = subprocess.run(cmd, cwd=REPO_ROOT).returncode
    rec = {"stage": stage, "cmd": cmd, "key": key, "status": "ok" if rc == 0 else f"rc={rc}",
           "seconds": round(time.time() - t0)}
    run["stages"].append(rec)
    if rc:
        run["status"] = f"failed at {stage}"
        save_manifest(m)
        raise SystemExit(f"build: {stage} failed (rc={rc}); the manifest says so: {MANIFEST}")
    m["stages"][stage] = {"key": key, "at": run["started"]}
    save_manifest(m)


def atlas_stage(build_dir: Path, promote: bool, run: dict, m: dict) -> bool:
    """Build `<build>_next`, print its parity, and promote it only when asked. Returns whether
    the rest of the build should go on (it reads the PROMOTED atlas)."""
    nxt = build_dir.with_name(build_dir.name + "_next")
    run_stage("atlas", [sys.executable, "-m", "pipeline.atlas.build", "--full", "--out", str(nxt)],
              "-", run, m)
    run_stage("parity", [sys.executable, "-m", "pipeline.atlas.promote", str(nxt), "--dry-run"],
              "-", run, m)
    if not promote:
        print(f"\nbuild: {nxt} is built and its parity printed above. Promote it with\n"
              f"    python -m pipeline build --promote   (or: python -m pipeline.atlas.promote {nxt})\n"
              "and the rest of the build runs against it. Stopping here.")
        return False
    run_stage("promote", [sys.executable, "-m", "pipeline.atlas.promote", str(nxt)], "-", run, m)
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline build", description=__doc__.split("\n")[0])
    ap.add_argument("--atlas", action="store_true", help="build a side atlas <build>_next first")
    ap.add_argument("--promote", action="store_true",
                    help="promote <build>_next (built now with --atlas, or already on disk)")
    ap.add_argument("--force", nargs="*", default=[], choices=STAGES, metavar="STAGE",
                    help=f"rerun these stages and everything after them ({', '.join(STAGES)})")
    ap.add_argument("--no-tiles", action="store_true", help="stop after deliver (tiles take ~40 min)")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and stop")
    a = ap.parse_args(argv)

    build_dir = GENERATED.build()
    m = load_manifest()
    run = {"started": time.strftime("%Y-%m-%dT%H:%M:%S"), "stages": [], "status": "running",
           "argv": sys.argv[1:]}
    if not a.dry_run:
        preflight(want_tiles=not a.no_tiles)
        m["runs"].append(run)
        if a.atlas:
            if not atlas_stage(build_dir, a.promote, run, m):
                run["status"] = "atlas built, not promoted"
                save_manifest(m)
                return 0
        elif a.promote:
            nxt = build_dir.with_name(build_dir.name + "_next")
            run_stage("promote", [sys.executable, "-m", "pipeline.atlas.promote", str(nxt)], "-",
                      run, m)

    rows = plan(build_dir, set(a.force))
    cmds = commands(build_dir)
    print(f"build: atlas {build_dir}")
    for s, key, go in rows:
        skip = a.no_tiles and s == "tiles"
        print(f"  {s:<8} {key}  {'skip (--no-tiles)' if skip else 'RUN' if go else 'up to date'}")
    if a.dry_run:
        return 0
    for s, key, go in rows:
        if go and not (a.no_tiles and s == "tiles"):
            run_stage(s, cmds[s], key, run, m)

    from pipeline.common.vintage import report
    ok, msg = report(GENERATED.tiles, GENERATED.bundle / "bundle.sqlite", strict=not a.no_tiles)
    print(msg)
    run["status"] = "ok" if ok else "vintage mismatch"
    save_manifest(m)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

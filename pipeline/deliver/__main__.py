"""ONE DELIVERY: bundle -> verdicts -> status index -> UI export, each cut from the one before.

    python -m pipeline.deliver                       # every step, canonical paths
    python -m pipeline.deliver bundle [--build DIR] [--reaches DIR] [--out FILE]
    python -m pipeline.deliver verdicts [--bundle FILE] [--out FILE] [--workers N]
    python -m pipeline.deliver answers [--bundle FILE] [--export-dir DIR] [--out FILE]
    python -m pipeline.deliver status_index [--bundle FILE] [--out FILE]
    python -m pipeline.deliver card_check [--bundle FILE] [--export-dir DIR]
    python -m pipeline.deliver store [--bundle FILE] [--export-dir DIR] [--out FILE] [--blobs CODEC]
    python -m pipeline.deliver export [--bundle FILE] [--out FILE]
    python -m pipeline.deliver all --out-dir DIR [--build DIR] [--reaches DIR] [--entries DIR] [--tiles DIR] [--debug-json]
                                            [--hold-latest]   (T408: the new release waits in CANDIDATE)
                                            [--no-step-cache] (T522: run every step; default reuses unchanged ones)

STEP REUSE (T522): `all` keeps each step's outputs and what it read (`pipeline.common.stepcache`) in
`deliver-cache/` beside the staging directory; a step whose arguments, staging inputs, declared inputs (the source
tree, the atlas, and for the bundle the curated tree) and every file it read — its imported code included — are
unchanged is reused, not run. An answers-only edit reruns answers, the card gate, the store and the release, not the
bundle. The release and run-timing steps always run.

The artifacts a client reads — the bundle, the status index, and the export pair
(`ui-rules-export.json` + `ui-rules-guide.json`, one run) — are three commands
(`pipeline.deliver.bundle`, `pipeline.deliver.status_index`, `pipeline.deliver.export`),
and each still runs alone — iterating on the export rebuilds only the export. The REGS STORE
(`pipeline.deliver.store`) is the release's input: the answers and the export words as SQLite, built from the files
beside it and proved to decode back to them byte for byte. NO `regs.sqlite` IS KEPT (P20): `all` builds it in its
staging directory, the release ships its tables (water, regs-<region>, regs-sid), and every reader reads those
(`pipeline.deliver.store.release.province_store`); `--debug-json` keeps a copy at `store.DEBUG_OUT`. What this adds is
the CHAIN: `all` writes them all into one directory from one bundle, and every step reads its input from the previous step's
output and nothing else, so the index and the export are cut from the bundle bytes beside them.
The vintage line (`pipeline.common.vintage`) and the two digests every artifact carries
(`section_handles`, `reach_digest`; the status index header holds both since version 2) are how
a reader proves it.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline.common.curated import GENERATED


def _bundle(a) -> Path:
    from pipeline.deliver.bundle.build import build
    build_dir = a.build or GENERATED.require_build()
    out = a.out or (GENERATED.bundle / "bundle.sqlite")
    print(f"bundling {build_dir} -> {out}")
    build(build_dir, out, reaches=a.reaches, entries=a.entries, tiles=a.tiles)
    return out


def _verdicts(bundle: Path, out: Path, workers: int) -> Path:
    from pipeline.deliver.verdicts.build import build
    build(str(bundle), out, workers=workers)
    return out


def _status_index(bundle: Path, out: Path) -> Path:
    from pipeline.deliver import status_index as si
    si.main(["--bundle", str(bundle), "--out", str(out)])
    return out


def _export(bundle: Path, out: Path) -> Path:
    from pipeline.deliver.export import __main__ as export_main
    # a REFUSED export (problems, a lossy or dangling encoding) leaves the old pair on disk and
    # must fail the command, never exit 0
    if export_main.main(["--bundle", str(bundle), "--out", str(out)]):
        raise SystemExit(f"pipeline.deliver: the export was refused — {out} was not written")
    return out


#: (T522) the steps `all` may reuse: each writes files into the staging directory and nothing else. `release` and
#: `runtiming` write release folders and take seconds: they always run.
CACHED_STEPS = ("bundle", "verdicts", "status_index", "export", "answers", "card_check", "store")


class _Steps:
    """(T522) THE STEP CACHE of one `all` run (`pipeline.common.stepcache`): a step whose arguments, staging files,
    declared inputs (`declared`) and every file it read last time — its code as imported included — are unchanged is
    REUSED (its recorded outputs placed in staging), else it runs under the recorder and is kept. `cache` None: every
    step runs and nothing is kept (`--no-step-cache`). When the enclosing build stage is itself recorded
    (PIPELINE_TRACE set by `pipeline build`), each step's modules — run or reused — are added to that trace, so the
    stage's code key names every step's code."""

    def __init__(self, d: Path, cache, declared: dict | None, bundle_declared: dict | None):
        import os
        self.d, self.cache = d, cache
        self.declared, self.bundle_declared = declared or {}, bundle_declared or {}
        self.stage_trace = os.environ.get("PIPELINE_TRACE")


def _step(args: list, steps: "_Steps | None" = None) -> None:
    """One step of the chain, as `python -m pipeline.deliver <step> …` in a fresh process."""
    import os
    import shutil
    import subprocess
    import tempfile
    import time
    print(f"pipeline.deliver: {' '.join(args)}", flush=True)
    t0 = time.time()
    name = args[0]
    cache = steps.cache if steps is not None and name in CACHED_STEPS else None
    if cache is None:
        # (run under the stage's recorder when there is one: it inherits PIPELINE_TRACE)
        subprocess.run([sys.executable, "-m", "pipeline.deliver", *args], check=True)
        print(f"pipeline.deliver: {name} done in {time.time() - t0:.0f}s", flush=True)   # T295: per-step timings
        return
    from pipeline.common import stepcache
    d = steps.d
    argv = [str(x).replace(str(d), "<staging>") for x in args]
    declared = steps.bundle_declared if name == "bundle" else steps.declared
    rec, why = cache.lookup(name, argv, d, declared)
    if rec is not None:
        cache.restore(name, rec, d)
        if steps.stage_trace:
            stepcache.write_modules_trace(Path(steps.stage_trace), rec["fingerprint"]["modules"], name)
        print(f"pipeline.deliver: {name} REUSED in {time.time() - t0:.0f}s ({why}; "
              f"{len(rec['outputs'])} output(s) from {cache.root / name})", flush=True)
        return
    print(f"pipeline.deliver: {name} runs: {why}", flush=True)
    before = stepcache.snapshot(d, cache.memo)
    tdir = Path(tempfile.mkdtemp(prefix=f".trace-{name}-", dir=d.parent))
    try:
        subprocess.run([sys.executable, "-m", "pipeline.deliver", *args], check=True,
                       env={**os.environ, "PIPELINE_TRACE": str(tdir)})
        trace = stepcache.Trace(tdir)
        print(f"pipeline.deliver: {name} {cache.record(name, argv, d, declared, before, trace)}", flush=True)
        if steps.stage_trace:
            Path(steps.stage_trace).mkdir(parents=True, exist_ok=True)
            for f in tdir.glob("*.trace"):
                shutil.copyfile(f, Path(steps.stage_trace) / f"{name}-{f.name}")
    finally:
        shutil.rmtree(tdir, ignore_errors=True)
    print(f"pipeline.deliver: {name} done in {time.time() - t0:.0f}s", flush=True)   # T295: per-step timings


def step_declared(build_dir: Path) -> tuple[dict, dict]:
    """(T522) What the recorder cannot see, for (every cached step, the bundle): stamps of every file under the source
    tree and the atlas (GDAL and SQLite read them in C); the bundle also every curated file but the review state
    (`build.NOT_BUILD_INPUTS`: the marks change with every review click and no stage reads them)."""
    from pipeline.build import not_build_inputs
    from pipeline.common.curated import CURATED, REPO_ROOT, SOURCE
    from pipeline.common.stepcache import declared_stamps
    every = declared_stamps([Path(SOURCE), Path(build_dir)])
    skip = [Path(p).resolve() for p in not_build_inputs()]
    cur = {k: v for k, v in declared_stamps([Path(CURATED.base)]).items()
           if not any(s == (REPO_ROOT / k).resolve() or s in (REPO_ROOT / k).resolve().parents for s in skip)}
    return every, {**every, **cur}


def canonical_homes(debug: bool = False) -> dict:
    """Where each file `deliver all` writes lives when no --out-dir is given: {file name: its live path}. The regs store
    has no home (P20): it is kept only with `debug` (`--debug-json`), at `store.DEBUG_OUT`."""
    from pipeline.deliver.answers.card_check import STAMP_NAME
    from pipeline.deliver.store import DEBUG_OUT
    from pipeline.deliver.export.contract import OUT as _EXPORT_OUT
    b, r = GENERATED.bundle, _EXPORT_OUT.parent
    return {"bundle.sqlite": b / "bundle.sqlite", "verdicts.sqlite": b / "verdicts.sqlite",
            "status_index.bin": b / "status_index.bin",
            # (owner 2026-10-29) the continuation links the gate held, beside the bundle that listed them
            "continuations_held.json": b / "continuations_held.json",
            "ui-rules-export.json": _EXPORT_OUT, "ui-rules-guide.json": r / "ui-rules-guide.json",
            "ui-rules-answers.json": r / "ui-rules-answers.json", STAMP_NAME: r / STAMP_NAME,
            **({"regs.sqlite": DEBUG_OUT} if debug else {})}


def out_dir_release_problems(rel_root: Path, tiles_dir: Path, check_tiles: bool) -> list[str]:
    """FAST-FAIL (T483, AGENTS 20a) for `--out-dir`: the release step and the vintage check read the released tiles
    (with --tiles) and the released basemap under `<out-dir>/release/`. A scratch out-dir without them failed only
    after the bundle, answers and store were built (both the T481 and T482 lanes, 2026-11-07); this is checked first."""
    import json
    out, live = [], Path(GENERATED.release)
    bm_meta = Path(tiles_dir) / "basemap.meta.json"
    if bm_meta.is_file():
        vid = json.loads(bm_meta.read_text(encoding="utf-8")).get("basemap_version")
        if vid and not (rel_root / "basemap" / vid / f"{vid}.pmtiles").exists():
            out.append(f"{rel_root / 'basemap' / vid / (vid + '.pmtiles')} is missing — "
                       f"ln -s {live.resolve() / 'basemap'} {rel_root / 'basemap'}")
    tv_meta = Path(tiles_dir) / "atlas.meta.json"
    if check_tiles and tv_meta.is_file():
        tv = json.loads(tv_meta.read_text(encoding="utf-8")).get("tile_version")
        if tv and not (rel_root / "tiles" / tv / f"atlas-{tv}.pmtiles").exists():
            out.append(f"{rel_root / 'tiles' / tv / ('atlas-' + tv + '.pmtiles')} is missing — "
                       f"ln -s {live.resolve() / 'tiles'} {rel_root / 'tiles'} (or release the tiles there)")
    return out


def _all(a) -> int:
    """THE CHAIN, ATOMIC (P07). Every step writes into a staging directory beside the destination (on the same disk, so
    `os.replace` is a rename). Only when the last step AND `vintage.report_all(strict)` pass is each file moved into
    place: into --out-dir, or to its canonical home (`canonical_homes`). A failed step or a vintage mismatch moves
    nothing, and the staging directory is removed either way, so the live files are never a mix of two runs.

    `--tiles DIR` (T136): the tile set the chain is cut against — the bundle's access measure, the vintage check and
    the release's tile block — instead of the live `GENERATED.tiles`; its released folder is looked up in the
    release root this run writes to (`<out-dir>/release`, or the live one). A scratch rebuild (a review rehearsed on
    copies) then reads and writes no live file."""
    import os
    import shutil
    import tempfile
    from pipeline.common import vintage
    dest = a.out_dir
    if dest:
        probs = out_dir_release_problems(dest / "release", a.tiles or Path(GENERATED.tiles), check_tiles=bool(a.tiles))
        if probs:
            print("pipeline.deliver: --out-dir is missing what the release step reads (nothing was built):\n  "
                  + "\n  ".join(probs), file=sys.stderr)
            return 1
    base = dest.parent if dest else GENERATED.bundle.parent
    base.mkdir(parents=True, exist_ok=True)
    d = Path(tempfile.mkdtemp(prefix=".deliver-staging-", dir=base))
    steps = None
    if not getattr(a, "no_step_cache", False):
        from pipeline.common.stepcache import StepCache
        declared, bundle_declared = step_declared(a.build or GENERATED.require_build())
        steps = _Steps(d, StepCache(base / "deliver-cache"), declared, bundle_declared)
    try:
        bundle = d / "bundle.sqlite"
        # EACH STEP IN ITS OWN PROCESS (DATAFLOW P1b, M10.1): CPython rarely hands a fragmented
        # heap back, so a step run in-process inherited the bundle build's peak. A step that
        # fails stops the chain (`check=True`); each still runs alone.
        b = ["bundle", "--out", str(bundle)]
        for flag, v in (("--build", a.build), ("--reaches", a.reaches), ("--entries", a.entries),
                        ("--tiles", a.tiles)):
            if v is not None:
                b += [flag, str(v)]
        _step(b, steps)
        _step(["verdicts", "--bundle", str(bundle), "--out", str(d / "verdicts.sqlite")], steps)
        _step(["status_index", "--bundle", str(bundle), "--out", str(d / "status_index.bin")], steps)
        _step(["export", "--bundle", str(bundle), "--out", str(d / "ui-rules-export.json")], steps)
        _step(["answers", "--bundle", str(bundle), "--export-dir", str(d)], steps)
        # THE CARD GATE (P05): every number on every card of the answers just written, proved
        # against the bundle's rules; a problem stops the chain here, before anything is packaged
        _step(["card_check", "--bundle", str(bundle), "--export-dir", str(d)], steps)
        _step(["store", "--bundle", str(bundle), "--export-dir", str(d), "--out", str(d / "regs.sqlite")], steps)
        # THE SHIPPED SET (P08): data/release/<data_release>/, from the files just made
        rel_root = (dest / "release") if dest else GENERATED.release
        tiles = ["--tiles", str(a.tiles), "--tiles-release-root", str(rel_root)] if a.tiles else []
        _step(["release", "--bundle", str(bundle), "--store", str(d / "regs.sqlite"), "--status-index",
               str(d / "status_index.bin"), "--export-dir", str(d), "--out-root", str(d / "release"), *tiles])
        # (T202) THE BASEMAP THE RELEASE NAMES LIVES BESIDE THE RELEASES (data/release/basemap/<id>/, T75), not in this
        # staging folder, which holds only the new r-* folder. The vintage check resolves the manifest's basemap url
        # under the staging release, so it is shown the released basemaps there; only r-* folders are promoted below.
        # (T373/T412) THE RUN-TIMING BASE OF THIS RELEASE'S MAP (`runtiming.feed.release.stage_base`), checked by the
        # vintage below and promoted with it; the feed itself is the workflow's (the staging upload starts it)
        _step(["runtiming", "--release-root", str(d / "release"), "--out", str(d / "runtiming"),
               *(["--tiles", str(a.tiles)] if a.tiles else []), *(["--build", str(a.build)] if a.build else [])])
        bm_root, bm_here = rel_root / "basemap", d / "release" / "basemap"
        if bm_root.is_dir() and bm_here.parent.is_dir() and not bm_here.exists():
            bm_here.symlink_to(bm_root.resolve(), target_is_directory=True)
        ok, msg = vintage.report_all(a.tiles or GENERATED.tiles, vintage.Artifacts.in_dir(d), strict=True)
        print(msg)
        if not ok:
            print("pipeline.deliver: vintage refused — nothing was promoted", file=sys.stderr)
            return 1
        debug = a.debug_json
        if not debug:                        # P20: the store was the release's input; the release holds its tables
            (d / "regs.sqlite").unlink(missing_ok=True)
        homes = canonical_homes(debug)
        # the release folder(s) go to data/release/ (or <out-dir>/release/), whole
        # only the r-* folders: the release step's LATEST pointer is re-written below, once the folder is in place (T72)
        releases = sorted(p for p in (d / "release").glob("r-*") if p.is_dir()) if (d / "release").exists() else []
        names = sorted(p.name for p in d.iterdir() if p.name not in ("release", "runtiming"))
        unknown = [n for n in names if dest is None and n not in homes]
        if unknown:
            raise SystemExit(f"pipeline.deliver: no home for {unknown} — nothing was promoted")
        if dest:
            dest.mkdir(parents=True, exist_ok=True)
        for n in names:
            target = dest / n if dest else homes[n]
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(d / n, target)
        for r in releases:
            rel_root.mkdir(parents=True, exist_ok=True)
            if (rel_root / r.name).exists():          # the same content id: identical bytes, replaced whole
                shutil.rmtree(rel_root / r.name)
            os.replace(r, rel_root / r.name)
        if releases:
            from pipeline.deliver.release.manifest import write_candidate, write_latest
            if getattr(a, "hold_latest", False):
                # (T297/T408) `pipeline release`: the new release waits as CANDIDATE; LATEST moves only after its checks
                write_candidate(rel_root, releases[-1].name)
            else:
                write_latest(rel_root, releases[-1].name)
                (rel_root / "CANDIDATE").unlink(missing_ok=True)     # a direct build supersedes a held one
        # (T412) the run-timing base of this release, where the staging upload reads it
        rt_home = (dest / "runtiming") if dest else GENERATED.runtiming / "feed" / "release"
        if (d / "runtiming").exists():
            if rt_home.exists():
                shutil.rmtree(rt_home)
            rt_home.parent.mkdir(parents=True, exist_ok=True)
            os.replace(d / "runtiming", rt_home)
        print(f"pipeline.deliver: promoted {len(names)} files to {dest or 'their canonical homes'}"
              + (f" and release {', '.join(r.name for r in releases)} to {rel_root}" if releases else ""))
        return 0
    finally:
        shutil.rmtree(d, ignore_errors=True)


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["release"]:                 # its flags are its own module's (argparse cannot pass them through)
        from pipeline.deliver.release.__main__ import main as release_main
        if release_main(argv[1:]):
            raise SystemExit("pipeline.deliver: the release was refused (a size gate) — nothing was packaged")
        return 0
    ap = argparse.ArgumentParser(prog="pipeline.deliver", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="step")
    b = sub.add_parser("bundle", help="the SQLite bundle from an atlas and its reach run")
    b.add_argument("--build", type=Path, default=None)
    b.add_argument("--reaches", type=Path, default=None)
    b.add_argument("--entries", type=Path, default=None)
    b.add_argument("--out", type=Path, default=None)
    b.add_argument("--tiles", type=Path, default=None,
                   help="the tile set the access measure reads (default: the live tiles)")
    v = sub.add_parser("verdicts", help="the reader's every answer, once: verdicts.sqlite")
    v.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    v.add_argument("--out", type=Path, default=None,
                   help="default: verdicts.sqlite beside the bundle")
    v.add_argument("--workers", type=int, default=4)
    s = sub.add_parser("status_index", help="the status index from a bundle")
    s.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    s.add_argument("--out", type=Path, default=GENERATED.bundle / "status_index.bin")
    e = sub.add_parser("export", help="the UI rules export from a bundle")
    e.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    e.add_argument("--out", type=Path, default=None,
                   help="the data file; ui-rules-guide.json is written beside it")
    an = sub.add_parser("answers", help="the answers file (answers/2) beside the export pair")
    an.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    an.add_argument("--export-dir", type=Path, default=None,
                    help="the export pair's directory (default: the canonical export's)")
    an.add_argument("--out", type=Path, default=None,
                    help="default: ui-rules-answers.json in the export's directory")
    an.add_argument("--workers", type=int, default=4)
    cc = sub.add_parser("card_check", help="the card gate on the shipped answers; writes card-check.json")
    cc.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    cc.add_argument("--export-dir", type=Path, default=None,
                    help="the answers' directory (default: the canonical export's)")
    sub.add_parser("release", help="the shipped set: data/release/<data_release>/ (P08); its own flags: "
                                   "python -m pipeline.deliver.release --help")
    st = sub.add_parser("store", help="the regs store (regs.sqlite) from the answers + export pair — a debug copy: "
                                      "`all` makes it in staging for the release and keeps none (P20)")
    st.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    st.add_argument("--export-dir", type=Path, default=None,
                    help="the export pair's and the answers' directory (default: the canonical export's)")
    st.add_argument("--out", type=Path, default=None,
                    help="default: pipeline.deliver.store.DEBUG_OUT (data/generated/debug/regs.sqlite)")
    st.add_argument("--blobs", default="zdict", choices=("json", "deflate", "zdict"))
    rt = sub.add_parser("runtiming", help="(T412) the run-timing base for the one release in --release-root")
    rt.add_argument("--release-root", type=Path, required=True)
    rt.add_argument("--out", type=Path, required=True)
    rt.add_argument("--tiles", type=Path, default=None)
    rt.add_argument("--build", type=Path, default=None)
    al = sub.add_parser("all", help="bundle, then the index and the export from THAT bundle")
    al.add_argument("--build", type=Path, default=None)
    al.add_argument("--reaches", type=Path, default=None)
    al.add_argument("--entries", type=Path, default=None)
    al.add_argument("--tiles", type=Path, default=None,
                    help="the tile set to cut against (default: the live tiles); its released folder is looked up "
                         "in this run's release root")
    al.add_argument("--out-dir", type=Path, default=None,
                    help="write bundle.sqlite, status_index.bin, ui-rules-export.json, "
                         "ui-rules-guide.json, ui-rules-answers.json (and, with --debug-json, regs.sqlite) here "
                         "(default: the canonical paths)")
    al.add_argument("--debug-json", action="store_true",
                    help="also keep the regs store the release was cut from (regs.sqlite: in --out-dir, else "
                         "pipeline.deliver.store.DEBUG_OUT); by default none is kept (P20)")
    al.add_argument("--no-step-cache", action="store_true",
                    help="(T522) run every step and keep none (default: a step whose inputs are unchanged is reused "
                         "from <out-dir parent or data/generated>/deliver-cache)")
    al.add_argument("--hold-latest", action="store_true",
                    help="(T408) name the new release in CANDIDATE, not LATEST: `pipeline release` moves LATEST "
                         "only after its checks pass")
    a = ap.parse_args(argv)
    if a.step in (None, "all"):
        if a.step is None:
            a.build = a.reaches = a.entries = a.tiles = None
            a.out_dir = None
            a.debug_json = False
            a.hold_latest = False
            a.no_step_cache = False
        return _all(a)
    if a.step == "runtiming":
        from pipeline.runtiming.feed.release import stage_base
        rels = sorted(p for p in a.release_root.glob("r-*") if p.is_dir())
        if len(rels) != 1:
            raise SystemExit(f"pipeline.deliver runtiming: expected one release in {a.release_root}, found {len(rels)}")
        stage_base(rels[0], a.tiles or GENERATED.tiles, a.out, a.build)
        return 0
    if a.step == "bundle":
        _bundle(a)
    elif a.step == "verdicts":
        _verdicts(a.bundle, a.out or a.bundle.with_name("verdicts.sqlite"), a.workers)
    elif a.step == "status_index":
        _status_index(a.bundle, a.out)
    elif a.step == "answers":
        from pipeline.deliver.answers.cli import main as answers_main
        from pipeline.deliver.export.contract import OUT as _EXPORT_OUT
        exp = a.export_dir or _EXPORT_OUT.parent
        out = a.out or exp / "ui-rules-answers.json"
        rc = answers_main(["build", "--bundle", str(a.bundle), "--export-dir", str(exp),
                           "--out", str(out), "--workers", str(a.workers)])
        if rc:
            raise SystemExit(f"pipeline.deliver: the answers were refused — {out} was not written")
    elif a.step == "card_check":
        from pipeline.deliver.answers.card_check import main as card_main
        from pipeline.deliver.export.contract import OUT as _EXPORT_OUT
        exp = a.export_dir or _EXPORT_OUT.parent
        if card_main(["--all", "--bundle", str(a.bundle), "--export-dir", str(exp)]):
            raise SystemExit(f"pipeline.deliver: the card gate found problems in {exp / 'ui-rules-answers.json'} "
                             "— nothing was packaged")
    elif a.step == "store":
        from pipeline.deliver.store import DEBUG_OUT
        from pipeline.deliver.store.__main__ import main as store_main
        from pipeline.deliver.export.contract import OUT as _EXPORT_OUT
        exp = a.export_dir or _EXPORT_OUT.parent
        out = a.out or DEBUG_OUT
        if store_main(["build", "--bundle", str(a.bundle), "--export-dir", str(exp),
                       "--out", str(out), "--blobs", a.blobs]):
            raise SystemExit(f"pipeline.deliver: the store was refused — {out} was not written")
    elif a.step == "export":
        from pipeline.deliver.export.contract import OUT as _EXPORT_OUT
        _export(a.bundle, a.out or _EXPORT_OUT)
    return 0


if __name__ == "__main__":
    sys.exit(main())

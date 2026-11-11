"""THE DATA UNDER TEST (T667 / T666 items 1-2): the ONE way a pipeline test reaches live data.

A test that follows the live tree asks here and nowhere else — never `GENERATED` / `CURATED`, a `data/...` path, an
override variable or `LATEST` itself (`pipeline/tests/anchors.py` finds the ones that do). So "which bundle, atlas,
release is this run about" has one answer per run, every runner gives the same one, and a release can point the whole
suite at its CANDIDATE by setting the environment once.

THE ENVIRONMENT CONTRACT (all optional; unset = this checkout's live tree). The same names the app suite reads
(`app/conformance/src/fixtureRelease.ts` `liveRelease`: `RELEASE_DIR`).

    RELEASE_DIR          the release folder under test, repo-relative or absolute (`data/release/r-…`). The release
                         command sets it to the CANDIDATE while its checks run, so nothing compares with LATEST before
                         promote. Unset: the release `LATEST` names under `release_root()`. A value naming no release
                         FAILS (it is never passed over for LATEST).
    UI_EXPORT_BUNDLE     bundle.sqlite            (default GENERATED.bundle / bundle.sqlite)
    UI_EXPORT_VERDICTS   verdicts.sqlite          (default beside the bundle)
    STATUS_INDEX         status_index.bin         (default beside the bundle)
    ANSWERS_EXPORT_DIR   the export / answers dir (default UI_EXPORT_DIR/regs, else GENERATED.base / regs)
    UI_EXPORT_DIR        a side set's `deliver all --out-dir` dir: its `release/` is the release root
    ATLAS_BUILD          the atlas build dir      (default GENERATED.build())
    UI_EXPORT_REACHES    the reach run            (default GENERATED.reaches / <build name>)
    UI_EXPORT_TILES      the tile layers dir      (default GENERATED.tiles / layers)

Every accessor goes through `conftest.need`: the calling test must carry the `needs_<kind>` marker, and a missing path
FAILS naming the command that makes it (no silent skips). `request` is a fixture's `request`, a node, or None (the
running test), as for `need`. `path("bundle")` is the same place UNCHECKED, for a module-level constant (no test is
running at import); the test still `need`s it or asks the accessor.

NAMES, NOT NUMBERS: `sections()` names a section handle by what it is — its blue line and the metre it starts at, or a
lake by its waterbody key — over the atlas's handle table. A rebuild that only renumbers changes no test; a section
that MOVED no longer resolves and fails saying which name.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

from pipeline.common import section_handles as H
from pipeline.common.curated import CURATED, GENERATED, REPO_ROOT
from pipeline.tests import conftest as C

RELEASE_ENV = "RELEASE_DIR"
RELEASE_HINT = "python -m pipeline release --staging (or python -m pipeline.deliver all); RELEASE_DIR names another"


def _env(name: str) -> Path | None:
    v = os.environ.get(name)
    return Path(v) if v else None


def _bundle() -> Path:
    return _env("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite"


def _export_dir() -> Path:
    side = _env("UI_EXPORT_DIR")
    return _env("ANSWERS_EXPORT_DIR") or (side / "regs" if side else GENERATED.base / "regs")


def _atlas() -> Path:
    return _env("ATLAS_BUILD") or GENERATED.build()


#: what can be asked for: name -> (the `needs_<kind>` it is, the hint when missing, where it is)
_WHERE = {
    "bundle": ("bundle", C.BUNDLE_HINT, _bundle),
    "verdicts": ("bundle", C.BUNDLE_HINT, lambda: _env("UI_EXPORT_VERDICTS") or _bundle().with_name("verdicts.sqlite")),
    "status_index": ("bundle", C.BUNDLE_HINT, lambda: _env("STATUS_INDEX") or _bundle().with_name("status_index.bin")),
    "export_dir": ("bundle", C.EXPORT_HINT, _export_dir),
    "export": ("bundle", C.EXPORT_HINT, lambda: _export_dir() / "ui-rules-export.json"),
    "answers": ("bundle", C.EXPORT_HINT, lambda: _export_dir() / "ui-rules-answers.json"),
    "atlas": ("atlas", C.ATLAS_HINT, _atlas),
    "reach_run": ("reach_run", C.REACH_HINT, lambda: _env("UI_EXPORT_REACHES") or GENERATED.reaches / _atlas().name),
    "tiles": ("tiles", C.TILES_HINT, lambda: GENERATED.tiles),
    "tile_layers": ("tiles", C.TILES_HINT, lambda: _env("UI_EXPORT_TILES") or GENERATED.tiles / "layers"),
}


def path(what: str) -> Path:
    """WHERE `what` is for this run (a key of `_WHERE`: bundle, verdicts, status_index, export_dir, export, answers,
    atlas, reach_run, tiles, tile_layers), unchecked: for a module-level constant and for messages. A test that reads it asks the
    accessor of the same name (or `need`s the constant), so the marker and the missing-file failure still hold."""
    if what not in _WHERE:
        raise KeyError(f"under_test.path: no {what!r}; one of {sorted(_WHERE)}")
    return Path(_WHERE[what][2]())


def _need(request, what: str) -> Path:
    kind, hint, _ = _WHERE[what]
    return C.need(request, kind, path(what), hint)


# ------------------------------------------------------------------------------------------------- the bundle and its kin
def bundle(request=None) -> Path:
    """bundle.sqlite under test (`needs_bundle`)."""
    return _need(request, "bundle")


def verdicts(request=None) -> Path:
    """verdicts.sqlite under test (`needs_bundle`)."""
    return _need(request, "verdicts")


def status_index(request=None) -> Path:
    """status_index.bin under test (`needs_bundle`)."""
    return _need(request, "status_index")


def export_dir(request=None) -> Path:
    """The export / answers directory under test (`ui-rules-export.json`, `ui-rules-answers.json`; `needs_bundle`)."""
    return _need(request, "export_dir")


def export(request=None) -> Path:
    """`ui-rules-export.json` under test (`needs_bundle`); read it through `export.files.load` / `load_export`."""
    return _need(request, "export")


def answers(request=None) -> Path:
    """`ui-rules-answers.json` under test (`needs_bundle`)."""
    return _need(request, "answers")


# ------------------------------------------------------------------------------------------------------- atlas, reach, tiles
def atlas(request=None) -> Path:
    """The atlas build directory under test (`needs_atlas`)."""
    return _need(request, "atlas")


def reach_run(request=None) -> Path:
    """The reach run under test (`needs_reach_run`): the one named for the atlas build."""
    return _need(request, "reach_run")


def reach_run_named(name: str, request=None) -> Path:
    """The reach run a BUNDLE names (its `meta.reach_run`; `needs_reach_run`): a run is paired with its atlas by
    digest, so its folder need not carry the build's name. `UI_EXPORT_REACHES` still wins (a side set's run)."""
    return C.need(request, "reach_run", _env("UI_EXPORT_REACHES") or GENERATED.reaches / name, C.REACH_HINT)


def tiles(request=None) -> Path:
    """The tiles directory under test (`needs_tiles`)."""
    return _need(request, "tiles")


def tile_layers(request=None) -> Path:
    """The tile layers directory under test (`layers/*.geojsonl`; `needs_tiles`)."""
    return _need(request, "tile_layers")


# ------------------------------------------------------------------------------------------------------------- the release
def release_root() -> Path:
    """Where releases live for this run (`conftest.release_root`: a side set's, else `data/release`)."""
    return C.release_root()


def release_at(env=os.environ, root: Path | None = None) -> Path:
    """THE RELEASE UNDER TEST as a folder: `RELEASE_DIR` when set (the release command's candidate), else the release
    `LATEST` names under `root` (default `release_root()`). Raises LookupError saying which is missing — a `RELEASE_DIR`
    with no manifest is never passed over for LATEST, and a root with no `LATEST` is never answered by the newest
    folder."""
    named = env.get(RELEASE_ENV)
    if named:
        d = Path(named) if Path(named).is_absolute() else Path(REPO_ROOT) / named
        if not (d / "manifest.json").is_file():
            raise LookupError(f"{RELEASE_ENV}={named}: no release there ({d}/manifest.json)")
        return d
    root = Path(root) if root is not None else release_root()
    pointer = root / "LATEST"
    if not pointer.is_file():
        raise LookupError(f"no release under test: {pointer} is missing and {RELEASE_ENV} is unset — {RELEASE_HINT}")
    d = root / pointer.read_text(encoding="utf-8").strip()
    if not (d / "manifest.json").is_file():
        raise LookupError(f"{pointer} names {d.name}, which has no manifest — re-run the release step")
    return d


def release(request=None) -> Path:
    """The release folder under test (`needs_bundle`, the marker a release read has always carried); fails when there
    is none, naming what is missing."""
    import pytest
    try:
        d = release_at()
    except LookupError as e:
        C.need(request, "bundle", release_root(), RELEASE_HINT)      # the marker check, and the root itself
        pytest.fail(str(e), pytrace=False)
    return C.need(request, "bundle", d, RELEASE_HINT)


# ---------------------------------------------------------------------------------------------------------- the whole trees
def curated():
    """THE LIVE CURATED TREE, read in place and never written (it is in git, so no marker): `curated().waters.cuts`.
    A test that WRITES curated data sandboxes it instead (`conftest.curated_root`)."""
    return CURATED


def generated():
    """The generated tree's paths, for a part no accessor above names (`generated().regs.dfo_salmon`,
    `generated().review`). The caller `need`s what it reads; prefer a named accessor when one fits."""
    return GENERATED


# -------------------------------------------------------------------------------------------------------- names, not numbers
class NoSuchSection(LookupError):
    """A section name that does not resolve in the build under test (the section moved, or the name is wrong)."""


class SectionNames:
    """Section handles BY NAME over one atlas build's handle table (`section_handles.txt`)."""

    def __init__(self, build: Path):
        self.build = Path(build)
        self._ids, self._by_id = H.read(self.build)

    def _handle(self, sid: str, asked: str) -> int:
        h = self._by_id.get(sid)
        if h is None:
            raise NoSuchSection(f"section names: no {asked} in {self.build} (handles {H.digest_for(self.build)}) — "
                                "the section moved or was re-cut; name it where it starts now")
        return h

    def stream(self, blk: int, metre: int) -> int:
        """The handle of the stream section of blue line `blk` that STARTS at `metre`."""
        return self._handle(f"{blk}:{metre}", f"stream section starts at {blk}:{metre}")

    def streams(self, at: Iterable[tuple[int, int]]) -> list[int]:
        """`stream` for each (blue line, start metre), in order."""
        return [self.stream(b, m) for b, m in at]

    def lake(self, wbk: int) -> int:
        """The handle of the lake section of waterbody key `wbk` (`lake:<wbk>`)."""
        return self._handle(f"lake:{wbk}", f"lake section lake:{wbk}")

    def name(self, handle: int) -> str:
        """The section id a handle names (`blk:metre` / `lake:wbk`), for a failure message."""
        return H.id_at(self._ids, handle)


def sections(request=None, build: Path | None = None) -> SectionNames:
    """Section names over the atlas under test (or over `build`, a directory holding a handle table)."""
    return SectionNames(build if build is not None else atlas(request))

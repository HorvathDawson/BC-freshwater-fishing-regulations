"""Where the data is — validated once, at import, by pydantic.

    from pipeline.common.curated import CURATED, SOURCE
    defs = load_split_defs(CURATED.waters.splits)
    roster = SOURCE / "bc_hydrometric_stations.json"

WHY A MODEL AND NOT `get_path("curated", "splits")`.

`ProjectConfig.get_path` returns `Path()` — the current directory — for a key that does not
exist. No exception, no warning. Hand that to a loader and you get an empty list, and an
empty list of curated cuts looks exactly like a build with no cuts to make. That is not a
hypothetical: a `--splits` flag with no default silently dropped all 376 curated cuts from
three full builds, one of which was promoted, and nothing anywhere looked wrong.

pydantic's `FilePath` and `DirectoryPath` refuse to construct if the path is not there. So
the failure moves from "a quiet wrong answer, three builds later" to "the process will not
start, and it names the key". That is the entire argument for the dependency, and pydantic
is already in `requirements.txt` for the parsing models.

THREE KINDS OF DATA, AND THE DIFFERENCE IS WHAT IT COSTS TO LOSE:

    source      fetched from an authority. Re-downloadable.
    generated   computed. Re-runnable.
    curated     human-owned. Not recreatable at any price.

The third splits again, and the split is what this module exists to make visible:

    authored    a person typed it — splits, name_variants, added_lakes
    promoted    a machine produced it and a person APPROVED it — the gauge matches,
                added_streams. It LOOKS generated, which is exactly the danger: someone
                re-runs the generator "just to check" and destroys the review.

A CURATED PATH IS NEVER OVERRIDABLE BY AN ENVIRONMENT VARIABLE. That is the
`review_build` failure with a new coat of paint — the app rebuilt one directory and read
another and neither said so. There is one answer to "where is the curated data" and it
lives in `config.yaml`. Generated paths live there too, under `generated:` — see `GENERATED`.
"""

from __future__ import annotations

import functools
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, DirectoryPath, FilePath, field_validator

#: The repo root. `parents[2]` because this module sits at pipeline/common/ — it was
#: parents[1] when it lived at pipeline/, and the move silently repointed config.yaml at
#: pipeline/config.yaml, which does not exist. Exactly the class of bug this file prevents
#: everywhere else, so it gets an assertion rather than a comment.
ROOT = Path(__file__).resolve().parents[2]
assert (ROOT / "config.yaml").is_file(), f"repo root wrong: no config.yaml under {ROOT}"

#: THE repo root, for the handful of modules that legitimately need one. Import this instead
#: of counting `parents[N]` yourself: the count depends on how deep the module sits, so it
#: silently becomes wrong the moment a package moves — which is exactly what happened to
#: bundle/, tiles/ and parsing/ when they were grouped, and none of them raised.
REPO_ROOT = ROOT
CONFIG = ROOT / "config.yaml"


def _absolute(v: object) -> object:
    """Resolve against the repo root, so nothing depends on the current directory.

    `pipeline/regs/dfo_salmon/match.py` used a bare `Path("pipeline/atlas/splits.json")`, which worked
    only because everything happens to be run from the repo root. A path that is correct
    from one directory and silently wrong from another is the same class of bug as a
    missing one, and harder to see.
    """
    if isinstance(v, str):
        p = Path(v)
        return p if p.is_absolute() else ROOT / p
    return v


class Waters(BaseModel):
    """Curation that shapes the atlas itself."""

    model_config = ConfigDict(frozen=True)

    splits: FilePath
    name_variants: FilePath
    areas: FilePath
    #: Hand-drawn admin polygons — closure zones a regulation states as an AREA rather than a
    #: reach. Selected by an `areas.json` def that names it in `file`; see `load_area_polys`.
    added_areas: FilePath
    added_lakes: FilePath
    #: PROMOTED, not authored — 361 minted streams whose negative `blk` ids are a live ABI.
    added_streams: FilePath
    #: The worklist for minting more added lakes. Zero code references BY DESIGN; the
    #: minting is a hand process. Declared here so a dead-file sweep cannot delete it.
    ungazetted: FilePath

    _abs = field_validator("*", mode="before")(_absolute)


class Entries(BaseModel):
    """The per-region regulation corpora. Same KIND of thing, separate models.

    `catalogue` is the format doc 18 describes — a rule is a TYPE plus named CONDITIONS and the
    label is generated. `synopsis` held the prose format it replaces and is gone; the water-specific
    tables are reparsed into `catalogue` (`run_parse.sh catalogue`).
    """

    model_config = ConfigDict(frozen=True)

    catalogue: DirectoryPath
    dfo_salmon: DirectoryPath

    _abs = field_validator("*", mode="before")(_absolute)


class Regulations(BaseModel):
    model_config = ConfigDict(frozen=True)

    overrides: FilePath
    entries: Entries

    _abs = field_validator("overrides", mode="before")(_absolute)


class Domain(BaseModel):
    """One matching domain: an external id -> FWA keys, plus the hand review of it.

    `review` is an INPUT to matching and is never written by a program. `matches` is
    regenerated wholesale. Two files rather than one is what makes a regenerate PHYSICALLY
    unable to harm the review — the safety is in the filesystem, not in code remembering.

    `None` means "declared, not built yet". Stocking and bathymetry are named before they
    exist so the freshness gate is already waiting for them.
    """

    model_config = ConfigDict(frozen=True)

    matches: FilePath | None = None
    review: FilePath | None = None
    #: Gauges only: ECCC's stated water-body type, scraped once. An input to matching.
    waterbody_type: FilePath | None = None

    _abs = field_validator("*", mode="before")(_absolute)


class RunTiming(BaseModel):
    """Authored run-timing curation. See `data/curated/runtiming/review.json`."""

    model_config = ConfigDict(frozen=True)

    review: FilePath

    _abs = field_validator("*", mode="before")(_absolute)


class Curated(BaseModel):
    """Every path whose loss costs human time rather than CPU."""

    model_config = ConfigDict(frozen=True)

    base: DirectoryPath
    waters: Waters
    regulations: Regulations
    gauges: Domain
    stocking: Domain
    bathymetry: Domain
    runtiming: RunTiming

    _abs = field_validator("base", mode="before")(_absolute)

    def domain(self, name: str) -> Domain:
        """`c.domain("gauges")` — for tools that loop over all three."""
        got = getattr(self, name, None)
        if not isinstance(got, Domain):
            raise KeyError(f"{name!r} is not a matching domain; "
                           f"try one of gauges, stocking, bathymetry")
        return got


class DataTree(BaseModel):
    """Where fetched and computed data live. Not curated; losing either costs machine time."""

    model_config = ConfigDict(frozen=True)

    source: DirectoryPath
    #: Created on demand — a fresh checkout has run nothing, so this must not require it.
    generated: Path

    _abs = field_validator("*", mode="before")(_absolute)


# ======================================================================================
# generated/ — everything a program writes
# ======================================================================================


class GeneratedAtlas(BaseModel):
    """Graph builds. One subdirectory per region, plus `full/`."""

    model_config = ConfigDict(frozen=True)

    builds: Path
    #: The build the review app serves AND rebuilds into. One name, read by both — these
    #: were two independent hard-codings of "output/v2/full", and the app could rebuild one
    #: directory and read another with nothing saying so.
    default_build: str = "full"

    _abs = field_validator("builds", mode="before")(_absolute)


class GeneratedRegs(BaseModel):
    model_config = ConfigDict(frozen=True)

    extraction: Path
    parsing: Path
    parse: Path
    dfo_salmon: Path
    entries_backup: Path

    _abs = field_validator("*", mode="before")(_absolute)


class GeneratedGauges(BaseModel):
    model_config = ConfigDict(frozen=True)

    feeds: Path

    _abs = field_validator("*", mode="before")(_absolute)


class Generated(BaseModel):
    """Where computed data goes. Losing any of it costs CPU, never human time.

    Every field is a plain `Path`, not a `DirectoryPath`: a fresh checkout has run nothing,
    so requiring these to exist would make the config unloadable before the first build.
    That permissiveness is exactly what `output/` got wrong — a missing output directory was
    created on demand, so a build could write somewhere new and a reader could go on reading
    the old place, and neither would say so.

    The rule that replaces it is asymmetric, and it is the whole point of this class:

        a WRITER may create its directory      -> `mkdir(...)`
        a READER may not                       -> `require_build(...)`, which raises

    `require_build` names the command that produces what is missing, so the failure is one
    line and actionable instead of a FileNotFoundError three frames down naming a pickle.
    """

    model_config = ConfigDict(frozen=True)

    base: Path
    atlas: GeneratedAtlas
    reaches: Path
    bundle: Path
    tiles: Path
    added_streams: Path
    scratch: Path
    regs: GeneratedRegs
    gauges: GeneratedGauges
    runtiming: Path

    _abs = field_validator("base", "reaches", "bundle", "tiles", "added_streams", "scratch",
                           "runtiming", mode="before")(_absolute)

    # --- builds -----------------------------------------------------------------------

    def build(self, name: str | None = None) -> Path:
        """The path of a build. Does NOT check it exists — for writers and for messages."""
        return self.atlas.builds / (name or self.atlas.default_build)

    def require_build(self, name: str | None = None) -> Path:
        """The path of a build that must already be there, or raise saying how to make it."""
        p = self.build(name)
        if not p.is_dir():
            raise FileNotFoundError(
                f"no build at {p} — build it:\n"
                f"    PYTHONPATH=\"$PWD\" .venv/bin/python -m pipeline.atlas.build "
                f"--full --out {p}"
            )
        return p

    def registry(self, name: str | None = None) -> Path:
        """`registry.json` of a build that must already be there.

        This replaces `default_registry_path()`, which returned
        `output/pipeline/graph/registry.json` — a directory that never existed on disk — and
        was the declared default of seven parser tools.
        """
        return self.require_build(name) / "registry.json"

    def mkdir(self, *parts: str) -> Path:
        """A directory under `generated/`, created. For WRITERS only."""
        p = self.base.joinpath(*parts)
        p.mkdir(parents=True, exist_ok=True)
        return p


@functools.lru_cache(maxsize=2)
def load(config: Path | None = None) -> Curated:
    """The validated curated tree. Cached, because validation stats every file.

    Raises `pydantic.ValidationError` naming the offending key when a path is wrong — which
    is the whole point, and is why nothing here has a fallback default.
    """
    path = config or CONFIG
    blob = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "curated" not in blob:
        raise KeyError(f"{path} has no `curated:` tree — see the restructure plan")
    return Curated.model_validate(blob["curated"])


@functools.lru_cache(maxsize=2)
def load_data_tree(config: Path | None = None) -> DataTree:
    """Where `source/` and `generated/` are."""
    path = config or CONFIG
    blob = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "data_tree" not in blob:
        raise KeyError(f"{path} has no `data_tree:` — see the restructure plan")
    return DataTree.model_validate(blob["data_tree"])


@functools.lru_cache(maxsize=2)
def load_generated(config: Path | None = None) -> Generated:
    """The generated tree. Cross-checked against `data_tree.generated` so the two roots
    cannot drift into naming different directories."""
    path = config or CONFIG
    blob = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "generated" not in blob:
        raise KeyError(f"{path} has no `generated:` tree — `output:` was retired into it")
    got = Generated.model_validate(blob["generated"])
    root = DataTree.model_validate(blob["data_tree"]).generated
    if got.base != root:
        raise ValueError(
            f"config.yaml disagrees with itself: data_tree.generated={root} but "
            f"generated.base={got.base}. They must name the same directory."
        )
    return got


class _Lazy:
    """`CURATED.waters.splits` without paying validation at import of an unrelated module.

    Importing `pipeline.common.curated` must not stat twenty files just because something wanted a
    type from this module. The first ATTRIBUTE access loads and validates; everything after
    is cached.
    """

    def __init__(self, loader):
        self._loader = loader

    def __getattr__(self, name: str):
        return getattr(self._loader(), name)

    def __repr__(self) -> str:
        return f"<lazy {self._loader.__name__} {CONFIG}>"

    # --- so a lazy DIRECTORY behaves like the Path it stands for -----------------------
    #
    # `SOURCE` is meant to be usable anywhere a Path is. Without these, `str(SOURCE)` gives
    # the repr above and `Path(str(SOURCE))` silently becomes a relative path named
    # "<lazy ...>" — which is how the bundler ended up looking for its roster in a directory
    # that could never exist, and reporting success with five tables missing.

    def _dir(self):
        got = self._loader()
        return got.source if hasattr(got, "source") else got

    def __truediv__(self, other):
        """So `SOURCE / "roster.json"` reads naturally where SOURCE is a directory."""
        return self._dir() / other

    def __fspath__(self) -> str:
        return str(self._dir())

    def __str__(self) -> str:
        return str(self._dir())


#: The curated tree. Import this, not the loader.
CURATED = _Lazy(load)

#: `SOURCE / "bc_hydrometric_stations.json"` — fetched data, addressed through config so a
#: fetcher and a reader cannot disagree about where a file went.
SOURCE = _Lazy(load_data_tree)


#: `GENERATED.tiles`, `GENERATED.require_build()` — computed data, addressed through config
#: so a writer and a reader cannot disagree about where a build went.
GENERATED = _Lazy(load_generated)


def generated(*parts: str) -> Path:
    """A path under `data/generated/`, creating the directory. Nothing here is precious."""
    p = load_data_tree().generated.joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

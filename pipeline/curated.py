"""Where the curated data is — validated once, at import, by pydantic.

    from pipeline.curated import CURATED
    defs = load_split_defs(CURATED.splits)

WHY A MODEL AND NOT `get_path("curated", "splits")`.

`ProjectConfig.get_path` returns `Path()` — the current directory — for a key that does not
exist. No exception, no warning. Hand that to a loader and you get an empty list, and an
empty list of curated cuts looks exactly like a build with no cuts to make. That is not a
hypothetical: a `--splits` flag with no default silently dropped all 376 curated cuts from
three full builds, one of which was promoted, and nothing anywhere looked wrong.

pydantic's `FilePath` and `DirectoryPath` refuse to construct if the path is not there. So
the failure moves from "a quiet wrong answer, three builds later" to "the process will not
start, and names the key". That is the entire argument for the dependency, and pydantic is
already in `requirements.txt` for the parsing models.

THREE KINDS OF PATH, AND THEY VALIDATE DIFFERENTLY:

    authored        FilePath / DirectoryPath. A human typed it; if it is missing, something
                    is deeply wrong and the process must stop.
    reviewed        FilePath where built, `None` where not. `stock_match.json` and
                    `chart_match.json` do not exist yet and are declared as null on purpose,
                    so the day they land there is one place to point at them.
    optional        `Path` with no existence check — nothing today, kept for the shape.

WHAT THIS IS NOT. It is not a general config layer. `ProjectConfig` still owns `output:`,
`llm:` and the rest; this covers the tree where a wrong path costs human work rather than
CPU. Widening it is fine, but the value here is the strictness, and strictness is only
affordable where every file genuinely must exist.
"""

from __future__ import annotations

import functools
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, DirectoryPath, FilePath, field_validator

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config.yaml"


def _absolute(v: object) -> object:
    """Resolve against the repo root, so nothing depends on the current directory.

    `pipeline/dfo_salmon/match.py` used a bare `Path("pipeline/splits.json")`, which worked
    only because everything happens to be run from the repo root. A path that is correct
    from one directory and silently wrong from another is the same class of bug as a missing
    one, and harder to see.
    """
    if isinstance(v, str):
        p = Path(v)
        return p if p.is_absolute() else ROOT / p
    return v


class Entries(BaseModel):
    """The two per-region regulation corpora. Same KIND of thing, separate models."""

    model_config = ConfigDict(frozen=True)

    synopsis: DirectoryPath
    dfo_salmon: DirectoryPath

    _abs = field_validator("*", mode="before")(_absolute)


class Matches(BaseModel):
    """Machine-produced, human-reviewed. `None` means "declared, not built yet"."""

    model_config = ConfigDict(frozen=True)

    gauge: FilePath | None = None
    waterbody_type: FilePath | None = None
    stocking: FilePath | None = None
    charts: FilePath | None = None

    _abs = field_validator("*", mode="before")(_absolute)


class Curated(BaseModel):
    """Every path whose loss costs human time rather than CPU."""

    model_config = ConfigDict(frozen=True)

    base: DirectoryPath
    splits: FilePath
    name_variants: FilePath
    overrides: FilePath
    areas: FilePath
    added_lakes: FilePath
    added_streams: FilePath
    gauge_review: FilePath
    entries: Entries
    matches: Matches

    _abs = field_validator("base", "splits", "name_variants", "overrides", "areas",
                           "added_lakes", "added_streams", "gauge_review",
                           mode="before")(_absolute)


@functools.lru_cache(maxsize=1)
def load(config: Path | None = None) -> Curated:
    """The validated tree. Cached, because validation stats every file.

    Raises `pydantic.ValidationError` naming the offending key when a path is wrong — which
    is the whole point, and is why nothing here has a fallback default.
    """
    path = config or CONFIG
    blob = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "curated" not in blob:
        raise KeyError(f"{path} has no `curated:` tree — see pipeline/docs/16")
    return Curated.model_validate(blob["curated"])


class _Lazy:
    """`CURATED.splits` without paying validation at import of an unrelated module.

    Importing `pipeline.curated` must not stat twelve files just because something wanted a
    type from this module. The first ATTRIBUTE access loads and validates; everything after
    is cached.
    """

    def __getattr__(self, name: str):
        return getattr(load(), name)

    def __repr__(self) -> str:
        return f"<CURATED {CONFIG}>"


#: The tree. Import this, not the loader.
CURATED = _Lazy()

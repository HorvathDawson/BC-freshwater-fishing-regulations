"""Shared IO + parsing helpers for the parse pipeline — the single home for the small utilities that
were duplicated across export / dispatch / ingest / validate / synth.

STRICT LEAF: this module imports only stdlib + `entry_models`. It must never import
prompts/export/ingest/dispatch (that keeps the package's dependency graph acyclic).

Groups:
- Paths        : `default_work_dir`, `entries_dir`, `region_ids`
- LLM output   : `parse_response` (→ [{index, entry}]), `extract_json_obj` (reviewer {verdict, issues})
- Batches      : `load_batch_items`, `load_all_batch_items`
- EntryFiles   : `read_entryfile`, `read_entries_dir`, `load_existing_entry_regs`,
                 `load_existing_entry_ids`, `write_entryfile` (atomic, via the model), `atomic_write`
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Iterable

from pipeline.regs.parsing.entry_models import Entry, EntryFile
from pipeline.common.curated import CURATED, GENERATED, SOURCE
from pipeline.common.curated import REPO_ROOT

_ROOT = REPO_ROOT


# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #

def default_work_dir() -> Path:
    """The parse work dir (batches/responses/reviews) — `<generated>/regs/parse`, unrelated to
    the FWA graph artifacts. Transient: run_parse.sh deletes it on a fresh run, and every
    verdict that matters is written back onto the EntryFiles, which are curated."""
    return GENERATED.regs.parse


def entries_dir() -> Path:
    """The checked-in EntryFiles dir — the single source of truth for parsed entries.

    THE ONE PLACE THIS IS RESOLVED. Four modules used to re-derive it as
    `Path(__file__).parent / "entries"`, so the corpus had five definitions of where it
    lived and moving it would have moved only one of them. They all call this now.
    """
    return CURATED.regulations.entries.catalogue


def entries_root() -> Path:
    """The directory the per-source EntryFile directories live under.

    Derived from `entries_dir()` rather than configured separately, so the two cannot name
    different places. There is no `curated.regulations.entries.root` for the same reason
    there is no second definition of anything else here.
    """
    return entries_dir().parent


def entry_sources() -> list[tuple[str, Path]]:
    """`(name, dir)` for every EntryFile source under the root, synopsis first.

    A SOURCE IS A DIRECTORY OF EntryFiles, and this checks rather than assumes: the DFO
    salmon directory sits under the same root and holds `region-*.json` files of a
    completely different shape (`waters`, `locations`, `scopes` — curated DFO data that has
    not been converted to entries yet). Globbing every subdirectory would read those as
    EntryFiles, find no `entries` key, and quietly contribute nothing. So a directory
    qualifies only if it actually contains entries, and `skipped_sources()` says which did
    not, because a source that silently contributes nothing is the failure this guards.

    Synopsis leads because it is the corpus everything else is a supplement to; the rest
    follow in name order so the merge is deterministic.
    """
    root, first = entries_root(), entries_dir()
    out: list[tuple[str, Path]] = []
    for d in sorted(root.iterdir(), key=lambda p: (p != first, p.name)):
        if d.is_dir() and _holds_entries(d):
            out.append((d.name, d))
    return out


def skipped_sources() -> list[str]:
    """Directories under the entries root that are NOT EntryFile sources, with a reason."""
    root, out = entries_root(), []
    for d in sorted(root.iterdir()):
        if not d.is_dir() or _holds_entries(d):
            continue
        files = list(d.glob("region-*.json"))
        out.append(f"{d.name}: "
                   + ("no region-*.json" if not files
                      else f"{len(files)} region file(s), none carrying an `entries` key"))
    return out


def _holds_entries(d: Path) -> bool:
    for p in sorted(d.glob("region-*.json")):
        try:
            if json.loads(p.read_text(encoding="utf-8")).get("entries"):
                return True
        except (OSError, ValueError):
            continue
    return False


def read_all_entries() -> dict[str, dict]:
    """Every entry from every source, merged — what a CONSUMER of the corpus reads.

    NOT what the parser reads. `entries_dir()` is the synopsis directory and stays that way:
    every tool that re-parses, prunes, backfills or remaps is about the synopsis and writes
    back into it, so pointing those at a merged view would have them write another source's
    entries into synopsis files.

    AN ID COLLISION IS FATAL, not namespaced. Namespacing would rename ids that are already
    written into reach runs and bundles, and an id that changes meaning between builds is
    the one thing `entry_id` may never do. Two sources claiming the same id is a curation
    mistake with two curators behind it, and it should stop the build and name them both.
    """
    merged: dict[str, dict] = {}
    origin: dict[str, str] = {}
    for name, d in entry_sources():
        for eid, entry in read_entries_dir(d).items():
            if eid in merged:
                raise SystemExit(
                    f"entry_id {eid!r} is claimed by two sources: {origin[eid]!r} and "
                    f"{name!r}. Ids are written into reach runs and bundles, so one of them "
                    f"has to be renamed at the source rather than resolved here.")
            merged[eid] = entry
            origin[eid] = name
    return merged


def region_ids(dir_: Path | None = None) -> list[str]:
    """Region ids (e.g. ['1','2',…]) for which an EntryFile exists."""
    d = dir_ or entries_dir()
    return [p.stem.split("region-")[1] for p in sorted(d.glob("region-*.json"))]


# --------------------------------------------------------------------------- #
# LLM output parsing (fenced ```json``` + the CLI's {result} envelope)
# --------------------------------------------------------------------------- #

def _unwrap_envelope(text: str) -> str:
    """The `claude` CLI (`--output-format json`) wraps the reply in `{... "result": "<reply>"}`; raw
    modes print the reply directly. Return the inner reply either way."""
    t = (text or "").strip()
    try:
        env = json.loads(t)
        if isinstance(env, dict) and "result" in env:
            return str(env["result"]).strip()
    except json.JSONDecodeError:
        pass
    return t


def _strip_fences(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("```"):
        lines = t.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        t = "\n".join(lines).strip()
    return t


def parse_response(text: str) -> list[dict]:
    """A parse response → a list of `{index, entry}` objects. Tolerates the CLI envelope and ```fences```;
    accepts a JSON array, a single `{index, entry}`, or a bare object with an `entry`."""
    data = json.loads(_strip_fences(_unwrap_envelope(text)))
    if isinstance(data, dict) and "entry" in data:
        return [data]
    if not isinstance(data, list):
        raise ValueError(f"expected a JSON array of {{index, entry}}, got {type(data).__name__}")
    return data


def extract_json_obj(text: str) -> dict:
    """A single JSON object (the reviewer's `{verdict, issues}`) from CLI/fenced output; {} on failure."""
    try:
        obj = json.loads(_strip_fences(_unwrap_envelope(text)))
        return obj if isinstance(obj, dict) else {}
    except json.JSONDecodeError:
        return {}


# --------------------------------------------------------------------------- #
# Batch files
# --------------------------------------------------------------------------- #

def load_batch_items(batch_path: str | Path) -> dict[int, dict]:
    """index -> batch item ({entry_id, name, region, raw_regs, bindable_ids, …})."""
    data = json.loads(Path(batch_path).read_text(encoding="utf-8"))
    return {it["index"]: it for it in data.get("items", [])}


def load_all_batch_items(batches_dir: Path) -> dict[int, dict]:
    """Merge the item maps of every batch file in a dir."""
    items: dict[int, dict] = {}
    for p in sorted(Path(batches_dir).glob("batch_*.json")):
        items.update(load_batch_items(p))
    return items


# --------------------------------------------------------------------------- #
# EntryFiles
# --------------------------------------------------------------------------- #

def read_entryfile(path: Path) -> dict[str, dict]:
    """One region EntryFile -> {entry_id: entry_dict}. Missing file -> {}."""
    if not Path(path).exists():
        return {}
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {e["entry_id"]: e for e in data.get("entries", [])}


def read_entries_dir(dir_: Path | None = None) -> dict[str, dict]:
    """Every entry across all region EntryFiles -> {entry_id: entry_dict}."""
    d = dir_ or entries_dir()
    out: dict[str, dict] = {}
    for p in sorted(Path(d).glob("region-*.json")):
        out.update(read_entryfile(p))
    return out


def load_existing_entry_regs(dir_: Path | None = None) -> dict[str, str]:
    """entry_id -> regs_verbatim for every entry already on disk (used by the export's change filters)."""
    return {eid: e.get("regs_verbatim", "") for eid, e in read_entries_dir(dir_).items()}


def load_existing_entry_ids(dir_: Path | None = None) -> set[str]:
    """entry_ids already present in the checked-in EntryFiles."""
    return set(read_entries_dir(dir_))


def atomic_write(path: Path, text: str) -> None:
    """Write via a temp file + os.replace so a crash never leaves a half-written (corrupt) file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def write_entryfile(path: Path, region: str, entries: Iterable) -> None:
    """Atomically write one region EntryFile through the `EntryFile` model (canonical serialization,
    sorted by entry_id). `entries` may be `Entry` objects or plain dicts. Routing every write through
    the model is the guard against silently persisting an off-schema entry."""
    coerced = [e if isinstance(e, Entry) else Entry(**e) for e in entries]
    ef = EntryFile(region=region, entries=sorted(coerced, key=lambda e: e.entry_id))
    atomic_write(Path(path), ef.model_dump_json(indent=2))

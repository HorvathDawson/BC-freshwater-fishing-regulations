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
from pipeline.common.curated import CURATED, SOURCE
from pipeline.common.curated import REPO_ROOT

_ROOT = REPO_ROOT


# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #

def default_work_dir() -> Path:
    """The parse work dir (batches/responses/reviews) — a top-level `<root>/output/parse`, unrelated to
    the FWA graph artifacts."""
    return _ROOT / "output" / "parse"


def entries_dir() -> Path:
    """The checked-in EntryFiles dir — the single source of truth for parsed entries.

    THE ONE PLACE THIS IS RESOLVED. Four modules used to re-derive it as
    `Path(__file__).parent / "entries"`, so the corpus had five definitions of where it
    lived and moving it would have moved only one of them. They all call this now.
    """
    return CURATED.regulations.entries.synopsis


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

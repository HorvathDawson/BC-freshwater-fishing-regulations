"""Shared IO + parsing helpers for the parse pipeline — the single home for the small utilities that
were duplicated across export / dispatch / ingest / validate.

STRICT LEAF: this module imports only stdlib (and `catalogue`, lazily, to write). It must never import
prompts/export/ingest/dispatch (that keeps the package's dependency graph acyclic).

Groups:
- Paths         : `default_work_dir`, `entries_dir`, `region_ids`
- LLM output    : `parse_response` (→ [{index, entry}]), `extract_json_obj` (reviewer {verdict, issues})
- Batches       : `load_batch_items`, `load_all_batch_items`
- Reviews       : `read_reviews`, `read_review_findings` (the work dir's reviews/, joined to entry_ids)
- Region files  : `read_entryfile`, `read_entries_dir`, `load_existing_entry_regs`,
                  `load_existing_entry_ids`, `write_entryfile` (atomic, via the model), `atomic_write`

"Region file" = one `region-*.json` of catalogue entries under `entries_dir()`.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Iterable

from pipeline.common.curated import CURATED, GENERATED, SOURCE
from pipeline.common.curated import REPO_ROOT

_ROOT = REPO_ROOT


# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #

def default_work_dir() -> Path:
    """The parse work dir (batches/responses/reviews) — `<generated>/regs/parse`, unrelated to
    the FWA graph artifacts. Transient: an export replaces its batches and drops the responses
    and reviews that no longer describe them.

    REVIEW FINDINGS LIVE HERE AND ONLY HERE. A catalogue entry has no field for a verdict and
    never will: a review is a finding about one parse, and `repass` reads it from `reviews/`."""
    return GENERATED.regs.parse


def entries_dir() -> Path:
    """The checked-in catalogue region files — the single source of truth for parsed entries.

    THE ONE PLACE THIS IS RESOLVED. Four modules used to re-derive it as
    `Path(__file__).parent / "entries"`, so the corpus had five definitions of where it
    lived and moving it would have moved only one of them. They all call this now.
    """
    return CURATED.regulations.entries.catalogue


def entries_root() -> Path:
    """The directory the per-source entry directories live under.

    Derived from `entries_dir()` rather than configured separately, so the two cannot name
    different places. There is no `curated.regulations.entries.root` for the same reason
    there is no second definition of anything else here.
    """
    return entries_dir().parent


def entry_sources() -> list[tuple[str, Path]]:
    """`(name, dir)` for every entry source under the root, synopsis first.

    A SOURCE IS A DIRECTORY OF REGION FILES CARRYING `entries`, and this checks rather than
    assumes: the DFO salmon directory sits under the same root and holds `region-*.json` files
    of a completely different shape (`waters`, `locations`, `scopes` — curated DFO data that
    has not been converted to entries yet). Globbing every subdirectory would read those as
    entry files, find no `entries` key, and quietly contribute nothing. So a directory
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
    """Directories under the entries root that are NOT entry sources, with a reason."""
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
    """Region ids (e.g. ['1','2',…]) for which a region file exists."""
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
# Reviews — the second pass's findings, kept in the work dir
# --------------------------------------------------------------------------- #

#: The severities a repass acts on. `low` is a nit and is not worth a stronger model's credits.
REPASS_SEVERITIES = ("high", "medium")

#: What `dispatch` writes when a reviewer's reply is not a readable verdict. It is a FAILED
#: review, not a clean one: an unreadable reply used to be stored as `{verdict: "", issues: []}`,
#: which every reader took for "nothing wrong".
REVIEW_FAILED = "review_failed"


def read_reviews(work_dir: Path) -> dict[int, dict]:
    """batch id -> `{"state", "issues", "review"}` for every `reviews/batch_NNN.review.json`.

    `state` is one of:
      `pass` / `changes_requested` — the reviewer's verdict
      `failed`  — the reply could not be read (see `REVIEW_FAILED`); it says nothing either way
      `stale`   — the batch's response is newer than the review, so it reviewed another parse
    """
    reviews_dir = Path(work_dir) / "reviews"
    responses_dir = Path(work_dir) / "responses"
    out: dict[int, dict] = {}
    for p in sorted(reviews_dir.glob("batch_*.review.json")):
        bid = int(p.name[len("batch_"):].split(".")[0])
        obj = json.loads(p.read_text(encoding="utf-8"))
        resp = responses_dir / f"batch_{bid:03d}.json"
        if obj.get("error") or obj.get("verdict") not in ("pass", "changes_requested"):
            state = "failed"
        elif resp.exists() and resp.stat().st_mtime > p.stat().st_mtime:
            state = "stale"
        else:
            state = obj["verdict"]
        out[bid] = {"state": state, "issues": list(obj.get("issues") or []), "review": p}
    return out


def read_review_findings(work_dir: Path,
                         severities: tuple[str, ...] = REPASS_SEVERITIES) -> dict[str, list[str]]:
    """entry_id -> the reviewer's findings on it, as hint lines for a re-parse.

    A review is written per BATCH and names a row by its `index`; the entry it is about is the
    batch item with that index. The join is done here, once, against the batch file the review
    was made from — `batches/batch_NNN.json` — because the index means nothing on its own.

    Only `pass`/`changes_requested` reviews count; a failed or stale review is no finding. An
    issue whose index is not in its batch is an error, not something to skip: it means the
    batch was replaced under the review, and acting on it would re-parse the wrong water.
    """
    work_dir = Path(work_dir)
    sev = set(severities)
    out: dict[str, list[str]] = {}
    for bid, rv in sorted(read_reviews(work_dir).items()):
        if rv["state"] not in ("pass", "changes_requested"):
            continue
        wanted = [i for i in rv["issues"] if isinstance(i, dict) and i.get("severity") in sev]
        if not wanted:
            continue
        batch_path = work_dir / "batches" / f"batch_{bid:03d}.json"
        if not batch_path.exists():
            raise ValueError(f"{rv['review'].name}: no {batch_path.name} to join its rows to")
        by_index = {it["index"]: it["entry_id"] for it in load_batch_items(batch_path).values()}
        for iss in wanted:
            eid = by_index.get(iss.get("index"))
            if eid is None:
                raise ValueError(f"{rv['review'].name}: issue index {iss.get('index')!r} is not a "
                                 f"row of {batch_path.name}")
            out.setdefault(eid, []).append(
                f"[{iss['severity']}] {iss.get('problem', '')}"
                + (f" -> fix: {iss['fix']}" if iss.get("fix") else ""))
    return out


# --------------------------------------------------------------------------- #
# Region files (catalogue entries)
# --------------------------------------------------------------------------- #

def read_entryfile(path: Path) -> dict[str, dict]:
    """One region file -> {entry_id: entry_dict}. Missing file -> {}."""
    if not Path(path).exists():
        return {}
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {e["entry_id"]: e for e in data.get("entries", [])}


def read_entries_dir(dir_: Path | None = None) -> dict[str, dict]:
    """Every entry across all region files -> {entry_id: entry_dict}."""
    d = dir_ or entries_dir()
    out: dict[str, dict] = {}
    for p in sorted(Path(d).glob("region-*.json")):
        out.update(read_entryfile(p))
    return out


def load_existing_entry_regs(dir_: Path | None = None) -> dict[str, str]:
    """entry_id -> regs_verbatim for every entry already on disk (used by the export's change filters)."""
    return {eid: e.get("regs_verbatim", "") for eid, e in read_entries_dir(dir_).items()}


def load_existing_entry_ids(dir_: Path | None = None) -> set[str]:
    """entry_ids already present in the checked-in region files."""
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


def dump_entry(entry) -> dict:
    """ONE catalogue entry as it is stored. THE ONE SERIALISER — ingest and every repair tool go
    through it, so the files cannot drift apart by writer.

    `by_alias`: `while`/`except` are Python keywords held as `while_`/`except_`; dumped without it
    a rule is written under a name the JSON schema does not use. `exclude_defaults`: a default
    re-derives on load, so writing it back only buries what the rule actually says.

    EXCEPT A DISCRIMINATOR. A licensing record's `kind` is a Literal with a default, so
    `exclude_defaults` dropped it — and `kind` is the tag the union is read back by. Every
    entry with a licensing record was written in a shape that no longer loaded (88 of them
    when this was found). `kind` is restored wherever the full dump has it."""
    terse = json.loads(entry.model_dump_json(exclude_defaults=True, by_alias=True))
    full = json.loads(entry.model_dump_json(by_alias=True))
    _restore_kind(terse, full)
    return terse


def _restore_kind(terse, full) -> None:
    if isinstance(terse, dict) and isinstance(full, dict):
        if "kind" in full and "kind" not in terse:
            terse["kind"] = full["kind"]
        for k, v in terse.items():
            if k in full:
                _restore_kind(v, full[k])
    elif isinstance(terse, list) and isinstance(full, list) and len(terse) == len(full):
        for a, b in zip(terse, full):
            _restore_kind(a, b)


def _indent_of(path: Path) -> int:
    """The indent a region file is already written with, so a write changes only what changed.
    The files are indent 1, except region-7 (indent 2); a new file is indent 1."""
    if not path.exists():
        return 1
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2:
        return 1
    return (len(lines[1]) - len(lines[1].lstrip(" "))) or 1


def write_entryfile(path: Path, region: str, entries: Iterable) -> None:
    """Atomically write one region file of catalogue entries.

    ORDER: a file that is sorted by entry_id (every water table) stays sorted. The zone files
    (`region-7a`, `region-7b`, `region-provincial`) are in the book's order, not the id's, and
    keep the order they are given in — sorting them would rewrite every line of the file.

    `entries` may be `CatalogueEntry` objects, which are serialised by `dump_entry`, or plain
    dicts, which are written exactly as given — an untouched neighbour read from the file stays
    byte-for-byte what it was, rather than being re-serialised into a different spelling.

    THE OUTPUT IS WHAT IS VALIDATED, as a whole file through `CatalogueFile`. Validating the
    objects before serialising proved only that the input was good: it passed while
    `dump_entry` was writing licensing records that could not be read back."""
    from pipeline.regs.parsing.catalogue import CatalogueEntry, CatalogueFile
    path = Path(path)
    rows = [dump_entry(e) if isinstance(e, CatalogueEntry) else e for e in entries]
    on_disk = list(read_entryfile(path))
    if on_disk == sorted(on_disk):
        rows.sort(key=lambda e: e.get("entry_id", ""))
    doc = {"region": region, "entries": rows}
    CatalogueFile.model_validate(json.loads(json.dumps(doc)))
    atomic_write(path, json.dumps(doc, indent=_indent_of(path), ensure_ascii=False) + "\n")

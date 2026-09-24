"""Ingest agent responses into the checked-in catalogue entry files — the validation gate.

Takes the JSON a parsing agent produced for one or more batches, runs every entry through the SAME
gate the agent was told to run (`validate_catalogue`), and writes only what passes.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.ingest_catalogue \
        batches/batch_000.json responses/batch_000.json --dry-run

Two rules that are not negotiable, both learned the hard way:

* **`regs_verbatim` is taken from the BATCH, never from the model.** The agent writes both the
  passage and the rules that quote it, so a model-supplied passage makes the chain of custody
  self-referential — an invented sentence validates against its own invention. Two did.
* **Nothing partial is written.** An entry either validates whole or is reported and left out. A
  half-ingested entry is a water with some of its regulations, which is indistinguishable from a
  water with no regulation.

And one guard, because a re-parse replaces an entry wholesale and there is no `locked` any more:

* **A curator's edit is not overwritten.** Every entry ingest writes is recorded in a ledger in the
  work dir (`ingested.json`: entry_id -> digest of the entry as written). Before replacing an
  entry already on disk, ingest compares the file's copy with the ledger. If they differ — a
  curator bound a reach, added a tributary exclude, or anything else since the parse — or the
  ledger has no record of it, the entry is KEPT and reported, and nothing about it is written.
  `--replace-edited` overrides that after you have read the report (the curated files are in
  git, so `git diff` shows what the replace changed).

  THE LEDGER IS SEEDED, NOT GROWN FROM NOTHING. With no `ingested.json` every entry is one "the
  ledger never saw", so the first repass would keep all of them. `--seed-ledger` records every
  entry as it is on disk NOW — declaring the checked-in corpus the curated truth — and from then
  on only an edit made after the seed is kept. `run_parse.sh seed-ledger` runs it (no credits).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

from pipeline.regs.parsing import io
from pipeline.regs.parsing.catalogue import CatalogueEntry
from pipeline.regs.parsing.io import dump_entry
from pipeline.regs.parsing.validate_catalogue import check_entry, squash


def load_batch(paths: list[str]) -> dict[str, dict]:
    """{key: batch item} keyed by BOTH `entry_id` and `#index`.

    The agent is told to copy each item's `index` back verbatim, and that is the reliable join:
    `entry_id` is a value the model retypes, so keying on it alone makes a typo look like an
    invented entry. A batch file is ours — `{batch, rows_digest, items: [...]}` — and nothing
    else is read."""
    items: dict[str, dict] = {}
    for p in paths:
        data = json.loads(Path(p).read_text(encoding="utf-8"))
        for it in data["items"]:
            items.setdefault(str(it["entry_id"]), it)
            items[f"#{it['index']}"] = it
    return items


def response_rows(path: str | Path) -> list[dict]:
    """A response file -> its entries, each carrying `_batch_index` (the join key).

    ONE SHAPE: the array `dispatch` writes, `[{"index": N, "entry": {...}}, ...]`. Anything else
    is refused with the file named — a response in another shape was produced by something
    other than this pipeline, and guessing at it is how a prose-era file was once ingested as
    catalogue output."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{Path(path).name}: a response is a JSON array of {{index, entry}}")
    out: list[dict] = []
    for row in data:
        if not (isinstance(row, dict) and isinstance(row.get("entry"), dict)
                and isinstance(row.get("index"), int)):
            raise ValueError(f"{Path(path).name}: every row must be {{\"index\": int, "
                             f"\"entry\": {{...}}}} — got {str(row)[:80]!r}")
        out.append(dict(row["entry"], _batch_index=row["index"]))
    return out


def _passthrough(item: dict) -> dict:
    """The entry fields that are facts about the synopsis ROW, exactly as the batch carries them."""
    return {
        "entry_id": str(item["entry_id"]),
        "regs_verbatim": str(item["raw_regs"]),
        "name": item.get("name") or "",
        "display_name": item.get("display_name") or "",
        "region": item.get("region") or "",
        "symbols": list(item.get("symbols") or []),
        "source_pages": list(item.get("pages") or []),
        "matched": ([item["item_id"], *(item.get("also_item_ids") or [])]
                    if item.get("item_id") else []),
    }


def ingest(candidates: list[dict], batch: dict[str, dict]) -> tuple[dict[str, CatalogueEntry],
                                                                    list[str]]:
    accepted: dict[str, CatalogueEntry] = {}
    problems: list[str] = []
    for data in candidates:
        eid = str(data.get("entry_id") or "<no entry_id>")
        idx = data.pop("_batch_index", None)
        item = batch.get(f"#{idx}") if idx is not None else batch.get(eid)
        if item is not None and item.get("entry_id"):
            # IDENTITY COMES FROM THE BATCH, exactly like regs_verbatim. The agent retypes
            # entry_id and 375 of 1099 came back without their `@MU` suffix — close enough to
            # look right, different enough that `--skip-existing` no longer recognises the row.
            # 368 entries were stored under the truncated id, so a resume re-parsed waters that
            # were already done and would have written each one twice under two ids.
            eid = str(item["entry_id"])
            data["entry_id"] = eid
        if item is None:
            problems.append(f"{eid}: not in the batch — an entry_id was invented or altered")
            continue
        # EVERY FACT ABOUT THE ROW COMES FROM THE BATCH — the model gets no vote on any of them,
        # and nothing it wrote in these fields survives, even where the batch is empty. Each was
        # left to the model once and came back wrong: 843 of 1,021 names retyped (66 losing the
        # parenthetical that carries the reach), Classified kept on 20 of 68 rows, Stocked on 11
        # of 304. `symbols` are the printed glyphs and nothing else: a water the TEXT calls
        # classified is a fact for the rules, not a symbol.
        data = json.loads(json.dumps(data))     # deep copy: the split check rewrites in place
        data.update(_passthrough(item))
        source = data["regs_verbatim"]
        # `item` carries the boundary menu, so this also checks every split id and rewrites an
        # alias to its canonical spelling — the same gate the agent runs on itself.
        entry, errors = check_entry(data, source, item)
        fatal = [e for e in errors if not e.startswith("ADVISORY")]
        for e in errors:
            problems.append(f"{'ADVISORY ' if e.startswith('ADVISORY') else ''}{eid}: {e}")
        if entry is not None and not fatal:
            accepted[eid] = entry
    return accepted, problems


def default_ledger() -> Path:
    """Where ingest records what it wrote — beside the batches and reviews it was made from."""
    return io.default_work_dir() / "ingested.json"


def digest(entry: dict) -> str:
    return hashlib.sha256(json.dumps(entry, sort_keys=True, ensure_ascii=False)
                          .encode("utf-8")).hexdigest()


def _read_ledger(path: Path) -> dict[str, str]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def seed_ledger(out_dir: Path, ledger: Path) -> int:
    """Record every entry in `out_dir` as ingest's own write, exactly as it is on disk now.

    Digested from the file's dicts, as `write` compares them, so an entry left untouched after the
    seed is replaceable by a repass and one edited after it is KEPT. Replaces any ledger there is:
    seeding declares the checked-in corpus the truth. Returns the number of entries recorded."""
    record = {eid: digest(e) for eid, e in io.read_entries_dir(Path(out_dir)).items()}
    io.atomic_write(ledger, json.dumps(record, indent=1, sort_keys=True) + "\n")
    return len(record)


def write(accepted: dict[str, CatalogueEntry], out_dir: Path, dry_run: bool = False, *,
          ledger: Path, replace_edited: bool = False) -> tuple[dict[str, int], list[str]]:
    """Merge into region files by the entry's own `region`, replacing an entry of the same id —
    unless the copy on disk is not the one ingest last wrote (see the module docstring).

    Returns ({file name: entries written}, [entry ids KEPT because they were edited])."""
    record = _read_ledger(ledger)
    by_region: dict[str, list[CatalogueEntry]] = defaultdict(list)
    for e in accepted.values():
        by_region[e.region or "unknown"].append(e)

    written: dict[str, int] = {}
    kept: list[str] = []
    for region, entries in sorted(by_region.items()):
        path = out_dir / f"region-{region}.json"
        merged: dict[str, object] = dict(io.read_entryfile(path))   # file order, dicts as-is
        n = 0
        for e in entries:
            on_disk = merged.get(e.entry_id)
            if on_disk is not None and not replace_edited \
                    and record.get(e.entry_id) != digest(on_disk):
                kept.append(e.entry_id)
                continue
            merged[e.entry_id] = e
            n += 1
        # write_entryfile validates the WHOLE file as written — a duplicate entry_id or a
        # broken neighbour is a failure of the file, and writing it would ship the break.
        if not dry_run and n:
            io.write_entryfile(path, region, merged.values())
            for e in entries:
                if merged[e.entry_id] is e:
                    record[e.entry_id] = digest(dump_entry(e))
        written[path.name] = n
    if not dry_run and any(written.values()):
        io.atomic_write(ledger, json.dumps(record, indent=1, sort_keys=True) + "\n")
    return written, kept


def run(batch_paths: list[str], response_paths: list[str], out_dir: str,
        dry_run: bool = False, ledger: Path | None = None, replace_edited: bool = False) -> int:
    batch = load_batch(batch_paths)
    candidates: list[dict] = []
    for p in response_paths:
        candidates += response_rows(p)
    accepted, problems = ingest(candidates, batch)
    for p in problems:
        print(("WARN " if p.startswith("ADVISORY") else "FAIL ") + p)

    written, kept = write(accepted, Path(out_dir), dry_run=dry_run,
                          ledger=ledger or default_ledger(), replace_edited=replace_edited)
    rejected = len(candidates) - len(accepted)
    print(f"\n{len(accepted)}/{len(candidates)} entries accepted"
          + (f", {rejected} rejected" if rejected else ""))
    for name, n in sorted(written.items()):
        print(f"  {'would write' if dry_run else 'wrote'} {n} entries -> {name}")
    if kept:
        print(f"\nKEPT {len(kept)} entr(y/ies) — the curated copy is not the one ingest last "
              f"wrote (edited since, or parsed before the ledger), so the re-parse was NOT "
              f"applied:")
        for eid in kept:
            print(f"  {eid}")
        print("  Compare each with its response; re-run with --replace-edited to overwrite them.")
    return 1 if (rejected or kept) else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    # `action="append"` alone takes ONE value per flag, and run_parse.sh passes a shell GLOB — so
    # `--batch batches/batch_*.json` handed argparse 46 paths, it consumed one, and the run died on
    # "unrecognized arguments" AFTER the whole parse had been paid for. `extend` + `nargs="+"`
    # accepts both a glob and a repeated flag.
    ap.add_argument("--batch", action="extend", nargs="+",
                    help="batch file(s) from the exporter; a glob is fine")
    ap.add_argument("--response", action="extend", nargs="+",
                    help="candidate JSON from the agent; a glob is fine")
    ap.add_argument("--out", default="data/curated/regulations/entries/catalogue")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--ledger", type=Path, default=None,
                    help="what ingest last wrote, per entry (default: <work dir>/ingested.json)")
    ap.add_argument("--replace-edited", action="store_true",
                    help="replace entries even where the curated copy differs from what ingest "
                    "last wrote — overwrites curator edits; read the KEPT report first")
    ap.add_argument("--seed-ledger", action="store_true",
                    help="record every entry now in --out as ingest's own write (see the module "
                    "docstring), and ingest nothing")
    a = ap.parse_args()
    if a.seed_ledger:
        if a.batch or a.response:
            ap.error("--seed-ledger ingests nothing; it takes no --batch/--response")
        ledger = a.ledger or default_ledger()
        print(f"seeded {ledger}: {seed_ledger(Path(a.out), ledger)} entries recorded as ingested")
        sys.exit(0)
    if not (a.batch and a.response):
        ap.error("--batch and --response are required (or --seed-ledger)")
    sys.exit(run(a.batch, a.response, a.out, dry_run=a.dry_run, ledger=a.ledger,
                 replace_edited=a.replace_edited))


if __name__ == "__main__":
    main()

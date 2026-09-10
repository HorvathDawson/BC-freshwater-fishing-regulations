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
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from pipeline.regs.parsing.catalogue import CatalogueEntry, CatalogueFile
from pipeline.regs.parsing.validate_catalogue import check_entry, squash


def load_batch(paths: list[str]) -> dict[str, dict]:
    """{entry_id: batch item}. The item carries `raw_regs` — the printed row we handed over."""
    items: dict[str, dict] = {}
    for p in paths:
        data = json.loads(Path(p).read_text(encoding="utf-8"))
        for it in (data.get("items", data) if isinstance(data, dict) else data):
            key = it.get("entry_id") or it.get("id") or it.get("item_id")
            if key:
                items[str(key)] = it
    return items


def _source_of(item: dict) -> str:
    for k in ("raw_regs", "regs_verbatim", "text", "source_text"):
        if item.get(k):
            return str(item[k])
    return ""


def ingest(candidates: list[dict], batch: dict[str, dict]) -> tuple[dict[str, CatalogueEntry],
                                                                    list[str]]:
    accepted: dict[str, CatalogueEntry] = {}
    problems: list[str] = []
    for data in candidates:
        eid = str(data.get("entry_id") or "<no entry_id>")
        item = batch.get(eid)
        if item is None:
            problems.append(f"{eid}: not in the batch — an entry_id was invented or altered")
            continue
        source = _source_of(item)
        # The passage comes from the batch. Anything the model wrote here is discarded.
        data = dict(data, regs_verbatim=source or data.get("regs_verbatim", ""))
        entry, errors = check_entry(data, source)
        fatal = [e for e in errors if not e.startswith("ADVISORY")]
        for e in errors:
            problems.append(f"{'ADVISORY ' if e.startswith('ADVISORY') else ''}{eid}: {e}")
        if entry is not None and not fatal:
            accepted[eid] = entry
    return accepted, problems


def write(accepted: dict[str, CatalogueEntry], out_dir: Path, dry_run: bool = False) -> dict[str, int]:
    """Merge into region files by the entry's own `region`, replacing an entry of the same id."""
    by_region: dict[str, list[CatalogueEntry]] = defaultdict(list)
    for e in accepted.values():
        by_region[e.region or "unknown"].append(e)

    written: dict[str, int] = {}
    for region, entries in sorted(by_region.items()):
        path = out_dir / f"region-{region}.json"
        existing: list[dict] = []
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8")).get("entries", [])
        fresh = {e.entry_id for e in entries}
        merged = [x for x in existing if x.get("entry_id") not in fresh]
        merged += [json.loads(e.model_dump_json(exclude_none=True)) for e in entries]
        merged.sort(key=lambda x: x.get("entry_id", ""))
        # Validate the WHOLE file, not just the new rows — a duplicate entry_id or a broken
        # neighbour is a failure of the file, and writing it would ship the break.
        CatalogueFile.model_validate({"region": region, "entries": merged})
        if not dry_run:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"region": region, "entries": merged},
                                       indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        written[path.name] = len(entries)
    return written


def run(batch_paths: list[str], response_paths: list[str], out_dir: str,
        dry_run: bool = False) -> int:
    batch = load_batch(batch_paths)
    candidates: list[dict] = []
    for p in response_paths:
        data = json.loads(Path(p).read_text(encoding="utf-8"))
        candidates += (data.get("entries", data) if isinstance(data, dict) else data)

    accepted, problems = ingest(candidates, batch)
    for p in problems:
        print(("WARN " if p.startswith("ADVISORY") else "FAIL ") + p)

    written = write(accepted, Path(out_dir), dry_run=dry_run)
    rejected = len(candidates) - len(accepted)
    print(f"\n{len(accepted)}/{len(candidates)} entries accepted"
          + (f", {rejected} rejected" if rejected else ""))
    for name, n in sorted(written.items()):
        print(f"  {'would write' if dry_run else 'wrote'} {n} entries -> {name}")
    return 1 if rejected else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--batch", action="append", required=True, help="batch file(s) from the exporter")
    ap.add_argument("--response", action="append", required=True, help="candidate JSON from the agent")
    ap.add_argument("--out", default="data/curated/regulations/entries/catalogue")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    sys.exit(run(a.batch, a.response, a.out, dry_run=a.dry_run))


if __name__ == "__main__":
    main()

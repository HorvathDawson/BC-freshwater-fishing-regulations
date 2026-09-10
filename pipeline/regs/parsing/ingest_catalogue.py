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
    """{key: batch item} keyed by BOTH `entry_id` and `index`.

    The agent is told to copy each item's `index` back verbatim, and that is the reliable join:
    `entry_id` is a value the model retypes, so keying on it alone makes a typo look like an
    invented entry."""
    items: dict[str, dict] = {}
    for p in paths:
        data = json.loads(Path(p).read_text(encoding="utf-8"))
        for it in (data.get("items", data) if isinstance(data, dict) else data):
            for key in (it.get("entry_id"), it.get("id"), it.get("item_id")):
                if key:
                    items.setdefault(str(key), it)
            if it.get("index") is not None:
                items[f"#{it['index']}"] = it
    return items


_RETIRED_FIELDS = ("restriction_type", "details", "rule_text", "exempts_from", "display_location")


def is_stale(rows: list) -> bool:
    """True if this response was written by the RETIRED prose parser.

    Responses live in a work dir that survives between runs, and dispatch skips a batch that
    already has one — which is what makes a run resumable. It also means a response from a
    previous FORMAT era is silently reused: 22 files from the prose parser sat in the work dir and
    were ingested as if they were catalogue output, and 669 rules failed as "extra inputs are not
    permitted" with nothing pointing at the real cause. Cheap to detect, so detect it."""
    for row in rows:
        if not isinstance(row, dict):
            continue
        for r in ((row.get("entry", row) or {}).get("rules") or []):
            if isinstance(r, dict) and any(k in r for k in _RETIRED_FIELDS):
                return True
    return False


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
        idx = data.pop("_batch_index", None)
        item = batch.get(eid) or (batch.get(f"#{idx}") if idx is not None else None)
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
        source = _source_of(item)
        # The passage comes from the batch. Anything the model wrote here is discarded.
        data = json.loads(json.dumps(data))     # deep copy: the split check rewrites in place
        data["regs_verbatim"] = source or data.get("regs_verbatim", "")
        # `item` carries the boundary menu, so this also checks every split id and rewrites an
        # alias to its canonical spelling — the same gate the agent runs on itself.
        entry, errors = check_entry(data, source, item)
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
    stale: list[str] = []
    for p in response_paths:
        data = json.loads(Path(p).read_text(encoding="utf-8"))
        rows = data.get("entries", data) if isinstance(data, dict) else data
        if is_stale(rows):
            stale.append(Path(p).name)
            continue
        for row in rows:
            # The dispatcher writes the batch ENVELOPE: [{"index": N, "entry": {...}}]. Ingest used
            # to expect a bare entry, so every row read as "<no entry_id>" and a fully paid parse
            # ingested nothing. Accept both shapes; carry the index along as the join key.
            if isinstance(row, dict) and "entry" in row and isinstance(row["entry"], dict):
                entry = dict(row["entry"])
                if row.get("index") is not None:
                    entry.setdefault("_batch_index", row["index"])
                candidates.append(entry)
            else:
                candidates.append(row)

    if stale:
        print(f"SKIPPED {len(stale)} response file(s) written by the RETIRED prose parser — delete "
              f"them and re-parse those batches:\n  {', '.join(stale)}\n")
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
    # `action="append"` alone takes ONE value per flag, and run_parse.sh passes a shell GLOB — so
    # `--batch batches/batch_*.json` handed argparse 46 paths, it consumed one, and the run died on
    # "unrecognized arguments" AFTER the whole parse had been paid for. `extend` + `nargs="+"`
    # accepts both a glob and a repeated flag.
    ap.add_argument("--batch", action="extend", nargs="+", required=True,
                    help="batch file(s) from the exporter; a glob is fine")
    ap.add_argument("--response", action="extend", nargs="+", required=True,
                    help="candidate JSON from the agent; a glob is fine")
    ap.add_argument("--out", default="data/curated/regulations/entries/catalogue")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    sys.exit(run(a.batch, a.response, a.out, dry_run=a.dry_run))


if __name__ == "__main__":
    main()

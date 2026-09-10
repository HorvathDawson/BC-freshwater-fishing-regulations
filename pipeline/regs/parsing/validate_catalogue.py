"""Self-check a candidate catalogue parse — the SAME gate ingest runs.

Designed for the parsing agent to run on itself until clean, so a bad candidate never reaches a
human. Needs only the batch (which carries each item's printed text) and the candidate JSON.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.validate_catalogue \
        batch_000.json candidate.json

Exit 0 = every entry valid. 1 = at least one failed, with the reason printed.

Three layers, and the third is the one that matters:

  1. the MODEL — types, conditions, enums, arithmetic  (CatalogueRule)
  2. the ENTRY — each rule's verbatim inside its own regs_verbatim  (CatalogueEntry)
  3. THE SOURCE — regs_verbatim against the text the agent was GIVEN

Layer 2 alone is not chain of custody: the author writes both sides, so an invented sentence passes.
Two did, in the first authored pass. Layer 3 is what closes it.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Iterable

from pydantic import ValidationError

from pipeline.regs.parsing.catalogue import CatalogueEntry, RuleType, label

_DASH = dict.fromkeys(map(ord, "‐‑‒–—―−"), "-")


def squash(text: str) -> str:
    """Normalise away what carries no meaning: emphasis, bullets, blockquotes, dash variants."""
    t = text.translate(_DASH).replace("*", "")
    t = re.sub(r"(?m)^\s*[>|]\s?", " ", t)
    t = re.sub(r"(?m)^\s*[-•]\s+", " ", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return re.sub(r"\s*-\s*", "-", t)


def check_entry(entry_data: dict, source_text: str) -> tuple[CatalogueEntry | None, list[str]]:
    """Returns (entry, errors). `source_text` is the printed row the agent was given."""
    errors: list[str] = []
    try:
        entry = CatalogueEntry.model_validate(entry_data)
    except ValidationError as exc:
        for e in exc.errors():
            loc = ".".join(str(x) for x in e["loc"])
            errors.append(f"{loc}: {e['msg']}")
        return None, errors

    # LAYER 3 — the passage must be the text we handed over, not a rewrite of it.
    if source_text:
        if squash(entry.regs_verbatim) not in squash(source_text):
            errors.append(
                "regs_verbatim is not a contiguous run of the source row — it has been reworded, "
                "reordered or stitched. Quote the printed text.")

    for rule in entry.rules:
        # Every number must be in the rule's OWN sentence. This is what stops a limit being
        # attributed to a rule whose text never stated it.
        for field in ("take", "over_cm", "under_cm", "max_kmh", "max_power_kw", "per_daily",
                      "max_gap_mm", "min_gap_cm", "max_weight_kg"):
            value = getattr(rule, field)
            if value in (None, 0):
                continue
            printed = f"{value:g}" if isinstance(value, float) else str(value)
            if printed not in rule.verbatim:
                errors.append(
                    f"{rule.rule_id}: {field}={printed} does not appear in its own verbatim "
                    f"({rule.verbatim[:70]!r})")

        if rule.within and rule.within not in {r.rule_id for r in entry.rules}:
            errors.append(f"{rule.rule_id}: within={rule.within!r} names no rule in this entry")

        if not label(rule).strip():
            errors.append(f"{rule.rule_id}: generates an empty label")

    by_id = {r.rule_id: r for r in entry.rules}
    for rule in entry.rules:
        if rule.within and rule.take is not None:
            parent = by_id.get(rule.within)
            if parent and parent.take is not None and not parent.unlimited \
                    and rule.take > parent.take:
                errors.append(
                    f"{rule.rule_id}: takes {rule.take} inside a parent of {parent.take} — a "
                    f"sub-limit cannot exceed what it sits in. If it genuinely does, it REPLACES "
                    f"the parent for its species and is not a sub-limit.")

    # A row whose printed text is long but which produced one short rule has almost certainly
    # dropped something. Advisory, not fatal — some rows really are one sentence.
    if source_text and len(squash(source_text)) > 200 and len(entry.rules) == 1:
        errors.append(
            f"ADVISORY: {len(squash(source_text))} characters of source produced a single rule — "
            f"check for a restriction you have not carried across")

    return entry, errors


def _source_for(item: dict) -> str:
    for key in ("raw_regs", "regs_verbatim", "text", "source_text"):
        if item.get(key):
            return str(item[key])
    return ""


def run(batch_path: str, candidate_path: str) -> int:
    batch = json.loads(Path(batch_path).read_text(encoding="utf-8"))
    items = batch.get("items", batch) if isinstance(batch, dict) else batch
    by_id = {i.get("entry_id") or i.get("id"): i for i in items}

    candidate = json.loads(Path(candidate_path).read_text(encoding="utf-8"))
    entries = candidate.get("entries", candidate) if isinstance(candidate, dict) else candidate

    failed = 0
    for data in entries:
        eid = data.get("entry_id", "<no entry_id>")
        source = _source_for(by_id.get(eid, {}))
        if eid not in by_id:
            print(f"FAIL {eid}: not in the batch — an entry_id was invented or altered")
            failed += 1
            continue
        _, errors = check_entry(data, source)
        fatal = [e for e in errors if not e.startswith("ADVISORY")]
        for e in errors:
            print(f"{'WARN' if e.startswith('ADVISORY') else 'FAIL'} {eid}: {e}")
        if fatal:
            failed += 1

    total = len(entries)
    print(f"\n{total - failed}/{total} entries valid")
    return 1 if failed else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("batch")
    ap.add_argument("candidate")
    sys.exit(run(**vars(ap.parse_args())))


if __name__ == "__main__":
    main()

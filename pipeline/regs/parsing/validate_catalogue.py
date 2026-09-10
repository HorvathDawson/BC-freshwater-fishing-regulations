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

and, where the batch item is available, the REACH: every split id must be a cut-point on that
water (an invented one otherwise reaches the corpus and only surfaces later as a rule that
silently selects nothing), with alias ids rewritten to the canonical spelling.

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

from pipeline.regs.parsing.catalogue import CatalogueEntry, squash, RuleType, label

# `squash` is the ONE normaliser and lives in catalogue.py, beside the model validator that
# also needs it — two copies drifted once and cost a whole parse.
def boundary_ids(item: dict) -> tuple[set[str], dict[str, str]]:
    """(every bindable id, {alias: canonical id}) for one batch item.

    A cut-point can answer to several authored ids — "McIntyre Dam" and the gauge that resolved to
    the identical measure are one point, and a confluence is named from either bank. Both BIND,
    because `extent.py` resolves either. Only one is STORED."""
    allowed: set[str] = set(item.get("bindable_ids") or ())
    canon: dict[str, str] = {}
    for b in (item.get("boundaries") or ()):
        if not b:
            continue
        allowed.add(b[0])
        for a in (b[3] if len(b) > 3 else ()):
            allowed.add(a)
            canon[a] = b[0]
    return allowed, canon


def default_extents(data: dict) -> int:
    """Give every rule that says nothing about location the whole water, IN PLACE.

    The parse prompt states this default — "Every rule needs `extents`. The default is the
    whole water" — and the model mostly did not write it: it put the reach on the ENTRY and
    left the rules bare. 1,957 of 2,397 rules arrived with no extents, and a rule with no
    extents binds to NOTHING, so 73% of the corpus resolved to no water at all.

    Written here rather than defaulted at resolution time so the corpus states its own reach
    and the resolver has one less way to disagree with it.

    NOT applied to a rule that DESCRIBES a place it could not bind — `extent_text` or
    `unresolved_locators`. "500 m upstream and downstream of Causeway Road" with no boundary
    to bind to is a real, specific location, and widening it to the whole water applies a
    500 m closure to kilometres of it. Measured on the first full parse: of 1,957 bare rules,
    1,752 say nothing about location and 205 do.

    Returns how many rules were given a default.
    """
    n = 0
    for r in (data.get("rules") or ()):
        if not isinstance(r, dict) or r.get("extents"):
            continue
        if (r.get("unresolved_locators") or []) or (r.get("extent_text") or "").strip():
            continue
        r["extents"] = [{"op": "whole"}]
        n += 1
    return n


def canonicalise_splits(data: dict, item: dict) -> list[str]:
    """Rewrite alias split ids to their canonical id, IN PLACE, and report ids this water cannot
    bind. Returns error strings (empty = clean).

    This lives in the validator, not only in ingest, because the agent is told to run the validator
    on itself until clean — a check it cannot run is a defect it cannot fix, and an invented
    cut-point is otherwise invisible until it silently selects no sections at reach resolution."""
    if item.get("no_registry"):
        return []                    # nothing to bind; the parser is told to emit extents: []
    allowed, canon = boundary_ids(item)
    if not allowed:
        return []                    # a batch written before boundaries were exported
    errors: list[str] = []

    def visit(extents, where: str) -> None:
        for ex in extents or ():
            if not isinstance(ex, dict):
                continue
            fixed = []
            for sid in (ex.get("splits") or ()):
                if sid in canon:
                    fixed.append(canon[sid])        # an alias — store the canonical spelling
                elif sid in allowed:
                    fixed.append(sid)
                else:
                    errors.append(f"{where}: split id {sid!r} is not a cut-point on this water "
                                  f"— it was invented or belongs to another item")
                    fixed.append(sid)
            if fixed:
                ex["splits"] = fixed

    visit(data.get("extents"), "entry scope")
    for i, r in enumerate(data.get("rules") or ()):
        if isinstance(r, dict):
            visit(r.get("extents"), f"rule {i + 1}")
    return errors


def check_entry(entry_data: dict, source_text: str,
                item: dict | None = None) -> tuple[CatalogueEntry | None, list[str]]:
    """Returns (entry, errors). `source_text` is the printed row the agent was given.

    `item` is that row's batch item. Given one, extents are checked against its boundary menu and
    alias ids are rewritten to canonical — so pass it whenever it is available."""
    errors: list[str] = []
    default_extents(entry_data)
    if item is not None:
        # Before model validation: this rewrites aliases, and the rewritten value is what the
        # returned entry must carry.
        errors += canonicalise_splits(entry_data, item)
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
        _, errors = check_entry(data, source, by_id.get(eid))
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

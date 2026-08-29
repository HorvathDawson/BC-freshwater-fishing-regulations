"""Write and read a reach-builder run.

**JSONL, not parquet.** Parquet is available, but these tables are small (3,038 rules,
~7.6k bindings) and the properties that matter here are *byte-identical rebuilds* and
*a readable diff in review*. Sorted JSONL gives both for free; parquet's row-group
metadata and compression settings are one more thing that has to be pinned to stay
deterministic. Revisit if a table reaches millions of rows.

Determinism: every row is written in sorted key order with sorted JSON keys and no
timestamps. `report.json` is the one file carrying a duration, so it is excluded from the
digest — otherwise no two builds could ever match.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

from pipeline.reach.models import Outcome, iter_entries

TABLES = ("rule_section", "rule_unresolved", "rule_extent", "rule_diagnostic")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


def write_run(out_dir: str | Path, result, entries=None) -> dict[str, int]:
    """Write the four tables + report.json. Returns row counts per table."""
    out = Path(out_dir)

    # rule_section — the bindings. One row per (rule, section) so it joins cleanly.
    sections = [
        {"entry_id": b.entry_id, "rule_id": b.rule_id, "section_id": s}
        for b in result.bindings if b.outcome is Outcome.bound
        for s in b.sections
    ]
    sections.sort(key=lambda r: (r["entry_id"], r["rule_id"], r["section_id"]))

    # rule_unresolved — EVERY failure, typed. The table that must never be silently short.
    unresolved = [
        {"entry_id": b.entry_id, "rule_id": b.rule_id,
         "reason": b.reason.value if b.reason else None, "detail": b.detail}
        for b in result.bindings if b.outcome is Outcome.unresolved
    ]
    unresolved.sort(key=lambda r: (r["entry_id"], r["rule_id"]))

    # rule_extent — the AUTHORED form, kept beside the resolution so a binding can be
    # explained and rebuilt without re-deriving it from geometry.
    extents: list[dict] = []
    for e in iter_entries(entries or []):
        for rule in e.get("rules") or []:
            for seq, ex in enumerate(rule.get("extents") or []):
                extents.append({
                    "entry_id": e["entry_id"], "rule_id": rule["rule_id"], "seq": seq,
                    "op": ex.get("op"), "splits": list(ex.get("splits") or ()),
                    "item_id": ex.get("item_id"), "item_ids": list(ex.get("item_ids") or ()),
                    "area_id": ex.get("area_id"),
                })
    extents.sort(key=lambda r: (r["entry_id"], r["rule_id"], r["seq"]))

    diagnostics = [
        {"entry_id": d.entry_id, "rule_id": d.rule_id, "kind": d.kind,
         "payload": json.dumps(d.payload, sort_keys=True)}
        for d in result.diagnostics
    ]
    diagnostics.sort(key=lambda r: (r["entry_id"], r["rule_id"], r["kind"], r["payload"]))

    tables = {"rule_section": sections, "rule_unresolved": unresolved,
              "rule_extent": extents, "rule_diagnostic": diagnostics}
    for name, rows in tables.items():
        _write_jsonl(out / f"{name}.jsonl", rows)

    report = asdict(result.report)
    report["digest"] = digest(result)
    (out / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {k: len(v) for k, v in tables.items()}


def digest(result) -> str:
    """Content hash of the BINDINGS — the thing that must not change between identical
    builds. Excludes timings and anything else that varies for non-data reasons."""
    payload = [
        [b.entry_id, b.rule_id, b.outcome.value, list(b.sections),
         b.reason.value if b.reason else None]
        for b in result.bindings
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]


def read_run(run_dir: str | Path) -> dict[tuple[str, str], dict]:
    """Read a previous run back as ``{(entry_id, rule_id): {...}}`` — for `diff`."""
    run = Path(run_dir)
    out: dict[tuple[str, str], dict] = {}

    for line in (run / "rule_section.jsonl").read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        r = json.loads(line)
        key = (r["entry_id"], r["rule_id"])
        out.setdefault(key, {"outcome": "bound", "sections": set(), "reason": None})
        out[key]["sections"].add(r["section_id"])

    p = run / "rule_unresolved.jsonl"
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            r = json.loads(line)
            out[(r["entry_id"], r["rule_id"])] = {
                "outcome": "unresolved", "sections": set(), "reason": r["reason"]}
    return out

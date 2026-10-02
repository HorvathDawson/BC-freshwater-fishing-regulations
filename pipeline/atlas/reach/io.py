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

from pipeline.atlas.reach.models import Outcome, iter_entries

TABLES = ("rule_section", "rule_unresolved", "rule_extent", "rule_diagnostic")
#: The licensing records, placed by the same builder. Separate files, because a record is not a
#: rule — its id is unique within its entry among LICENSING records, not among rules — and a
#: reader that joined the two on (entry_id, id) would be joining different namespaces.
LICENSING_TABLES = ("licensing_placement", "licensing_section", "licensing_diagnostic")
#: `steelhead: known | possible` per section (`pipeline.atlas.reach.steelhead`): one row per section
#: that has the attribute, with the row that makes it known (or `zp:steelhead` for possible).
STEELHEAD_TABLE = "steelhead_presence"


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


def write_run(out_dir: str | Path, result, entries=None) -> dict[str, int]:
    """Write the rule tables, the licensing tables and report.json. Returns row counts per table."""
    out = Path(out_dir)

    # rule_section — the bindings. One row per (rule, section) so it joins cleanly.
    #
    # `scope` says WHY this rule reaches this section: "reach" if the rule names this water,
    # "trib" if it arrived by the tributary walk. It is not decoration. A tributary sweep is
    # the part of this pipeline that can be wrong over thousands of kilometres at once — the
    # Kootenay case in tributaries.py is 3,205 km of water that a prefix shortcut would have
    # closed — and a row that cannot say how it got here cannot be audited by anyone.
    sections = [
        {"entry_id": b.entry_id, "rule_id": b.rule_id, "section_id": s,
         "scope": "trib" if s in trib else "reach"}
        for b in result.bindings if b.outcome is Outcome.bound
        for trib in (frozenset(b.via_tributary),)
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

    # licensing_placement — EVERY placed record, once, with where it ended up. The table that
    # says a record exists at all; `licensing_section` only says where the bound ones are.
    lic = list(result.licensing)
    tables["licensing_placement"] = [
        {"entry_id": p.entry_id, "record_id": p.record_id, "kind": p.kind,
         "placement": p.placement, "reason": p.reason, "detail": p.detail,
         "tributaries_pending": p.tributaries_pending}
        for p in lic]
    # licensing_section — one row per (record, section), `scope` as the rules have it, plus
    # `trib_pending` for a walk that was not done: a pending binding is never complete.
    tables["licensing_section"] = sorted((
        {"entry_id": p.entry_id, "record_id": p.record_id, "kind": p.kind, "section_id": s,
         "scope": ("trib_pending" if p.tributaries_pending
                   else "trib" if s in trib else "reach")}
        for p in lic if p.placement == "sections"
        for trib in (frozenset(p.via_tributary),)
        for s in p.sections),
        key=lambda r: (r["entry_id"], r["record_id"], r["section_id"]))
    tables["licensing_diagnostic"] = sorted((
        {"entry_id": d.entry_id, "record_id": d.rule_id, "kind": d.kind,
         "payload": json.dumps(d.payload, sort_keys=True)}
        for d in result.licensing_diagnostics),
        key=lambda r: (r["entry_id"], r["record_id"], r["kind"], r["payload"]))
    tables[STEELHEAD_TABLE] = list(getattr(result, "steelhead", None) or [])
    for name, rows in tables.items():
        _write_jsonl(out / f"{name}.jsonl", rows)

    report = asdict(result.report)
    report["digest"] = digest(result)
    report["licensing_digest"] = licensing_digest(result)
    report["steelhead_digest"] = hashlib.sha256(json.dumps(
        tables[STEELHEAD_TABLE], sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
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


def licensing_digest(result) -> str:
    """Content hash of the licensing placements — kept apart from `digest`, so adding licensing
    to a run did not move the rules' digest and a rules diff stays a rules diff."""
    payload = [
        [p.entry_id, p.record_id, p.kind, p.placement, list(p.sections), p.reason,
         p.tributaries_pending]
        for p in result.licensing
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

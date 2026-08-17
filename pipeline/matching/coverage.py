"""Match-coverage audit — how many synopsis rows resolve to a registry item, and why the rest don't.

Read-only diagnostic. Runs the matcher over every synopsis row against a built registry, buckets the
results, categorizes the misses (park/reserve, watershed, region-note, empty-regs, true miss), and
reports each override's HEALTH against the archive schema: does it resolve to a NAMED item, is it a
FEATURE-PIN (typed ids present but none named -> the resolver's job: nameless oxbows / unnamed lakes /
poly-fid), an ADMIN/area pin, or a SKIP. Overrides are hand-curated and NEVER deleted.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.matching.coverage --registry output/v2/full/registry.json
"""

from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path

from pipeline.matching.matcher import (
    _mu_disambiguate, _norm, _pick_override, build_id_index, build_name_index,
    build_override_index, load_overrides, match_rows, override_typed_ids, parse_reg_mus, region_num,
)
from pipeline.registry import load_registry
from pipeline.parsing.rows import load_synopsis_rows

_PARKISH = re.compile(r"\b(park|reserve|sanctuary|protected|conservancy|wma|wildlife management)\b", re.I)
_WATERSHED = re.compile(r"\bwatershed(s)?\b", re.I)
_REGIONNOTE = re.compile(r"\b(region|all waters|province|zone|all lakes|all streams|these waters)\b", re.I)


def _categorize_miss(row: dict) -> str:
    water = row.get("water", "")
    if not row.get("raw_regs", "").strip():
        return "empty_regs"
    if _WATERSHED.search(water):
        return "watershed"
    if _PARKISH.search(water):
        return "parkish"
    if _REGIONNOTE.search(water) or not water.strip():
        return "region_note"
    return "true_miss"


def _md_row(cells: list[str]) -> str:
    return "| " + " | ".join(c.replace("|", "\\|") for c in cells) + " |"


def _name_only(registry, name_index, row) -> tuple[str, str, list[str]]:
    """What plain name+MU matching (NO override) would do: (status, id, candidates). The honest test of
    whether an override earns its keep — if name matching lands the same item, the override is a curation
    RECORD (kept for its note/refs) rather than doing routing work. status ∈ matched|ambiguous|unmatched."""
    cands = list(name_index.get(_norm(row.get("water", "")), []))
    if not cands:
        return "unmatched", "", []
    if len(cands) == 1:
        return "matched", cands[0], cands
    narrowed, _ = _mu_disambiguate(cands, registry, region_num(row), parse_reg_mus(row))
    if len(narrowed) == 1:
        return "matched", narrowed[0], narrowed
    return "ambiguous", "", narrowed


def _classify_override(e: dict, resolved: list[str], unresolved: list[str],
                       name_status: str, name_id: str) -> tuple[str, str]:
    """(ov_type, STATE) for one override entry (archive schema).

      SKIP         — intentional drop (in-season correction / not-found / variant_of). Keep.
      ADMIN        — admin/area targets (parks/WMA/zones) -> resolver membership. Keep.
      RESOLVED     — >=1 typed id lands a NAMED registry item. `ov_type` says whether name+MU matching
                     AGREES (curation record) or the override is ESSENTIAL (name amb/none/different).
      FEATURE-PIN  — typed ids present but NONE land a named item -> nameless features (oxbow channels,
                     unnamed lakes) / poly-fid -> the reg→feature resolver binds them to graph sections.
      NOTE-ONLY    — no typed ids, no skip, no admin (a bare note) -> falls through to name matching."""
    if e.get("skip"):
        return "skip", "SKIP"
    admin = e.get("admin_targets") or []
    typed = override_typed_ids(e)
    if resolved:
        essential = not (name_status == "matched" and name_id == resolved[0] and len(resolved) == 1)
        n = f"resolved({len(resolved)})" + (f"+{len(unresolved)}pin" if unresolved else "")
        return (f"{n} essential" if essential else f"{n} agrees"), "RESOLVED"
    if typed:
        return f"feature-pin({len(unresolved)})", "FEATURE-PIN"
    if admin:
        return f"admin({len(admin)})", "ADMIN"
    return "note-only", "NOTE-ONLY"


def _resolve_entry(e: dict, id_index) -> tuple[list[str], list[str]]:
    """(resolved named item_ids, unresolved typed ids) for an override entry."""
    resolved, unresolved = [], []
    for t in override_typed_ids(e):
        hit = id_index.get(t)
        if hit and hit not in resolved:
            resolved.append(hit)
        elif not hit:
            unresolved.append(t)
    return resolved, unresolved


def write_tables(out_dir: Path, results, rows, registry, ov_index, id_index) -> None:
    """Write the override health audit + the non-override misses as markdown review tables."""
    out_dir.mkdir(parents=True, exist_ok=True)
    name_index = build_name_index(registry)

    audit = []
    for r in results:
        if r.via not in ("override", "override_feature", "override_skip", "override_alias"):
            continue
        row = rows[r.index]
        e = _pick_override(ov_index.get(_norm(r.water), []), region_num(row), parse_reg_mus(row))
        if not e:
            continue
        resolved, unresolved = _resolve_entry(e, id_index)
        nstatus, nid, ncands = _name_only(registry, name_index, row)
        ov_type, state = _classify_override(e, resolved, unresolved, nstatus, nid)
        audit.append((state, ov_type, nstatus, row, r, e, resolved, unresolved, ncands))

    order = {"FEATURE-PIN": 0, "RESOLVED": 1, "ADMIN": 2, "SKIP": 3, "NOTE-ONLY": 4}
    audit.sort(key=lambda a: (order.get(a[0], 9), a[1], _norm(a[3].get("water", ""))))
    counts = Counter(a[0] for a in audit)

    hdr = ["#", "water", "region", "mu", "STATE", "ov_type", "name_only", "resolved_ids",
           "unresolved_pins", "note"]
    lines = [f"# Override health audit ({len(audit)} rows hit an override)", "",
             "Overrides are hand-curated and **never deleted**. **RESOLVED** = a typed id lands a named "
             "registry item (`ov_type` says whether name matching agrees or the override is essential). "
             "**FEATURE-PIN** = typed ids present but none named → nameless oxbows / unnamed lakes / "
             "poly-fid → the reg→feature resolver. **ADMIN** = park/WMA/zone target → resolver "
             "membership. **SKIP** = intentional drop. **NOTE-ONLY** = bare note, falls through to name.", "",
             "**State tally:** " + ", ".join(f"{k}={counts.get(k, 0)}" for k in order), "",
             _md_row(hdr), _md_row(["---"] * len(hdr))]
    for state, ov_type, nstatus, row, r, e, resolved, unresolved, ncands in audit:
        note = (e.get("note", "") or e.get("skip_reason", "") or "")[:160]
        lines.append(_md_row([str(r.index), r.water, str(row.get("region", "")), str(row.get("mu", "")),
                              state, ov_type, nstatus, ", ".join(resolved) or "-",
                              ", ".join(unresolved) or "-", note]))
    (out_dir / "overrides_audit.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    misses = [r for r in results if r.status in ("unmatched", "ambiguous")]
    mlines = [f"# Real misses ({len(misses)}) — no override, unmatched or ambiguous by name+MU", "",
              _md_row(["#", "water", "region", "mu", "status", "category", "reason", "candidates"]),
              _md_row(["---"] * 8)]
    for r in sorted(misses, key=lambda r: (r.status, _norm(r.water))):
        row = rows[r.index]
        cands = "<br>".join(r.candidates[:6]) + ("…" if len(r.candidates) > 6 else "")
        mlines.append(_md_row([str(r.index), r.water, str(row.get("region", "")), str(row.get("mu", "")),
                               r.status, _categorize_miss(row), r.reason, cands]))
    (out_dir / "real_misses.md").write_text("\n".join(mlines) + "\n", encoding="utf-8")

    print("\n=== review tables written ===")
    print(f"  {out_dir / 'overrides_audit.md'}  ({len(audit)} rows)  " +
          " ".join(f"{k}={counts.get(k, 0)}" for k in order))
    print(f"  {out_dir / 'real_misses.md'}  ({len(misses)} rows)")


def run(registry_path: str, overrides_path: str | None, show: int, tables_dir: str | None) -> None:
    registry = load_registry(registry_path)
    rows = load_synopsis_rows()
    ov_path = overrides_path or (Path(__file__).resolve().parent / "overrides.json")
    overrides = load_overrides(ov_path)
    id_index = build_id_index(registry)
    ov_index = build_override_index(overrides)
    results = match_rows(rows, registry, overrides)

    n_items = len(registry)
    kinds = Counter(it.kind for it in registry.values())
    print(f"registry: {n_items} items ({dict(kinds)})")
    print(f"overrides: {len(overrides)} entries")
    print(f"rows: {len(rows)}\n")

    status = Counter(r.status for r in results)
    hit = status["matched"] + status["override"]
    print("=== match status ===")
    for k in ("matched", "override", "feature_pin", "ambiguous", "unmatched", "skip"):
        print(f"  {k:14} {status.get(k, 0)}")
    print(f"  {'HIT %':14} {100 * hit / max(len(rows), 1):.1f}%  (matched+override; feature_pin -> resolver)\n")

    print("=== resolution path (via) ===")
    for k, n in Counter(r.via or "(unmatched/ambiguous)" for r in results).most_common():
        print(f"  {k:20} {n}")

    # override health across ALL entries (not just those a row hit)
    ov_state = Counter()
    for e in overrides:
        resolved, unresolved = _resolve_entry(e, id_index)
        _, state = _classify_override(e, resolved, unresolved, "", "")
        ov_state[state] += 1
    print("\n=== override health (all entries) ===")
    for k in ("RESOLVED", "FEATURE-PIN", "ADMIN", "SKIP", "NOTE-ONLY"):
        print(f"  {k:14} {ov_state.get(k, 0)}")

    misses = [(r, rows[r.index]) for r in results if r.status in ("unmatched", "ambiguous")]
    cats = Counter(_categorize_miss(row) for _, row in misses)
    print(f"\n=== {len(misses)} misses by category ===")
    for cat, n in cats.most_common():
        print(f"  {cat:12} {n}")

    print(f"\n=== sample misses (up to {show} per category) ===")
    seen: Counter = Counter()
    for r, row in misses:
        cat = _categorize_miss(row)
        if seen[cat] >= show:
            continue
        seen[cat] += 1
        print(f"  [{cat}] {r.status:9} '{r.water}' (mu={row.get('mu')}) — {r.reason}")

    if tables_dir:
        write_tables(Path(tables_dir), results, rows, registry, ov_index, id_index)


def main() -> None:
    ap = argparse.ArgumentParser(description="Match-coverage audit over the synopsis rows.")
    ap.add_argument("--registry", default="output/v2/full/registry.json")
    ap.add_argument("--overrides", default=None)
    ap.add_argument("--show", type=int, default=15)
    ap.add_argument("--tables", nargs="?", const="pipeline/docs/coverage",
                    help="write overrides_audit.md + real_misses.md review tables to this dir "
                         "(default: pipeline/docs/coverage)")
    args = ap.parse_args()
    run(args.registry, args.overrides, args.show, args.tables)


if __name__ == "__main__":
    main()

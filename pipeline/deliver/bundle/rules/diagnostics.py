"""The reach run's diagnostic tables as the bundle keeps them: ambiguous cuts
and any run table keyed by section handle (`run_rows`, refused when the run and the atlas differ)."""
from __future__ import annotations

import json


def ambiguous_cuts(rows: list[dict], table: str = "rule_diagnostic",
                   id_key: str = "rule_id") -> list[tuple]:
    """`rule_ambiguous_cut`'s rows from a run's `rule_diagnostic` rows — or, with
    `table="licensing_diagnostic", id_key="record_id"`, `licensing_ambiguous_cut`'s. A run that
    predates the ruling (no `candidates` / `toward`) picked nearest the mouth and recorded nothing
    about the choice: refused, never shipped as if it had."""
    out = []
    for r in rows:
        if r["kind"] != "ambiguous_cut":
            continue
        p = json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]
        if "candidates" not in p or "toward" not in p:
            raise SystemExit(f"{table}: {r['entry_id']} {r[id_key]} records an ambiguous cut "
                             f"({p.get('split_id')}) without its candidates — the run predates the "
                             f"2026-10-22 ruling; re-run the reach builder")
        out.append((r["entry_id"], r[id_key], int(p["extent"]), p["split_id"], float(p["used"]),
                    json.dumps([float(x) for x in p["candidates"]]), str(p["toward"])))
    out.sort()
    return out


def run_rows(rows: list[dict], sid: dict[str, int], what: str) -> list[dict]:
    """A reach-run table keyed by handle: every `section_id` becomes `sid`. REFUSED when the run
    names a section this atlas does not have — the run and the atlas are then different builds."""
    out, unknown = [], []
    for r in rows:
        h = sid.get(r["section_id"])
        if h is None:
            unknown.append(r["section_id"])
            continue
        out.append({**{k: v for k, v in r.items() if k != "section_id"}, "sid": h})
    if unknown:
        raise SystemExit(f"{what}: {len(unknown):,} section(s) are not in the handle table (e.g. "
                         f"{unknown[:3]}) — the reach run and the atlas disagree")
    return sorted(out, key=lambda r: r["sid"])

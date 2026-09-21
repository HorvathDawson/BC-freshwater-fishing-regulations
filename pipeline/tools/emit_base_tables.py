"""Emit the standing (base) tables and their print checks as one JSON payload.

The base tables are the foundation everything else is validated against: a section's table
is its region's base plus named overrides, so a base that is wrong is a defect on every
water in the region at once. They were reachable only through a `<select>` underneath the
section table on each artifact — hidden beneath the thing they are meant to validate, and
split across two artifacts with different layouts, so reviewing one region meant reading
two pages.

This writes what a review surface needs, per region and kind, in one file:

    {"regions": [{key, region, kind, title, quota: {...}, gear: {...}}], "meta": {...}}

Both halves carry the TABLE (what we would show) and the CHECKS (each printed line beside
the line we computed from it, with its citation). Nothing here resolves anything — it is
`provenance.base_tables`, `method_provenance.base_tables`, `quota_print.all_panels` and
`method_print.all_panels` put side by side.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.emit_base_tables -o out.json

The two print modules name their fields inversely — a quota `Check.line` is the PRINTED
line and `.found` is ours; a gear `Check.line` is OURS and `.sentence` is printed. They are
normalised here to `printed` / `ours` so one renderer reads both.
"""
from __future__ import annotations

import argparse
import json
import sys

from pipeline.regs.table import provenance, method_provenance
from pipeline.regs.table import quota_print as QP, method_print as MP


def log(*a):
    print(*a, file=sys.stderr)


def _check(c, printed_attr: str, ours_attr: str) -> dict:
    """One line of a print diff, normalised. `why` carries the reason a tick rests on an
    argument rather than a found sentence — a ✓ with neither is refused upstream."""
    return {
        "ok": bool(c.ok),
        "printed": (getattr(c, printed_attr, "") or "").strip(),
        "ours": (getattr(c, ours_attr, "") or "").strip(),
        "why": (getattr(c, "why", "") or "").strip(),
        "label": (getattr(c, "label", "") or "").strip(),
        "rule": (getattr(c, "rule", "") or "").strip(),
    }


def _panel(p, printed_attr: str, ours_attr: str) -> dict:
    checks = [_check(c, printed_attr, ours_attr) for c in p.checks]
    return {
        "title": getattr(p, "title", ""),
        "source": getattr(p, "source", ""),
        "md5": getattr(p, "md5", ""),
        "gov": getattr(p, "gov", ""),
        "checks": checks,
        "ok": sum(1 for c in checks if c["ok"]),
        "n": len(checks),
    }


def _quota_table(region: str, kind: str) -> dict:
    """The rows AND the presentation pass over them.

    `provenance.present` is what makes the table a reader's table rather than a dump: one
    entry per kind of fish, **never two rows for one species** — where wild and hatchery
    differ they become two LINES under the one name — a shared number as a BAND above its
    members carrying once whatever is true of all of them, and a fish that stands outside the
    shared number still drawn under its group. Rendering the raw rows instead loses every one
    of those and splits a species across the table.
    """
    rows = _quota_rows(region, kind)
    d = {"rows": rows}
    return {"rows": rows, "present": provenance.present(d) if rows else {"entries": [], "bands": {}},
            "always": _always(region, kind)}


def _always(region: str, kind: str) -> list:
    """The rules that are true on every water and can never be drawn on a map.

    "Within 23 m downstream of any fishway, canal, obstacle or leap" and "within 100 m of any
    government counting, passing or rearing facility" bind everywhere, but because nothing can
    place them they were filed as "somewhere" caveats and rendered as the faintest text on the
    page, under a collapsed disclosure. A man standing below a fishway reads the big number and
    never sees the rule that governs him. They are general rules and belong at the top.
    """
    L = QP.base_ledger(region, kind)
    out, seen = [], set()
    for a in L.allowances:
        if getattr(a.applies, "kind", "") != "somewhere":
            continue
        rid_ = a.source.rule_id
        if rid_ in seen:
            continue
        seen.add(rid_)
        out.append({"rule": rid_, "where": a.applies.detail or a.source.place,
                    "verbatim": a.source.verbatim, "who": a.source.who,
                    "says": a.word() if hasattr(a, "word") else ""})
    return out


def _quota_rows(region: str, kind: str) -> list:
    """The base table's own rows.

    NOT `provenance.base_tables()`: that list is derived from the sections that ship, so it
    is keyed by label, omits Haida Gwaii and Region 7B entirely, and carries composites
    ("Regions 3 and 5 · streams") that no single region owns. The print panels are the
    complete spine — one per region per kind — so the rows are built from the same ledger
    the panel checked, and every region gets a table whether a section uses it or not.
    """
    L = QP.base_ledger(region, kind)
    return [provenance.row_json(L, r) for r in rows_of(L)]


def rows_of(L):
    from pipeline.regs.table import rows as R
    return R.rows(L)


def collect() -> dict:
    # -- the print diffs, keyed (region, kind) -------------------------------------------
    # THE TWO SIDES NAME THE PROVINCE DIFFERENTLY — quota's panel is region "p", kind "any"
    # (it stands for both kinds); gear's is region "province", one per kind. Left alone they
    # produce three provincial entries, none of them whole.
    def reg_key(r):
        return "province" if str(r) in ("p", "province") else str(r)

    qp = {}
    for p in QP.all_panels():
        qp[(reg_key(p.region), p.kind)] = _panel(p, "line", "found")
    mp = {}
    for reg, kind, p in MP.all_panels():
        mp[(reg_key(reg), kind)] = _panel(p, "sentence", "line")

    keys = sorted({(r, k) for r, k in set(qp) | set(mp) if k in ("lake", "stream")},
                  key=lambda k: (str(k[0]), str(k[1])))
    out = []
    for region, kind in keys:
        q_panel = qp.get((region, kind)) or qp.get((region, "any"))
        m_panel = mp.get((region, kind)) or mp.get((region, "any"))
        try:
            q = _quota_table(region, kind) if kind in ("lake", "stream") else {}
        except Exception as e:                      # a region with no base of that kind
            log(f"  no quota base for {region}·{kind}: {e}")
            q = {}
        try:
            g_table = method_provenance.base_table(region, kind) if kind in ("lake", "stream") else None
        except Exception as e:
            log(f"  no gear base for {region}·{kind}: {e}")
            g_table = None
        out.append({
            "key": f"{region}:{kind}",
            "region": region,
            "kind": kind,
            "title": (q_panel or m_panel or {}).get("title") or f"{region} · {kind}",
            "quota": {"rows": q.get("rows") or [], "present": q.get("present") or {},
                      "print": q_panel},
            "gear": {"table": g_table, "print": m_panel},
            "always": (q.get("always") or []),
        })

    def tally(side):
        ok = sum(r[side]["print"]["ok"] for r in out if r[side]["print"])
        n = sum(r[side]["print"]["n"] for r in out if r[side]["print"])
        return {"ok": ok, "n": n}

    # A READER LOOKS FOR A FAMILY FIRST. "My fish is a char" should find one place on every
    # region's table, whether that region writes "Trout/char: 5" as one number (Region 2) or
    # writes "Trout: 4" and releases char separately (Region 1). The page groups by this.
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS
    fams = {"TROUT_CHAR": sorted(SPECIES_GROUPS["TROUT_CHAR"]),
            "TROUT": sorted(SPECIES_GROUPS["TROUT"]),
            "CHAR": sorted(SPECIES_GROUPS["CHAR"])}
    return {"regions": out,
            "meta": {"quota": tally("quota"), "gear": tally("gear"), "families": fams}}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-o", "--out", help="write here (default: stdout)")
    a = ap.parse_args()
    d = collect()
    log(f"{len(d['regions'])} region tables · "
        f"quota {d['meta']['quota']['ok']}/{d['meta']['quota']['n']} · "
        f"gear {d['meta']['gear']['ok']}/{d['meta']['gear']['n']}")
    text = json.dumps(d, separators=(",", ":"))
    if a.out:
        open(a.out, "w").write(text)
        log(f"wrote {a.out} ({len(text)/1e6:.1f} MB)")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

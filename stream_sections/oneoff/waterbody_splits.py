"""Waterbody-grouped split curation — pivot locator rows by their SOURCE REG ENTRY and
reconcile against the parsed synopsis so every split in a reg is accounted for.

WHY: `14-locators-to-curate.json` is a flat list of 651 locator rows. A single waterbody's
splits are scattered across many rows and were curated in isolation, so it was easy to miss one
(e.g. DEAN RIVER's canyon reaches) or to mistake one row for a duplicate. This tool groups rows
by reg entry (waterbody + MU + reg text) and links each BOUNDARY in the reg text to the curated
row(s) that resolve it, flagging:
  - MISSING  : a reg boundary with NO locator row  (a split we never captured)
  - DUP      : more than one row for the same boundary
  - ORPHAN   : a locator row that matches no reg boundary (name/except rows are expected orphans)
so a waterbody can be marked COMPLETE only when every boundary is resolved.

LINK KEY: normalize(locator.full_regulation) == normalize(synopsis.regs_verbatim). Verified to
join 343/343 locator regs (all 651 rows). Within an entry, a reg boundary (`rules[].location_text`)
links to a locator row by normalized equality / containment of `locator_text`.

The curated data in `14-locators-to-curate.json` stays the SOURCE OF TRUTH — this tool is a
regenerable VIEW + completeness check over it. Nothing here mutates curation.

Run from repo root:
    .venv/bin/python -m stream_sections.oneoff.waterbody_splits            # write JSON+MD, print summary
    .venv/bin/python -m stream_sections.oneoff.waterbody_splits show "DEAN RIVER"   # one waterbody's card
    .venv/bin/python -m stream_sections.oneoff.waterbody_splits missing     # every MISSING boundary
    .venv/bin/python -m stream_sections.oneoff.waterbody_splits incomplete  # entries not yet fully resolved
See docs/18-waterbody-split-curation.md for the workflow.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOCATORS = ROOT / "stream_sections/docs/14-locators-to-curate.json"
SYNOPSIS = ROOT / "output/pipeline/parsing/synopsis_parsed.json"
OUT_JSON = ROOT / "stream_sections/docs/waterbody-splits.json"
OUT_MD = ROOT / "stream_sections/docs/waterbody-splits.md"

RESOLVED = {"curated", "manual", "not_applicable", "deferred"}  # not `todo`/`likely_na`


def norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("*", "")).strip().lower()


def entry_id(name: str, mus: tuple[str, ...], reg: str) -> str:
    h = hashlib.sha1(f"{name}|{','.join(mus)}|{reg}".encode()).hexdigest()[:8]
    return h


def _matches(loc_text: str, rule_text: str) -> bool:
    a, b = norm(loc_text), norm(rule_text)
    if not a or not b:
        return False
    return a == b or a in b or b in a


def build():
    locators = json.loads(LOCATORS.read_text())["locators"]
    synopsis = json.loads(SYNOPSIS.read_text())

    # synopsis boundary-rules keyed by normalized reg (dedup, non-empty location_text only)
    syn_rules: dict[str, list[str]] = defaultdict(list)
    for e in synopsis:
        nr = norm(e.get("regs_verbatim"))
        for rule in e.get("rules", []):
            lt = (rule.get("location_text") or "").strip()
            if lt and lt not in syn_rules[nr]:
                syn_rules[nr].append(lt)

    # group locator rows by reg entry (name, mus, reg)
    entries: dict[tuple, list[dict]] = defaultdict(list)
    for r in locators:
        key = (r["name_verbatim"], tuple(r.get("mus") or []), norm(r.get("full_regulation")))
        entries[key].append(r)

    cards = {}
    for (name, mus, reg), rows in entries.items():
        eid = entry_id(name, mus, reg)
        boundaries = []
        linked_row_ids: set[str] = set()
        for rule in syn_rules.get(reg, []):
            hits = [r for r in rows if _matches(r.get("locator_text", ""), rule)]
            for r in hits:
                linked_row_ids.add(r["id"])
            statuses = [r["status"] for r in hits]
            if not hits:
                flag = "MISSING"
            elif len(hits) > 1 and len({norm(r.get("locator_text", "")) for r in hits}) == 1:
                flag = "DUP"
            else:
                flag = "OK"
            boundaries.append({
                "location_text": rule,
                "row_ids": [r["id"] for r in hits],
                "statuses": statuses,
                "resolved": all(s in RESOLVED for s in statuses) if statuses else False,
                "flag": flag,
            })
        orphans = [
            {"id": r["id"], "locator_text": r.get("locator_text", ""), "src": r.get("src"),
             "status": r["status"]}
            for r in rows
            if r["id"] not in linked_row_ids and norm(r.get("locator_text", ""))
        ]
        n_missing = sum(1 for b in boundaries if b["flag"] == "MISSING")
        n_unresolved = sum(1 for b in boundaries if b["flag"] != "MISSING" and not b["resolved"])
        todo_rows = [r["id"] for r in rows if r["status"] in ("todo", "likely_na")]
        if not boundaries:
            completeness = "NO_SPLITS"
        elif n_missing:
            completeness = "MISSING_SPLITS"
        elif n_unresolved or todo_rows:
            completeness = "INCOMPLETE"
        else:
            completeness = "COMPLETE"
        cards[eid] = {
            "entry_id": eid,
            "name": name,
            "mus": list(mus),
            "reg_text": reg,
            "n_boundaries": len(boundaries),
            "boundaries": boundaries,
            "orphan_rows": orphans,
            "todo_row_ids": todo_rows,
            "completeness": completeness,
        }
    return cards


def _rank(card):
    order = {"MISSING_SPLITS": 0, "INCOMPLETE": 1, "COMPLETE": 2, "NO_SPLITS": 3}
    return (order[card["completeness"]], -card["n_boundaries"])


def write_outputs(cards):
    OUT_JSON.write_text(json.dumps(cards, indent=2, ensure_ascii=False) + "\n")
    lines = ["# Waterbody split-curation cards (generated by oneoff/waterbody_splits.py)\n",
             "One card per reg entry (waterbody + MU + reg text). `MISSING` = a reg boundary with no",
             "curated row. Sorted worst-first. Regenerate; do not hand-edit.\n"]
    from collections import Counter
    tally = Counter(c["completeness"] for c in cards.values())
    lines.append("| completeness | entries |")
    lines.append("|---|--:|")
    for k in ["MISSING_SPLITS", "INCOMPLETE", "COMPLETE", "NO_SPLITS"]:
        lines.append(f"| {k} | {tally.get(k, 0)} |")
    lines.append("")
    for c in sorted(cards.values(), key=_rank):
        if c["completeness"] in ("COMPLETE", "NO_SPLITS"):
            continue
        lines.append(f"## {c['name']}  ·  MU {c['mus']}  ·  [{c['completeness']}]  ({c['entry_id']})")
        for b in c["boundaries"]:
            mark = {"MISSING": "❌ MISSING", "DUP": "⚠️ DUP", "OK": "•"}[b["flag"]]
            rows = ", ".join(f"{i}[{s}]" for i, s in zip(b["row_ids"], b["statuses"])) or "—"
            lines.append(f"- {mark} “{b['location_text'][:70]}” → {rows}")
        if c["orphan_rows"]:
            lines.append(f"  - orphan rows: " + ", ".join(f"{o['id']}({o['src']})" for o in c["orphan_rows"]))
        lines.append("")
    OUT_MD.write_text("\n".join(lines) + "\n")


def main():
    args = sys.argv[1:]
    cards = build()
    if args and args[0] == "show":
        q = " ".join(args[1:]).lower()
        for c in sorted(cards.values(), key=_rank):
            if q in c["name"].lower():
                print(json.dumps(c, indent=2, ensure_ascii=False))
        return
    if args and args[0] == "missing":
        for c in sorted(cards.values(), key=_rank):
            for b in c["boundaries"]:
                if b["flag"] == "MISSING":
                    print(f"{c['name']:38.38s} MU{c['mus']} | {b['location_text']}")
        return
    if args and args[0] == "incomplete":
        for c in sorted(cards.values(), key=_rank):
            if c["completeness"] in ("MISSING_SPLITS", "INCOMPLETE"):
                miss = sum(1 for b in c["boundaries"] if b["flag"] == "MISSING")
                print(f"[{c['completeness']:14s}] {c['name']:36.36s} MU{c['mus']} "
                      f"boundaries={c['n_boundaries']} missing={miss} todo={len(c['todo_row_ids'])}")
        return
    # default: write + summary
    write_outputs(cards)
    from collections import Counter
    tally = Counter(c["completeness"] for c in cards.values())
    n_missing = sum(1 for c in cards.values() for b in c["boundaries"] if b["flag"] == "MISSING")
    print(f"entries: {len(cards)}  | {dict(tally)}")
    print(f"MISSING boundaries (splits with no row): {n_missing}")
    print(f"wrote {OUT_JSON.relative_to(ROOT)} and {OUT_MD.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

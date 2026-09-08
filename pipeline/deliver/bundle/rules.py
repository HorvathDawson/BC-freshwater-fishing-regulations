"""The regulations, from the reach builder's output into the bundle.

FOUR TABLES, AND ONE OF THEM IS THE WHOLE DESIGN.

`entry` and `rule` are transcription — the curated corpus, carried verbatim. The interesting
pair is `section_ruleset` and `ruleset`, and what they are is a compression that only works
because of something true about regulations rather than about data: a rule applies to a
STRETCH of river, and a stretch is many sections. The obvious table — one row per (section,
rule) — is 1,720,243 rows and 69.6 MB on a 51.8 MB bundle. Those rows carry 1,905 distinct
answers. Interning them costs 12.1 MB and reads four times faster.

THIS MODULE CONSUMES, IT DOES NOT DERIVE. Everything about which water a rule covers —
the four tributary states, the per-rule carve-outs, the reach-scoped walk with its three
guards — is decided in `pipeline.atlas.reach` and arrives here already resolved. Re-deriving
any of it from the curated flags would put a second implementation of "which water does this
rule cover" in the codebase, which is exactly how three copies of the trust rule happened.
The flags are read here for ONE thing (`uncertain`) and that is not a re-derivation.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def _windows(rule: dict) -> list[dict]:
    """The rule's date strings, as the STRUCTURED windows the client reads.

    THIS SHIPPED WRONG ONCE. The curated field holds the dates verbatim — `["Apr 1-Oct 31"]`,
    exact substrings of the rule text, so the chain of custody back to the synopsis holds —
    and the first version of this writer put those strings straight into the column. The
    client reads `{from: {month, day}, to: {month, day}}` and every regulation screen threw
    `Cannot read properties of undefined (reading 'month')` the moment it evaluated a rule
    with a season on it.

    `pipeline.regs.parsing.dates` already derives the structure, deterministically and under
    test, and is the hallucination guard for these strings besides. Parsing them a second
    time here would be a second answer to "when is this rule in force".

    A string that does not parse is DROPPED rather than guessed at, which makes the rule
    all-year — the wider, safer reading. `date_parse_errors` is what turns a bad date into a
    curation failure; that is its job, not this one's.
    """
    from pipeline.regs.parsing.dates import parse_date_windows

    return [{"from": {"month": w.start_month, "day": w.start_day},
             "to": {"month": w.end_month, "day": w.end_day}}
            for w in parse_date_windows(list(rule.get("dates") or []))]


def _specificity(rule: dict) -> str:
    """`section | mu | area` — WHERE THE RULE WAS WRITTEN, which is what drives precedence.

    A rule written for this water displaces a zone default that contradicts it, so the app
    has to be able to tell the two apart. Every rule in the corpus today is `section`: all
    3,050 name a river or a lake, and the two extents that name an area name it as a
    boundary rather than as the rule's own scope. `mu` appears when zone regulations are
    parsed; deriving it here rather than assuming "section" everywhere is what stops that
    day being a silent behaviour change.
    """
    for ex in rule.get("extents") or []:
        if ex.get("area_id"):
            return "area"
    return "section"


def _jsonl(path: Path):
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def intern_sets(rows) -> tuple[dict[str, int], list[list[tuple[str, str, str]]]]:
    """`(section -> set_id, sets)` from the reach builder's (rule, section, scope) rows.

    The set is over (entry_id, rule_id, SCOPE), not just the rule, so a section reached as a
    tributary and a section the rule names directly land in different sets even when the
    rules are identical. That is the point: `scope` is how a reader is told the difference
    between "no fishing here" and "no fishing here, because this creek joins a closed
    stretch of the Skeena", and 98.6% of all bindings are tributary ones. It costs 249 sets.

    Sets are sorted before interning so the same corpus always produces the same ids — a
    bundle whose set numbering moved between builds would diff as though every section had
    changed.
    """
    by_section: dict[str, set[tuple[str, str, str]]] = {}
    for r in rows:
        by_section.setdefault(r["section_id"], set()).add(
            (r["entry_id"], r["rule_id"], r["scope"]))

    intern: dict[frozenset, int] = {}
    sets: list[list[tuple[str, str, str]]] = []
    section_set: dict[str, int] = {}
    for section in sorted(by_section):
        key = frozenset(by_section[section])
        got = intern.get(key)
        if got is None:
            got = intern[key] = len(sets)
            sets.append(sorted(key))
        section_set[section] = got
    return section_set, sets


def write(db: sqlite3.Connection, reaches: Path, entries_dir: Path, cov,
          build_dir: Path | None = None) -> None:
    """Write `entry`, `rule`, `section_ruleset` and `ruleset`."""
    sections_file = reaches / "rule_section.jsonl"
    if not sections_file.exists():
        for t in ("entry", "rule", "section_ruleset", "ruleset"):
            cov.skip(t, f"no reach run at {reaches}")
        return

    # THE 88, FIRST. A rule nobody could place must never render as "no rules here" — it can
    # only ever raise "unknown" — so the flag has to be on the rule row itself, and it is
    # read from the reach builder's own failure table rather than guessed at from the
    # curation. `rule_unresolved` is the table that must never be silently short.
    unresolved = {(r["entry_id"], r["rule_id"]) for r in _jsonl(reaches / "rule_unresolved.jsonl")}

    entry_rows, rule_rows = [], []
    for path in sorted(entries_dir.rglob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        for e in doc.get("entries", []):
            ident = e.get("identity") or {}
            matched = e.get("matched") or []
            entry_rows.append((
                e["entry_id"],
                # The first match, or nothing. An entry that never matched a water keeps its
                # rules and its text and carries a null item — the app shows "we have a rule
                # for a water we cannot place" rather than dropping it (77 of these).
                matched[0] if matched else None,
                ident.get("display_name") or ident.get("name"),
                # What the curator wrote, kept whole — see the note in schema.sql.
                ident.get("name"),
                e.get("regs_verbatim"),
                # Provenance is nested under `source` now; it was a flat `source_symbols`
                # until the page number joined it and made it obvious they were one fact.
                json.dumps((e.get("source") or {}).get("symbols") or [],
                           separators=(",", ":")),
                json.dumps(ident.get("mus") or [], separators=(",", ":")),
                json.dumps((e.get("source") or {}).get("pages") or [],
                           separators=(",", ":")),
            ))
            for r in e.get("rules") or []:
                rule_rows.append((
                    e["entry_id"], r["rule_id"], r.get("restriction_type"),
                    _specificity(r),
                    json.dumps(_windows(r), separators=(",", ":")),
                    json.dumps(r.get("species") or [], separators=(",", ":")),
                    # The precedence key, and the sentence. Not the same field — see the
                    # note on `rule.subject` in schema.sql.
                    ",".join(r.get("exempts_from") or []) or None,
                    r.get("details"),
                    1 if (e["entry_id"], r["rule_id"]) in unresolved else 0,
                    r.get("rule_text"), r.get("display_location"),
                ))

    # NAMED, not positional. A `pages` column was added to the schema while this line kept
    # seven placeholders, and nothing caught it until 90 seconds into a province-wide rebuild
    # — which then wrote a 42 MB bundle with zero entries in it. Naming the columns makes that
    # failure impossible rather than merely tested.
    db.executemany("INSERT INTO entry (entry_id, item_id, name, full_name, verbatim, symbols,"
                   "                   mus, pages) VALUES (?,?,?,?,?,?,?,?)", entry_rows)
    cov.filled("entry", len(entry_rows))
    db.executemany("INSERT INTO rule VALUES (?,?,?,?,?,?,?,?,?,?,?)", rule_rows)
    cov.filled("rule", len(rule_rows))

    section_set, sets = intern_sets(_jsonl(sections_file))
    # The section is named by its HANDLE here, as everywhere else in the bundle. A rule bound
    # to a section the handle table does not know means the reach run and the atlas are not
    # the same build — which would silently bind rules to the wrong water, so it stops here.
    from pipeline.common.section_handles import read as _read_handles

    if build_dir is None:
        raise SystemExit("rules.write needs build_dir to resolve section handles")
    _, sid = _read_handles(build_dir)
    _unknown = [s for s in section_set if s not in sid]
    if _unknown:
        raise SystemExit(
            f"section_ruleset: {len(_unknown):,} bound sections are not in the handle table "
            f"(e.g. {_unknown[:3]}) — the reach run and section_handles.txt disagree")
    db.executemany("INSERT INTO section_ruleset (sid, set_id) VALUES (?,?)",
                   [(sid[k], v) for k, v in section_set.items()])
    cov.filled("section_ruleset", len(section_set))
    db.executemany("INSERT INTO ruleset VALUES (?,?,?,?)",
                   ((i, e, r, s) for i, rows in enumerate(sets) for e, r, s in rows))
    cov.filled("ruleset", sum(len(s) for s in sets))

    n_uncertain = sum(1 for r in rule_rows if r[8])
    print(f"     rules: {len(entry_rows):,} entries · {len(rule_rows):,} rules "
          f"({n_uncertain} uncertain) · {len(section_set):,} sections carry one, "
          f"sharing {len(sets):,} distinct sets")

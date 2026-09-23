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

    # The catalogue calls this `windows`; the retired prose rule called it `dates`. Same
    # strings, same guarantee — exact substrings of the sentence.
    raw = rule.get("windows")
    if raw is None:
        raw = rule.get("dates")
    return [{"from": {"month": w.start_month, "day": w.start_day},
             "to": {"month": w.end_month, "day": w.end_day}}
            for w in parse_date_windows(list(raw or []))]


def _mus_of(entry_id: str) -> list[str]:
    """The MUs a catalogue entry covers, from its own id.

    `entry_id` is `r{region}:{slug}@{mus}` and the suffix is the MUs the synopsis ROW was
    printed under — which is exactly what the prose entry carried as `identity.mus`. Reading
    it here rather than storing it twice is what keeps the two from disagreeing."""
    return entry_id.split("@", 1)[1].split("+") if "@" in entry_id else []


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
        # `area_kind` counts as much as `area_id`. It names a FAMILY of areas — every
        # national park, every ecological reserve — and a rule written against one is a zone
        # rule by any reading. Checking only `area_id` scoped five of them as `section`,
        # which would have let a province-wide park closure outrank the water-specific
        # regulation it is supposed to sit under.
        if ex.get("area_id") or ex.get("area_kind"):
            return "area"
    return "section"


#: Already a column, or meaningless to a client. Everything else the rule actually set goes
#: into `conditions` as JSON, so adding a condition to the catalogue needs no schema change.
#:
#: `extents` USED TO BE ON THIS LIST and the bundle shipped only `scope` — `section` or `area` —
#: which cannot tell "within Region 4" from "within Management Units 1-1 to 1-6". Those are the
#: two the ladder must separate: the first is a region's standing table, the second an override
#: on a handful of streams. So `corpus.catalogue()` read them back out of the curated files at
#: RUN time, which is a fallback: a reader could get an answer the bundle never agreed to, and
#: staleness stopped being visible. They ship here now and that reader is gone.
_NOT_CONDITIONS = frozenset({
    "rule_id", "type", "verbatim", "species", "species_except", "windows", "take",
    "may_target", "extent_text", "review_reason",
    "unresolved_locators",
    # Build-time only: a carve-out the reach builder applies before any section reaches the
    # bundle. Shipping it as a `condition` would put a resolver's input in front of a reader.
    "tributary_excludes",
})


def _rule_row(entry_id: str, raw: dict, uncertain: bool, entry_extents=None):
    """One `rule` row from one catalogue rule.

    Validated through `CatalogueRule` rather than read off the dict, because `family`,
    `dimension` and `label` are all DERIVED — and deriving them here from raw fields would put
    a second implementation of each in the codebase. `label` in particular has one home for
    the same reason the gauge trust wording does: a rule worded two ways is two rules to a
    reader.
    """
    from pipeline.regs.parsing.catalogue import CatalogueRule, label as rule_label

    if "type" not in raw:
        # The prose model is gone. Writing NULLs here would give the bundle rule rows that
        # exist and say nothing, which is indistinguishable from a water with no rules.
        raise ValueError(
            f"{entry_id}/{raw.get('rule_id')}: rule has no `type` — this is a retired prose "
            f"rule and the bundle no longer has columns for it")
    r = CatalogueRule.model_validate(raw)
    # BY_ALIAS, OR THE PYTHON NAME SHIPS. `while`, `except` and `with` are Python keywords, so
    # the fields are `while_`/`except_`/`with_` in the model and the JSON name is the alias. A
    # dump without this puts `while_` in the bundle, where a reader looking for `while` finds
    # nothing and the circumstance a rule binds in silently disappears.
    dumped = r.model_dump(exclude_none=True, mode="json", by_alias=True)
    # A RULE WITH NO EXTENTS OF ITS OWN TAKES ITS ENTRY'S. 130 rules do, and reading only the
    # rule dict wrote `[]` for every one of them — which says "binds nowhere", not "binds
    # wherever the entry does". `corpus.catalogue()` applied this inheritance when it read the
    # curated files at run time; shipping the extents without it would have moved the fallback
    # rather than removed it, and quietly unbound 130 rules on the way.
    if not dumped.get("extents") and entry_extents:
        dumped["extents"] = list(entry_extents)
    # `False` IS A VALUE, and dropping it lost the only field that separates a permission from
    # a prohibition. `permitted: false` on "No spear fishing of any kind is permitted in Region
    # 1, 2 and 4" was stripped, so the client could not tell it from "Spear fishing is
    # permitted" except by reading the English — the two rendered as a bare contradiction.
    # Same for `required: false`, which is what makes an exemption an exemption.
    #
    # `v not in (...)` also matched by EQUALITY, so `0` matched `False` and `take: 0` would
    # have gone the same way if take were not already a column of its own.
    # Absent means false for most flags, so carrying them doubles the column for nothing. For
    # these four it does NOT: `permitted: false` is the whole content of "no spear fishing is
    # permitted", and `required: false` is what makes an exemption an exemption.
    _FALSE_MEANS_SOMETHING = ("permitted", "required", "on_retention", "when_open")
    _EMPTY = ((), [], {}, "")
    conditions = {k: v for k, v in dumped.items()
                  if k not in _NOT_CONDITIONS
                  and not any(v is e or v == e for e in _EMPTY)
                  and (v is not False or k in _FALSE_MEANS_SOMETHING)}
    return (
        entry_id, r.rule_id, r.type.value, r.family, r.dimension, rule_label(r),
        _specificity(raw),
        json.dumps(_windows(raw), separators=(",", ":")),
        json.dumps(list(r.species), separators=(",", ":")),
        json.dumps(list(r.species_except), separators=(",", ":")),
        r.take,
        None if r.may_target is None else int(r.may_target),
        json.dumps(conditions, separators=(",", ":"), sort_keys=True) or None,
        1 if uncertain else 0,
        r.verbatim, r.extent_text or None,
    )


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
            # A catalogue entry is FLAT: name/region at the top, the MUs the synopsis row was
            # printed under encoded in entry_id after the `@`, provenance in `source_pages` /
            # `symbols`. The retired prose entry nested all of it under `identity`/`source`.
            ident = e.get("identity") or {}
            matched = e.get("matched") or []
            entry_rows.append((
                e["entry_id"],
                # The first match, or nothing. An entry that never matched a water keeps its
                # rules and its text and carries a null item — the app shows "we have a rule
                # for a water we cannot place" rather than dropping it (77 of these).
                matched[0] if matched else None,
                ident.get("display_name") or e.get("display_name")
                or ident.get("name") or e.get("name"),
                # What the page printed, kept whole — see the note in schema.sql.
                ident.get("name") or e.get("name"),
                e.get("regs_verbatim"),
                # Provenance is nested under `source` now; it was a flat `source_symbols`
                # until the page number joined it and made it obvious they were one fact.
                json.dumps(e.get("symbols") or (e.get("source") or {}).get("symbols") or [],
                           separators=(",", ":")),
                json.dumps(ident.get("mus") or _mus_of(e["entry_id"]), separators=(",", ":")),
                json.dumps(e.get("source_pages") or (e.get("source") or {}).get("pages") or [],
                           separators=(",", ":")),
                e.get("scope_note") or None,
                json.dumps(e.get("extents") or [], separators=(",", ":")),
            ))
            for r in e.get("rules") or []:
                rule_rows.append(_rule_row(
                    e["entry_id"], r, (e["entry_id"], r.get("rule_id")) in unresolved,
                    e.get("extents")))

    # NAMED, not positional. A `pages` column was added to the schema while this line kept
    # seven placeholders, and nothing caught it until 90 seconds into a province-wide rebuild
    # — which then wrote a 42 MB bundle with zero entries in it. Naming the columns makes that
    # failure impossible rather than merely tested.
    db.executemany("INSERT INTO entry (entry_id, item_id, name, full_name, verbatim, symbols,"
                   "                   mus, pages, scope_note, extents)"
                   " VALUES (?,?,?,?,?,?,?,?,?,?)",
                   entry_rows)
    cov.filled("entry", len(entry_rows))
    # COLUMNS NAMED, for the third time and the same reason. This was eleven positional
    # placeholders, and adding `limits` to the schema made it eleven values for twelve
    # columns — the fault that once shipped a 42 MB bundle with no entries in it.
    db.executemany("INSERT INTO rule (entry_id, rule_id, type, family, dimension, label,"
                   "                  scope, windows, species, species_except, take,"
                   "                  may_target, conditions, uncertain, verbatim, extent_text) "
                   "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rule_rows)
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

    # COUNTED FROM THE SET, NOT FROM A TUPLE INDEX. This read `r[8]` — which is
    # `json.dumps(species)`, a string that is never empty ("[]" at minimum) and therefore
    # always truthy. Every build reported EVERY rule as uncertain: 3,422 of 3,422, a number
    # so obviously wrong it read as normal. The column itself was always right (256), so
    # nothing downstream was affected and nothing failed — only the line a person reads to
    # decide whether a build is healthy. The INSERT below names its columns for exactly this
    # reason; the summary went positional and drifted the moment a field moved.
    n_uncertain = len(unresolved & {(r[0], r[1]) for r in rule_rows})
    print(f"     rules: {len(entry_rows):,} entries · {len(rule_rows):,} rules "
          f"({n_uncertain} uncertain) · {len(section_set):,} sections carry one, "
          f"sharing {len(sets):,} distinct sets")

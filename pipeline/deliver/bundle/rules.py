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


def _when(r) -> str | None:
    """The rule's `when` — dates, hours, weekdays and unparsed seasons — as its own column.

    THE SEASON WAS LOST FROM THE APP. This column was `windows`, filled from the rule's raw
    `windows` or, failing that, `dates` — the prose model's date STRINGS, parsed a second time by
    `pipeline.regs.parsing.dates`. A catalogue rule has neither: its season is `when`, already
    structured by the model. So every rule shipped `windows = []`, which the client reads as ALL
    YEAR, and every seasonal closure in the province (0 of 3,269 rules had a window; 617 did before
    the `when` migration) read as in force every day. Nothing failed, because `[]` is a valid
    season.

    Now the model's own `When` ships whole and in the model's own shape (by alias, as
    `conditions` carried it), and there is no fallback: a rule's season comes from `when` or it
    has none. `unparsed` ships too — a season the parser could not read is NOT all year, and the
    client must treat such a rule as uncertain (core/status.ts), never as always in force.
    NULL = no `when` = all year, per the synopsis.
    """
    if r.when is None or r.when.is_empty():
        return None
    return json.dumps(r.when.model_dump(mode="json", by_alias=True, exclude_none=True),
                      separators=(",", ":"), sort_keys=True)


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
    # `when`, `while` and `standing` are columns of their own, so each has one home.
    "rule_id", "type", "verbatim", "species", "species_except", "when", "while", "standing",
    "take",
    "may_target", "extent_text", "review_reason",
    "unresolved_locators",
    # Build-time only: a carve-out the reach builder applies before any section reaches the
    # bundle. Shipping it as a `condition` would put a resolver's input in front of a reader.
    "tributary_excludes",
    # A column of its own, RESOLVED: see `_exempts`.
    "exempts",
})


def _zone_region(entry_id: str) -> str:
    """`r4:…` / `z4:…` -> "4", `z7a:…` -> "7a", `zp:…` -> "p"."""
    return entry_id.split(":", 1)[0][1:]


def _exempts(entry_id: str, r, zones: dict[str, list[str]], rules_of: dict[str, set[str]]):
    """The rule's `exempts`, each RESOLVED to the entry it lifts — the `exempts` column.

    EXEMPTIONS WERE APPLIED NOWHERE. 88 rules carry one, 63 of them "Exempt from spring
    closure", and the bundle shipped the field buried in `conditions`, which the client never
    selected — so the North Thompson read CLOSED on May 1 beside its own "Exempt from spring
    closure", and about 27k sections carried a zone default next to the rule that lifts it.

    Resolved HERE, once, so the client matches exact ids and never a bare name (AGENTS 8):

      `default_id`  a zone default by its slug: every zone entry `z<region>:<slug>` of THIS
                    rule's region (Region 7's rows reach both 7A and 7B). NEVER the rule's own
                    entry — `z6:steelhead_stream_closure` names its own slug, and a rule that
                    lifts itself deletes itself on every water it covers (the self-lift; its
                    `review_reason` carries it).
      `target`      one rule by id, in `entry_id` when the exemption says so, else in this entry.

    An exemption that resolves to nothing lifts nothing, which is only allowed when the rule
    says why in `review_reason`; otherwise the build stops, because a lift that silently fails
    leaves a closure standing where the book lifted it.

    Shipped as `[{"default_id", "entry_id"} | {"target", "entry_id"}]` (+ the authored `note`
    when there is one), one item per entry lifted. NULL = the rule lifts nothing."""
    out: list[dict] = []
    for x in r.exempts:
        got: list[dict] = []
        if x.default_id:
            region = _zone_region(entry_id)
            for z in zones.get(x.default_id, ()):
                zr = _zone_region(z)
                if z != entry_id and (zr == region or (region and zr[:-1] == region
                                                       and zr[-1:] in ("a", "b"))):
                    got.append({"default_id": x.default_id, "entry_id": z})
        if x.target:
            in_entry = x.entry_id or entry_id
            if x.target in rules_of.get(in_entry, ()) and not (
                    in_entry == entry_id and x.target == r.rule_id):
                got.append({"target": x.target, "entry_id": in_entry})
        if x.note:
            got = [dict(g, note=x.note) for g in got]
        if not got and not r.review_reason:
            raise SystemExit(
                f"{entry_id}/{r.rule_id}: exempts {x.model_dump(exclude_none=True)} lifts no "
                f"rule in the corpus. Name the rule (`target` + `entry_id`), or say why in "
                f"`review_reason` — a lift that silently resolves to nothing leaves the "
                f"closure standing where the book lifted it")
        out += got
    return json.dumps(out, separators=(",", ":"), sort_keys=True) if out else None


def _rule_row(entry_id: str, raw: dict, uncertain: bool, siblings=None, zones=None,
              rules_of=None, unresolved: str | None = None):
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
    # THE RULE'S OWN EXTENTS, AND ONLY THOSE. A rule with none used to be given its entry's here,
    # so 139 rules the reach builder left UNBOUND — "on parts", a place it could not draw —
    # shipped `extents: [{op: whole}]` beside `uncertain = 1`: the bundle claiming the whole water
    # for a rule placement had refused to widen (AGENTS 13). Inheritance is placement's job, and
    # placement has already done it; what a rule binds is in `ruleset`, not here.
    #
    # No flag in the model means something when False, so False is dropped like any default.
    # (`v is False`, not `v == False`: `0 == False`, and a zero is a value.)
    _EMPTY = ((), [], {}, "")
    conditions = {k: v for k, v in dumped.items()
                  if k not in _NOT_CONDITIONS
                  and not any(v is e or v == e for e in _EMPTY)
                  and v is not False}
    return (
        entry_id, r.rule_id, r.type.value, r.family, r.dimension, rule_label(r, siblings),
        _specificity(raw),
        _when(r),
        # WHILE, AS A COLUMN, because the client decides an OUTCOME from it: "only non-game fish
        # may be speared" is take 0 on every game fish WHILE spear fishing, and read without the
        # `while` it is "No fishing" on 1,674 of 1,693 rulesets — every river in B.C. The app
        # looked for it as `conditions.method`, a field that no longer exists, in a column its
        # query never selected. NULL = the rule binds whatever method you use.
        json.dumps(list(r.while_), separators=(",", ":")) if r.while_ else None,
        # STANDING: the rule holds everywhere but its place is unknowable — "no fishing within 23 m
        # downstream of any fishway" is bound to every section because no dataset of fishways
        # exists. It must be SHOWN and must never decide a water's colour: take 0 on every game
        # fish, read as a closure, painted every section in the province CLOSED once the spear
        # rule stopped doing it. A column, because the client decides an outcome from it.
        1 if r.standing else 0,
        json.dumps(list(r.species), separators=(",", ":")),
        json.dumps(list(r.species_except), separators=(",", ":")),
        _exempts(entry_id, r, zones or {}, rules_of or {}),
        r.take,
        None if r.may_target is None else int(r.may_target),
        json.dumps(conditions, separators=(",", ":"), sort_keys=True) or None,
        1 if uncertain else 0,
        # WHY it could not be placed — "reason: detail", as the licensing tables have it.
        unresolved,
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
    """Write `entry`, `rule`, `section_ruleset`, `ruleset`, and every licensing table."""
    sections_file = reaches / "rule_section.jsonl"
    if not sections_file.exists():
        for t in ("entry", "rule", "section_ruleset", "ruleset"):
            cov.skip(t, f"no reach run at {reaches}")
        return

    # THE 88, FIRST. A rule nobody could place must never render as "no rules here" — it can
    # only ever raise "unknown" — so the flag has to be on the rule row itself, and it is
    # read from the reach builder's own failure table rather than guessed at from the
    # curation. `rule_unresolved` is the table that must never be silently short.
    unresolved = {(r["entry_id"], r["rule_id"]): f"{r['reason']}: {r['detail']}"
                  for r in _jsonl(reaches / "rule_unresolved.jsonl")}

    from pipeline.regs.parsing.catalogue import CatalogueEntry

    from pipeline.regs.parsing.io import _holds_entries

    entry_rows, rule_rows = [], []
    ces = []
    docs = []
    # EVERY ENTRY SOURCE, and nothing else. A source is a directory of region files carrying
    # `entries` (see `io.entry_sources`); the DFO directory beside the catalogue holds region
    # files of another shape and is not one. Inside a source, a region file WITHOUT `entries` is
    # a defect, and stops the build — reading `doc.get("entries", [])` skipped it in silence.
    for d in sorted({p.parent for p in entries_dir.rglob("region-*.json")}):
        if not _holds_entries(d):
            continue
        for path in sorted(d.glob("region-*.json")):
            doc = json.loads(path.read_text(encoding="utf-8"))
            if "entries" not in doc:
                raise SystemExit(f"{path}: a region file in an entry source has no `entries`")
            for e in doc["entries"]:
                # Read through the model, so a key from a retired shape is refused, not read.
                docs.append((e, CatalogueEntry.model_validate(e)))
    # What an exemption may name: zone entries by slug, and every entry's rule ids.
    zones: dict[str, list[str]] = {}
    rules_of: dict[str, set[str]] = {}
    for _, ce in docs:
        rules_of[ce.entry_id] = {r.rule_id for r in ce.rules}
        if ce.entry_id.startswith("z"):
            zones.setdefault(ce.entry_id.split(":", 1)[1], []).append(ce.entry_id)
    for e, ce in docs:
        ces.append(ce)
        matched = list(ce.matched)
        entry_rows.append((
            ce.entry_id,
            # The first match, or nothing. An entry that never matched a water keeps its
            # rules and its text and carries a null item — the app shows "we have a rule
            # for a water we cannot place" rather than dropping it (77 of these).
            matched[0] if matched else None,
            ce.display_name or ce.name,
            # What the page printed, kept whole — see the note in schema.sql.
            ce.name,
            ce.regs_verbatim,
            json.dumps(list(ce.symbols), separators=(",", ":")),
            json.dumps(_mus_of(ce.entry_id), separators=(",", ":")),
            json.dumps(list(ce.source_pages), separators=(",", ":")),
            ce.scope_note or None,
            json.dumps(e.get("extents") or [], separators=(",", ":")),
            # EVERY water matched; `item_id` above is only the first.
            json.dumps(matched, separators=(",", ":")),
        ))
        # A rule may name another in its entry (`suspended_while`), and its label says what
        # that rule is in words — so each label is built with its siblings to hand.
        siblings = {r.rule_id: r for r in ce.rules}
        for r in e.get("rules") or []:
            k = (e["entry_id"], r.get("rule_id"))
            rule_rows.append(_rule_row(e["entry_id"], r, k in unresolved, siblings, zones,
                                       rules_of, unresolved=unresolved.get(k)))

    # NAMED, not positional. A `pages` column was added to the schema while this line kept
    # seven placeholders, and nothing caught it until 90 seconds into a province-wide rebuild
    # — which then wrote a 42 MB bundle with zero entries in it. Naming the columns makes that
    # failure impossible rather than merely tested.
    db.executemany("INSERT INTO entry (entry_id, item_id, name, full_name, verbatim, symbols,"
                   "                   mus, pages, scope_note, extents, matched)"
                   " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                   entry_rows)
    cov.filled("entry", len(entry_rows))
    # COLUMNS NAMED, for the third time and the same reason. This was eleven positional
    # placeholders, and adding `limits` to the schema made it eleven values for twelve
    # columns — the fault that once shipped a 42 MB bundle with no entries in it.
    db.executemany("INSERT INTO rule (entry_id, rule_id, type, family, dimension, label,"
                   "                  scope, when_, while_, standing, species, species_except,"
                   "                  exempts, take, may_target, conditions, uncertain, unresolved,"
                   "                  verbatim, extent_text) "
                   "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rule_rows)
    cov.filled("rule", len(rule_rows))

    # One pass over the bindings (149 M rows on the full corpus), noting every rule it names.
    bound_rules: set[tuple[str, str]] = set()

    def _noting(rows):
        for r in rows:
            bound_rules.add((r["entry_id"], r["rule_id"]))
            yield r

    section_set, sets = intern_sets(_noting(_jsonl(sections_file)))

    # THE REACH RUN MUST BE THIS CORPUS'S. A run built before a rule was removed still binds it,
    # and `ruleset` then names a rule the `rule` table does not have — measured on the bundle
    # built after the licensing rules left: 144 such rules, 31,354 ruleset rows, in 2,347 of the
    # 2,375 sets. The reader's JOIN drops such a row without a word, so the check is here: every
    # rule the run placed exists, and every rule that exists was placed or explained.
    placed_rules = bound_rules | set(unresolved)
    have_rules = {(r[0], r[1]) for r in rule_rows}
    gone, never = sorted(placed_rules - have_rules), sorted(have_rules - placed_rules)
    if gone or never:
        raise SystemExit(
            f"rules: the reach run at {reaches} is not this corpus's — it binds {len(gone):,} "
            f"rule(s) the corpus no longer has (e.g. {gone[:3]}) and never saw {len(never):,} it "
            f"does have (e.g. {never[:3]}). Re-run the reach builder:\n"
            f"    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")

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

    # AND AGAIN AGAINST THE ROWS WRITTEN, not the input they came from: a ruleset row naming a
    # rule the bundle does not hold is a regulation that exists on the map and nowhere else.
    orphan = db.execute(
        "SELECT rs.entry_id, rs.rule_id FROM ruleset rs LEFT JOIN rule r "
        "ON r.entry_id = rs.entry_id AND r.rule_id = rs.rule_id "
        "WHERE r.rule_id IS NULL LIMIT 5").fetchall()
    if orphan:
        raise SystemExit(f"ruleset names rules that are not in `rule`: {orphan}")

    # Licensing: the other half of each entry, placed by the same reach run.
    from pipeline.deliver.bundle import licensing as _licensing
    _licensing.write(db, reaches, ces, cov, sid)

    # COUNTED FROM THE SET, NOT FROM A TUPLE INDEX. This read `r[8]` — which is
    # `json.dumps(species)`, a string that is never empty ("[]" at minimum) and therefore
    # always truthy. Every build reported EVERY rule as uncertain: 3,422 of 3,422, a number
    # so obviously wrong it read as normal. The column itself was always right (256), so
    # nothing downstream was affected and nothing failed — only the line a person reads to
    # decide whether a build is healthy. The INSERT below names its columns for exactly this
    # reason; the summary went positional and drifted the moment a field moved.
    n_uncertain = len(set(unresolved) & {(r[0], r[1]) for r in rule_rows})
    print(f"     rules: {len(entry_rows):,} entries · {len(rule_rows):,} rules "
          f"({n_uncertain} uncertain) · {len(section_set):,} sections carry one, "
          f"sharing {len(sets):,} distinct sets")

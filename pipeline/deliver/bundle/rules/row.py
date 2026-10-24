"""One catalogue rule -> its `rule` row (`_rule_row`): `when` as its own column, the fields that are not
columns into `conditions`, its specificity, and its resolved lifts (`exempts`, from `lifts`)."""
from __future__ import annotations

import json
from pipeline.deliver.bundle.rules.lifts import _exempts


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
    client must treat such a rule as uncertain, never as always in force (the reader belongs in
    app/packages/core/src/regulations.ts; the export's `guide.time` says so).
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
        # A PART OF A WATERSHED (`Extent.watershed`) is an area cut by code, like the whole
        # watershed (`area:basin:`) it is part of — not the river the cut sits on.
        if ex.get("watershed"):
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
    "may_target", "extent_text", "review_reason", "undrawn_part",
    "unresolved_locators",
    # Build-time only: a carve-out the reach builder applies before any section reaches the
    # bundle. Shipping it as a `condition` would put a resolver's input in front of a reader.
    "tributary_excludes",
    # A column of its own, RESOLVED: see `_exempts`.
    "exempts",
    # Build-time only: which closure a lift reaches in another region (`_equivalent_closures`).
    # What it decided ships RESOLVED, as `exempts[].equivalent`.
    "closure_kind",
})


def _rule_row(entry_id: str, raw: dict, uncertain: bool, siblings=None, zones=None,
              rules_of=None, unresolved: str | None = None, place_of=None, entries=None,
              closures=None, regions=None, blankets=None):
    """One `rule` row from one catalogue rule.

    Validated through `CatalogueRule` rather than read off the dict, because `family`,
    `dimension` and `label` are all DERIVED — and deriving them here from raw fields would put
    a second implementation of each in the codebase. `label` in particular has one home for
    the same reason the gauge trust wording does: a rule worded two ways is two rules to a
    reader.
    """
    from pipeline.regs.parsing.catalogue import (_COUNTED_TYPES, CatalogueRule, Obligation,
                                                 compose, label_parts)

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
    # THE CLOCK, ON THE RULES THAT COUNT AND ONLY THOSE. The model leaves `period` unset where the
    # book's default (daily) holds; a counting rule ships its clock outright so no reader has to
    # know the default, and no other rule ships one (1,549 bait bans and boat rules said "daily").
    if r.type in _COUNTED_TYPES:
        conditions["period"] = r.clock.value
    # LAW IS THE DEFAULT: `obligation` ships only where the book gives ADVICE ("should"). It sat
    # on all 3,348 rules as "must", a key a reader had to learn to ignore.
    if conditions.get("obligation") == Obligation.must.value:
        del conditions["obligation"]
    # `entries`: what a rule lifts in ANOTHER entry is named in words, not by that entry's slug.
    parts = label_parts(r, siblings, place_of, entries)
    return (
        entry_id, r.rule_id, r.type.value, r.family, r.dimension,
        # THE LINE, AS PARTS, and the one preview composed from them (`catalogue.compose`). The
        # place is named from the extents, in the book's words (`place_names`), so two rules of
        # one entry on different reaches never share a line.
        compose(parts, r.verbatim),
        json.dumps(parts, separators=(",", ":"), ensure_ascii=False),
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
        _exempts(entry_id, r, zones or {}, rules_of or {}, closures, regions, blankets),
        r.take,
        None if r.may_target is None else int(r.may_target),
        json.dumps(conditions, separators=(",", ":"), sort_keys=True) or None,
        1 if uncertain else 0,
        # WHY it could not be placed — "reason: detail", as the licensing tables have it.
        unresolved,
        r.verbatim, r.extent_text or None, r.undrawn_part or None,
    )

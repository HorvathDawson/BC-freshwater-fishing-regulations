"""What a rule IS, read the one way every consumer reads it: whether it keeps fish (`keeps`), closes
(`closure_grade`) or releases (`catch_and_release`, `release_origins`), and the statement it makes
(`statement`, `same_statement`). The aggregate groups a rule may count or close as a class."""
from __future__ import annotations

from pipeline.common import entry_id as EID
from pipeline.regs.parsing.catalogue_parts.vocab import says_no_fishing


def _zone_region(entry_id: str) -> str:
    """`r4:…` / `z4:…` -> "4", `z7a:…` -> "7a", `zp:…` -> "" (`entry_id.region_of`; never equal to a
    region, as the "p" it used to say never was)."""
    return EID.region_of(entry_id) or ""


#: Groups that are NOT the name of a fish: an aggregate the book counts or closes as a class
#: ("Trout/char: 5", "No fishing in any stream"). A rule naming one of these names no single fish,
#: so it never takes part in a lift by naming (`_named_lifts`). "Bass" and "whitefish" are the
#: book's names for fish and count as naming each of theirs. "Char" stays an aggregate HERE — a
#: water's "char catch and release" derives no lift of a region's closure naming one char — though
#: the competition reads it as naming char (`read.names_fish`, `catalogue.NAMING_GROUPS`).
AGGREGATE_GROUPS = frozenset({"ALL_GAME_FISH", "ALL_FIN_FISH", "TROUT_CHAR", "CHAR",
                              "PROTECTED_SPECIES", "SALMON"})


#: The two origins a fish can have. A rule with no `origin` holds for both.
ORIGINS = frozenset({"wild", "hatchery"})


def release_origins(x: dict) -> frozenset[str] | None:
    """THE ORIGINS A RULE RELEASES OUTRIGHT — `None` when it is not an outright release.

    A bundle rule (`read.rules` shape) releases a fish outright when it is a `retention_limit`
    with `take: 0` that holds at every length (no `lengths`: "No wild trout over 50 cm" releases a
    size class, and the 4 under it still stand), whatever the means (no `while`), however the fish
    was caught (no `caught`: "any fish snagged must be released" releases only a snagged fish, user
    ruling Q38 — read as outright it would silence every quota in B.C.), whatever is
    targeted (no `when_targeting`), and as a statement of its own (not a `within` clause, not a
    record-keeping duty). A closure is one. What it releases is its `origin`, or both.

    Shared by the competition (`read.effective_rules`) and anything that reasons about what a
    water row withholds, so the two cannot disagree on what "catch and release" is."""
    if x.get("type") != "retention_limit" or x.get("take") != 0:
        return None
    if x.get("lengths") or x.get("while") or x.get("caught") or x.get("when_targeting") \
            or x.get("within") or x.get("record_retention") or x.get("dimension") == "lift":
        return None
    return frozenset({x["origin"]}) if x.get("origin") else ORIGINS


#: What keeps a "no fishing" from being the whole answer: a closure carrying any of these holds
#: only for some fish, some gear, some part or some shore — it is PARTIAL (`closure_grade`).
CLOSURE_CONDITIONS = ("lengths", "origin", "while", "caught", "when_targeting", "within", "side")


#: What `closure_grade` answers for a closure (None: not one), in wire order (`types.ClosureGrade`).
CLOSURE_GRADES = ("full", "partial")


def catch_and_release(x: dict) -> bool:
    """THE OTHER READING OF `take: 0`: the fish may still be fished for, and every one goes back
    ("Rainbow trout catch and release") — never a closure (`closure_grade`). With `closure_grade`,
    the only reader of `may_target` downstream of the bundle (`test_one_data_flow` gate 2). Reads
    a bundle rule or the export's flattened fields (no `type`: a retention statement)."""
    return x.get("type", "retention_limit") == "retention_limit" and x.get("take") == 0 \
        and bool(x.get("may_target"))


def closure_grade(x: dict) -> str | None:
    """THE ONE CLOSURE PREDICATE: "full" for an unconditional "no fishing" (a `retention_limit`
    with take 0, may not fish for it, and none of `CLOSURE_CONDITIONS`, drawn — not a note held on
    its water for an undrawn part); "partial" for a closure that carries a condition or is such a
    note; None for anything that is not a closure (a quota, a release the fish may still be
    fished for, a lift).

    Every reader asks this function, with the edge it needs stated at the call: the status index
    colours CLOSED on "full" only (`status_index.is_full_closure`); the competition treats any
    closure as a closure that speaks for every fish it covers (`read.effective_rules`) and lets a
    "full" one beat another region's quota (`read.stricter`); the export's sample cases take any
    closure (`pipeline.deliver.export.cases._closure`). Four spellings with four exclusion lists lived at those
    call sites before; a rule with `side` was a closure to the reader and not to the map.

    Reads a bundle rule (`read.rules`: `undrawn_part`) or the export's flattened fields
    (`not_yet_mapped`): both spellings of the same column."""
    # `type` when the dict carries one (a bare {take, may_target} statement is a retention one)
    if not says_no_fishing(x.get("type", "retention_limit"), x.get("take"), x.get("may_target")):
        return None
    if any(x.get(k) for k in CLOSURE_CONDITIONS):
        return "partial"
    if str(x.get("undrawn_part") or "").strip() or x.get("not_yet_mapped"):
        return "partial"
    return "full"


def keeps(x: dict) -> frozenset[str] | None:
    """THE ORIGINS A RULE LETS AN ANGLER KEEP, when an outright release must silence it: a
    `retention_limit` that allows something (`take` above 0, `unlimited`) or states only sizes
    (`take` absent) — whatever conditions it holds under (origin, water, while, size). `None` for
    a rule that keeps nothing (another release: equally strict, it stands beside), for a rule that
    is no count of fish (a record-keeping duty; the possession multiplier "twice the daily quota",
    which has no number of its own to keep), and for a lift.

    THE ONE "DOES THIS RULE KEEP A FISH?" (T125 item 2; it was `yields_to_release`): the reader's
    competition asks it, and so does the answers' paper-licence duty (`display.record_kept`: a
    record duty stands only where a keeping quota for its fish speaks), which used to re-read the
    rule's `dimension` string for its own take-above-0 test."""
    if x.get("type") != "retention_limit" or x.get("record_retention") \
            or x.get("dimension") == "lift":
        return None
    take = x.get("take")
    if not (x.get("unlimited") or (take is not None and take > 0)
            or (take is None and x.get("lengths"))):
        return None
    return frozenset({x["origin"]}) if x.get("origin") else ORIGINS


def _leaves(codes) -> frozenset[str]:
    from pipeline.regs.parsing.catalogue import expand_species
    return frozenset(expand_species(list(codes or [])))


def statement(x: dict) -> tuple:
    """WHAT A QUOTA IS ABOUT, without its number: the fish (leaves, less `species_except`), its
    size bounds (each band's ends, and whether the band keeps none), its origin, its water kind,
    the means it holds under (`while`), how the fish was caught (`caught`), what must be targeted
    (`when_targeting`), and its clock.

    Two quotas with the same statement say the same thing with different numbers — Kokanee: 10 at
    a lake beside the region's Kokanee: 5 — and only then does the water's number replace the
    zone's (`same_statement`).

    THE PRINTED WORD "TROUT" IS ONE STATEMENT, whether or not its row's mention of char takes the
    char out of it (`catalogue.trout_scope_problems`, user ruling 2026-09-28): Amor Lake's "Trout
    daily quota = 2" (its row names no char: trout and char) and Region 1's "Trout: 4" (its box
    releases "All char": trout only) both say "trout", so for a trout the lake's 2 replaces the
    4 — and Region 1's char release still binds the char. So a `CHAR` exception on `TROUT_CHAR` is
    the word's scope, not part of what the statement is about."""
    bands = tuple((b.get("min_cm"), b.get("max_cm"), b.get("take") == 0, bool(b.get("closed")))
                  for b in (x.get("lengths") or []))
    sp = list(x.get("species") or [])
    out = [c for c in (x.get("species_except") or [])
           if not (c == "CHAR" and "TROUT_CHAR" in sp)]
    return (_leaves(sp) - _leaves(out),
            bands, x.get("origin"), x.get("water"),
            tuple(sorted(x.get("while") or [])), tuple(sorted(x.get("when_targeting") or [])),
            x.get("period") or "daily", bool(x.get("record_retention")),
            tuple(sorted(x.get("caught") or [])))


def same_statement(a: dict, b: dict) -> bool:
    """DO TWO QUOTAS SAY THE SAME THING (`statement`: same fish or group, size bounds, origin,
    water kind, means, target and clock)? Between a water's quota and a zone's (user rulings
    2026-09-26): when they do, the WATER's number replaces the zone's, larger or smaller —
    Tranquille Lake's "kokanee daily quota = 10" over Region 3's "Kokanee: 5", never the smaller
    of the two. When they do not, both speak: the Dean's "Steelhead daily quota = 1" counts
    toward Region 5's "Trout/char: 5". A water row printing a larger number for a fish than the
    zone's aggregate LIFTS the aggregate for that fish (`exempts`) — it is not decided here.
    Both must be keeping quotas (`keeps`)."""
    return statement(a) == statement(b)

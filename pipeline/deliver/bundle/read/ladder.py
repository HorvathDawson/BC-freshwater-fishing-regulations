"""Who speaks — the ladder, executable (`effective_rules`).

THE REFERENCE SEMANTICS THE APP MUST MATCH. The export's `guide.ladder` states the ruling in words and this
states it in code, so the words can be tested on real sections. It is deliberately small and reads the bundle
only. When app/packages/core/src/regulations.ts grows its reader, it answers these same questions the same
way, and `pipeline/tests/test_competition.py` is the list of cases it must reproduce.

The named POLICY switches (`MOOT_SIZE_CLAUSE_HIDDEN`, `WATER_CLOSURE_DOMINANT`, `OWN_ROW_BEATS_INHERITED`,
`OWN_ROW_REPLACES_INHERITED`, `NEARER_ROW_BEATS_FARTHER`) live here, beside the steps that read them, so a
test that switches one off (on this module) proves the step is what decides it.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import List
from pipeline.common import entry_id as EID
from pipeline.deliver.bundle.read.records import BUNDLE, rules
from pipeline.deliver.bundle.read.source import source_of
from pipeline.deliver.bundle.read.facts import (
    _base_dimension, _spoken_lengths, _steelhead_min_cm, as_rainbow, covers, in_force, names_fish,
    not_yet_mapped, released_on_water, speaks_for, steelhead_rules, steelhead_water
)


_RULES_BY_PATH: dict = {}

#: POLICY (RULINGS Q9): a zone-side size-only clause made moot by an outright
#: release or closure of the same fish is not shown (`effective_rules` step 5b). Named so a test can
#: switch it off and prove the step is what hides the clause.
MOOT_SIZE_CLAUSE_HIDDEN = True

#: POLICY (RULINGS Q10): a water's OWN full closure in force is the most dominant
#: rule — it silences every keeping rule for the fish it covers, of any source and any key, save a
#: superior authority's (`effective_rules` step 4c). Named so a test can switch it off and prove the
#: step is what silences them.
WATER_CLOSURE_DOMINANT = True

#: POLICY (RULINGS T4): a tributary with a row of its
#: own gets BOTH its own row's rules and the rules it inherits by the tributary walk ("Kettle River's
#: tributaries"), and where the two speak to the same thing (one competition key, one fish) ITS OWN
#: ROW SPEAKS — before naming: Granby River's own "trout/char daily quota = 1" (upstream of Burrell
#: Creek) beats the inherited "Rainbow trout catch and release" for a rainbow, although the inherited
#: line names the fish. A CLOSURE IS NOT A STATEMENT AN OWN ROW OUT-RANKS: an inherited "No Fishing
#: Jul 25-Sept 15" still closes Granby River on its dates (it gives way only to a lift — a printed
#: exemption, or a dated opening inside it — exactly as a zone closure does under the strict
#: lift, RULINGS L1), and an inherited keeping rule an own CLOSURE beats is silenced as before.
#: `effective_rules` step 4; named so a test can switch it off and prove the step decides it.
OWN_ROW_BEATS_INHERITED = True

#: POLICY (RULINGS C-5): INHERITED RULES BEHAVE LIKE ZONE RULES —
#: a rule of the water's own row REPLACES an inherited rule of the same kind (type and dimension)
#: on every day, as a row's dated bait ban replaces its zone's all-year one (L8). Granby River's
#: own "bait ban Apr 1-Oct 31" replaces Kettle River's tributaries' all-year "bait ban": on Nov 15
#: Granby has no bait ban. Quotas keep Q5 (`OWN_ROW_BEATS_INHERITED`, on the days both hold, as a
#: water's quota meets a zone's) and closures still combine (L20). `effective_rules` step 3b;
#: named so a test can switch it off and prove the step decides it.
OWN_ROW_REPLACES_INHERITED = True

#: POLICY (RULINGS T14): WHERE TWO ✱ ROWS COMPETE ON A TRIBUTARY,
#: THE ROW OF THE WATER NEAREST THAT STREAM WINS — on the Duncan River's tributaries the Duncan's "bull
#: trout daily quota = 2 (any size)" beats Kootenay Lake's tributaries' "Bull trout catch and release".
#: Both rules reach the stream by the tributary walk (the `inherited` rung); NEARER is the bundle's
#: `trib_nearer` (`bundle/nearer.py`: near's own water drains into far's, so walking down from the
#: stream near's water is met first). Decided BEFORE NAMING, as the own row is (T4) — the nearer row is
#: the more specific statement for the stream — and never a closure either way (an inherited closure
#: is only lifted). A farther row's release it beats releases nothing (step 5). `effective_rules` step
#: 4; named so a test can switch it off and prove the step decides it.
NEARER_ROW_BEATS_FARTHER = True

#: The states of a rule that is IN the answer (`effective_rules`). With `trace=True` the answer also
#: holds the rules that took part and lost, each with a state outside this set ("lifted",
#: "displaced", "moot"), its `reason` (`LOSS_REASONS`) and `by` (the rule that beat it).
SPEAKERS = ("speaks", "beside", "shown", "not_yet_mapped")
SPEAKER_STATES = frozenset(SPEAKERS)

#: Why a rule that took part lost — one reason per rule, the step that removed it first.
LOSS_REASONS = {
    "lifted": "lifted",                      # step 3: an exemption in force lifts it
    "ladder": "displaced",                   # step 4: a better rung of its key beats it
    "own_row": "displaced",                  # step 4: the water's own row beats an inherited rule
    "water_dates": "displaced",              # step 4a: a water row's own dates override it
    "zone_release": "displaced",             # step 4b: its table's release empties it
    "closure": "displaced",                  # step 4c: a full closure beats it by the ladder
    "water_closure": "displaced",            # step 4c: the water's own full closure (DENETIAH)
    "size_release": "displaced",             # step 4d: a water's size-limited release covers it
    "water_release": "displaced",            # step 5: the water's release silences the zone
    "same_row_release": "displaced",         # step 5: its row's dated release or closure (RU-3)
    "moot_size_clause": "moot",              # step 5b: "doesn't matter today"
    "stricter_region": "displaced",          # step 6: the other region's stricter rule
    "same_as_peer": "displaced",             # step 6: the other region's identical line (RU-8)
    "nearer_row": "displaced",               # step 4: the nearer ✱ row beats a farther one (T14)
}

#: The states a loser takes (`LOSS_REASONS` values, in their first order): lifted, displaced, moot.
LOSER_STATES = tuple(dict.fromkeys(LOSS_REASONS.values()))

#: The origins a reader may ask about (`effective_rules(origin=…)`); None = not known.
ASKABLE_ORIGINS = ("hatchery", "wild")

#: How a rule reaches a section (`ruleset.via`): by its own extents, or by the tributary walk.
VIAS = ("reach", "trib")


def place(rule: dict, via: str) -> int:
    """A rule's rung WHERE IT IS BOUND — `source_of(rule).rank`, with a water row reaching the
    section by the tributary walk at the `inherited` rung (1): the ladder's place, as step 4 reads
    it. The ONE spelling (DATAFLOW M1); every consumer that orders rules by place asks this."""
    if via not in VIAS:
        raise ValueError(f"read.place: via {via!r} is not one of {VIAS}")
    r = rule["_rank"]                 # `_rules_of` works it out once per rule (`source_of`)
    return 1 if via == "trib" and r == 0 else r


_ladder_place = place


#: (near, far) water-row entry pairs per bundle path (`bundle.nearer`, table `trib_nearer`)
_NEARER_BY_PATH: dict = {}


def _nearer_of(path: str) -> frozenset:
    """The bundle's `trib_nearer` pairs (RULINGS T14), read once per path. A real bundle without the
    table is refused (rebuild it); a hand-made one (no `rule` table) or a test's seeded rule table
    (a path that is no file) has none unless a test seeds `_NEARER_BY_PATH`."""
    got = _NEARER_BY_PATH.get(path)
    if got is not None:
        return got
    import os
    if path in _RULES_BY_PATH and not os.path.exists(path):
        got = frozenset()
    else:
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            if "trib_nearer" in names:
                got = frozenset(db.execute("SELECT near, far FROM trib_nearer"))
            elif "rule" in names:
                raise SystemExit(f"read: {path} has no `trib_nearer` table — rebuild the bundle "
                                 f"(`python -m pipeline.deliver.bundle`)")
            else:
                got = frozenset()
        finally:
            db.close()
    _NEARER_BY_PATH[path] = got
    return got


def _rules_of(path: str) -> dict:
    """Every rule of a bundle by `(entry, rule)`, each with its ladder rank (`source_of`) worked
    out once — read once per bundle path."""
    got = _RULES_BY_PATH.get(path)
    if got is None:
        got = _RULES_BY_PATH[path] = {(x["entry"], x["rule"]): x for x in rules(path)}
        for x in got.values():
            x["_rank"] = source_of(x).rank
    return got


def base_region(entry_id: str) -> str | None:
    """`z3:…` -> "3", `z7a:…` -> "7a"; `None` for the province's table (`zp:`) and every row."""
    return EID.zone_region(entry_id)


def _count(x: dict) -> float:
    return float("inf") if x.get("unlimited") else float(x["take"])


def stricter(a: dict, b: dict) -> bool:
    """Is rule `a` STRICTER than rule `b` about the same fish — so that, between two regions'
    zone rules on one lake, `a` applies and `b` does not (`effective_rules` step 6)?

      a closure (take 0, may not fish for it; at every size, whatever the means) beats a retention
        rule that is not a closure;
      an outright release (`rules.release_origins`) beats a quota keeping only origins it releases
        (`rules.keeps`);
      of two quotas stating the same thing (`rules.same_statement`), the lower beats the higher.

    Anything else is not stricter: two statements sit beside each other, and a gear or method rule
    is never displaced by one of another region (both apply)."""
    from pipeline.deliver.bundle.rules import (closure_grade, release_origins, same_statement,
                                               keeps)
    if b.get("type") != "retention_limit" or a.get("type") != "retention_limit":
        return False
    # a FULL closure (`rules.closure_grade`): unconditional, at every size, whatever the means
    shut = lambda x: closure_grade(x) == "full"          # noqa: E731
    if shut(a):
        return not shut(b)
    if shut(b):
        return False
    rel, kept = release_origins(a), keeps(b)
    if rel and kept and kept <= rel:
        return True
    ka = keeps(a)
    if ka and kept and a.get("take") is not None and (b.get("take") is not None
                                                        or b.get("unlimited")) \
            and same_statement(a, b) and (not A_NARROWER_CAP_NEVER_DISPLACES_A_WIDER
                                          or _fish_of(a) >= _fish_of(b)):
        return _count(a) < _count(b)
    return False


#: OWNER 2026-10-24 (T183 follow-ups): ON A PIECE IN TWO REGIONS EVERY CAP IS THE STRICTER OF THE TWO — and a cap
#: over FEWER fish is another cap, not a stricter copy. Region 6's "1 trout from streams July 1-Oct 31" (trout/char
#: less char) and Zone 7A's "2 from streams" (trout and char) say the same WORD ("from streams", `same_statement`
#: keeps a CHAR exception out of the statement), but Region 6's holds no char: had it displaced Zone 7A's for a
#: rainbow, the rainbow would count toward no stream share of 2 and an angler could keep 1 trout AND 2 char from
#: the stream. Both bind: the stricter of each. A quota displaces the other region's only when it holds every fish
#: that one holds. False = the CHAR scope ignored here too (the mutation pin: Big Bend Creek keeps 3).
A_NARROWER_CAP_NEVER_DISPLACES_A_WIDER = True


def _fish_of(x: dict) -> frozenset:
    """The fish a quota holds: its species' leaves less EVERY exception (a CHAR exception included)."""
    from pipeline.deliver.bundle.rules.predicates import _leaves
    return _leaves(x.get("species") or []) - _leaves(x.get("species_except") or [])


def effective_rules(section: int, on, fish: str, path: str = BUNDLE, *,
                    by_naming: bool = True, origin: str | None = None,
                    trace: bool = False, at=None) -> List[dict]:
    """THE RULES THAT SPEAK FOR ONE FISH, ON ONE SECTION, ON ONE DAY — the ladder as code.

    `section` is a bundle `sid`, `on` a `datetime.date` or `(month, day)`, `fish` a leaf species
    code. Returns the rules bound to the section that say something about that fish on that day,
    each a `rules()` dict with `state` added: "speaks", "beside" (of unreadable season, or on one
    half of the channel only — `side` — or, asked without a moment, in force only some hours or
    weekdays: shown, never displacing), "shown" (never competes), or
    "not_yet_mapped" (holds only in a part nothing draws). Sorted by `rid`.

    `at` is the MOMENT asked (`calendar.Moment`: a class of weekdays, inside or outside the key's
    hours window). Asked at one, a weekday or hours rule DECIDES like any other rule — in force at
    the moments it covers, out at the others (`in_force`; RULINGS D4). The verdicts
    ask every moment of every key (`calendar.moments`).

      0. A RAINBOW OVER 50 CM IS A STEELHEAD where the bundle says anadromous rainbow are found
         (`steelhead_water`, p.80): asked about "RB" there, every rule is read over rainbow of 50
         cm or less (`as_rainbow`) — one speaking only of rainbow over 50 cm speaks for no
         rainbow, and a rainbow release "(50 cm or less)" is an outright release. The larger
         fish is asked about as "ST".
      1. IN FORCE ON THE DAY. A rule whose `when` excludes the day is out, and so is one dormant
         under `suspended_while` while its named closure is in force here (no rule carries it
         today — licensing records do; the branch holds the reading for when one does).
      2. ABOUT THIS FISH (`speaks_for`). Competition is PER FISH: two rules compete only for the
         fish both speak for, so Zone B's "Bull trout … release" never touches what "Trout/char:
         5" says about a rainbow.
      3. LIFTS. A lift from a rule in force here removes the lifted rule for this fish — outright,
         or when its `species` holds the fish and its `when` holds the day. A lift that holds only
         while fishing FOR something (`when_targeting`) or while doing something (`while`), or
         only some hours or weekdays when no moment is asked, or only for a fish of some origin or size (`origin`, `lengths`: known
         only once it is caught), leaves the rule standing (the angler is unknown). Lifts are
         printed exemptions, or DERIVED at build (`basis: names_the_fish`, `rules._named_lifts`): a
         water row naming a fish its region closes lifts that closure for the fish both name — the
         only way a closure leaves (step 4 never displaces one) — never further than the lifter
         covers, and never a closure that prints its own exemption list.
      4. COMPETITION, on `(type, dimension)`. Among competitors, for this fish:
           a SUPERIOR authority first (nothing below it opens what it closed); then
           NAMING — a rule that names the fish beats one naming a group that holds it (Zone B's
           bull trout release beats Kakwa Lake's "Trout/char daily quota = 2" for bull trout,
           although the lake row is the more specific place); then
           (a `within` clause is named at its parent quota's level — "Trout/char: 5, but not more
           than 1 bull trout" is a trout/char quota, and does not name bull trout over a water's
           "Trout/char catch and release"); then
           PLACE — `source_of(rule).rank`: this water, then inherited by the tributary walk (a
           water rule reaching this section `via: trib`), then an area, the region, the province.
         THE WATER'S OWN ROW BEATS WHAT IT INHERITS, BEFORE NAMING (RULINGS T4,
         `OWN_ROW_BEATS_INHERITED`): a tributary with a row of its own (Granby River under "Kettle
         River's tributaries") gets both, and on one key its own rule speaks — "trout/char daily
         quota = 1" over the inherited "Rainbow trout catch and release" for a rainbow. Never a
         closure either way: an inherited closure is only lifted, an own closure beats as before.
         THE NEARER ✱ ROW BEATS A FARTHER ONE, BEFORE NAMING (RULINGS T14,
         `NEARER_ROW_BEATS_FARTHER`): two rows reaching the stream by the walk, the one whose water
         drains into the other's (`trib_nearer`) speaks — on the Duncan's tributaries the Duncan's
         "bull trout daily quota = 2" over Kootenay Lake's tributaries' bull trout release. Never a
         closure either way.
         So a water row that itself names the fish ("Bull trout daily quota = 1") beats the
         zone's bull trout rule: both name it, and the water is more specific.
         A rule is displaced only by a better rule of ANOTHER quota family: a `within` clause and
         its parent quota are one statement ("Trout/char: 5, but not more than 3 lake trout")
         and never displace each other. Ties all speak.
         A RULE THAT IS ITSELF DISPLACED DISPLACES NOTHING (RULINGS Q24): a rule is out
         only when a rule that stands beats it (Cheslatta Lake, Nov 15: Region 6's lake trout
         release, overridden by the lake's own dated quota, no longer takes Region 6's
         "Trout/char: 5" and "3 Dolly Varden/bull trout and/or lake trout" with it).
         A RULE DISPLACES ONLY OVER THE LENGTHS IT SPEAKS ABOUT (`covers`, RULINGS Q25): "Rainbow trout
         over 50 cm catch and release" never displaces a quota counting smaller rainbow.
         A WATER'S QUOTA AND THE ZONE'S (RULINGS Q4), both keeping fish — a quota
         written for this water (or reaching it by the walk) and a zone, area or provincial one:
           the SAME statement (`rules.same_statement`: the same fish or group, size bounds,
             origin, water kind, means, target and clock) — the WATER's number replaces the
             zone's, larger or smaller, whatever naming says: Tranquille Lake's "kokanee daily
             quota = 10" replaces Region 3's "Kokanee: 5" (never the smaller of the two);
           DIFFERENT statements sit beside each other and both speak: the Dean's "Steelhead
             daily quota = 1" counts toward Region 5's "Trout/char: 5";
           a LARGER number for a fish than the zone's aggregate gives it is printed as a lift of
             the zone's quota for that fish (`exempts`, step 3): Kootenay Lake's "rainbow trout
             daily quota = 10 (any size)" lifts Region 4's "Trout/char: 5" and its "1 rainbow or
             cutthroat over 50 cm" for rainbow, so the 10 speaks alone.
         A DATED ZONE RELEASE OR CLOSURE is never displaced by a water's quota WITH NO DATES OF
           ITS OWN (RULINGS Q6) unless the water's rule is the exact same statement on
           the same dates: Shuswap Lake's "Char daily quota = 1" leaves Region 3's "Lake trout
           from Oct 15-Jan 31" release speaking beside it on those dates. Only a lift removes it
           otherwise.
         A WATER ROW PRINTING ITS OWN DATES FOR THE FISH overrides a dated zone release or quota
           for that fish on the days both hold (RULINGS Q6): Cheslatta
           Lake's "Lake trout … quotas = 3" (Nov 1-Sept 14) replaces Region 6's "Lake trout from
           Fraser and Skeena Watersheds, Sept 15-Nov 30" release on Nov 1-30. Only for the same
           fish or subject, a quota or release against a release or quota — never a closure.
         A CLOSURE ("No fishing": take 0, may not fish for it) is never displaced — "this water
         overrides regional always, except closures unless they are lifted in this water's regs".
         Only a lift removes it; it still displaces what ranks below it — and it speaks for every
         fish it covers AS IF IT NAMED IT, so a water's "No Fishing, Nov 1-Apr 30" silences the
         zone's "Burbot: 5" on its dates (read as a group rule, it let the 5 speak beside it).
     4a. A WATER ROW'S OWN DATES OVERRIDE A ZONE RELEASE LIMITED TO A KIND OF WATER (RULINGS
         Q7): as `water_dates_override` does for any dated zone release, on the
         base dimension — the release's `water: stream` is part of its dimension, so step 4
         never set it against the row. Michel Creek's own "Trout/char catch and release, June
         15-Mar 31" replaces Region 4's "Trout/char release: in streams from Nov 1-Mar 31" on
         the overlap. The overridden release displaces nothing in 4b and releases nothing in 5.
     4b. A ZONE RELEASE LIMITED TO A KIND OF WATER (`water: stream`, `released_on_water`), in
         force on that kind of water, displaces its own region's table's quotas and clauses that
         keep the fish, in the same dimension read without conditions — as Region 3's "Lake trout
         from Oct 15-Jan 31" does with no `water`: "Bull trout (Dolly Varden) from streams, Aug
         1-Oct 31" silences "Trout/char: 5", "1 over 50 cm" and "1 bull trout or lake trout" for
         a bull trout on a Region 3 stream; Region 4's "Trout/char release: in streams from Nov
         1-Mar 31" silences Region 4's quotas for every trout and char on its streams. A size
         clause of another dimension ("none under 60 cm"), a closure, a water row and another
         region's rule are untouched.
         GENERALISED (RU-4, RULINGS Q12): a zone release printed with NO water kind (and no
         `while` / `when_targeting` condition) displaces its own table's keeping rules in the
         same base dimension whatever THEIR water condition, THE SAME KEY INCLUDED — Region 5's
         "ALL STEELHEAD" release empties "2 per day … from streams" for a steelhead too, and
         the 7B grayling release May 1-Jun 15 silences that table's "2 per day" and "1 over 45
         cm" on its dates (a tie step 4 left standing). "Base dimension" is the dimension without
         its `@` conditions: a size clause with no count ("none under 60 cm", `daily/size`) is
         another one, which 4b and 4c do not reach — step 5b does.
     4c. A FULL CLOSURE DISPLACES THE KEEPING RULES IT BEATS BY THE LADDER WHATEVER THEIR KEY
         (RU-7, RULINGS Q12): a blanket stream closure (`water: stream`) silences the region's
         "5 per day" and "1 over 50 cm" as it silences its own key's "4 from streams".
         A WATER'S OWN FULL CLOSURE IS THE MOST DOMINANT RULE (RULINGS Q10):
         a full closure written for this water (a row's rule bound here by its own place, not the
         tributary walk, not an area row) silences EVERY rule that would let the fish be kept, of
         any source and any key — zone, area rows, other water rows reached by the walk, other
         dimensions (possession, annual, sizes) — on its dates. Denetiah Creek's "No fishing, Jul
         1-15" silences the Liard River watershed row's bull trout "1 in possession"; the ladder
         and naming no longer matter there. Only a SUPERIOR authority's rule stands, and the
         closure's own lifters (a rule exempting it in part) speak beside it.
     4d. A WATER'S SIZE-LIMITED RELEASE MEETS THE ZONE'S SIZE CLAUSE (RU-5, RULINGS Q12):
         Lakelse Lake's "none over 50 cm" displaces Region 6's "no more than 1 over 50 cm" — a
         zone keeping rule whose spoken lengths lie wholly inside the class the water releases.
      5. A WATER'S RELEASE SILENCES THE ZONE FOR THAT FISH (RULINGS Q3): an outright
         release in force here, written for this water or reached by the tributary walk,
         displaces every zone/area/provincial quota that would keep the fish, WHATEVER its
         conditions, when every origin that quota keeps is released here (see step 5 below,
         `rules.release_origins`, `rules.keeps`). Such a release counts even when
         step 4 put it behind a zone release naming the fish, and one step 4 put behind a
         looser zone rule naming the fish (one that lets it be kept) speaks again.
         AND THE ROW'S OWN UNDATED QUOTA (RU-3, RULINGS Q12): a dated outright release OR
         CLOSURE of the same row displaces that row's undated quota for the fish on its dates
         (the Thompson's May catch and release over its "2 per day"; Adams, Big and Sulphurous
         lakes' lake trout releases over their "1 per day"; and, as built, a row's dated
         closure over its own quota — Quatse r1 May 1-Jun 15 over r2, the Region 7 lakes'
         "No fishing Nov 1-Apr 30" over their quotas, Kitimat r2 Mar 16-May 31 over its
         hatchery steelhead 2, the Okanagan's Oct 1-Nov 15 over its perch quota).
     5b. A ZONE SIZE CLAUSE MADE MOOT BY AN OUTRIGHT RELEASE OF THE SAME FISH is not shown (RULINGS
         Q9): a zone-side size-only clause ("none under 60 cm") gives way to any
         surviving outright release or closure in force here — water, zone or superior — that
         releases every origin it keeps, over its lengths: Bonaparte Lake, Nov 1, lake trout shows
         Region 3's release and not the clause. The release or closure itself is untouched.
      6. TWO REGIONS' BASES — THE MOST STRICT APPLIES (RULINGS Z4). A lake straddling
         a region line binds both regions' zone rules; neither outranks the other (step 4 does not
         set them against each other). Per fish, a zone rule of one region is displaced by a
         `stricter` zone rule of the other: a closure beats open, a release beats a quota, the
         lower of two quotas stating the same thing beats the higher; different statements sit
         beside; gear and method rules of both apply. Two regions' IDENTICAL statements (the
         same number) are shown once (RU-8, RULINGS Q12).
      0'. A STEELHEAD IS A RAINBOW WHERE NO STEELHEAD RULE APPLIES (RULINGS
         S7): "ST" on a section the provincial steelhead set does not reach
         (`steelhead_rules`) is answered as "RB", over every length.
      Rules that never compete pass through with state "shown": `standing`, the information
      family. A "beside" rule neither displaces nor is displaced. Lift-only rules (dimension
      `lift`) state nothing and are not returned.
      A RULE IN A PART NOBODY HAS DRAWN (`not_yet_mapped`: `undrawn_part` — "No Fishing within 200
      m of Bush-Sullivan Bridge" held on the whole of Kinbasket Lake) is returned with state
      "not_yet_mapped" when it is in force and about the fish, and takes no part in anything
      above: it displaces nothing, lifts nothing, silences nothing and suspends nothing (RULINGS
      E2). It is a place on the water the map cannot show yet, never the water.

    `by_naming=False` ranks by place alone — the ladder before the naming ruling — and exists
    only so an audit can list what the ruling changed.

    `origin` ("hatchery" / "wild"; None, the default, is today's answer: the origin is not known)
    answers for a fish of that origin (gap G2, the consumer page's reading): a lift limited to one
    origin LIFTS OUTRIGHT when that origin is asked (Kitimat's "hatchery rainbow 5" lifts Region
    6's quotas for a hatchery rainbow), and a lift for the other origin does not lift at all. With
    no origin asked such a lift leaves its target standing, `partly_lifted`. Nothing else reads the
    origin: rules of the other origin are still returned, as they are today.

    `trace=True` (gap G1) also returns every rule that TOOK PART and LOST — in force, about the
    fish, not a lift-only rule — with `state` "lifted", "displaced" or "moot" (a size clause that
    doesn't matter today, step 5b), `reason` (`LOSS_REASONS`: the step that removed it first) and
    `by` (the rid of the rule that beat it); a partly lifted rule carries `lifted_in_part_by`. The
    same code path: the rules in `SPEAKER_STATES` are exactly the untraced answer, and with the
    default off the answer is unchanged."""
    _leaf_fish(fish)
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        bound = db.execute("SELECT r.entry_id, r.rule_id, r.via FROM section_ruleset s JOIN ruleset r "
                           "ON r.set_id = s.set_id WHERE s.sid = ?", (section,)).fetchall()
        steelhead_here = fish == "RB" and steelhead_water(db, section)
        # WHETHER STEELHEAD RULES APPLY HERE (`section_steelhead_rules`, the reach run's answer).
        # A hand-made bundle of rule sets (the tests' `_tiny`: no `rule` table, no view) says
        # nothing of steelhead: the fish is answered as asked. A REAL bundle missing the view is
        # refused by `steelhead_rules` (review F7: probing the view here and defaulting to True
        # answered ST as asked on such a bundle, silently).
        hand_made = not db.execute("SELECT 1 FROM sqlite_master WHERE name IN "
                                   "('rule', 'section_steelhead_rules')").fetchone()
        rules_here = True if fish != "ST" or hand_made else steelhead_rules(db, section)
    finally:
        db.close()
    return effective_rules_bound(bound, steelhead_here, on, fish, path, by_naming=by_naming,
                                 steelhead_rules_here=rules_here, origin=origin, trace=trace,
                                 at=at)


def origin_matters(bound, path: str = BUNDLE) -> bool:
    """WHETHER THE ORIGIN ASKED CAN CHANGE THIS ANSWER: only a lift limited to one origin reads it
    (`effective_rules_bound` step 3 — "Nothing else reads the origin"). Where no rule of the set
    lifts anything for one origin, the hatchery and wild answers ARE the answer with the origin
    not known, and need not be asked (the verdicts stage's shortcut; proved over every key by
    `test_verdicts.py`). The reader owns this predicate, beside the only code that reads it."""
    every = _rules_of(path)
    return any(x.get("origin") for e, r, _ in bound for x in every[(e, r)].get("exempts") or [])


def _leaf_fish(fish: str) -> None:
    from pipeline.regs.parsing.catalogue import expand_species
    if expand_species([fish]) != [fish]:
        raise ValueError(f"effective_rules: {fish!r} is a group, not one fish — ask about a leaf "
                         f"code ({', '.join(expand_species([fish]))})")


def effective_rules_bound(bound, steelhead_here: bool, on, fish: str, path: str = BUNDLE, *,
                          by_naming: bool = True, steelhead_rules_here: bool = True,
                          origin: str | None = None, trace: bool = False,
                          at=None) -> List[dict]:
    """`effective_rules` with the section's bindings already in hand — the SAME code, minus the
    three lookups that are all a section contributes: its `(entry_id, rule_id, via)` rows (its
    ruleset), whether the steelhead definition holds there (`steelhead_water`, which matters
    only for "RB") and whether the steelhead rules apply there (`steelhead_rules`, which matters
    only for "ST"). Every section sharing a ruleset (and the two flags) gets the same answer,
    which is what lets `pipeline.deliver.status_index` evaluate 2,103 rulesets instead of 1.9 M
    sections. Not a second reading of the ladder: `effective_rules` calls this.

    A STEELHEAD IS A RAINBOW WHERE NO STEELHEAD RULE APPLIES (RULINGS S7): asked
    about "ST" on a section the provincial steelhead set does not reach (the Okanagan River, the
    Fraser in 7A — `steelhead_rules_here` False), the answer is the RAINBOW's, read over every
    length (no 50 cm cap: the fish is a rainbow of any size there). The export says the same in
    words (`steelhead_rules: false`); this is the one definition the status index and every
    client reader follow.

    `origin`, `trace` and `at` (the moment): see `effective_rules`.

    The steps of `effective_rules`' docstring run in order, one function each, over one `_Ask`
    (the question and every set the steps share); every removal goes through `_Ask.lose`."""
    _leaf_fish(fish)
    if origin is not None and origin not in ASKABLE_ORIGINS:
        raise ValueError(f"effective_rules: origin {origin!r} — ask "
                         f"{' or '.join(ASKABLE_ORIGINS)}, or None (not known)")
    if fish == "ST" and not steelhead_rules_here:
        fish, steelhead_here = "RB", False
    steelhead_here = bool(steelhead_here) and fish == "RB"
    orig = _rules_of(path)
    s = _Ask(orig=orig, every=orig, here={(e, r): via for e, r, via in bound if (e, r) in orig},
             fish=fish, on=on, at=at, origin=origin, by_naming=by_naming,
             nearer=_nearer_of(path) if NEARER_ROW_BEATS_FARTHER else frozenset())
    _step0_steelhead_sizes(s, steelhead_here)
    _step1_in_force(s)
    _step3_lifts(s)
    _step3b_own_row_replaces(s)
    _step2_about_this_fish(s)
    _step4_competition(s)
    _step4a_water_dates_on_a_water_kind(s)
    _step4b_zone_release(s)
    _step4c_closures(s)
    _step4d_size_release(s)
    _step5_water_release(s)
    _step5b_moot_size_clause(s)
    _step6_two_regions(s)
    return _answer(s, trace)


@dataclass(eq=False, slots=True)
class _Ask:
    """ONE QUESTION TO THE LADDER (`effective_rules_bound`): one ruleset, one fish, one day (and
    moment, and origin) — and every set its steps hand on to the next. The predicates the steps
    share are its methods; a step's own helpers sit beside the step."""
    orig: dict                # every rule of the bundle by (entry, rule), as stored
    every: dict               # the same, each read over the rainbow's sizes where step 0 says so
    here: dict                # (entry, rule) -> via ("reach" / "trib"): the section's ruleset
    fish: str
    on: object
    at: object
    origin: str | None
    by_naming: bool
    nearer: frozenset = frozenset()                    # (near, far) entries (`trib_nearer`, T14)
    # WHY EACH LOSER LOST (gap G1): rule -> (reason, the rule that beat it), kept on every call —
    # the steps remove rules only through `lose`, so the trace is the same code path as the
    # answer; `trace` only decides whether the losers are returned.
    why: dict = field(default_factory=dict)
    partly_by: dict = field(default_factory=dict)
    no_rainbow: set = field(default_factory=set)      # step 0: speaks for no rainbow here
    top: float = float("inf")                          # step 0: the longest fish asked about
    state: dict = field(default_factory=dict)          # step 1: in_force, "yes" / "part" / "no"
    undrawn: set = field(default_factory=set)          # `not_yet_mapped` rules
    live: set = field(default_factory=set)             # step 1: in force at all
    lifted: set = field(default_factory=set)           # step 3
    partly: set = field(default_factory=set)           # step 3: lifted in part
    replaced: set = field(default_factory=set)         # step 3b
    cand: set = field(default_factory=set)             # step 2: about this fish, not lifted
    keyed: dict = field(default_factory=dict)          # step 4: competitors by (type, dimension)
    out_: set = field(default_factory=set)             # the answer, as the steps leave it
    overridden: set = field(default_factory=set)       # zone rules a surviving water row's own dates overrode
    rel: dict = field(default_factory=dict)            # step 5: releaser -> the origins it releases

    def lose(self, k, reason: str, by) -> None:
        """Take `k` out of the answer: `reason` (`LOSS_REASONS`) and `by`, the rule that beat it —
        the first removal is the one recorded."""
        self.out_.discard(k)
        self.why.setdefault(k, (reason, by))

    def competes(self, k) -> bool:
        x = self.every[k]
        return self.state[k] == "yes" and not x.get("standing") \
            and x.get("family") != "information" and k not in self.undrawn

    def closure(self, k) -> bool:
        # any closure, conditioned or not (`rules.closure_grade`): it speaks for every fish it
        # covers, and the condition travels with it
        from pipeline.deliver.bundle.rules import closure_grade
        return closure_grade(self.every[k]) is not None

    def order(self, k) -> tuple:
        x = self.every[k]
        rank = x["_rank"]
        if self.here[k] == "trib" and rank == 0:
            rank = 1
        # A `within` clause is named at its PARENT's level: "Trout/char: 5, but not more than 1
        # bull trout" is a trout/char quota with a sub-limit, not a bull trout rule — read as one,
        # it would reopen bull trout at a water printing "Trout/char catch and release".
        parent = self.every.get((k[0], x["within"])) if x.get("within") else None
        named = 0 if (not self.by_naming or self.closure(k)
                      or names_fish(parent or x, self.fish)) else 1
        return (0 if rank < 0 else 1, named, rank)

    def family(self, k) -> tuple:
        x = self.every[k]
        return (k[0], x.get("within") or x.get("condition_of") or k[1])

    def place(self, k) -> int:
        return _ladder_place(self.every[k], self.here[k])

    def row_area(self, k) -> bool:
        """A WATER TABLE'S AREA ROW (`source_of`: Scope.area on an `r<n>:` entry — the CVWMA
        waters, Bowron Lake Park waters, the Liard River watershed). It ranks as an area, so a
        named water's own row beats it by place; against the ZONE it is still a row of the water
        tables, and the water-vs-zone rulings below read it as one (`water_side`)."""
        return EID.is_water_row(str(k[0])) and self.every[k]["_rank"] == 2

    def water_side(self, k) -> bool:
        """Written for this water (or reaching it by the walk), or a water table's area row."""
        return 0 <= self.place(k) <= 1 or self.row_area(k)

    def zone_side(self, k) -> bool:
        """A zone, area or provincial table's rule — not a water table's row."""
        return self.place(k) >= 2 and not self.row_area(k)

    def water_and_zone(self, o, k) -> bool:
        """Two quotas that both let the fish be kept, one written for this water (or reaching it
        by the walk), the other a zone, area or provincial one — never a superior authority's."""
        from pipeline.deliver.bundle.rules import keeps
        return bool(keeps(self.every[o]) and keeps(self.every[k])
                    and ((self.water_side(o) and self.zone_side(k))
                         or (self.water_side(k) and self.zone_side(o))))

    def dated_zone_release(self, k) -> bool:
        """A zone, area or provincial rule keeping NONE of the fish (take 0: a release, or a
        closure) on printed dates — Region 3's "you must release … Lake trout from Oct 15-Jan 31"."""
        x = self.every[k]
        return self.zone_side(k) and x.get("type") == "retention_limit" and x.get("take") == 0 \
            and bool((x.get("when") or {}).get("dates"))

    def exact_same(self, o, k) -> bool:
        """The water's rule says EXACTLY what the dated zone rule says — the same fish, sizes,
        origin, water kind, means and target (`rules.statement`) on the same dates."""
        from pipeline.deliver.bundle.rules import same_statement
        every = self.every
        return same_statement(every[o], every[k]) and \
            (every[o].get("when") or {}).get("dates") == every[k]["when"]["dates"]

    def dated_zone_retention(self, k) -> bool:
        """A zone, area or provincial retention rule on printed dates that states a NUMBER for
        the fish — a release (take 0) or a quota — and is no closure: Region 6's "Lake trout from
        Fraser and Skeena Watersheds, Sept 15-Nov 30" (must release)."""
        x = self.every[k]
        return self.zone_side(k) and x.get("type") == "retention_limit" and not self.closure(k) \
            and (x.get("take") is not None or bool(x.get("unlimited"))) \
            and not x.get("record_retention") and bool((x.get("when") or {}).get("dates"))

    def water_dates_override(self, o, k) -> bool:
        """A WATER ROW PRINTING ITS OWN DATES FOR THE FISH overrides a dated zone rule for that
        fish, on the days both hold (RULINGS Q6). Cheslatta and Murray lakes print
        "Lake trout catch and release, Sept 15-Oct 31" and "quotas = 3" (Nov 1-Sept 14); Region
        6 prints "Lake trout from Fraser and Skeena Watersheds, Sept 15-Nov 30" (release). On Nov
        1-30 both are in force and the lake's 3 replaces the region's release; on Sept 15-Oct 31
        the lake's own release speaks. The overlap needs no arithmetic: both rules are in force on
        the day asked, or this is never asked.

        Only COMPATIBLE rules: `o` is written for this water (or reaches it by the walk) and
        states a number for the fish (a quota or a release — not a size clause, a duty, a lift)
        on dates of its OWN; `k` is a dated zone retention rule stating a number
        (`dated_zone_retention` — never a CLOSURE: a blanket spring closure still closes); and
        both are about THE SAME FISH OR SUBJECT — the water row names this fish, or the two
        state the same set of fish (`rules.statement`). The water rule must hold for every fish
        the zone rule does: no narrower origin, water kind, means or target. A water row with no
        dates of its own leaves the dated zone release speaking (Shuswap, `beats`)."""
        from pipeline.deliver.bundle.rules import statement
        x, z = self.every[o], self.every[k]
        if not (self.water_side(o) and x.get("type") == "retention_limit"
                and (x.get("take") is not None or x.get("unlimited"))
                and not x.get("within") and not x.get("record_retention")
                and bool((x.get("when") or {}).get("dates")) and self.dated_zone_retention(k)):
            return False
        if not (names_fish(x, self.fish) or statement(x)[0] == statement(z)[0]):
            return False
        for c in ("origin", "water"):
            if x.get(c) and x.get(c) != z.get(c):
                return False
        for c in ("while", "caught", "when_targeting"):
            if x.get(c) and set(x[c]) != set(z.get(c) or []):
                return False
        return True

    def zone_line_restated(self, o, k) -> bool:
        """A ZONE TABLE'S LINE ABOUT ONE WATER, SAID AGAIN BY THAT WATER'S ROW (the UI consumer's
        report). Region 3 prints "Annual catch quota for Shuswap Lake: … Char-Lake trout and
        Bull trout (Dolly Varden): 5 over 60 cm" (p.28) and the Shuswap row "Char daily quota = 1
        (none under 60 cm), annual quota = 5" (p.32): one limit, printed twice. The zone line names
        the lake (an item extent), so it ranks as the water's own and neither displaced the other —
        the page showed the annual 5 twice. When a zone table's (`z<region>:`) keeping quota and
        the water row's (`r…`) are the SAME STATEMENT (`rules.same_statement`), the row's speaks,
        as a water's number replaces the zone's (`water_and_zone`)."""
        from pipeline.deliver.bundle.rules import same_statement, keeps
        return (EID.is_water_row(str(o[0])) and base_region(k[0]) is not None
                and self.water_side(o) and self.water_side(k)
                and bool(keeps(self.every[o])) and bool(keeps(self.every[k]))
                and same_statement(self.every[o], self.every[k]))

    def beats(self, o, k) -> bool:
        """Does `o` displace `k` for this fish (of another quota family, not two regions' peers)?

        Between a WATER quota and a ZONE quota that both keep fish (RULINGS Q4):
          THE SAME STATEMENT (`rules.same_statement`: the same fish or group, size bounds,
            origin, water kind, means, target and clock) — the WATER's number replaces the
            zone's, larger or smaller: never the smaller of the two, and never undone by naming
            (Tranquille Lake's "kokanee daily quota = 10" replaces Region 3's "Kokanee: 5");
          DIFFERENT STATEMENTS sit beside each other — both speak (the Dean's "Steelhead: 1"
            counts toward Region 5's "Trout/char: 5"). A water row printing a LARGER number for a
            fish than the zone's aggregate is a different statement: it takes the fish out of the
            aggregate by LIFTING the zone's quota for that fish (`exempts`, step 3 — Kootenay
            Lake's "rainbow trout daily quota = 10 (any size)" lifts Region 4's "Trout/char: 5"
            for rainbow), never by this comparison.
        A DATED ZONE RELEASE OR CLOSURE IS NOT SILENCED BY A WATER'S QUOTA (RULINGS
        Q6): a water rule that lets the fish be kept displaces a zone rule keeping none of
        it on printed dates only when it is the EXACT same statement on the same dates
        (`exact_same`). Shuswap Lake's "Char daily quota = 1" names lake trout, and by naming and
        place it silenced Region 3's "Lake trout from Oct 15-Jan 31" release — the stricter rule,
        and no override of it. A lift the row prints, or a derived lift of a closure that sends
        the reader to the tables, still removes it (step 3).
        A WATER ROW WITH DATES OF ITS OWN FOR THE FISH overrides a dated zone release or quota
        for it on the days both hold (RULINGS Q6, `water_dates_override`): Cheslatta
        Lake's "Lake trout daily and possession quotas = 3" (Nov 1-Sept 14) replaces Region 6's
        "Lake trout from Fraser and Skeena Watersheds, Sept 15-Nov 30" release on Nov 1-30.
        Everything else: the better rung (`order`) displaces.

        A RULE DISPLACES ONLY OVER THE LENGTHS IT SPEAKS ABOUT (`covers`, RULINGS Q25): "Rainbow trout
        over 50 cm catch and release" never displaces a rule that speaks about smaller fish."""
        from pipeline.deliver.bundle.rules import same_statement, keeps
        every = self.every
        if not covers(every[o], every[k], self.top):
            return False
        if self.water_dates_override(o, k):
            return True
        if self.water_and_zone(o, k):
            return self.water_side(o) and same_statement(every[o], every[k])
        if self.water_side(o) and keeps(every[o]) and self.dated_zone_release(k):
            return self.exact_same(o, k)
        if self.zone_line_restated(o, k):
            return True
        if self.own_over_inherited(o, k):
            return True
        if self.own_over_inherited(k, o):
            return False
        if self.nearer_over_farther(o, k):
            return True
        if self.nearer_over_farther(k, o):
            return False
        return self.order(o) < self.order(k)

    def nearer_over_farther(self, o, k) -> bool:
        """THE NEARER ✱ ROW BEATS A FARTHER ONE (RULINGS T14, `NEARER_ROW_BEATS_FARTHER`): both
        reach this section by the tributary walk as water rows, `o`'s water is nearer the stream than
        `k`'s (`trib_nearer`), and neither is a closure (an inherited closure is only lifted)."""
        return (o[0], k[0]) in self.nearer and self.here[o] == "trib" and self.here[k] == "trib" \
            and self.every[o]["_rank"] == 0 and self.every[k]["_rank"] == 0 \
            and not self.closure(o) and not self.closure(k)

    def precedes(self, o, k) -> bool:
        """A precedence that decides BEFORE NAMING: the own row over what it inherits (T4), the nearer
        ✱ row over a farther one (T14) — the edges a cycle is broken at (step 4)."""
        return self.own_over_inherited(o, k) or self.nearer_over_farther(o, k)

    def own_over_inherited(self, o, k) -> bool:
        """THE WATER'S OWN ROW BEATS WHAT IT INHERITS (RULINGS T4, `OWN_ROW_BEATS_INHERITED`):
        `o` is written for this water (bound here by its own place, not the walk) and `k` reaches
        it only by the tributary walk, and neither is a closure (an inherited closure is lifted,
        never out-ranked; an own closure already beats every keeper by the ladder)."""
        return OWN_ROW_BEATS_INHERITED and self.place(o) == 0 and self.here[k] == "trib" \
            and self.every[k]["_rank"] == 0 and not self.closure(o) and not self.closure(k)

    def base(self, k) -> str | None:
        """The region whose OWN table (`z<region>:`, not the province's) wrote the rule."""
        return base_region(k[0]) if self.every[k]["_rank"] >= 2 else None

    def peers(self, o, k) -> bool:
        """Two regions' zone rules on one section — a lake straddling their line (step 6)."""
        a, b = self.base(o), self.base(k)
        return a is not None and b is not None and a != b

    def clock(self, k) -> str:
        # the rule's clock alone: "daily/size" (sizes with no count) and "daily" are one clock
        return _base_dimension(self.every[k]).split("/", 1)[0]

    def said(self, k) -> str:
        if k in self.undrawn:
            return "not_yet_mapped"
        return "speaks" if self.competes(k) else "beside" if self.state[k] == "part" else "shown"


def _step0_steelhead_sizes(s: _Ask, steelhead_here: bool) -> None:
    # 0. WHERE A RAINBOW OVER 50 CM IS A STEELHEAD (p.80), a rainbow rule speaks only for rainbow
    #    of 50 cm or less: each rule is read over that range (`as_rainbow`), and one that speaks
    #    only of rainbow over 50 cm ("1 over 50 cm") speaks for no rainbow here — the fish is a
    #    steelhead, asked about as "ST".
    # the lengths a rule may speak about here (`covers`): every rainbow is 50 cm or less where a
    # larger one is a steelhead
    s.top = float(_steelhead_min_cm()) if steelhead_here else float("inf")
    if steelhead_here:
        orig = s.orig
        s.every = every = dict(orig)
        for k in s.here:
            if speaks_for(orig[k], s.fish):
                v = as_rainbow(orig[k])
                if v is None:
                    s.no_rainbow.add(k)
                else:
                    every[k] = v


def _step1_in_force(s: _Ask) -> None:
    """1. IN FORCE ON THE DAY (and moment), the half of the channel, the undrawn parts, and the
    rules dormant under their closure."""
    every, here = s.every, s.here
    state = s.state = {k: in_force(every[k].get("when"), s.on, s.at) for k in here}
    # ONE HALF OF THE CHANNEL (`side`: Kitimat River's "No Fishing on the west half of river …"):
    # the rule holds on part of the section's width, so, like a rule holding some hours, it is
    # shown BESIDE the rules the other half answers to and displaces none (RULINGS E4).
    for k in here:
        if every[k].get("side") and state[k] == "yes":
            state[k] = "part"
    undrawn = s.undrawn = {k for k in here if not_yet_mapped(every[k])}
    for k in here:                                          # 1. dormant under its closure
        sw = every[k].get("suspended_while")
        if sw and state.get((k[0], sw)) == "yes" and (k[0], sw) not in undrawn:
            state[k] = "no"
    s.live = {k for k, st in state.items() if st != "no"}


def _step3_lifts(s: _Ask) -> None:
    """3. LIFTS (see `effective_rules`)."""
    every, state, live, undrawn = s.every, s.state, s.live, s.undrawn
    fish, origin, on, at = s.fish, s.origin, s.on, s.at
    lifted, partly, partly_by, why = s.lifted, s.partly, s.partly_by, s.why
    for k in sorted(live):                                   # 3. lifts
        if state[k] != "yes" or k in undrawn:
            continue
        for x in every[k].get("exempts") or []:
            t = (x["entry_id"], x["rule_id"])
            # the lift's fish, read as a rule's species ("ALL_FIN_FISH" is every fin fish)
            if t not in live or ("species" in x
                                 and not speaks_for({"species": x["species"]}, fish)):
                continue
            # A lift for some anglers (a target, a means) or some fish (an origin, a size — which
            # the angler learns only once it is caught) leaves the rule standing, partly lifted.
            # ASKED FOR ONE ORIGIN (gap G2): a lift for that origin alone lifts it outright, a lift
            # for the other origin not at all.
            if origin is not None and x.get("origin"):
                if x["origin"] != origin:
                    continue
                some = x.get("when_targeting") or x.get("while") or x.get("lengths")
            else:
                some = x.get("when_targeting") or x.get("while") or x.get("origin") \
                    or x.get("lengths")
            if some:
                partly.add(t)
                partly_by.setdefault(t, []).append(k)
                continue
            got = in_force(x.get("when"), on, at) if "when" in x else "yes"
            if got == "yes":
                lifted.add(t)
                why.setdefault(t, ("lifted", k))
            elif got == "part":
                partly.add(t)
                partly_by.setdefault(t, []).append(k)


# 3b. THE WATER'S OWN ROW REPLACES WHAT IT INHERITS OF THE SAME KIND (RULINGS C-5,
#     `OWN_ROW_REPLACES_INHERITED`): an inherited rule (a water row reaching the section by the
#     tributary walk) gives way, ON EVERY DAY, to a rule of the water's own row of the same type
#     and dimension that speaks for the fish at least as widely — whether or not the own rule
#     is in force today. Not a quota (Q5 decides those on the days both hold, step 4) and not
#     a closure (an inherited closure is lifted, never replaced).
def _own_replacement(s: _Ask, k):
    from pipeline.deliver.bundle.rules import closure_grade
    every, here = s.every, s.here
    x = every[k]
    if here[k] != "trib" or x["_rank"] != 0 or x.get("type") == "retention_limit" \
            or x.get("dimension") == "lift" or closure_grade(x) is not None:
        return None
    for o in sorted(here):
        y = every[o]
        if here[o] == "reach" and y["_rank"] == 0 and o not in s.undrawn \
                and (y.get("type"), y.get("dimension")) == (x.get("type"), x.get("dimension")) \
                and not y.get("standing") and not y.get("exempts") \
                and closure_grade(y) is None and speaks_for(y, s.fish) \
                and all(not y.get(c) or y.get(c) == x.get(c)
                        for c in ("when_targeting", "while", "caught", "origin", "water", "lengths",
                                  "side", "within", "condition_of")):
            return o
    return None


def _step3b_own_row_replaces(s: _Ask) -> None:
    """3b. THE WATER'S OWN ROW REPLACES WHAT IT INHERITS OF THE SAME KIND (`_own_replacement`)."""
    if OWN_ROW_REPLACES_INHERITED:
        for k in sorted(s.live - s.lifted):
            o = _own_replacement(s, k)
            if o is not None:
                s.replaced.add(k)
                s.why.setdefault(k, ("own_row", o))


def _step2_about_this_fish(s: _Ask) -> None:
    """2. ABOUT THIS FISH (`speaks_for`): the candidates — in force, not lifted, not replaced
    (3b), not a rainbow rule for the steelhead's sizes (0), not a lift-only rule — and, of them,
    the competitors keyed on `(type, dimension)` for step 4."""
    every, fish = s.every, s.fish
    s.cand = {k for k in s.live - s.lifted - s.no_rainbow - s.replaced
              if speaks_for(every[k], fish) and every[k].get("dimension") != "lift"}
    keyed = s.keyed
    for k in s.cand:
        if s.competes(k):
            keyed.setdefault((every[k]["type"], every[k]["dimension"]), []).append(k)


def _step4_competition(s: _Ask) -> None:
    """4. COMPETITION, on `(type, dimension)` (see `effective_rules`; `_Ask.beats`)."""
    # A DISPLACED RULE DISPLACES NOTHING (RULINGS Q24). At Cheslatta Lake on Nov 15 Region
    # 6's lake trout release (Sept 15-Nov 30) names the fish and so beats the region's "Trout/char:
    # 5" and "3 Dolly Varden/bull trout and/or lake trout"; the lake's own dated "quotas = 3"
    # overrides that release (`water_dates_override`). Read as "beaten by ANY competitor", the
    # release — itself gone — still took the 5 and the 3 with it, and they vanished on exactly the
    # days the release was overridden. A rule is out only when a rule that SURVIVES beats it: the
    # rules nothing beats stand; whatever a standing rule beats is out; a rule every one of whose
    # beaters is out stands; repeat until nothing moves.
    # A CYCLE (answers 2.6, Clearwater Creek gnis:7628, Arctic grayling): the creek's own "Catch and
    # release" (a group) loses to Zone 7B's "Arctic grayling: 2" by naming; that quota loses to the
    # inherited "Arctic grayling catch and release" (Williston Lake's tributaries) by place; and the
    # inherited line loses to the creek's own row (Q5, "before naming"). Read as "beaten by any rule
    # still undecided" every one fell and the grayling had no answer at all. The own row's precedence
    # is the stronger ruling (T4: it decides BEFORE naming), so a cycle is broken there first: a rule
    # an undecided OWN ROW beats over what it inherits falls, and the reading resumes. Only a cycle
    # with no such edge falls back to the old reading ("beaten by any rule still undecided").
    s.out_ = set(s.cand)
    family, peers, beats, closure = s.family, s.peers, s.beats, s.closure
    for group in s.keyed.values():
        beaters = {k: [o for o in group if o != k and family(o) != family(k)
                       and not peers(o, k) and beats(o, k)]
                   for k in group if not closure(k)}
        stands, falls = {k for k in group if not beaters.get(k)}, set()
        while True:
            moved = True
            while moved:
                moved = False
                for k in group:
                    if k in stands or k in falls:
                        continue
                    if any(o in stands for o in beaters[k]):
                        falls.add(k)
                        moved = True
                    elif all(o in falls for o in beaters[k]):
                        stands.add(k)
                        moved = True
            undecided = [k for k in group if k not in stands and k not in falls]
            broken = [k for k in undecided
                      if any(o in undecided and s.precedes(o, k) for o in beaters[k])]
            if not broken:
                break
            falls |= set(broken)
        falls |= {k for k in group if k not in stands}        # a cycle with no own-row edge
        for k in sorted(falls):
            won = sorted(o for o in beaters[k] if o in stands) or sorted(beaters[k])
            s.lose(k, "water_dates" if s.water_dates_override(won[0], k)
                   else "own_row" if s.own_over_inherited(won[0], k)
                   else "nearer_row" if s.nearer_over_farther(won[0], k) else "ladder", won[0])
        s.overridden |= {k for k in falls
                         if any(o in stands and s.water_dates_override(o, k) for o in beaters[k])}


def _step4a_water_dates_on_a_water_kind(s: _Ask) -> None:
    # 4a. A WATER ROW'S OWN DATES OVERRIDE A ZONE RELEASE LIMITED TO A KIND OF WATER (RULINGS
    #     Q7). "Bull trout (Dolly Varden) from streams, Aug 1-Oct 31" carries `water:
    #     stream`, part of its dimension (`daily@water=stream`), so it never met a stream row's own
    #     dated "Bull trout daily quota = 1, Sept 1-Oct 31" in step 4, and `water_dates_override`
    #     never ran. It runs here, on the base dimension: a surviving water rule printing its own
    #     dates for the fish overrides such a release on the days both hold, exactly as it does a
    #     dated zone release with no water kind. The overridden release then displaces nothing
    #     (step 4b) and releases nothing (step 5).
    every, out_, top = s.every, s.out_, s.top
    for k in sorted(out_):
        if not (s.competes(k) and s.zone_side(k) and released_on_water(every[k])):
            continue
        won = [o for o in sorted(s.cand)
               if o in out_ and s.competes(o) and o != k
               and _base_dimension(every[o]) == _base_dimension(every[k])
               and covers(every[o], every[k], top) and s.water_dates_override(o, k)]
        if won:
            s.lose(k, "water_dates", won[0])
            s.overridden.add(k)


def _step4b_zone_release(s: _Ask) -> None:
    # 4b. A ZONE RELEASE LIMITED TO A KIND OF WATER, ON THAT WATER (RULINGS Z9). Region 3's
    #     "Lake trout from Oct 15-Jan 31" (no `water`) meets "Trout/char: 5" and its clauses in
    #     step 4 and displaces them; "Bull trout (Dolly Varden) from streams, Aug 1-Oct 31" carries
    #     `water: stream`, which is part of its dimension, so it met none of them and "Dolly Varden
    #     — 1 per day" spoke beside "release all" on the same stream on the same day. A release
    #     `released_on_water`, surviving step 4, displaces its OWN REGION'S TABLE's rules that keep
    #     the fish (`rules.keeps`, every origin kept released here) in the same
    #     dimension read without conditions ("daily": the quota, its "1 over 50 cm", "4 from
    #     streams"), never a size clause of another dimension ("none under 60 cm" stays beside),
    #     never a closure (it keeps nothing), never a water row, never another region's rule.
    #     Bound here is being on its kind of water (its extents draw only that kind).
    #     GENERALISED (RU-4, RULINGS Q12): a zone release printed with NO water kind met only the
    #     rules of its own key in step 4, so Region 5's "you must release: ALL STEELHEAD" silenced
    #     "Trout/char: 5" and left "2 per day … from streams" (`daily@water=stream`) speaking for a
    #     steelhead on every Region 5 stream. Its own table's keeping rules give way in the same
    #     base dimension WHATEVER their water condition: the release holds on every water, so the
    #     "from streams" clause is a clause of the quota it empties for that fish.
    #     ITS REAL REACH (review F4): "keepers in the base dimension" includes the SAME key, so it
    #     also breaks a same-key tie INSIDE one table — the 7B grayling release May 1-Jun 15
    #     silences that table's "2 per day" and "1 over 45 cm" on its dates (book-correct: the
    #     fish is released then). Only an UNCONDITIONED release reaches this far:
    #     `release_origins` is None for a `while` / `when_targeting` release (pinned by
    #     test_a_conditioned_zone_release_leaves_its_tables_quotas).
    from pipeline.deliver.bundle.rules import closure_grade, release_origins, keeps
    every, out_ = s.every, s.out_
    competes, place, base, family = s.competes, s.place, s.base, s.family
    for k in sorted(out_):
        if not (competes(k) and place(k) >= 2 and base(k) is not None):
            continue
        kind = released_on_water(every[k])
        if kind is None:
            if not (release_origins(every[k]) and not every[k].get("water")
                    and closure_grade(every[k]) is None):
                continue
            kind = "*"                                     # a release on every kind of water
        freed = release_origins(every[k])
        for o in sorted(out_):
            kept = keeps(every[o])
            if o != k and competes(o) and place(o) >= 2 and base(o) == base(k) \
                    and family(o) != family(k) and kept and kept <= freed \
                    and (every[o].get("water") in (None, kind) if kind != "*" else
                         # the keeper's only condition beyond the base is its water kind
                         str(every[o].get("dimension")) in (
                             _base_dimension(every[k]),
                             f"{_base_dimension(every[k])}@water={every[o].get('water')}")) \
                    and _base_dimension(every[o]) == _base_dimension(every[k]):
                s.lose(o, "zone_release", k)


def _on_its_water(x: dict) -> bool:
    """A rule printed for a kind of water is bound here only on that kind (every extent
    draws it: `feature_types: [water]`), or it carries no water kind at all."""
    water = x.get("water")
    if not water:
        return True
    exts = x.get("extents") or []
    return bool(exts) and all(list(e.get("feature_types") or []) == [water] for e in exts)


def _own_water_closure(s: _Ask, k) -> bool:
    return WATER_CLOSURE_DOMINANT and EID.is_water_row(str(k[0])) and s.place(k) == 0 \
        and not s.row_area(k)


def _step4c_closures(s: _Ask) -> None:
    # 4c. A CLOSURE SPEAKS FOR EVERY FISH IT COVERS AS IF IT NAMED IT, WHATEVER ITS KEY (RU-7,
    #     RULINGS Q12). Step 4 lets a closure displace the keeping rules of its own key; a blanket
    #     stream closure carries `water: stream` (`daily@water=stream`) and so never met the
    #     region's "5 per day" or "1 over 50 cm" (`daily`): Michel Creek on May 1 showed Region 4's
    #     "No fishing in streams, Apr 1-Jun 14" beside its "5 per day". A FULL closure in force
    #     here (`rules.closure_grade`), bound on its kind of water, displaces every rule that keeps
    #     the fish in the same base dimension that it BEATS BY THE LADDER (`order`: exactly what
    #     step 4 asks of two rules of one key, so a water row naming the fish still speaks beside
    #     a zone closure here as it does there), over the lengths it covers — never two regions'
    #     peers (step 6 reads those), never its own family. Page noise only: the status index
    #     already read the closure.
    #     A WATER'S OWN FULL CLOSURE IS THE MOST DOMINANT RULE (RULINGS Q10).
    #     Denetiah Creek prints "No fishing, Jul 1-15"; the Liard River watershed row (an AREA row
    #     of the water tables, so `water_side`, which step 5 never lets a water release silence)
    #     prints "Dolly Varden/bull trout — 1 in possession (30-50 cm only)". Step 4 took the
    #     row's daily 1 (same key, better rung) but the possession 1 is another key, and it spoke
    #     beside the closure. A closure WRITTEN FOR THIS WATER (a row's rule — `r…` entry — bound
    #     here at rank 0: not reached by the walk, not an area row) in force here silences, for
    #     every fish it covers, every rule that would let the fish be kept, WHATEVER ITS SOURCE
    #     AND KEY: zone, area rows, other rows (by the walk), possession, annual, size-only — the
    #     book closes the water, nothing is kept there those days. Two exceptions: a SUPERIOR
    #     authority's rule (the ladder's top: nothing here outranks it), and the closure's OWN
    #     LIFTERS (a rule that exempts it in part — an origin, a target — speaks beside it; one
    #     that lifts it outright has already removed it in step 3). A zone closure keeps RU-7's
    #     reach (by the ladder, its own base dimension).
    from pipeline.deliver.bundle.rules import closure_grade, keeps
    every, out_, top = s.every, s.out_, s.top
    competes, family, order, peers = s.competes, s.family, s.order, s.peers
    for k in sorted(out_):
        x = every[k]
        if not (competes(k) and closure_grade(x) == "full" and _on_its_water(x)):
            continue
        own = _own_water_closure(s, k)
        lifters = {o for o in s.here
                   if any((t["entry_id"], t["rule_id"]) == k for t in every[o].get("exempts") or [])}
        for o in sorted(out_):
            if o == k or not competes(o) or family(o) == family(k) \
                    or not keeps(every[o]) or not covers(x, every[o], top):
                continue
            if own and order(o)[0] == 1 and o not in lifters:
                s.lose(o, "water_closure", k)
            elif not peers(o, k) and _base_dimension(every[o]) == _base_dimension(x) \
                    and order(k) < order(o):
                s.lose(o, "closure", k)


def _released_lengths(x: dict, top: float) -> list | None:
    # the class the rule releases outright: its take-0 bands, for every angler (no `while`,
    # no target, not a clause) — the origin it holds for is checked against the keeper's
    if x.get("while") or x.get("caught") or x.get("when_targeting") or x.get("within"):
        return None
    bands = [b for b in (x.get("lengths") or []) if b.get("take") == 0]
    return _spoken_lengths({"lengths": bands}, top) if bands else None


def _step4d_size_release(s: _Ask) -> None:
    # 4d. A WATER'S SIZE-LIMITED RELEASE MEETS THE ZONE'S SIZE CLAUSE FOR THE SAME SIZES (RU-5,
    #     RULINGS Q12). Lakelse Lake's "Rainbow trout (none over 50 cm)" is sizes with no count
    #     (`daily/size`); Region 6's "no more than 1 over 50 cm" is a count (`daily`), so the two
    #     never met and the page said "1 over 50 cm" on a lake where none over 50 cm may be kept.
    #     The stricter-at-water ruling (RULINGS Q3) and `covers` decide it: a water-side rule
    #     RELEASING a size class (a band with take 0) displaces a zone-side keeping rule of the
    #     same base dimension whose spoken lengths lie wholly inside the released class. The
    #     zone's "5 per day" (every length) stays: the 5 still counts the rainbow under 50 cm.
    from pipeline.deliver.bundle.rules import ORIGINS, keeps
    every, out_, top = s.every, s.out_, s.top
    competes, family, clock = s.competes, s.family, s.clock
    for o in sorted(out_):
        freed_lengths = _released_lengths(every[o], top) if competes(o) and s.water_side(o) \
            and every[o].get("type") == "retention_limit" else None
        if not freed_lengths:
            continue
        freed_origins = frozenset({every[o]["origin"]}) if every[o].get("origin") else ORIGINS
        for k in sorted(out_):
            kept = keeps(every[k])
            spoken = _spoken_lengths(every[k], top) if competes(k) and s.zone_side(k) \
                and kept and kept <= freed_origins and not s.closure(k) \
                and family(o) != family(k) and clock(k) == clock(o) else None
            if spoken and all(any(lo >= x and hi <= y for x, y in freed_lengths)
                              for lo, hi in spoken):
                s.lose(k, "size_release", o)


def _beaten_by(s: _Ask, k) -> list:
    """The competitors ranked above `k` by the ladder (`order`) — save a rule `k` itself beats as
    the water's own row over what it inherits (`own_over_inherited`, Q5: `beats` never lets the
    inherited rule win that pair, so it is no beater of `k` here either; Clearwater Creek's own
    release is not "beaten" by the Williston tributaries' inherited grayling release)."""
    every = s.every
    g = s.keyed.get((every[k]["type"], every[k]["dimension"]), [])
    return [o for o in g if s.order(o) < s.order(k) and s.family(o) != s.family(k)
            and not s.peers(o, k) and not s.precedes(k, o)]


def _same_row_dated_release(s: _Ask, k) -> bool:
    """A DATED OUTRIGHT RELEASE OF THE SAME ROW DISPLACES THE ROW'S OWN UNDATED QUOTA FOR
    THE FISH ON ITS DATES (RU-3, RULINGS Q12). The Thompson below Kamloops Lake prints "Trout
    and char — 2 per day" and, for its CNR stretch, "trout/char catch and release … May
    1-31"; Adams Lake "Lake trout — release all, Oct 15-Jan 31" beside "bull trout and lake
    trout — 1 per day". Two rules of one row rank alike (same place, same naming), so step 4
    tied them and the page said "2 per day" and "release all" at once in the one month the
    row singles out. The dated release is the row's own exception to its general number: on
    its dates the number is silent, as a water's release silences the zone's. Only this
    shape — the release DATED, the quota UNDATED, both the row's own (`water_side`), another
    family — never the reverse (an undated release beside a dated keeping window is a window
    the row prints to OPEN the fish, and both stand as printed).

    ITS REAL REACH (review F4): `rel` holds every rule `release_origins` reads as a release,
    and a CLOSURE (take 0, may not fish) is one — so a row's DATED CLOSURE silences the row's
    own undated quota on its dates too (Quatse r1 over r2, the Region 7 lakes' winter
    closures over their quotas, Kitimat's Mar 16-May 31 over its hatchery 2). That is the
    book: on those dates the water is closed to the fish. 356 answers over every key."""
    return _same_row_releaser(s, k) is not None


def _same_row_releaser(s: _Ask, k):
    """The dated release or closure of `k`'s own row that silences it (`_same_row_dated_release`),
    or None."""
    from pipeline.deliver.bundle.rules import keeps
    every, rel = s.every, s.rel
    x = every[k]
    if (x.get("when") or {}).get("dates") or not (x.get("take") or x.get("unlimited")):
        return None                       # a size clause is its own subject (Koocanusa)
    return next((o for o in sorted(rel)
                 if o != k and o[0] == k[0] and s.water_side(o) and s.family(o) != s.family(k)
                 and bool((every[o].get("when") or {}).get("dates"))
                 and _base_dimension(every[o]) == _base_dimension(x)
                 and keeps(x) <= rel[o]), None)


def _step5_water_release(s: _Ask) -> None:
    # 5. A WATER'S RELEASE SILENCES THE ZONE FOR THAT FISH (RULINGS Q3). Competition
    #    keys on (type, dimension), and a zone quota's conditions are part of its dimension, so
    #    Coquihalla's "Trout/char (including steelhead) catch and release" never met Region 2's
    #    "2 hatchery steelhead" (`daily@origin=hatchery`) and both spoke. An outright release in
    #    force here (`release_origins`) that is written for this water (or reaches it by the
    #    tributary walk) displaces every zone or provincial quota that would let the angler keep
    #    that fish (`keeps`), whatever conditions the quota holds under — provided
    #    every origin the quota could keep is released here (by the water row or by the zone's
    #    own releases: Chilliwack's "hatchery cutthroat catch and release" beside Region 2's
    #    "wild trout/char from streams" releases every cutthroat, so "Trout/char: 4" is silent;
    #    Morris Lake's "Wild trout/char catch and release" leaves the region's 4 for hatchery
    #    trout). A rule about another fish never gets here (`speaks_for`), and a closure is never
    #    displaced (a closure keeps nothing).
    #    The releases are read from EVERY competitor in force, not only step 4's survivors: a
    #    water release that lost step 4 to a release NAMING the fish (Pine River's "Catch and
    #    release all fish" under Zone B's "Bull trout … release") still releases it, and one that
    #    lost to a looser zone rule naming the fish (Adams River's "Rainbow trout and char catch
    #    and release" under Region 3's "Lake trout: none under 60 cm") is the stricter water rule
    #    and speaks again — naming only lets a STRICTER zone rule beat a water row's group. A
    #    release displaced by a superior authority that lets the fish be kept stays displaced and
    #    releases nothing here (one displaced by a superior CLOSURE — a national park — still
    #    releases: nothing the region keeps survives either).
    from pipeline.deliver.bundle.rules import release_origins, keeps
    every, out_, why, rel = s.every, s.out_, s.why, s.rel
    competes, water_side, zone_side = s.competes, s.water_side, s.zone_side
    for k in s.cand:
        o = release_origins(every[k]) if competes(k) else None
        # A release a water row's own dates overrode releases nothing here (RULINGS Q26): it is not in
        # force at this water on this day, whatever step 4's naming said about it. Nor does an
        # INHERITED release the water's own row beat (Q5): the own row answers for the fish here.
        # Nor a release a NEARER ✱ row beat (T14): the nearer row answers for the fish here.
        if not o or k in s.overridden or (k not in out_
                                          and why.get(k, ("",))[0] in ("own_row", "nearer_row")):
            continue
        by = _beaten_by(s, k)
        if any(every[b]["_rank"] < 0 and keeps(every[b]) for b in by):
            continue
        rel[k] = o
        if k not in out_ and water_side(k) and by \
                and all(zone_side(b) and keeps(every[b]) for b in by):
            out_.add(k)
            why.pop(k, None)
    water_rel = frozenset().union(*[o for k, o in rel.items() if water_side(k)])
    released = frozenset().union(*rel.values())

    if water_rel:
        for k in sorted(out_):
            kept = keeps(every[k])
            if not (kept and competes(k) and kept <= released and kept & water_rel):
                continue
            if zone_side(k):
                s.lose(k, "water_release",
                       next(o for o in sorted(rel) if water_side(o) and rel[o] & kept))
            elif water_side(k) and _same_row_dated_release(s, k):
                s.lose(k, "same_row_release", _same_row_releaser(s, k))


def _step5b_moot_size_clause(s: _Ask) -> None:
    # 5b. A ZONE SIZE CLAUSE MADE MOOT BY AN OUTRIGHT RELEASE OF THE SAME FISH IS NOT SHOWN (RULINGS
    #     Q9, option A — what the consumer's front end already does). A
    #     clause stating only sizes ("none under 60 cm", `daily/size`) is its own dimension, so
    #     steps 4b/4c (base dimension "daily") never reached it and it spoke beside Region 3's
    #     "Lake trout from Oct 15-Jan 31" on Bonaparte Lake on Nov 1, beside the stream release and
    #     the spring stream closure on Eleven Mile Creek, and beside a national park's closure —
    #     while a water's own release (step 5) already silenced it. One rule now, WHOEVER wrote the
    #     release: a zone-side size-only keeper (`zone_side`: zone, area, province — not a row) gives
    #     way to a SURVIVING, competing outright release or closure (`release_origins`; in force on
    #     this day, whatever its rank — water, zone, superior) of another family that releases every
    #     origin the clause keeps (a wild-only release leaves a hatchery-only clause standing), over
    #     every length the clause speaks of (`covers`), on the same clock. The release or closure is
    #     untouched: a closure is still a closure (no gear in the water), a release still a release.
    #     The clause has nothing left to say today: it is not in the answer, and a traced answer
    #     returns it as "moot" ("doesn't matter today"), `by` the release.
    from pipeline.deliver.bundle.rules import release_origins, keeps
    every, out_, top = s.every, s.out_, s.top
    competes, family, clock = s.competes, s.family, s.clock
    moot_by = [o for o in out_ if competes(o) and release_origins(every[o])] \
        if MOOT_SIZE_CLAUSE_HIDDEN else []
    for k in sorted(out_):
        x = every[k]
        if not (competes(k) and s.zone_side(k) and x.get("type") == "retention_limit"
                and x.get("take") is None and not x.get("unlimited") and x.get("lengths")):
            continue
        kept = keeps(x)
        won = [o for o in sorted(moot_by)
               if o != k and family(o) != family(k) and kept <= release_origins(every[o])
               and covers(every[o], x, top) and clock(o) == clock(k)] if kept else []
        if won:
            s.lose(k, "moot_size_clause", won[0])


def _step6_two_regions(s: _Ask) -> None:
    # 6. TWO REGIONS' BASES: THE MOST STRICT APPLIES (RULINGS Z4). A section drawn across
    #    a region line — a lake, and since T183 a stream piece too — binds both regions' zone rules
    #    (`registry.regions.region_members`) — Ahbau Lake (51 % Region 5, 49 % Zone 7A), Mara Lake
    #    (61 % Region 3, 39 % Region 8), the Fraser's straddling pieces. Neither table
    #    outranks the other by place, so step 4 set them against each other not at all (`peers`);
    #    here, per fish, a rule of one region is displaced by a STRICTER rule of the other:
    #      a closure beats a retention rule that is not one (closed beats open);
    #      an outright release beats a quota keeping only origins it releases;
    #      the lower of two quotas stating the same thing (`rules.same_statement`) beats the
    #      higher — "Kokanee: 5" over "Kokanee: 10".
    #    Quotas stating different things sit beside each other (the stricter binds by itself), and
    #    gear and method rules are never displaced here: both regions' restrictions apply.
    from pipeline.deliver.bundle.rules import same_statement, keeps
    every, out_, base, peers = s.every, s.out_, s.base, s.peers
    mine = [k for k in out_ if s.competes(k) and base(k) is not None]
    if len({base(k) for k in mine}) > 1:
        gone = {k for k in mine
                if any(peers(o, k) and stricter(every[o], every[k]) for o in mine)}
        for k in sorted(gone):
            s.lose(k, "stricter_region", next(o for o in sorted(mine)
                                              if peers(o, k) and stricter(every[o], every[k])))
        # TWO REGIONS' IDENTICAL STATEMENTS ARE ONE (RU-8, RULINGS Q12): Ahbau Lake (Regions 5
        # and 7A) printed "Trout and char — 5 per day" and "1 over 50 cm" twice, once per table.
        # Of two peers that are the same statement with the same number, one is shown — the
        # lower entry id (z5 before z7a: deterministic, no section lookup; display only, the
        # number is the same either way).
        left = sorted(k for k in mine if k not in gone)
        for i, k in enumerate(left):
            for o in left[:i]:
                if o in out_ and k in out_ and peers(o, k) \
                        and every[o].get("type") == "retention_limit" \
                        and every[k].get("type") == "retention_limit" \
                        and keeps(every[o]) and keeps(every[k]) \
                        and same_statement(every[o], every[k]) \
                        and every[o].get("take") == every[k].get("take") \
                        and bool(every[o].get("unlimited")) == bool(every[k].get("unlimited")):
                    s.lose(k, "same_as_peer", o)


def _answer(s: _Ask, trace: bool) -> List[dict]:
    """The answer: every rule left in, with its state; with `trace`, every loser too."""
    every, orig, partly, partly_by = s.every, s.orig, s.partly, s.partly_by

    def name(k) -> str:
        return f"{k[0]}::{k[1]}"

    def body(k) -> dict:
        return {a: b for a, b in orig[k].items() if a != "_rank"}

    answer = [(name(k), dict(body(k), state=s.said(k),
                             **({"partly_lifted": True} if k in partly else {}),
                             **({"lifted_in_part_by": sorted(name(o) for o in partly_by[k])}
                                if trace and k in partly_by else {})))
              for k in s.out_]
    if trace:
        # THE LOSERS (gap G1): every rule that took part — in force, about this fish, not a
        # lift-only rule, not a rainbow rule for the steelhead's sizes (step 0: about another
        # fish) — and is not in the answer, with the step that removed it and the rule that won.
        took_part = {k for k in s.live - s.no_rainbow
                     if speaks_for(every[k], s.fish) and every[k].get("dimension") != "lift"}
        for k in took_part - s.out_:
            reason, by = s.why[k]
            answer.append((name(k), dict(body(k), state=LOSS_REASONS[reason], reason=reason,
                                         by=name(by))))
    return [x for _, x in sorted(answer, key=lambda p: p[0])]

# Gear — what is wrong with it, and the shape that fixes it

**Status: the types are implemented and validated; the DATA is not migrated.** `Slot`,
`GearClause`, `GearWhen` and `Conduct` are in `catalogue.py` with their validation, and
`CatalogueRule` carries `gear` and `conduct`. Nothing writes them yet — the old fields are still
the ones in use. It is written down because six
independent reviews of the whole gear corpus converged on the same answer, and because three of
the defects below are wrong answers in the shipped data today.

898 rules are about HOW you may fish rather than what you may keep: `tackle_restriction`,
`bait_restriction`, `method_rule`, `handling_rule`. They use a scattered set of fields —
`allowed`, `permitted`, `required`, `barbless`, `hook_count`, `lure`, `bait`, `method`,
`max_lines`, `max_flies`, `max_weight_kg`, `min_gap_cm`, `max_gap_mm`, `when_targeting`, `reason`.

---

## 1. The defects, verified against the corpus

### A flag that inverts the field beside it — the `band` failure, again

`required` appears **25 times and is `false` all 25**. A field with one value is not data; it is a
polarity bit. And it carries two opposite readings:

    r5:chilko_river.r8   "Steelhead Stamp not required"
                         {document: "steelhead_stamp", required: false}   -> the DOCUMENT is waived

    zp:barbless_single_hook_streams.r1   "use barbed hooks"
                         {barbless: true, required: false}                -> ???

The first is the corpus's own convention: `required: false` waives the thing beside it. Read the
second the same way and it says **barbless is not required — barbed hooks are legal in every
river, stream, creek and slough in British Columbia.** The law is the opposite. The verbatim is a
fragment off a printed "You must not:" list whose stem is not stored, and `required: false` is the
only trace of it.

It is noise rather than convention, and the proof is inside the entry: `r3` — the OTHER HALF OF
THE SAME PRINTED SENTENCE, "a hook with more than one point in any river, stream, creek or slough
in B.C." — stores `{hook_count: 1}` with no `required`. Every one of the ~37 regional twins
("Single barbless hook: must be used in all streams of Region N, all year") states the same law
with no flag either. One sentence, two rules, two conventions, and the minority spelling inverts.

Second instance, on a number: `zp:terminal_tackle.r2`, "Angle with more than one line, EXCEPT a
person who is alone in a boat on a lake may angle with two lines" stores
`{max_lines: 2, required: false, reason: "alone in a boat"}`. Its sibling `r1` stores the general
cap as a bare `{max_lines: 1}`. So the 2 is the EXCEPTION's allowance carrying the PROHIBITION's
polarity bit, and read literally the rule forbids the one thing the sentence exists to permit.

### One field, three measurements — the `over_cm` failure

`hook_count: 1` (52 occurrences, never any other value) counts three different things:

    terminal_tackle.r6      "only one hook, one artificial lure OR one artificial fly
                             is attached"                    -> attachments per line
    barbless_single_hook_streams.r3
                            "a hook with more than one point" -> POINTS PER HOOK
    set_lining.r2           "one line with one hook"          -> hooks per line

**A treble hook is one hook with three points.** `hook_count: 1` permits it under the first
reading and bans it under the second, and no neighbour decides which — both are
`tackle_restriction`, both province-wide.

`lure` has the same disease across a smaller range: `lure: "artificial_fly"` ("artificial fly
only") constrains the terminal tackle; `lure: "fly_fishing"` ("Fly fishing only") constrains the
rod, reel and line as well. On the Campbell River the two sit on adjoining reaches.

`reason` carries six or seven jobs decided by whichever polarity field happens to be present: a
carve-out ("does not apply to downrigger weights"), a gating condition ("alone in a boat"), the
prohibited act's own name where `method: "other"` is a null ("chumming"), an obligation ("marked
with the angler's name, address and telephone number"), an unmodelled second conjunct, and — once
— an actual reason ("for the conservation of chinook and coho salmon stocks").

### Sentences no field can hold

    zp:allowable_methods.r1   "angle with a downrigger, provided the fishing line is attached
                               to the downrigger by a quick-release mechanism"
                              -> {} — zero payload fields
    zp:terminal_tackle.r5     "Use a light in any manner to attract fish, unless the light is
                               submerged and attached to the fishing line within 1 m of the hook"
                              -> {required: false} and nothing else
    zp:bait.r6                "you must not have more than 1 kg of roe ... unless ..."
                              -> {bait: "roe", allowed: true}
    zp:prohibited_methods.r5  "All other methods of taking fin fish and crayfish are illegal."
                              -> typed `advisory`, no fields

The roe rule is the sharpest: a sentence whose operative words are "you must not" is stored as a
permission, the 1 kg cap is gone, and `max_weight_kg` EXISTS in the schema and is used two rules
away. Nothing was split onto a sibling; it was dropped. The last is the sentence that makes the
whole method system default-deny, stored as a note.

### An omission the shape cannot see

Eight waters whose verbatim says "single barbless hook" store only `{barbless: true}`, with no
`hook_count` and no sibling carrying it: **naglico, natadesleen, ogston, pettry, sardis_park_pond,
sayres, silver_lake, pend_doreille**. A barbless treble is legal there per the data. Compare
`osoyoos_lake.r5`, whose verbatim genuinely is "Barbless hook July 1-Oct 31" with no "single" and
which correctly omits the field — so this is a defect, not a convention.

---

## 2. The shape

Settled after three independent designs. `gear` is a MAP from measurand to bound, plus `while`,
`conduct` and the fields the rule already has.

```json
"gear":    {"<slot>": {"max": n} | {"min": n} | {"allow": [...]} | {"ban": [...]} | {"must_be": [...]}},
"while":   ["set_lining"],
"conduct": ["do_not_waste_catch"]
```

**A MAP, NOT A LIST.** Two bounds on one measurand are then unwriteable by JSON itself, and no
ordering is needed — checked across all 141 provincial and regional rules, not one needs two bounds
on the same measurand. An ordered first-match list was the wrong borrowing: `lengths` is ordered
because its ranges partition ONE axis and genuinely overlap. Gear clauses sit on different axes, so
order is noise — and ordering would install a second conflict resolver beside the scope ladder that
already settles rule-against-rule.

**THE SLOT NAME CARRIES MEASURAND, DIRECTION AND UNIT.**

    COUNTED / MEASURED
    lines_per_angler       attachments_per_line   hooks_per_line   flies_per_line
    points_per_hook        hook_gap_min_mm        hook_gap_max_mm
    weight_per_line_kg     roe_in_possession_kg   light_to_hook_mm

    CHOSEN FROM A SET
    bait   lure   method   barb   (barbed | barbless)

    HOW THE THING MUST BE (`must_be`)
    set_lining   crayfish_trapping   downrigger   light   ice_hut

`hook_count` confused TWO hook measurements — how many hooks on the line, and how many POINTS on
one hook. A treble is one hook with three points, so the same `1` banned it and permitted it. Those
are the two that were genuinely conflated.

`attachments_per_line` is NOT a third meaning of the same thing, and calling it one blurred the
diagnosis. "only one hook, one artificial lure OR one artificial fly is attached" is a property of
the LINE — one item on it, drawn from three kinds — and it was filed under `hook_count` because
there was nowhere else to put it. A missing slot, not a confused one.
`min_gap_cm: 3` and `max_gap_mm: 15` were one measurement in two units; there is one slot and one
unit, so 3 cm is entered as `30`.

**`barb` IS AN ORDINARY SET SLOT** — `{allow: ["barbless"]}`. An earlier draft made it
`barbs_per_hook: {max: 0}`, on the reasoning that `{barbless: true, required: false}` was inverted
and therefore the boolean had to go. That diagnosis was wrong: what inverted it was `required`, a
SECOND FIELD that reinterpreted the first. `barb` was never the problem, and with `required` gone
there is nothing left to flip it. "A maximum of zero barbs" is cleverness where the book says
barbed or barbless.

**SETS TAKE ONE OF THREE BOUNDS, AND `allow` MEANS EXACTLY ONE THING.**

    {allow: [...]}   these are permitted; NOTHING ELSE IS SAID   "worms may be used in streams"
    {only:  [...]}   these MEMBERS and no other MEMBER            "fly fishing only"
    {ban:   [...]}   these are prohibited                        "bait ban"
    except: [...]    members a `ban` does not reach              "…other than roe"

**`only` CLOSES THE SLOT, NOT THE PLACE, AND THE PRINTED WORD DOES BOTH.** "You may ONLY fish with
a set line in lakes of Region 6 and Region 7A" is not `method: {only: ["set_lining"]}` — that says
set lining is the one lawful method on those lakes, outlawing fly fishing and trolling on every one
of them. There the word scopes the PLACE, and a place-scoped "only" is two rules: `allow` where the
book permits it, and a ban elsewhere carrying `derived_from`, which is how `zp:set_lining.r1b` is
already written. Reach for `only` when the sentence narrows WHAT, never WHERE.

An earlier draft had `allow` and `ban` alone, and `allow` was silently doing two jobs — a
permission that forbids nothing, and a whitelist that forbids everything else. That is the
`over_cm` disease: one key, two meanings, decided by the reader's guess at context. `only` is the
word "only" promoted out of the prose, and it is the bound that lets "Fly fishing only" close the
lure slot without a second field asserting the closure.

**`ban` NAMES ITS MEMBERS.** Direction is the key,
so a neighbour cannot flip it. An earlier draft made `allow: []` the total ban — compact, and
dangerous: an empty list is also what a dropped key, a failed parse, a serializer omitting empties
and a half-filled field all produce, so every one of those accidents would have become a
province-wide ban. "Bait ban" is `{"bait": {"ban": ["any_bait"]}}`, and `any_bait` has to be typed.
THE MOST DANGEROUS STATEMENT MUST NOT BE THE EASIEST ONE TO PRODUCE BY ACCIDENT. Vocabularies are closed: `bait`, `lure`, `method`. There is
no `other` member, which is what kills `{method: "other", reason: "chumming"}`.

**`must_be: [...]` IS PRESENCE-ONLY.** How the thing must be built or carried — the downrigger's
quick-release, the light's submersion, the crayfish trap's circular openings, the set line's
marking, the ice hut's removal. Presence asserts; absence is silence; negation is not expressible,
so there is no `false` to write.

**MEMBERS HAVE PARENTS, DECLARED IN THE REGISTER.** `ban: ["any_bait"]` has to cover roe,
invertebrates and fin fish; `allow: ["dead_fin_fish"]` has to sit visibly under a `fin_fish` ban so
a reader can see it is a carve-out and not a contradiction. The containment is a small PARENT map
(`roe -> any_bait`, `dead_fin_fish -> fin_fish`, `fly -> any_lure`), declared once. Without it a
consumer has to know by hand that roe is bait.

**AN EXEMPTION WITH A `while` LIFTS ONLY INSIDE THAT CIRCUMSTANCE.** This is the most consequential
rule here and it was found by converting, not by design. "Dead fin fish may be used when set
lining" exempts the province-wide fin fish ban — but only WHILE set lining. Applied everywhere, it
lifted the ban outright and Atlin's bait tile went from "banned" to "no rule at all". The ban
stands, and the exception rides beside it as "dead fish while set lining".

This is the Babine rule on a new axis. `z6`'s steelhead closure carried an exemption whose PLACE
could not be drawn; applied everywhere it deleted the closure from a river the note never
exempted, and the standing rule is that a lift whose place cannot be drawn is not applied. A lift
whose CIRCUMSTANCE is not met is the same failure and gets the same answer.

**`while` SPLITS A FIELD THAT DID TWO JOBS.** `method` is the SUBJECT of an assertion on 135 rules
(`{method: set_lining, permitted: false}`) and the CIRCUMSTANCE on 10 others
(`set_lining.r3` is a retention limit that happens WHILE set lining). Which job it was doing had to
be inferred from the other fields present — the `over_cm` disease in a field nobody had examined.
The assertion is `gear.method`; the circumstance is `while`.

**A "PROVIDED THAT" SENTENCE IS TWO RULES SHARING A VERBATIM.** One says the means is allowed; the
other holds the condition under `while`, so the condition binds only while you are using the thing.
This is the one-sentence-many-rules convention the corpus already runs on, not a new mechanism.

    "angle with a downrigger, provided the line is attached by a quick-release"
      {gear: {method: {allow: ["downrigger"]}}}
      {gear: {downrigger: {must_be: ["quick_release_to_line"]}}, while: ["downrigger"]}

    "Use a light … unless submerged and attached within 1 m of the hook"
      {gear: {method: {allow: ["light"]}}}
      {gear: {light: {must_be: ["submerged"]}, light_to_hook_mm: {max: 1000}}, while: ["light"]}

**A DEVICE IS NOT A MEANS OF FISHING, AND PUTTING IT IN `method` FABRICATES A PERMISSION.** The
goal — one vocabulary for `while` — is met by letting `while` draw from method members AND
spec-slot names, since a spec slot's name already IS a means token (`set_lining`,
`crayfish_trapping`). `while: ["downrigger"]` then has a referent with no lookup and no membership.

What membership cost: the two-rules pattern turns a "provided that" sentence into an allow plus a
condition, and it is POLARITY-BLIND. `zp:allowable_methods` is headed "angle with a downrigger,
PROVIDED…" — an allowable list, the grant is real. `zp:terminal_tackle` is headed "It is UNLAWFUL
to…" — its light clause grants nothing. Through one template both produced
`method: {allow: [...]}`, so a Region 6 lake answered "what may I fish with here" with `light`, the
one piece of tackle the province forbids outright. That is `{method: "ice_fishing",
permitted: true}` on a hut-removal warning, rebuilt on a new field.

THE TOKEN IS THE SAME ONLY WHERE THE OBJECT IS THE MEANS. An ice hut is not how you ice fish, it is
something you leave on the lake, so `ice_hut` is its own spec slot carrying `while: ["ice_fishing"]`
— the object and the circumstance are genuinely two things there and the rule does not reach it.

**`conduct` NAMES THE ACT IN ITS LAWFUL DIRECTION** — `do_not_waste_catch`, not `waste_catch` plus a
flag. The direction is declared once per act, so there is no polarity key to set backwards.

## 3. Tested against the rules that matter

The gear and method rules that carry the hard cases are **not** on individual waters — they are in
the provincial and regional entries, where one sentence binds a whole region and where this repo
is responsible for the curation directly. There are 62 of them, 39 distinct. Every one was written
out in the shape and run through the model's own validation:

**34 fit.** Including the four defects section 1 names:

    zp:barbless_single_hook_streams.r1   "use barbed hooks"
      [{slot: barb, allow: [], when: {water: stream}}]
      — byte-identical to the nine regional twins. `required: false` has nowhere to go.

    zp:bait.r1    "fin fish … is prohibited"   [{slot: bait, of: ["fin_fish"], allow: []}]
    z1:bait_ban_streams.r1   "Bait ban"        [{slot: bait, allow: []}]
      — a PARTIAL ban and a TOTAL one, which without `of` are the same record.

    zp:terminal_tackle.r1 + r2   one sentence, the exception ordered first
      [{slot: lines_per_angler, max: 2, when: {water: lake, angler: alone_in_boat}},
       {slot: lines_per_angler, max: 1}]

    zp:set_lining.r4   "Set lines must be marked with angler's name, address, and telephone number"
      conduct: [{must: "mark_gear", with: [angler_name, address, telephone],
                 when: {method: set_lining}}]
      — a duty, in the list for duties, with no permission fabricated to house it.

The five that do not fit are below, and they are the same five the reviewers named independently.
The worked conversions are kept as a runnable fixture beside this file.

## 4. What this does NOT fix

Every reviewer named the same three, independently. They are not gear problems and a gear shape
does not reach them.

**Deferral between rules** — `zp:bait.r4`, "…in streams as bait UNLESS A BAIT BAN APPLIES".
Also "when open" (`gordon_river.r3`) and "(see tables for exclusions)" (`z7a:set_lining.r1`). `exempts` expresses a permission
that WINS; there is no way to write one that YIELDS, and no rule identifiers to point at for a
whole class like "a bait ban". This is the same gap as the self-lifting Babine closure.

**Exclusivity — "only here"** — `zp:set_lining.r1`. "You may only fish with a set line in lakes of
Region 6 and Region 7A" binds a permission to two places; the exclusion of everywhere else is not writable, because
`extents` has no negation. The cost is already visible: ten-odd Zone A lakes each carry their own
"no set lines" rule because the complement could not be said once.

**`when` becomes the next `reason` unless it is closed.** Every proviso wants a new key —
`alone_in_boat`, `submerged`, `within_m_of_hook`, `quick_release`. If the vocabulary is open, the
dumping ground has been renamed. It must be a closed enum with anything outside it raising a
review flag, and that decision is the difference between this shape working and merely moving the
problem.

**It prevents inversion, not omission.** The eight lost "single"s become a missing
`points_per_hook` clause — still silently absent, because the shape of a thing that is not there is
nothing. The check must be seeded from the VERBATIM, never from the record: a check that reads the
record and asks whether its clauses are consistent is a tautology, which this project has already
been burned by. Declare printed IDIOMS — "single barbless hook", "bait ban", "no smaller than N cm"
— each with its exact expansion, and fail any rule whose verbatim matches an idiom and whose `gear`
map is not that expansion. The verbatim is independent evidence because it is COPIED, NOT AUTHORED,
and is the one field a curator cannot get wrong.

The same test catches the residual direction error: "unlawful / prohibited / must not / no person
shall" must produce `ban`; "may / are allowed / is permitted" must produce `allow`. The old
inversion was undecidable from the record; this one is a lexical check away.

**AND IT CREATES A SURFACE THAT DID NOT EXIST.** Meaning moves into the register — slot kinds, units,
conduct directions, and the fact that `weight_per_line_kg` excludes downrigger weight. That is what
makes each record small enough to be unambiguous, and the cost is that ONE WRONG LINE IN THE
REGISTER SILENTLY CHANGES EVERY RECORD THAT USES IT. `lengths` has no such surface: a range carries
its own number and nothing outside it can reinterpret it. Three things contain it, none a shape —
the register is reviewed as law diff by diff; the idiom checks surface a wrong declaration as a
mismatch across many rules at once; and mutation tests flip one slot kind, one conduct direction,
one lattice edge and assert something observable breaks. A declaration that can be flipped with no
effect is not load-bearing and should not exist.

Two more the 62 surfaced that the water-level rules never would:

**A proviso about how equipment is assembled.** `zp:allowable_methods.r1` — "angle with a
downrigger, PROVIDED the fishing line is attached to the downrigger by a quick-release mechanism",
and `zp:terminal_tackle.r5` — "unless the light is submerged AND attached to the fishing line
WITHIN 1 M of the hook". Neither is a count, a measure of one object, or a member of a set. They
degrade to `when.note`, which is `reason` in better clothes — the note at least costs a
`review_reason`, so the gap is visible rather than absorbed.

**Advice with no number.** `z8:crayfish_traps_turtles.r2` — "encouraged to use traps with
MINIMALLY-SIZED circular openings". `obligation: should` carries the force and nothing carries the
specification, because the book prints none.

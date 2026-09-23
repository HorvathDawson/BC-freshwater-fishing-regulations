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

    lines_per_angler   attachments_per_line   hooks_per_line   flies_per_line
    points_per_hook    barbs_per_hook         hook_gap_min_mm  hook_gap_max_mm
    weight_per_line_kg roe_in_possession_kg   light_to_hook_mm

`hook_count` was three quantities — attachments on the line, hooks on the line, POINTS on one hook
— and a treble is one hook with three points. You can no longer write `1` without choosing which.
`min_gap_cm: 3` and `max_gap_mm: 15` were one measurement in two units; there is one slot and one
unit, so 3 cm is entered as `30`.

**`barbs_per_hook: {max: 0}` IS "barbless".** Barbs go on the same axis as every other quantity, so
no boolean exists to invert and `{barbless: true, required: false}` has no spelling at all. The
opposite — barbs permitted — is correctly the ABSENCE of a clause, not a value.

**SETS ARE `{allow: [...]}` OR `{ban: [...]}`, and `ban` is non-empty.** Direction is the key, so it
cannot be flipped by a neighbour. An earlier draft made `allow: []` the ban; that was wrong, because
an empty list is what a dropped key, a failed parse and a serializer omitting empties all produce —
it turns an omission into an inversion. Vocabularies are closed: `bait`, `lure`, `method`. There is
no `other` member, which is what kills `{method: "other", reason: "chumming"}`.

**`must_be: [...]` IS PRESENCE-ONLY.** How the thing must be built or carried — the downrigger's
quick-release, the light's submersion, the crayfish trap's circular openings, the set line's
marking, the ice hut's removal. Presence asserts; absence is silence; negation is not expressible,
so there is no `false` to write.

**`while` SPLITS A FIELD THAT DID TWO JOBS.** `method` is the SUBJECT of an assertion on 135 rules
(`{method: set_lining, permitted: false}`) and the CIRCUMSTANCE on 10 others
(`set_lining.r3` is a retention limit that happens WHILE set lining). Which job it was doing had to
be inferred from the other fields present — the `over_cm` disease in a field nobody had examined.
The assertion is `gear.method`; the circumstance is `while`.

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

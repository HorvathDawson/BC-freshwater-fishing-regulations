# ui-rules-answers.json — format `answers/2`

The answers file is a third file beside the UI export pair (`ui-rules-export.json` +
`ui-rules-guide.json`). The export says what the book says; the answers file says what it comes to,
for every part of every named water in the export — and, since 2.4, every part of the unnamed water
(every other section in B.C.) — on every day, for every fish and origin, and for every angler profile. The export format is unchanged. The answers file replaces nothing until it is
verified 1-to-1 against the consumer page (COMPARISON.md).

- **Built by** `python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE
  [--verdicts FILE] [--workers N]` (35 s on 4 processes, 3.9 GB peak, measured 2026-10-08; the
  verdicts default to `verdicts.sqlite` beside the bundle). The build refuses to write a file that
  does not decode back to what it built, and validates every value of every section against its
  answers/2 model first (`model.py`). It is deterministic (4 and 2 workers give the same bytes).
- **Reference decoder** `pipeline/deliver/answers/encode.py` `decode(wire, data)`; one tap is
  `encode.tap(...)`.
- **One source per fact (DATAFLOW).** Every rule state, every loser's `reason` and `by`: the
  verdicts stage's stored answer (`verdicts.sqlite`: `read.effective_rules_bound(trace=True,
  origin=…)` run ONCE per rule key, reading, fish and origin) — nothing in the answers calls the
  reader. Every requirement in force, and every record a part's sources consider:
  `read.requirements_in_force` (with the G5 steps). Every status: `reading.closed` / `key_meta.own`
  of the verdicts (the status index is the same projection). The parts: the bundle's `part` table.
  The calendar: `pipeline.deliver.calendar`. The decided answer: `rows` alone. The answers layer arranges what these say;
  the card's rows (`rows.py`) are the consumer page's own arrangement of the ladder's speakers, ported
  and proven exact on the page's own ladder (COMPARISON.md §1).
- **No fallbacks.** Anything that cannot be answered stops the build with an `AnswersError` naming
  it: an export pair from another bundle or with other rules or rule sets; a part whose sections
  disagree on the steelhead rules, the water kind or tidal water; a part with no rule set that is not
  wholly outside B.C.; a reader answer naming a rule outside the set; a loser without a reason or
  `by`; a fish the card asks about that the ladder does not answer; a licence key no section has; a
  bundle whose licensing records are not the export's `licensing_ids`. States that ARE answers are
  written out: `no_rule`, `by_origin`, a `null` part key for a part outside B.C., a `null` decided
  answer (no rule in scope speaks).

## answers/2 (DATAFLOW P7, 2026-10-08) — what changed from answers/1

Nobody had consumed answers/1, so there is no migration note (user decision U3). The changes:

| change | answers/1 | answers/2 |
|---|---|---|
| the decided answer | `answer` section (`decide`) beside `rows` | `rows` only |
| a no-limit answer / row | `nolimit` | `no_limit` |
| a part with no run measure | `km: 1e9` | `km: null` |
| a part with no entry heading | `place: ""` | `place: null` |
| the ladder's fish | the game fish a member rule names, + ST | every fish the verdicts asked |
| the models | prose in `spec` | `schemas`: every frame's JSON Schema (strict, no extra field) |

The models are `pipeline/deliver/answers/model.py`; `python -m pipeline.deliver.answers.model`
writes `app/packages/core/src/answers.generated.{json,ts}` and `--check` fails CI on drift.

## answers/2.1 (2026-10-08) — moments, closing rules, side channels

Same format string (`answers/2`); the sections that changed carry new versions: **ladder 1, rows 3,
gear 3, display 3** (licence 2 unchanged). A reader that does not know section versions ladder 1 /
display 3 must refuse the file.

**Weekday and hours rules DECIDE (user ruling 2026-10-08; review D2, gap G5).** A rule whose `when`
holds on some weekdays ("Kokanee — 5 per day, on Saturdays and Sundays", Kootenay Lake Lower West Arm,
p.37) or some hours ("No fishing, one hour after sunset to one hour before sunrise", the Fraser above
Mission) was read "beside" — shown, deciding nothing — so the card answered Region 4's 15 kokanee a
day. Now the reader is asked at a MOMENT (`calendar.Moment`: a class of weekdays, and inside or
outside the key's one hours window — `calendar.moments` cuts each rule key's week and day by its own
rules) and such a rule decides at the moments it covers and is not in force at the others. The
verdicts store every moment (`verdicts/2`: tables `moment`, `segment.moment`, `reading.moment`); a key
with no such rule has one moment and its answers are unchanged (proved: every one of the 2,071 other
rule keys' frames, readings and runs is identical to answers/2's, and the status index is
byte-identical).

In this file: a key whose day reads differently by moment has slot 10 of its `keys` row set (an index
into `segment_moments`); its segments then REPEAT a start day, once per moment group, and
`segment_moments[...][s]` names each segment's moment in the top-level `moments` table:
`{weekdays: [names, Monday first], hours: null | {start, end, in}}`. Moments with equal answers at a
start are merged (inside and outside the window into the whole day, then alike weekday classes). The
tap (§2) picks the segment holding the date's weekday; of an hours pair, `in: false` is the answer at
every hour but the window, `in: true` the answer inside it. Every section's `at[k][s]` indexes the same
segments. Model: `schemas["top.moments"]`.

* Kootenay Lake — Lower West Arm, kokanee: Mon–Fri `release` (the arm's r4), Sat–Sun `keep 5` (r3).
* A night or hours closure: outside the window the closure is not in force (the card is the day's);
  inside it the closure speaks for every game fish, `display.status` `closed`, `closing` names it.
  A day is never closed by its night (the status index colours a day CLOSED only when every moment of
  it is closed — unchanged bytes).
* An angler closure for some weekdays (Region 6's "non-guided non-resident aliens, on Saturdays and
  Sundays"): `speaks` on those days, absent on the others.
* Nothing else changes. 394 waters (every one binding one of the 22 rules, directly or by the
  tributary walk) — see the round's report.

**`display.closing` (gap G1).** Each display frame (version 3) carries `closing`: `[[rule, [fish]]]`,
every full closure that speaks, not partly lifted, for some game fish at that segment, with the game
fish it closes — `verdicts.project.closing`, the same predicate `reading.closed` is made of (status
`closed` exactly when every game fish is under one; the model refuses otherwise). The page reads it;
it never infers the closing rules from the ladder.

**Side-channel-only parts (review C2).** A part made only of side channels says where they join the
river, from the branch runs' `km_from`: "5 side channels, joining 1.1–5.5 km from the mouth", "Side
channel, joining 5.7 km from the mouth" (was "5 side channels", "Side channel (from the mouth up to the
top of the river)"); 26 parts, labels and `runs` only.

**The export's tidal note (review B17).** `waters[].tidal.guide` holds angler words only (it equals
`common.TIDAL_NOTE`); how to show it is in the field dictionary, never in the data.

## UR2 (2026-10-17) — rows 19: user panel round 2, small wording fixes

Six angler personas read v22 screenshots (USER-ROUNDS round 2); only wording changed, no shape.

1. The headline's unit says whose number it is: "2 a day per angler from this stream" (a lake "4 a day per angler"; the
   header leads with it).
2. The worked lines: "If you keep 3 hatchery rainbow trout" / "1 more fish of any kind, from here or from a lake." ("If
   you keep 2 …: none from other streams today — up to 2 more from here or from lakes."), the fish named by the lifting
   rule's origin (none where it keeps any). The gate now also holds every number in a keep-here line's words to the
   counts it proves (`kept`, `here`, `other`, `away`).
3. A kind held to fewer than its count tier says of how many: "Only 1 of the 2 may be hatchery Dolly Varden/bull trout
   or lake trout" (its `sentence` keeps "Of the 2 a day: only 1 may be …").
4. A steelhead kind keeping hatchery fish only: "Up to 2 may be hatchery steelhead only — wild steelhead go back". Where
   a rainbow over 50 cm is a steelhead (`steelhead_water`) the presence line says so, in the p.80 definition's terms
   ("a rainbow trout longer than 50 cm in waters where anadromous rainbow trout are found"): "Steelhead are known to be
   in this water, so a rainbow trout over 50 cm (20 in) counts as a steelhead." (`rows.presence_words`).
5. The stop duty: "after you keep 2, stop fishing for the day".

Fixes 6 (a closed day's gear note "No fishing here until Jul 1.") and 7 (a bait allowance moves to the Allowed column)
are the page's (regs-v11).

## UR1 (2026-10-17) — rows 18, gear 10, display 14: user panel round 1, small wording fixes

Six angler personas read v21 screenshots (USER-ROUNDS round 1); only wording changed, no shape.

- **A** a total says whose it is: "4 a day per angler across Region 2, lakes included" (the tree's total tier, the
  header's "· part of 4 a day per angler across …", `total_line`). On a water whose own rule lifts kinds out of the
  stream (lake) share, the share tier names them: "2 a day from this stream (part of the 4) — your hatchery rainbow
  trout don’t use these 2" (the lifting buckets' kept kinds, by name and origin).
- **B** a size tier: "Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"
  ("None of the fish in this box …"; "— except N <steelhead>" at 0). The page draws a size tier as a box.
- **D** a count tier: "1 cutthroat or rainbow trout in all"; its kinds' sentences "Of the 1 cutthroat or rainbow trout
  in all: …".
- **F** a species closure (`not_kept`, status closed) says the p.4 legend's "No fishing for": "Steelhead: no fishing for
  them, not even catch and release — release any you hook by accident".
- **G** the record duty "write each one on your licence right away" (kind `duties`; display titles and sentences
  "Write each hatchery steelhead on your licence right away"); weight "weight on the line up to 1 kg (2.2 lb)"
  (`gear.lb_of`); where every circumstance of the line count is simply being in a boat, the lines part reads "1 line from
  shore; any number from a boat" (the sentence "…, each with one hook, lure or fly.") and the Lines row "1 line (rod)
  from shore; no limit from a boat" (Kootenay Lake's main body). "While alone in a boat" is unchanged.

## ROUND19 (2026-10-17) — rows 17, display 13: a size tier holds what it caps, the origin with the fish, "50 cm (20 in)", the boundary standard, tiers that say their nesting

Owner review of v20 (ROUND19 1, 2, 3, 5, 6; item 4 is the page's); RULINGS P34-P38 (P31's placement and P30's
"50 cm / 20 in" superseded).

- **Sizes** (P34) read "50 cm (20 in)" everywhere: the tree, the header, `also_counts`, sentences, details, the lift lines
  and the headline's `also`, the kept-elsewhere lines, `table.bands[].says`, `caps[].says` and the display section's rule
  titles and sentences (`display.title` / `plain`). `common.cm_in`, `common.range_cm_in` ("25–33 cm (10–13 in)").
- **The origin with the fish** (P35): a kind keeping one origin says it in its name — `says` "Up to 2 may be hatchery
  brook, brown or cutthroat trout", "Up to 2 may be hatchery steelhead" — and its `detail` is its sizes only ("30 cm
  (12 in) or longer"; a steelhead "rainbow trout over 50 cm (20 in)"). `header.tag` is null (the names carry it). The
  card's `origin` says what to keep first: "Keep hatchery fish only — wild trout and char go back."
- **A size tier HOLDS every kind it caps** (P36): each kind row that may be kept over its size, nested in the tightest
  tier holding them all; where they span tiers it wraps those tiers whole (Chilliwack Lake: Region 2's 1 over 50 cm
  wraps the lake's own Dolly Varden tier, the trout and the lake trout). A kind that can never pass the size stays
  outside (the Vedder's hatchery rainbow, 50 cm or less; the Bella Coola's count tier of cutthroat ≤33 and rainbow
  ≤50). `says` "Only 1 of these may be over 50 cm (20 in)" (n 0: "None of these may be over …"; nested caps ", and only 1
  over 50 cm (20 in)"). RULINGS Q16: the steelhead the region's "2 over 50 cm" lets past sits INSIDE — "… — or 2, if one
  is a hatchery steelhead" (`extra` {n, species ["ST"], by the 2-over cap}), and the steelhead kind carries `size_note`
  {tier, says "it counts toward the 1 over 50 cm (20 in), but once you keep one, a second fish over 50 cm (20 in) may be
  kept"}. A cap on ONE kind row (Bennett Lake's lake trout "only 1 over 90 cm"), on part of a row (Glenn Creek's "only 1
  cutthroat trout over 50 cm"), or that no tier can hold without a kind that cannot pass it (a count tier holding a
  cutthroat that may pass 50 cm and a rainbow that may not) stays on the kinds' `also_counts`, as in ROUND18. The kinds
  under a size tier say no also_counts line for it. A kind's sentence is "of" its nearest COUNT tier ("Of the 2 a day:
  up to 2 may be hatchery brook, brown or cutthroat trout, 30 cm (12 in) or longer"); a kind is never above that tier's
  number (a size tier limits only what is over its size). The detail follows a comma.
- **The boundary standard** (P37): "X or less" / "X or longer" include X; "under X" / "over X" exclude X; a fish exactly
  at a cut belongs to the band whose words include it. `table.bands[]` and `cap_kinds[]` carry `from_in` / `to_in`
  (whether the band holds its ends; each cut belongs to exactly one of the two bands meeting at it, `model.CardTable`
  refuses otherwise). The side is the rule's own (`caps.rule_sides`): a take-0 band's bound goes to the keeping side
  (Q13, `catalogue.EXACT_BOUND_IS_LEGAL`; `closed` keeps it inside), a clause's "over X" leaves X below, a quota's keep
  band holds its bounds, the steelhead size leaves 50 cm a rainbow; a take-0 band decides first. Words by
  `common.band_words`: "under 30 cm (12 in)", "30–50 cm (12–20 in)", "over 50 cm (20 in)", "60 cm (24 in) or longer",
  a slot "over 60 cm (24 in), under 90 cm (35 in)". The card's own keep ranges ("30 cm (12 in) or longer", "50 cm (20 in)
  or less", "30–50 cm") already hold their bounds (a keep band is inclusive). Measuring is never that precise; the
  standard only makes every band's words agree with which band holds the cut.
- **Nesting and names in the words** (P38): a count tier "1 between cutthroat and rainbow trout" (its kinds' fish, in
  their order); an own or share tier holding fewer than the count tier around it "2 a day from this stream (part of the
  4)", "This river’s rule: up to 2 a day (part of the 4)"; the header "2 a day from this stream · part of 4 a day across
  Region 2, lakes included"; an own tier's `replaces` names the fish, with their origin where they keep one: "Replaces
  Region 2’s 2-from-streams limit and 30 cm (12 in) minimum size for hatchery rainbow trout" (also the bucket's
  `replaces`, in cm).
- **The gate** (`card_check.check_tree`, `check_caps`) adds: THE PLACEMENT of every size tier — each kind under it may be
  kept over its size; its caps (`by`, `extra.by`) check no kind OUTSIDE it over the size with a number (the reader's
  verdicts); every size cap a bucket states is a tier or on a kind; a size cap said on the kinds over two or more rows
  must be one no tier can hold (part of a row, or the tightest tier around them holds a kind that cannot pass the size
  beside them); a kind's number is no more than its nearest count tier's; a `size_note` names a size tier around its
  kind; numbers in words may include the tier's n + extra and "(part of the N)". Each table band's words say the ends it
  holds and each cut belongs to exactly one band.

## ROUND18 (2026-10-17) — rows 16: the card as a tree, sizes in inches, the Q16 tier, a compact header, why an inherited rule applies

Owner review of v19 (ROUND18 1-4, 7, 8; items 1, 5, 6 are the page's); RULINGS P29-P33. `pipeline/deliver/answers/tree.py`.

- **`rows[].group.tree`** — ONE tier {node "tier", id "t1", tier "total", n, says, by, children}. Tiers: `total`
  ("4 a day across Region 2, lakes included"; a water's own total "This lake’s rule: up to 4 a day"), `own` (a water's
  own rule's bucket: says "This river’s rule: up to 4 a day", `whose`, `replaces` with inches, `why_here` "its row
  covers its tributaries, so it applies here" where the rule reached this water by the tributary walk, `line`), `share`
  ("2 a day from this stream"), `count` ("1 between them"), `size` (`over_cm`, `species` its cap names, `then`, `extra`
  {n, species}: RULINGS Q16 as one tier, "1 over 50 cm / 20 in a day — a second may be a hatchery steelhead"). An own tier
  sits under the share where it holds share kinds to fewer, else under the total; a size tier sits in the tier holding
  every kind its cap names (the total where it spans tiers) and holds the kinds wholly over its size it truly caps (a kind its `extra` lets past stays in the tier above: no kind sits under a tier smaller than its number), only where it
  limits them below that tier's number (or lets a steelhead past it).
- Kinds {node "kind", species, name, n, says "Up to 2 may be brook, brown or cutthroat trout", detail "hatchery, 30 cm
  / 12 in or longer", sizes {cm, in}, also_counts, duties, sentence, presence?}: each EXACTLY ONCE under the tightest
  tier. `also_counts` [{tier | null, by | null, n, over_cm (0: a count), then, species, says}] says every cap no tier of
  the kind holds that still binds it: a size cap "only 1 over 50 cm / 20 in, if you haven’t already kept a Dolly
  Varden/bull trout or lake trout over 50 cm" (n 1), "…, counting any … over 30 cm" (n > 1), "none over 50 cm / 20 in
  if you’ve already kept a …" (binding only through the other kinds), "only 1 cutthroat trout over 50 cm / 20 in"
  (holding some of the row's fish); a count "no more than 3 counting any lake trout you keep". A larger size cap is
  said after a smaller one with the kind held to the smaller number. `sentence` = "Of the {tier}: {says} ({detail}) —
  {also_counts joined by "; "}".
- **`rows[].group.header`** {says, tag}: the headline and the zone total in one line ("2 a day from this stream · 4
  across Region 2, lakes included"; "5 a day" where the headline is the total), `tag` "hatchery only" / "wild only".
- Sizes in cm AND whole inches (`common.inches`: the nearest, half up; 30 → 12, 50 → 20, 25 → 10, 33 → 13, 60 → 24,
  90 → 35): the tree, the lift lines and the headline's `also` ("hatchery rainbow trout 50 cm / 20 in or less"), the
  kept-elsewhere lines ("Kept trout or char over 50 cm / 20 in elsewhere today?"). The bucket fields keep cm only.
- **The gate** (`card_check.check_tree`): every kind once; the total tier = the group's total; every other tier's n =
  the most of its kinds together here; a size tier's n = the most of its cap's kinds over the size (each `then`), its
  kinds kept only over the size, its `extra` exactly that many steelhead more; every kind's n alone (each species) and
  its `sizes` exactly the lengths it may be kept at; every `also_counts` (its number over its size exactly where the
  kind keeps more; none after one of the other kinds for n 1; the kinds together for a count); every number in the
  tree's sentences and the header is one of those proved.

## ROUND16 (2026-10-16) — rows 15, licence 6, display 12: the quota table and cap system, whose limit, rule titles, under 16 by the book, nearer-row cautions and restatements

Owner review of v17 (ROUND16 1-4) and the owner's answers to r15_questions Q1/Q2; RULINGS P25-P28, T15, Li11.

- **`rows[].group.table`** {bands {species: [{from, to|null, says}]}, cells {species: [{n|null (no limit), why
  (released | closed | too small | too big | protected size; exactly when n is 0), origin (only this origin may be
  kept), by_origin {hatchery, wild} | null, as ("ST" a rainbow that size is a steelhead, its number the steelhead's;
  "RB" the other way)}]}, steelhead_cm} — rows: the group's kinds then the fish its row holds that nobody may keep
  here, book order. Bands come from the cut points of every rule covering the fish, merged where nothing kept HERE
  changes, and neighbouring bands keeping none for the same reason merged ("under 30 cm: too small").
- **`rows[].group.cap_kinds`** [{id, species, from, to|null, origin|null}] and **`rows[].group.caps`** [{take, kinds
  {here, stream, lake}, scope, by, says, if_kept_here}]: a basket {place: {kind id: count}} is LEGAL IFF every cap
  holds — the basket's fish of the cap's kinds number at most `take`; an `if_kept_here` cap holds only once the
  basket keeps one of its `here` kinds (a water's own number counts fish kept elsewhere, but never stops a fish kept
  elsewhere). Kinds are the size bands every rule's cut points draw (finer than the table's: they also tell fish
  kept elsewhere apart), split by origin only where a rule tells hatchery from wild; a kind a take-0 cap holds is
  left out of every other cap, and a cap another implies is dropped. The basket is read as "what may I still keep
  HERE after what I kept elsewhere today" (fish kept elsewhere first — RULINGS Q23b makes the order matter across
  waters). `by` the rule (cite its Book page), `scope` "here" | "here, counting fish kept elsewhere today" | "a day
  across Region 5" | "a day from Region 2 streams", `says` one sentence. The table's cells are read off the caps.
- **The gate** (`card_check.check_caps`): every cell = the engine's most of that fish alone at that size here (each
  origin a cell names), and the caps accept EXACTLY the baskets the engine accepts (`Engine.accepts`: the counter
  solver's compiled fish, equal to `legal` on random rule systems) — seeded random baskets with fish kept here, on
  another stream and on another lake (an origin-free kind as either origin, at a random length inside its band),
  each engine rule band at its take and one past it, and each cap at its take and one past it. Live: 15,862 groups.
- **2**: a water's own limit (`elsewhere.says`): "The Bella Coola River’s rule: 1 a day here. Brown trout, cutthroat
  trout or rainbow trout you kept anywhere else today count toward that 1." (Ape Creek).
- **3 (display 12)**: `rules[].title` — every rule's effect in words from its fields ("Release all steelhead",
  "Release lake trout, Oct 1–Nov 30"); rules with no generated form (gear, boats, notices, lifts) take their label,
  dates with an en dash. Show it as the heading wherever a rule is listed; the verbatim is the quote under it.
- **4 (licence 6)**: the under-16 summary says the book's p.6: B.C. resident "No licence or stamp needed, and you
  don’t have to fish with a licence holder. You keep your own limit." (`own_limit` true); non-resident "Fish with
  someone 16 or older who holds the licences and stamps this fishing needs: what you keep counts toward their limit.
  Or buy your own basic angling licence (…) and any supplementary licences and stamps you need, and keep your own
  limit."
- **Q1 (display 12)**: `inherited[].caution` (RULINGS T15): where a nearer water row binding the stream prints no
  such exemption — "Trout Lake’s tributaries’ row, nearer this stream, prints no such exemption; the Duncan River’s
  is applied here because its row covers all its tributaries." — and the export guide's gotcha
  `farther_exemption_nearer_silent` {says, key_on, rules {lift: {silent {entry: nearer_row | own_row}, items}}}.
- **Q2 (rows 15)**: `rows[].restates` also folds ANOTHER row's identical statement no nearer the stream than the
  winner's: "Zone B also prints this: “All from Williston Lake and its tributaries”."

## ROUND15 (2026-10-16) — rows 14, licence 5, display 11: size groups, shared caps, weekday parts, under 16

Owner review of v16 (ROUND15 1-8); RULINGS P19-P24; owner questions in handoff/PLAN/r15_questions.md.

- **Size cap = size group** (`rows[].group.buckets[].sizes` [{says, n, over_cm, by, species, not_counting,
  then [{n, over_cm, species}]}]): names EXACTLY the kinds it can bind today, book order ("Brown trout, Dolly
  Varden/bull trout, lake trout or brook trout: only 1 over 50 cm between them"). A sibling of `groups`, not a
  holder of kind rows: a cap spans buckets on 141 of 1,026 live cards, cuts a kind row on 56, overlaps a count
  group on 10 (scratch scan), so it NAMES its kinds and their rows stay in the bucket. Size notes are gone
  (`notes` keep only counts spanning buckets; the model refuses a size note). The gate proves the number, the
  nested caps, that each named kind may pass the size here and that no kind the rule binds goes unnamed.
- **`buckets[].between`**: "Up to 2 a day between them" on an own-rule bucket whose cap holds several kinds
  together (null otherwise).
- **`rows[].restates`** [{r, of, says}]: a rule of the winner's own row stating exactly what it states (fish,
  number, days, sizes, origin, water) is role `agrees` (one ladder line) and a note with its book words — the
  Lower West Arm's r5 (combined kokanee quota with the Upper West Arm).
- **`rows[].week`** adds `status` ("Weekends: keep 5 · Weekdays: release") and `parts` [{species, days "Sat–Sun",
  when "Saturday and Sunday", weekdays, status, n, says "keep up to 5", rules, today}] on every moment's frame;
  `restates` carried to every moment's frame of the row.
- Whose words: a tributaries-only rule by the walk "the Bella Coola River’s rule for its tributaries"; "the" only
  before river/channel/canal/slough/arm/narrows (`common.the_name`, also display's inherited line). Each kept
  kind's `detail` keeps its origin ("hatchery, 30 cm or longer"). "Kept 2 on another stream?" → "None more here".
  No presence line on a wholly closed water (every row closed).
- **Licence 5**: frame `under_16` [{residency, label, none_needed, with_adult {who, counts} | null, buy [{doc,
  name, prices}], own_limit, says}] per residency (not guided, no status), read off the profile answers;
  statics `residency_labels`, `profile_labels` (60, in `profiles` order). Wire `frames` rows are
  [holds, documents, under_16?].

## ROUND14 (2026-10-16) — rows 13, display 10: nested count groups, whose bucket, steelhead in its section, weekdays in words, one not-kept line per row; the nearer ✱ row

Same format string (`answers/2`); **rows 13, display 10** (ladder 1, gear 9, licence 4 unchanged). Owner review
of v15 (ROUND14 1, 3-6); RULINGS T14, P15-P18.

* **1 (reader, bundle; display 10).** Two ✱ rows competing on a tributary: the row of the water NEAREST the
  stream wins (`read.NEARER_ROW_BEATS_FARTHER`, bundle tables `item_down` / `trib_nearer`, `bundle/nearer.py`):
  a loser's reason `nearer_row` (appended to `read.LOSS_REASONS`, so existing reason indexes are unchanged);
  `display.reasons.nearer_row` {lost: "Replaced by {by}: its water is nearer this stream.", won: …}. The Duncan
  River's own tributaries keep 2 bull trout (the Duncan's rule) where Kootenay Lake's tributaries' release spoke.
* **3 (rows 13).** `buckets[].groups` [{says, n, by, kinds}]: a count over SOME of a bucket's kinds (each kind
  row wholly inside it) is a nested group holding those kind rows with their own sizes — "Cutthroat trout or
  rainbow trout: 1 between them" (the Bella Coola mainstem, Apr 1-May 31, r6) — and those kinds leave the
  bucket's `kinds`; show groups before the bucket's size notes; size caps stay `notes`. The gate proves each
  group's kinds together keep exactly `n`. A water's own pool printing the share's very number is its own
  bucket ("This river’s rule", r5's 2), not the region's share ("all Region 5 streams combined"); its kinds
  carry no "counts toward the 2 from streams".
* **4 (rows 13).** `presence` {line: possible_with_rules | known_with_rules, says} sits INSIDE the steelhead
  section, exactly once: the card's steelhead kind row (`buckets[].kinds[].presence`, or a group's), else its
  not-kept steelhead line (`not_kept[].presence`), else the steelhead's own row (`rows[].presence`). A
  steelhead line with no steelhead section is a build error (`rows.place_presence`). `known_no_rules` (about
  rainbow trout) stays only the frame's `steelhead_line`.
* **5 (rows 13).** `rows[].week` {species, says}: a row whose fish answer differently by WEEKDAY carries the
  window in words on every moment's frame of the day — Kootenay Lake's Lower West Arm kokanee "Keep up to 5 on
  Saturdays and Sundays; release Monday to Friday" (`rows.say_weekdays`, applied in `answers.build` over the
  segments repeating one start; frames cut by an hours window are untouched). Show it in place of the day's
  keep / release words; read the year strip at a day's best answer over its weekday moments (never a list of
  weekend dates, never "release until Oct 16 / keep from Oct 17").
* **6 (rows 13).** `not_kept` is one line per status and deciding ROW (was: per rule), fish in the book's order
  (p.80, `card.book_order`), a closed group named with its members; `rules` every deciding rule (`by` the
  first): Vedder May 15 "Rainbow trout and cutthroat trout: release every one" (r6, r7); Ape Creek "Char (Dolly
  Varden/bull trout, lake trout and brook trout): release every one".

## ROUND13 (2026-10-16) — rows 12, gear 9, display 9: no size on a kind keeping none, species closures said, what a lift replaces

Same format string (`answers/2`); **rows 12, gear 9, display 9** (ladder 1, licence 4 unchanged). Owner review
of v14 (ROUND13 Z, S, G, D, P, A); RULINGS P13, P14, Q5a.

* **Z (rows 12).** A kind row keeping none (`n` 0: every band of the species releases) says no size:
  `detail` "catch and release" — the Bella Coola's cutthroat on Oct 10 ("none under 25 cm and all
  cutthroat trout catch and release") was "25 cm or longer". A kind keeping some keeps its size words.
* **S (rows 12).** `group.not_kept` [{species, status: closed | release, says, by, whose}]: every fish of the
  card's row (`all_members`) that no origin may keep here, one line per deciding rule — "Steelhead: no
  fishing for them" with `whose` "this river’s rule" (Bella Coola r13; the Fraser above Mission's r5 on Aug
  15-Dec 31; Region 6's May 15-June 15 steelhead stream closure), "Dolly Varden/bull trout: release every
  one" (a ✱ tributary of the Duncan, where Kootenay Lake's tributaries row releases bull trout). The page
  cites `by`'s Book page. The gate proves none may be kept (`card_check`, mutation-pinned).
* **D (rows 12).** `buckets[].replaces` {says, rules} on a water's own-rule bucket (`why` rule, a lifting
  entry's), null on every other: each cap of the region the bucket's lifting rules LIFT for its kinds
  (the reader's verdicts), named from the cap's own number and bands — a share "2-from-streams limit", a
  count "1-a-day limit", a size clause "1-over-50-cm limit", a floor "30 cm minimum size", a ceiling "50
  cm maximum size" — by its owner, for the fish it is lifted for: Vedder "Replaces Region 2’s
  2-from-streams limit and 30 cm minimum size for these fish"; Duncan "Replaces Region 4’s 2-from-streams
  limit for these fish, and its 1-over-50-cm limit for rainbow trout". The page shows it under the bucket
  title and no longer shows `lifts` (kept for proof and compatibility).
* **P (rows 12).** The lake line says the count left after one kept there: "Kept 1 at a lake?" — "Keep 2
  more here. Fish from lakes count toward the 4 a day, not toward the 2 from streams." (`here`, gate-proved;
  "Kept some at a lake?" stays only where no number is claimed).
* **G, P (gear 9).** `summary.says` names no hook when nothing restricts it (the `hook` part stays in
  `parts`, so `changes` can still say "any hook" on another day): Okanagan Lake "1 line (2 while alone in a
  boat) with one hook, lure or fly. Weight on the line up to 1 kg."; Vedder "1 line with a single barbless
  hook (one hook, lure or fly). Weight on the line up to 1 kg." The weight part is "weight on the line up to
  1 kg".
* **P (display 9).** `rules[].applies` names the book by its title: "This water’s own row in the 2025–2027
  BC Freshwater Fishing Regulations Synopsis: Cowichan River." (`common.BOOK_TITLE`).
* **A (bundle).** A water row's quota printing "(any size)" lifts EVERY size constraint of its region for its
  fish (`rules._any_size_lifts`, `exempts[].basis: any_size`): Robb Lake's "Bull trout daily quota = 1 (any
  size)" lifts Zone 7A's 1 over 50 cm for bull trout (its card said "Only 1 of the day’s trout and char may
  be over 50 cm" over it). The other "(any size)" rows (Duncan, Lardeau, Kootenay Lake Main Body, Quesnel
  Lake, Kaslo, Keen) already dropped every size constraint.

## ROUND12 (2026-10-16) — rows 11, gear 8: sizes said whole, own bucket first, every cap once, one list

Same format string (`answers/2`); **rows 11, gear 8** (ladder 1, licence 4, display 8 unchanged). Owner
review of v13 (ROUND12 2-3); RULINGS P9 (revised), P11 (revised).

* **1 (rows 11).** A kind row's `detail` says what is kept AND a lifted limit: where a water's rule lifts a
  minimum (or maximum) the open end reads "any length" — Vedder hatchery rainbow "any length, 50 cm or
  less" (p.22 "of any length 50 cm or less" lifts Region 2's 30 cm minimum); a slot reads as its ranges —
  Bennett Lake lake trout "60 cm or less, or 90 cm or longer". Every other detail is unchanged.
* **2 (rows 11).** `buckets`: the water's own rule bucket(s) first, then the region's share, then "The rest
  of the N a day".
* **3 (rows 11).** EVERY cap inside the group is a bucket note, said once where it binds, from the total's
  clauses and from caps outside it the reader says speak for a kind here: a count over two kind rows
  (`over_cm` 0: Bella Coola "Only 1 of these may be cutthroat trout or rainbow trout"; across buckets
  "Only 1 Dolly Varden/bull trout or lake trout a day in all"), a kind's own size cap (Bennett Lake "Only
  1 lake trout may be over 90 cm"), the region's 1 over 50 cm beside a lake's own quota (Deer Lake), a
  stream share's own size clause nested in order on one note (`then` [{n, over_cm, kinds}]: Region 8
  streams "Only 2 of these may be over 30 cm, and only 1 over 50 cm"). A cap another one implies (as
  small a size, no larger number, every kind: Wahleach Lake's 1 over 40 cm over Region 2's over-50 caps)
  or a count holding all its kinds with no larger number (Region 1's "2 over 50 cm" under the stream
  share of 2) is not said. The gate proves each note and each `then`.
* **4 (rows 11).** `group.worked` is ONE list — "How it adds up" and "Fished elsewhere today?" merged: the
  keep-N lines (lifted waters), then the kept-elsewhere examples they do not repeat ("Kept some at a
  lake?", over 50 cm across waters); on a water with no lift those examples are the list. Each line
  {kept, q, a, species, origin, share (keep-here lines), here, other, away (null: not claimed), at: here |
  stream | lake | elsewhere, over_cm, of, most: exactly | at_most}; the gate keeps the line's basket
  (`kept` of the first kind of `species` that may be kept so at `at`) and proves the numbers. "Kept 2
  elsewhere today? Keep 3 more here" is said only where the headline is the row's number (Ross Lake's own
  trout 1 counts fish kept elsewhere: its buckets say it). The card row's `elsewhere.ask` is empty
  (folded); `elsewhere.says` stays.
* **5 (gear 8).** `summary.says` has no em dash: Vedder "1 line with a single barbless hook (one hook, lure
  or fly). Weight up to 1 kg."; Okanagan Lake "1 line (2 while alone in a boat) with any hook (one hook,
  lure or fly). Weight up to 1 kg."; Kootenay Lake Main Body "1 line (no limit while in a boat) with any
  hook (one hook, lure or fly). Weight up to 1 kg."; Cowichan fly-only "Fly fishing only: 1 line with a
  single barbless hook (one fly). No float, sinker or attracting device." The weight part says "weight up
  to 1 kg"; `parts`, `terms`, `cites`, `changes` {when, says} unchanged in shape.

## ROUND11 (2026-10-16) — rows 10, gear 7, display 8: one size note per bucket, one gear sentence, inherited lifts

Same format string (`answers/2`); **rows 10, gear 7, display 8** (ladder 1, licence 4 unchanged); glossary 2.
Owner review of v12 (ROUND11 1-3); RULINGS P9 (revised), P11, P12.

* **A1 (rows 10).** `group.buckets[].notes` [{says, n, over_cm, kinds, not_counting}]: a size cap the
  bucket's kinds share, said ONCE — Vedder's share bucket "Only 1 of these may be over 50 cm, not counting
  steelhead"; a cap spanning buckets on the first bucket it touches, naming the day's fish (Chilliwack Lake:
  "Only 1 of the day’s trout and char may be over 50 cm"). Kind rows carry no size note ("only 1 over 50
  cm", "uses your …", "shares the … above" are gone); `notes` there is only "counts toward …". The card
  gate proves each note: at most `n` of `kinds` over `over_cm` together, and `not_counting` still keepable
  over the size after those `n`; `states` names the bucket note's cap.
* **A2 (gear 7).** `summary` {says, parts [{part: mode | lines | hook | attach | weight, says, cites,
  terms}], terms, cites, changes [{when, says, cites}]}: the day's terminal gear in one sentence — Vedder
  "1 line, a single barbless hook — one hook, lure or fly — and up to 1 kg of weight.", Okanagan Lake "1
  line (2 while alone in a boat), any hook — …", Cowichan's fly-only part "Fly fishing only: 1 line, a
  single barbless hook — one fly — with no float, sinker or attracting device." `terms` are glossary ids
  (`single_barbless_hook`, new `fly_fishing`, `artificial_fly`); `changes` the other days of the year at
  the frame's moment whose sentence differs, by what differs, in calendar order (Vedder Oct 10: {when:
  "May 1–31", says: "fly fishing only"}). A stream's Lines row `says` "1 line (rod)" (", in a boat too"
  dropped); a lake keeps its boat clause.
* **A3 (display 8).** Frame `inherited` [{says, by, lifts}]: a lift the tributary walk carries from a ✱ row
  that this answer relies on (the verdicts say the walked rule `lifted` a closure, release or gear rule
  here) — Iron River, Jul 15–Aug 31: "Open Jul 15–Aug 31: the Quinsam River’s exemption from the summer
  stream closure (MUs 1-1 to 1-6) covers its tributaries." `by` cites the row's Book page and is the rule
  whose `rules[].applies_trib` names the same water. A quota lift (T3a) is the card's lift line; never
  both. Absent where none.

## answers/2.8 (2026-10-12) — the quota card: water-first, each fact once, every number proved

Same format string (`answers/2`); **rows 8, display 7** (ladder 1, gear 5, licence 4 unchanged). A reader
that does not know rows 8 / display 7 must refuse the file. ROUND9 A1-A5, owner decisions D1/D2; RULINGS
Q23b (new, revises Q23's tally) and P9. The regs store format is unchanged (`regs-store/4`: frames and
rule facts are stored whole).

* **The card (rows 8 — A1, A3).** `rows[].group` keeps the DECISION (`pool`, `total`, `kinds`,
  `subcaps`) and replaces the 2.6 lead (`says`) and the 2.7 items (`items`) — they restated the kind rows
  in a second generator and drifted — with the card (`card.card_of`):
  `headline` {n, unit, says} this water's number with no lift (on a stream whose share holds every kind,
  the share: "2 a day from this stream"; else the total: "4 a day"); `origin` ("Hatchery only — release
  wild trout and char." where every kind keeps one origin, else null); `lifts` [{says, by, kinds, n}] a
  water's rule lifting kinds out of the share, said positively, `by` the rule (the page cites its Book
  page): "On this river, hatchery rainbow trout 50 cm or less: up to 4 a day — this river’s rule replaces
  Region 2’s 2-from-streams limit for them."; `total_line` under a share ("No more than 4 trout and char a
  day across Region 2, lakes included."), else null; `buckets` [{title, cap, by, why: share | rule |
  total, kinds [{species, name, detail, n, notes, duties}]}] — the kinds under the cap they draw from
  ("Share the 2 from streams"; "This river’s rule"; the total, untitled when alone), each kind row with
  its number, origin/size, over 50 cm said HERE ("only 1 over 50 cm"; "uses your 1 over 50 cm"; a size
  cap a second row shares: "shares the 1 over 50 cm above"), its duties ("10 a year", "stop fishing for
  the day after 2", "record each one on your licence") and its species (one picture each, P5); `worked`
  (A2) [{kept, q, a, species, origin, share, here, other, away}] RULINGS Q23b worked through for a lift
  past the share; `states` [{r, line}] the cap each card line states — never two.
  ROUND10 W1 (rows 9): ONE lift line per lifting ENTRY — a row lifting several kinds names each with its
  number, highest first: "On this river, its own rule replaces Region 4’s 2-from-streams limit for rainbow
  trout (up to 5 a day) and Dolly Varden/bull trout (up to 2 a day)."; a single lift reads as above.
  `lifts[].each` [{kinds, n, by, fish}] every lifting rule (each `by` cites its Book page), the lift's `by`
  and `n` the first's, `kinds` all; `headline.also` every lifted number above the headline, highest first.
  ROUND10 W4 (gear 6): every count row carries `cites` (every rule deciding it — its main clause's and
  each circumstance's — for its Book page: Kootenay Lake's boat clause) and the Lines row `says` it in
  plain words from those clauses: a stream "1 line (rod), in a boat too", a lake "1 line (rod); 2 while
  alone in a boat", Kootenay Lake Main Body "1 line (rod); no limit while in a boat".
  ROUND10 W2/W3: a part with a main run never mentions its side channels; no label is cut (`…`) — an end
  name is said whole, a part with no run shows its full label.
  A row's lifts name the water as "this river" (a row's own noun): the rows are keyed by CONTENT (a key is
  shared by every part with the same rule set), so a row string cannot carry the water's name; a ✱ row
  the walk carries names its row ("On the Duncan River and its tributaries").
* **Q23b (refined by the owner the same day).** On the lifted water the lifted kind and the other kinds are
  kept in any order (the lifted fish do not use the share there); every OTHER stream counts the lifted
  fish toward its tally; lakes face only the total. Vedder worked count: "Keep 4 rainbows here → that’s
  your trout and char for the day, everywhere." / "Keep 3 rainbows here → 1 more: from here, or a fish
  from a lake." / "Keep 2 rainbows here → you’re done on every other stream; up to 2 more from here or
  from lakes."
* **Q23b in the kept-elsewhere line (A2).** `elsewhere.ask` on a share: "Kept 2 on another stream? Keep 0
  here, except hatchery rainbow trout 50 cm or less: up to 2 more (4 in all)"; "Kept some at a lake? They
  count toward the 4 a day, not toward the 2 from streams"; and RULINGS Q16 across waters, in the sheet,
  never on the card: "Kept trout or char over 50 cm elsewhere today? It counts here too: at most 2 over
  50 cm a day in all — only 1 of them may be anything other than a hatchery steelhead." A partial share
  (Region 6's "1 trout from streams") says its lifted kinds "may go past it here".
* **Where you can't fish (display 7 — A4, D2).** `display.rules[].where` "Where you can’t fish" on the
  standing no-fishing place rules (`display.where_of`: the fishway 23 m and fish-facility 100 m
  buffers, p.9); page v36 shows them under that heading beside the open/closed strip (Keep side); every
  other standing rule stays at the foot of the card.
* **The card gate (A5).** `python -m pipeline.deliver.answers.card_check [--bundle] [--export-dir]
  [--items]` builds the rows and, for every part at Jan 15, May 15, Jun 15, Jul 15 and Oct 10 (each
  distinct card once), keeps baskets of fish under the RULES and the reader's VERDICTS (`card_check.Engine`
  — it never reads the card's arrangement) and holds each claim against them: each kind's number = the
  most of that kind alone here (and of each of its species alone), each bucket's cap = the most of its
  kinds together, the headline = the most of every kind here with the share's lifts set aside, the total
  = the most here, on other streams and on lakes, each worked count = what is left here / on another
  water of this kind / on the other kind after keeping that many of the lifted kind, and no cap stated
  twice. Mutation-pinned on the no-data fixture (`test_answers_28_card.py`: a broken lift, a broken tally).
* Page v36 reads the card (`cardBlock`): no "For every kind" checklist, no group sentence or items, the
  group's big number is `headline`.

## answers/2.7 (2026-10-11) — group wording, book pages for licensing, short part labels

Same format string (`answers/2`); **display 6** (ladder 1, rows 7, gear 5, licence 4 unchanged in
version — rows changes words only). A reader that does not know display 6 must refuse the file. The
regs store is `regs-store/4` (`part.short_s`). ROUND8 A1–A4; RULINGS Q4, Q16, Q23 unchanged.

* **Group items (rows, words only — A1).** RULINGS Q16 is ONE item: a size clause holding every kind of
  the total beside a smaller one of the same band that leaves kinds out ("No more than 2 over 50 cm." +
  "No more than 1 over 50 cm, not counting steelhead.") is "Over 50 cm: at most 2 — only 1 of them may
  be anything other than a hatchery steelhead." (`rows._merge_size_pairs`; the origin word where only
  that origin may be kept on the card; neither clause may carry a lift). A lift out of the stream (lake)
  share names WHOSE rule and WHAT it allows (`rows._lift_item`): "On this river: up to 4 hatchery rainbow
  trout 50 cm or less — the 2-from-streams limit doesn’t apply to them." (Vedder r9, p.22; the number is
  the lifting rule's own, else the total); an inherited ✱ row's lift "On the Duncan River and its
  tributaries: …" (Lardeau Creek). Where a rainbow over 50 cm is a steelhead (p.80, `steelhead_water`)
  the first "steelhead" of the items is defined inline: "Steelhead (rainbow trout over 50 cm) count
  toward the 4." `subcaps` (the decision) are unchanged. Vedder (mouth part), Oct 10: ["On this river:
  up to 4 hatchery rainbow trout 50 cm or less — the 2-from-streams limit doesn’t apply to them.",
  "Steelhead (rainbow trout over 50 cm) count toward the 4.", "No more than 1 Dolly Varden/bull trout or
  lake trout.", "Over 50 cm: at most 2 — only 1 of them may be anything other than a hatchery
  steelhead."]. 1,099 rows frames reworded (19 distinct item lists), nothing else in them.
* **A definition only defines (A2).** Page v36 wrote the group line as `<p class="gline">` holding the
  items' `<ul>`: HTML closes a `<p>` at a `<ul>`, so the items and the kept-elsewhere line fell out of
  the line, after its ⓘ tips — the design showed them under "What these words mean · Daily quota". The
  line is a `<div>`; the glossary term (`daily_quota`) was never the problem and is unchanged.
* **Book pages for licensing records (display 6 — A3).** Static `display.licensing` [{book}], aligned
  with the export's `licensing_ids`, as `rules[].book` (`display.licensing_books`): the page cites a
  licensing record ("Source" on a Classified Water, an exemption) as it cites a rule. Coverage: rules
  3,406 / 3,406 (page v36: 697 / 697) before and after; licensing records 0 / 135 before (page: 0 / 68),
  135 / 135 after (page: 68 / 68).
* **Short part labels (display 6 — A4).** `waters[].parts[].short`, the part strip's name, generic from
  the part's range END labels, mouth upward (`display.short_label`): "Mouth → Vedder Crossing Bridge",
  "CNR Mile 66 Trestle → CNR Bridge (mile 70.2)", "Site C dam → Hwy. 29 bridge (2 stretches)", "Side
  channel", "3 side channels"; a sign post loses "fishing boundary signs", each end is cut at 26
  characters, a part with no run keeps its full label ("Whole water"), cut at 60. An end reads "Mouth" only where the water truly ends; where it continues under another name (`continues:<item>`: a pure name change in FWA, nothing else joining) it names where the next water begins ("Vedder Crossing Bridge"), else "Y begins"; where it meets another stream and the two form the next water it reads "<other> confluence" ("Stamp River confluence"); a tributary keeps "Mouth" (RULINGS P10). Two parts of a water
  never share one (`short_labels` adds the full label's distinguishing tail). `label` stays the sheet's
  full name.
* Proved on a side build (`data/generated/scratch_a27`, the LIVE bundle and verdicts copied — no bundle
  code changed): status index and export byte-identical; every key's ladder, gear, licence and display
  frames identical; rows frames identical with `group.items` set aside; display statics identical but
  `waters[].parts[].short` and the new `licensing`; store round trip exact.

## answers/2.6 (2026-10-11) — zero hand corrections: the strip, the group wording, book links

Same format string (`answers/2`); **rows 7, gear 5, display 5** (ladder 1, licence 4 unchanged). A
reader that does not know these versions must refuse the file. Product-owner rule: every word the app
design shows comes from the pipeline.

* **The open-days strip (display 5).** Each status frame carries `strip`, the OR of the bits of the
  static `display.strip` legend [{state, bit, label, says}]: closed 1 (drawn alone — the gear of a closed
  water is moot), release 2 (open, but no game fish on the card may be kept), fly_only 4,
  artificial_fly_only 8, bait_ban 16; 0 open. ONE SOURCE: composed (`answers.Section.compose`,
  `display.compose`) from the frame's status and the `rows` and `gear` frames at the same key and segment
  (`gear.fly`, `gear.bait_ban`); a tidal frame has none. Vedder (mouth part): May 1–31 20 (fly fishing
  only + bait ban, p.22), Oct 10 0. Cowichan (p.15): Greendale Trestle → CNR Bridge (mile 70.2) fly only
  all year; upstream of the CNR Mile 66 Trestle fly only Sept 1–Nov 15 only (20 on Oct 10, 16 on May 15).
  Frames: 0 ×2,940, 1 ×3,259, 16 ×2,743, 20 ×126, 24 ×95, 18 ×25, 26 ×14, 2 ×5, 4 ×3.
* **Group wording (rows 7, user 2026-10-11).** `group.says` is the LEAD — the total and, where one holds
  every kind of it, the stream (lake) share of all of it — and `group.items` the rest as short sentences:
  "Up to 4 trout and char a day in all — no more than 2 of them from streams (all streams combined)." +
  ["Hatchery rainbow trout 50 cm or less don’t count toward the 2 from streams.", "Steelhead count
  toward the 4.", "No more than 1 Dolly Varden/bull trout or lake trout.", "No more than 2 over 50 cm.",
  "No more than 1 over 50 cm, not counting steelhead."] (was one sentence joined by ";").
* **How this was decided (display 5).** `display.rules[r]` adds `book` [[printed page, PDF page]] (the
  entry's printed page and the 1-based page of the synopsis PDF via the guide's `pdf_pages` — open
  `#page=N`), `applies` (one line: B.C.-wide, a region's table, an area, this water's own row, a federal
  or park authority; what a lift lifts) and `applies_trib` (where the tributary walk carries it:
  "Inherited from Williston Lake: the Region 7B table covers its tributaries."); static `display.reasons`
  {reason: {lost, won}} words every reader loss reason. Per rule, never per key. 10 random rules checked
  against the PDF text: 10 of 10 on the page they cite.
* **A line is a rod (gear 5, owner).** Rule labels say "At most 1 line (rod) per angler", "Unlimited lines
  (rods) per angler" (`labels.COUNT_NOUNS`); a set line keeps "line". Static `gear.count_nouns`
  {slot: [one, many]} for the page's count tags. 3 rule labels changed.
* **The Cowichan's cuts** are named as p.15 prints them: "CNR Mile 66 Trestle", "CNR Bridge (mile 70.2)"
  (curated `splits.json` labels; `pipeline.atlas.sidecars --splits-only` carries a label edit into
  `splits.resolved.json` without an atlas rebuild — the registry's own boundary label follows at the next
  atlas build). 3 Cowichan part labels changed.
* **No empty answer (reader).** Clearwater Creek gnis:7628 had no Arctic grayling answer (14 nulls, the
  only ones in the file): its own "Catch and release" lost to Zone B's "Arctic grayling: 2" by naming, that
  to the inherited Williston tributaries' release by place, and that to the creek's own row (Q5) — a
  cycle, and the old fallback dropped all three. `read._step4_competition` breaks a cycle at the own-row
  edge first (T4: before naming); `_beaten_by` no longer counts an inherited rule the own row beats. Now
  release all year (the creek's r1; Zone B's r4 May 1–Jun 15). `rows.no_answer_gate` refuses a null
  decided answer where a zone retention rule for the fish spoke or was displaced.
* Proved on a side build (`data/generated/scratch_a26`): status index byte-identical; every one of the 2,274
  keys' ladder, gear and licence frames identical but key 1151 (Clearwater Creek) in the ladder; rows and
  display frames identical with `group.says/items` and `strip` set aside, but key 1151; display statics
  differ only by the new fields, two grayling `subsets` and the Cowichan's labels; store round trip exact.

## answers/2.5 (2026-10-10) — group totals in Python; a size release is an answer

Same format string (`answers/2`); **rows 6** (ladder 1, gear 4, licence 4, display 4 unchanged in
version). A reader that does not know rows 6 must refuse the file. User rulings 2026-10-10: RULINGS
Q23, T3a, P8.

* **Group totals (rows F14).** Every keep row carries `elsewhere` {says, ask [[question, answer]]},
  the kept-elsewhere line; ONE row per total carries `group` {pool, total, kinds, subcaps [{r, take,
  water, over_cm, under_cm, of, lifted [{members, by}]}], says} (else null): the TOTAL the row's fish
  count toward (the row's pool, or the outer pool they are contained by, F4) — never the narrowed
  number —, every kind on the card counting toward it (steelhead stay in the trout/char group), each
  count or size clause of it holding some of them here, the kinds a water's own rule LIFTS out of a
  clause (they still count toward the total), and one sentence. A slot ("none between 36 and 44 cm")
  is no sub-cap; a size clause another makes redundant on the card is not said twice. Vedder (mouth
  part), Oct 10: "Up to 4 trout and char a day, steelhead included; at most 2 from streams (hatchery
  rainbow trout 50 cm or less excepted); at most 1 Dolly Varden/bull trout or lake trout; at most 2
  over 50 cm; at most 1 over 50 cm, not counting steelhead." and "Kept 2 on another stream? Keep 0
  here, except hatchery rainbow trout 50 cm or less: up to 4 in all" (the page said "2 a day in total,
  all kinds together" and "Keep 0 here"). Page v36's `groupBlock` reads both (P8). 12,577 groups;
  109 named waters have a stream-share lift (Vedder 4, Duncan ✱ tributaries 80, Kitimat ✱ tributaries
  24, Peace 1 — the bug study's 109), 724 with any lifted sub-cap (578 of them the Liard watershed's
  bull trout 30–50 cm).
* **A size release is an answer (rows F13).** Where no gate or pool speaks, a speaking take-0 size
  band releasing the fish's whole range on the part decides it (status release). Vedder (mouth part),
  May 1–31: hatchery rainbow was `null` and missing from the card; now "release", winner r6, whose
  `plain` is "Release every hatchery rainbow trout 50 cm or less, May 1–31." (was "Keep only rainbow
  trout 50 cm or longer"). 2 key-segments (the Vedder mouth part and three of its tributaries'
  key); no other decided answer is null under a speaking release.
* Proved on a side build (`data/generated/scratch_a25`): status index and export byte-identical;
  every same-content key's rows frame equals the live one with `group`/`elsewhere` removed, except
  those 2 key-segments; the display section differs only in r6's `plain` and `subsets`.

## answers/2.4 (2026-10-10) — every section has an answer (S1: the unnamed water)

Same format string (`answers/2`); **no section version changes** (ladder 1, rows 5, gear 4, licence 4,
display 4 — byte-for-byte the same frames for every named key, whose indexes do not move). New
top-level tables `places` and `unnamed`; `keys` grows by one row per unnamed CONTENT no named part
has, after the named parts' keys. A reader that does not know `unnamed` must not treat a key past
`unnamed.first` as a part of a named water.

**User ruling 2026-10-10:** "unnamed or named but unregulated waters should always show base
regs/zone and any area they are in, tiered properly." Before, keys existed only for the parts of named
waters (2,102); a tapped unnamed section (1.89 M of 1.96 M sections in B.C.) had a rule set but no
answer. **S1b (user, same day):** "for labels … mus just stored in a separate spot, just show regions
the section is in" — S1's first cut made one key per unnamed part AND place (2,272 keys, the MUs in
the name), which made the regs store's `section_akey` vary with the place (1.27 MB br). Now the key
is the content and the place is apart.

* **The unnamed parts are the bundle's** (`unnamed_part`, `bundle/derived.py`, the one place sections
  are grouped — gate 3): every section in B.C. that no named water owns, grouped by exactly an answers
  key's content — the facts a named part is keyed by (rule set, licensing set, province exceptions,
  steelhead water and presence, home region, tidal) and the water KIND. Where the sections lie is not
  a part fact.
* **Keys by content.** An unnamed part takes the key of its content (`common.unnamed_keys`): a named
  part's key where one holds the same content (the frames are the scope's either way — no copy), else
  a new key after the named ones. `unnamed.first` is the first such new key.
* **Kind.** An unnamed section's `kind` is `reach.water_kind.kind_of` (the owner item's kind, else the
  graph node's: stream / lake / wetland) — "we definitely know this from the build" (user). It is what
  every `feature_types` filter placed the rules by; now the answers key and the licence reader
  (`read.section_kind`, G5's wrong-water step) read it too. The old gap (138 rule keys, 91,480
  sections with no kind) came from copying kind only from named waters.
* **Tiering** follows from the rule set: the zone base (the region's tables, `rank` region) and every
  area row the section lies in (`rank` area — parks, WMAs, the Liard watershed …) compete in the
  reader exactly as on a named water; the card's `scope` badge says `region` / `area` / `bc`.
* **Where a section is** (every section in B.C., named or not): its REGION SET (the bundle's
  `region_place`, `section_ruleset.place`) and its MU SET (`mu_set`, `section_ruleset.mus` — stored,
  never in a name or a key, and NOT shipped in the regs store — the app reads MUs from the tiles'
  MU layer, or the bundle later).
* **`places`** `[{regions, label, names}]` — the bundle's region sets in order; `label` their words
  ("Region 5", "Regions 3 and 8"), `names` {lake, stream, wetland} the name of an unnamed answer
  there, whole, generated by `display.place_label` (`display.PLACE_TEMPLATE`): "Unnamed stream ·
  Region 5", "Unnamed lake · Regions 3 and 8", "Unnamed wetland · Region 7A". Model
  `schemas["top.places"]`.
* **`unnamed`** `{first, parts}` — `parts[up]` the key of the bundle's unnamed part `up`; every key from
  `first` on is taken by at least one part. Model `schemas["top.unnamed"]`.
* **How a section reaches its key and name.** Not through `parts` (an unnamed part has no item): the
  regs store (`regs.sqlite`, format regs-store/3) maps every section handle in B.C. to its akey
  (`section_akey`, nonzero for every one; 0 only outside B.C.), and its place (`section_place`); a section no named water owns is named `places[place].names[keys[akey − 1][7]]`.
  `encode.tap_key` is the tap for a key index.
* `about.counts` adds `unnamed_keys` (new keys), `unnamed_parts` and `places`.

## answers/2.3 (2026-10-09) — the rulings round (Q38, Q39, Q41) and gaps G2–G4

Same format string (`answers/2`); **rows 5, gear 4, licence 4, display 4** (ladder 1 unchanged). A
reader that does not know these versions must refuse the file.

* **A snagged fish (Q38 / RULINGS G6).** `zp:prohibited_methods.r3` is `caught: [foul_hooked]` (a
  condition on the FISH, `catalogue.CAUGHT_HOW`), no longer `while: [snagging]`: never an outright
  release, never a closure, its own key — it decides no quota for a fish hooked in the mouth.
  `display.rules[r].kind` `caught`, `plain` "Any fish snagged — even by accident — must be
  released."; every freshwater gear answer lists it in `caught` (gear 4), no longer in `while_rules`.
* **The possession quota's exception (Q39 / RULINGS Q15).** Every possession line of
  `display.rules[].plain` says it, in one wording (`display.POSSESSION_HOME`): "Possession: twice
  the daily quota (fish at home don’t count)."; "Have no more than 1 Arctic grayling in possession
  (fish at home don’t count)."
* **National parks (Q41 / RULINGS Z12).** `display.rules[r].unless_opened` — a full closure printed
  with a proviso (`condition_of`): "Closed unless opened by Parks Canada — a national park fishing
  permit is required."; a display frame's `unless_opened` [rule] names the closing rules whose
  proviso is the answer at that segment. A national park RESERVE (Pacific Rim, Gwaii Haanas, Gulf
  Islands) is closed by its own rule with no proviso, so it carries none: plainly closed.
* **G2 (licence 4).** `others` / `guiding` are [{req, paths}]: another angler's or a guide's
  requirement with how it is met as the record prints it (`satisfied_by`: `need`, `accompanied_by?`,
  `as?`, `quota?`).
* **G3 (display 4).** `display.rules[r].subsets` [{fish, for, plain?}]: the rule said for each
  subset of its fish a row's or an item's ladder lists it for (`display.subset_asks`, the page's
  `rowSources`/`forTxt`); `plain` null where the rule has no sentence (write `for` beside its label).
* **G4 (rows 5).** A cross-reference item of SEVERAL fish carries `ranges` [[fish, from_cm,
  to_cm|null]], each fish's own keep range (`origins` stays the one-fish form).

## answers/2.2 (2026-10-08) — the user-test round

Same format string (`answers/2`); **rows 4, licence 3**, a new top-level `glossary` (ladder 1, gear 3,
display 3 unchanged — byte-for-byte the same frames). A reader that does not know rows 4 / licence 3
must refuse the file.

* **A shared count is shared (rows F11).** `real_daily` now merges the kinds a COUNT limit holds
  together (the row's stream or lake share, a clause `within` the pool) into one unit worth its take;
  `real_daily.shared_count` [{take, members}] names the limits that bind. Root cause of the user-test
  bug ("Only 3 of the 5 can be brown, cutthroat, rainbow trout or steelhead", Babine/Skeena, p.49 "1
  trout from streams July 1-Oct 31"): F1 split the trout into one item per kind, each carrying the
  share's 1 as its own band, and `eff_cap` summed the items (1+1+1). Now 1. 1,897 waters change
  (almost all Region 6 streams; Region 2 and 7/6 lakes for the "DV/LT combined" clause).
* **Steelhead under a zone's size line (rows F1b amended, F12; catalogue gate).** Which fish a
  general size cap does not count is decided in rows (`conds[].except`), never on the page. Steelhead
  leave a zone's trout/char size line only where the same table prints its own steelhead quota for
  that size (`catalogue.steelhead_scope_problems`, a `CatalogueEntry` validator): Regions 1 and 2
  ("2 hatchery steelhead over 50 cm"), not 3, 5, 6, 7 or 8. Region 6's "1 over 50 cm (quota includes
  hatchery steelhead)" no longer carries `except: [RB]`. 620 waters.
* **The licence (licence L9, L10).** A document's `when` is the BROADEST requirement needing it,
  `also_when` the narrower ones (Babine's Steelhead Stamp: to fish at all Sep 1-Oct 31, else to fish
  for steelhead); `or` [{need, alt?, prices}] another way to satisfy the same need (Teslin and the
  other Yukon-border waters: basic licence OR Yukon angling licence — never two documents). An angler
  whose first way is already satisfied (exempt) buys nothing. 516 + 26 waters.
* **`glossary`** {version 1, terms [{id, term, says, example?, pages, quote, source}]} — generated by
  `glossary.py` from the export's rules, licensing records and entries and the book's text
  (`CURATED.regulations.reference`): 27 terms (18 + the 9 regions). Model `schemas["top.glossary"]`.

## 1. Pairing

| check | how |
|---|---|
| same bundle | `about.bundle` equals `about.bundle` in both export files (`reach_digest`, `section_handles`, …) |
| same rules | `about.export.rule_ids_sha256` is the first 16 hex digits of SHA-256(`rule_ids` joined by `"\n"`) |
| same format | `about.format == "answers/2"` |

Every rule ref is an index into the export's `rules` / `rule_ids`; every licensing ref an index into
its `licensing` / `licensing_ids`.

## 2. How a tap resolves (lookups only)

1. **Part → key.** `k = parts[item][i]`, `i` the part's index in the export's `waters[item].parts`.
   `null`: the part has no rule set — it is wholly outside B.C. (`water.outside_bc`). A section of no
   named water: `k` = its akey − 1 (the regs store's `section_akey`; answers 2.4), named by
   `places[p].names[keys[k][7]]`, `p` its place (the regs store's `section_place`).
2. **Date → segment.** Day number `d` (§3); `starts = segments[keys[k][9]]`; `s` = the last index with
   `starts[s] <= d`. Where `keys[k][10]` is not null and that start repeats, the segments sharing it
   are the day's moments: take the one whose `moments[segment_moments[keys[k][10]][s]]` holds the
   date's weekday (and, of an hours pair, `in` false for the day, `in` true for the window — show
   both).
3. **Section → frame.** `frame = sections[name].frames[sections[name].at[k][s]]`, decoded as the
   section says (§6).
4. **Fish, origin, profile.** In the ladder frame, the fish (index into `fish`) and the
   origin (none | hatchery | wild); in the rows frame, the fish code and hatchery | wild; in the
   licence frame, the profile index (mixed radix over `profile_dims`).

Every section uses the same `k` and `s`: they join with no remapping.

## 3. Calendar, segments, wrap-around, Feb 29

- **Day numbers** 1..366 on the leap calendar: Jan 1 = 1, Feb 29 = 60, Mar 1 = 61 in every year,
  Dec 31 = 366 (`calendar.day_of`). In a year without Feb 29, day 60 is never asked.
- **Segments** are the union of every section's cuts: a member rule's or a lift's `when` (ladder,
  rows, gear, display), a requirement's or a designation's `when` (licence). Day 1 always
  starts a segment; a reading across New Year is two segments sharing one value.
- **Feb 29 (ruling 2026-10-06).** A range printed to Feb 28 runs through Feb 29: the book is written
  for years without one and means "through the end of February" (`catalogue.range_days`, the one
  place dates become days). The Nicola below the lake is catch and release on Feb 29, as on Feb 28;
  the Feb-29-only segment v0 reported is gone.

## 4. The keys

`keys` rows: `[ruleset, licensing_set, steelhead_water, steelhead, steelhead_rules, province_except,
home_region, kind, tidal, segments, moments]` (`moments`: an index into `segment_moments` or null —
answers 2.1) — the reference harness's `partKey` plus the water's kind and
tidal water (`common.part_keys`). From `unnamed.first` on (answers 2.4), one row per
unnamed content no named part has (`common.unnamed_keys`), the same fields. Each section reads what it needs of it (`Section.scope`):

| section | scope (what its answers depend on) |
|---|---|
| ladder, gear, display | the rule key: (ruleset, steelhead_water, steelhead_rules) |
| rows | the rule key, the water's kind, the steelhead presence |
| licence | (licensing_set, province_except, tidal, kind, the rule set where a designation can sleep) |

## 5. Sections

### `ladder` (Stage 4)

Per fish and origin, every rule the traced reader returned (the stored verdict), for EVERY fish the
verdicts asked: every game fish (ST everywhere — answered as RB where no steelhead rule applies), and
crayfish, chinook or a protected fish where a member rule names it. The page filters to its own
fish. Field dictionary: the file's `spec`. The decided answer is `rows`' (`decided`); answers/1's
`answer` section, a second decision with its own tie-break, is gone.

### `rows` (Stages 5.1-5.8): today's card

Per (key, segment): `[spp, {fish: [hatchery, wild]}, [row], steelhead_line]`.

- `spp`: the fish the card asks about (5.1), in the page's order.
- `decided` (per fish and origin, or null): `status` (keep | no_limit | release | closed), `win`,
  `daily` — **the number after the winner's clauses** (5.2 step 5: a sub-limit covering every fish of
  the pool narrows it; an orphan clause whose own total was replaced still binds), `narrow`, `lines`
  (every line of steps 6-11, in the page's order: rel, cap, subcap, also, outer, steel, outercap,
  outersize, orphan, partly, caution, tnote, annual, duty, record, with band edges `a`/`b` in cm and
  `take`), `roles` ([rule, role, by]: governs, agrees, contains, also, narrows, limit, floor, season,
  duty, possession, falls, moot, replaced, lifted), `lift_notes`.
- `rows` (5.3): one per shared limit, in card order — `kind`, `pool` or `win`, `members`,
  `all_members` (with the fish that go back), `daily`, `narrow`, `everyone` and `groups` (the facts:
  origin, origin2, exc, xref and every line, merged across fish, with `members`, `rules`, `general`,
  `carve_of`), `prot` (the protected fish of an open-subject row), `wins`, `lift_notes`, `scope`
  ({of: water | area | region | bc, entry, share, apart}: the badge, 5.5 — `entry` the area's or
  region's entry, `share` = a stream (lake) share of the pool holds here, `apart` = the row's fish
  are counted apart from a wider total they lift and no `outer` total is left; every daily limit,
  a water's own too, counts fish kept elsewhere today), `conds` (rowConds: every condition on
  keeping; `who` the fish a go-back, group or origin2 condition is about; a cap with `except` is
  general but for those fish), `items` (5.8: ONE item per kind — each fish in exactly one item —
  with its keep bands `[[from_cm, to_cm|null, number]]` (the keep range is the first band's from
  and the last band's to; a band of 0 is a slot to release), `back`, `xref`, `sub`, `conds`
  (indexes of the row's `conds` about these fish), `against` (a fish with its own row: the number
  it counts toward here, `"unlimited"` for a total with no number; absent on other items) and per-origin `origins` for a fish with its own row), and `real_daily`
  (5.7: `{n, all, sum, capped_sum, rb, shared_cap [{take, over_cm}] | null, capped, open}` or null;
  "Really {sum} a day here" when `all`, "Only {capped_sum} of the {n} can be …" when `rb`).
  Section version 2 (the rows fixes, §8 F1-F10).
- `steelhead_line` (5.6): possible_with_rules | known_with_rules | known_no_rules | null.

### `gear` (Stage 7.1-7.6)

Per (key, segment): the resolved gear answer (`gear.resolve`) — counts, specs, elements, main and
circumstantial clauses, hook, fly, bait per element, ways to fish, conduct by moment, vessel rules,
timed / in-part / side / while rules, `overruled` ({rule, state, reason, by}: the reader's losers),
the rules that decide and those that repeat. A clause is `[rule, clause index into the rule's
gear]`. Static: `province_methods`, `parent`, `methods`, `moments`, `conduct_means`.

### `licence` (Stage 7.7)

Per (key, segment): `[holds, documents]` — the reader's requirements in force (holds, displaced,
wrong_water, waived, not_yet_mapped, also_printed, designations, stamp_period, contested,
considered) and 60 per-profile answers (documents to buy with when, base and prices; none_needed;
the angler's requirements with paths; exempt; others; guiding). Static: `profiles`, `profile_dims`.

### `display`

Per (key, segment): `{status, closing}` — base | own | closed, the verdicts' code at the
segment's moment; `closing` the full closures that speak, unlifted, with the game fish each closes
(version 3, gap G1). Static: `rules`
(per export rule: kind, closure, bands, plain sentence) and `waters` (per named water: per export
part its label, runs, place, hint, km, closed_all_year, paper_licence; the picker; unresolved
licensing records).

**Adding a section** takes an `answers.Section` (scope + prepare, or derive_from + derive, optional
static), an `encode.CODECS` entry and its text in `encode.SPEC` and this file; `spec_gaps` refuses a
file that lacks any of them (`test_answers_v0.py`, `test_answers_v1.py`).

## 6. Field dictionary

Shipped inside the file as `spec` (`encode.SPEC`); every key of the file and of each section is
described there, or the build stops.

## 7. Coverage: everything the consumer page v35 displays → where it comes from

**export** = in the export pair; **page** = presentation the page keeps (wording, layout, colour,
drawings, arithmetic over shipped numbers); otherwise the section and field. **GAP**: none.

### Status, strips, part picker (Stage 3)

| displayed | field(s) |
|---|---|
| Part label, "Two stretches", side channels | `display.waters[item].parts[i].label` / `.runs` |
| Entry place heading | `display.waters[item].parts[i].place`; picker `choices[].heading`, `headed` |
| "What sets it apart" hint, " · plus / without …" | `display.waters[item].parts[i].hint`, `.label` |
| Picker order, groups, "N sections", "Closed all year · N stretches" | `display.waters[item].picker.choices[]` {parts, closed, sections, heading, text}; `parts[i].closed_all_year`, `.km`, `.order` |
| "N sections outside B.C." | export `waters[].outside_bc` |
| Default part | page |
| Status line Open / Closed | `display` frame `status` (closed = the status index's) |
| "Open, except some parts" | `ladder` `not_yet_mapped` retention rules in force + export rule `family` |
| "Opens <date>", "Closed from <date>" | `display` status per segment + `segments`; the date lookup is page |
| "Trout closed until …" / "release only until …" | `rows` rows (kind release/closed) per segment |
| Water strip (365 days, legend) | `display` status per segment; drawing is page |
| Tidal banner + text | export `waters[].tidal.guide` (angler words only, B17); key `tidal`; the `display`, `gear` and `licence` frames of a tidal key (FIX D12) |
| Closed banner: closures in the chain, ranks | `display` frame `closing` (G1, decided) + `display.rules[].plain`; merging back-to-back runs is page |
| Per-group strip, short keeping windows | `rows` decided status per fish per segment, grouped by `rows` members |
| Run sentences, date chips | `rows`/`display` per segment + `segments`; wording and choice of chips page |

### The ladder (Stage 4) and "How this was decided" (Stage 6)

| displayed | field(s) |
|---|---|
| Every rule's state | `ladder.verdicts` |
| Lifted (with lifter), displaced (with winner), moot | `ladder` `lost` [rule, reason, by] |
| "partly lifted", "Lifted only when fishing for …" | `ladder` `partly`; `rows` `lift_notes` [lifter, qualifier] + export `verbatim` |
| Roles governs / agrees / contains / also / moot / narrows / limit / floor / season / duty / possession / falls / replaced / lifted | `rows.decided[].roles` [rule, role, by] |
| Tags ("Daily limit", "Release", "Closed", …) | page over `roles` + export `type` / `record_retention` |
| Plain sentence per rule | `display.rules[].plain` (null: page shortens `label`) |
| Place headings | export `provenance.rank`, entry name; trib from export ruleset |
| "Rules that don't apply here (N)", "Replaced by …" | `rows.decided[].roles` (losing roles with `by`) + export rank |
| Answer box "2 a day here, and they count toward 4 a day in Region 2" | `rows` row `daily`, `pool` take (export), `scope` |
| Row sources sheet (verbatim, who, role in words) | `rows.decided[].roles` merged over the row's fish (page) + export rule |
| Rule sheet | export `rules[]` |

### Today's card (Stage 5)

| displayed | field(s) |
|---|---|
| Which fish get a row (5.1) | `rows` `spp` |
| Per fish and origin: status, winner, number | `rows.decided` (status, win, daily) |
| Lines: release under / cap over / "only N of them" / outer total / also / orphan / partly / caution / trout note / annual / duty / record / steel | `rows.decided[].lines` |
| Main answer per fish (hatchery if keepable, else wild) | page over `rows` fish [hatchery, wild] |
| Rows: keep rows by pool, exc, xref, origin / origin2, merged lines, everyone / groups, protected row, row order | `rows.rows[]` (order is card order) |
| Row title | export `species` (group names) + row `pool` / `members` / `prot` (page words) |
| Row badge (This water / Region-wide / stream share / Separate from …) | `rows.rows[].scope`; "Separate from" = another row whose fish lift this pool (`roles` lifted) |
| Row value (N / No limit / Release / Closed) | `rows.rows[].kind`, `daily` |
| "N a day in total, all kinds together" | row `daily` + `all_members` |
| Scope sentence + worked examples | row `scope`, `daily`, pool take (export); arithmetic page |
| "Really N a day here." / "Only K of the N can be …" | `rows.rows[].real_daily` |
| "For every kind" ✓ lines, "Also" lines, counted-apart rows | `rows.rows[].conds` (+ `general`), `everyone`, `groups` |
| Steelhead presence line | `rows` frame `steelhead_line` |
| Fish items: keep range, band numbers, keep sentence, cap lines, shared cap, steelhead notes, own-row fish per origin, "Release every one until …", stream share | `rows.rows[].items` (bands, back, xref, sub, origins) + `conds` (streamcap) + `display.rules[].bands` |
| Duties list | `rows.decided[].lines` annual / record / duty |
| Simple / release / closed / protected rows, "Which fish (N)" | `rows.rows[]` kind, members, prot |
| "What they look like" drawings | page |
| Row's own year strip | `rows` per segment |
| Spots box (undrawn-part rules) | `ladder` `not_yet_mapped` + export `undrawn_part`, `parts`, `when` |
| One side of the channel box | `ladder` `beside` + export `side` |
| "Closed to some anglers" | `ladder` speaks + export `type: angler_closure` |
| "At certain times" / "differs on other days" | the day's other MOMENTS (`moments`, `segment_moments`): the segment at the date's weekday is the card; the others (other weekdays, inside an hours window) are named with their answer |
| "? Check" box | export `provenance.uncertain` / `why` |
| "No quota rules reach this part of the water" | `rows` with no row |
| "Anywhere in B.C." standing rules | `ladder` `shown` + export `standing`, `verbatim` |
| Possession footnote | `rows.decided[].roles` possession + export `per_daily` |
| Other fish fold, card order | page (card order shipped: `rows.rows` order) |

### Gear & licence (Stage 7)

| displayed | field(s) |
|---|---|
| Closed water: "No gear may go in the water…" | `display` status closed |
| Line & hooks tile and rows | `gear` counts, specs, elements, hook, fly, circumstantial |
| Bait tile and rows | `gear` bait[], bait_ban |
| Ways to fish tile and rows | `gear` ways[] + static province_methods |
| Boats tile | `gear` vessel {active, timed} + export rank |
| Always tile (conduct cards) | `gear` conduct {moment: [[act, [rule]]]} + static moments, conduct_means |
| All gear sources | `gear` decides, repeats, overruled |
| Licence: who is fishing | `licence` static profiles, profile_dims |
| Designation boxes | `licence` holds designations, stamp_period + export records |
| Records considered | `licence` holds considered |
| Requirements, waivers, displaced ("Not valid here"), wrong water, undrawn part | `licence` holds (holds, displaced, waived, wrong_water, not_yet_mapped, also_printed) and per profile requirements[] |
| Exemptions, paths, "You need", base licence, prices | `licence` per profile: exempt, requirements[].paths, documents[] (doc, when, base, prices) |
| "No licence needed to fish here" | `licence` per profile none_needed |
| Paper licence line | `display.waters[].parts[].paper_licence` + per profile requirements |
| Folds: guiding, other anglers | `licence` per profile guiding, others |
| "Check: not a Classified Water", unresolved record | `licence` holds contested; `display.waters[].unresolved_licensing` |

### Checks (Stage 8)

| displayed | field(s) |
|---|---|
| Export cases ✓ / ✕ | `guide.cases` vs `ladder` (the shipped states are the reader's) |

## 8. Decisions (judgment calls where the page or the rules are ambiguous)

Each: what · why · effect · how to undo. Also summarised in `handoff/DECISIONS.md` ("Answers layer
(v1)").

**Gear** (`gear.DECISIONS`): G1 a rule-level `when_targeting` makes its clauses circumstantial · the
book scopes the whole rule to the target · 108 golden gear records list the white-sturgeon dead-fish
clause as circumstantial where the page shows it as main · undo: read only clause targeting in
`gear._entries`. G2 gear in force is the reader's traced answer (no rule-by-rule workaround) · one
source; the competition now keys on conditions, means and acts · `overruled` lists the reader's
losers · undo: revert gear.states. G3-G6 as in `gear.DECISIONS`.

**Licence** (`licence.DECISIONS`): L1 restatement binds as the restated record (who, satisfied_by,
water); L2 displaced requirements sell nothing; L3 a book-known steelhead lake keeps the stamp
(AGENTS 54; 1,260 golden licence answers differ, page bug); L4 unknown kind keeps kind-scoped
requirements; L5 Métis its own status; L6 prices; L7 tidal water: no province-wide record (1,440
golden licence answers, page bug); L8 a document needed to fish here is base even while classified
(as the page groups it; undo: `and "on" not in b["when"]` in licence.documents).

**Display** (`display.DECISIONS`): D1 part facts from the decoded export; D2 page part order for
labels; D3 closed all year = status index; D4 steelhead line ships in `rows`; D5 `plain` for the
rule's own fish; D6 all splits for run-end names.

**Rows** (`rows.DECISIONS`): R1 a `moot` loser gets role moot; R2 rank = the reader's place; R3 the
page's member order; R4 the fish asked are the page's, a missing ladder fish is an error; R5 open-
subject rows from the reader asked about the subject's first fish; R6 lift notes are the page's
settle fact; R7 ICU collation for fish names; R8 the real daily limit reads every general cap
structurally.

**Rows fixes** (`rows.DECISIONS` F1-F10, handoff ROWS-REVIEW; where the page contradicts the book
the card follows the book and the port names the page bug, `reference/port.py` `PAGE_BUGS`):
F1 one item per kind (Anderson R.: brook/brown/cutthroat "up to 4, only 1 over 50", not "keep 1
over 60"); F1b a cap missing only the steelhead-filtered rainbow is general with `except: [RB]`;
F2 origin2 also when the other origin's keep RANGE differs ("No wild trout over 50 cm",
Chilliwack Lake); F3 lines about other fish compared by SET (Kitimat, Feb: the cutthroat/brown
release); F4 a fish lifted out of the narrowing clause counts toward the outer total (Vedder
hatchery rainbow 4 of 4, `against`); F5a slots are 0 bands (Teslin); F5b the zone's outer size cap
caps a water's own bands (Thompson below Kamloops L.); F5c a rainbow's range ends at 50 cm on a
steelhead water; F5d every shared cap (Region 8 "2 over 30"); F6 `possession_cap`, never a yearly
limit, decided once by `display.kind_of` (the display rule kind, the rows' line and role and the
plain sentence all read it; the page's "annual" is page bug d10); F7 `scope.apart` only when no outer total is left (Vedder r9, Kitimat r4/r5 still count
toward the region's); F8 `of: area` (Haida Gwaii, Bowron Lake Park, Liard); F9 a water's own
limit still counts fish kept elsewhere (the page's "Only fish kept on this lake count" is wrong);
F10 no presence data for any fish but steelhead: the book's group wording stays. Undo: revert the
rows-fixes commit (rows.py, the section's version 2).

**Keying and reader (v1)**:
- V1 Gear and conduct dimensions carry clause condition, means and acts, NOT the water kind · where
  two gear rules both bind they bind the same kind, and a water row's barbless rule must still
  replace the zone's · zp:set_lining.r2 stays displaced by z6/z7a set_lining.r2 (the same statement,
  closer rung) · undo: add `water` to the gear condition in `catalogue.CatalogueRule.dimension`.
- V2 handling_rule (20 province conduct rules) is keyed by its acts · two duties never replace each
  other · no answer changes (they never competed) · undo: return `t.value`.
- V3 G5 order: wrong water is read after the also_printed fold; a superior authority's displacement
  is read after wrong water; `displaced` lists the superior keys sorted; `also_printed` keeps only
  held or displaced keys · one obligation goes where its restated record goes · licence answers
  unchanged · undo: reorder in read.requirements_in_force.
- V4 The licence frame's `holds` leaves out displaced requirements (they are under `displaced`; the
  per-profile requirement rows still show them) · the reader's semantics · 3 key-runs · undo: list
  `rows` in licence.section wire.
- V5 The part key carries `kind` and `tidal` · the licence key reads both · v0 sections unchanged ·
  undo: drop them (the licence key would then not be a function of the part key).
- V6 Display status per segment is the status index's code — the verdicts' projection (base/own/closed); a tidal
  part reads its key's `tidal`, not a TIDAL code · one definition of closed · undo: map tidal keys.
- D12 (FIX round, user ruling 2026-10-06) TIDAL WATER IS A DOCUMENTED STATE. On a part key with
  `tidal` 1 the `display` frame is `{"status": "tidal", "tidal": true, "note", "see", "licence"}`
  (`common.TIDAL_STATE`: "Tidal water: a different regulation system … see the federal (DFO) tidal
  waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence
  is required"), the `gear` frame is `TIDAL_STATE` itself (never "angling: not allowed here"), and
  the `licence` frame's `holds.tidal` is `TIDAL_STATE` with every profile `{documents: [],
  none_needed: false, requirements: [], tidal: true}` (never "no licence needed"). Its ladder lists
  each fish with only the tidal row's own note, shown; its rows hold no rule. All year, one scope (`common.TIDAL_SCOPE`). The export's
  `waters[].tidal.guide` starts with the same words · undo: drop the `TIDAL_SCOPE` branches in
  display/gear/licence.
- V7 Feb 29 (coordinator ruling): a range to Feb 28 runs through Feb 29 · `catalogue.range_days` ·
  3 part-days of the v0 file change (the Nicola below the lake and two lakes), status index day 60
  only · undo: drop the `(2, 28)` branch in range_days.

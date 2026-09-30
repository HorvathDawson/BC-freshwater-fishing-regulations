# Current state — regulations pipeline, as of HEAD 228b9a7e + working tree (2026-09-29)

Rewritten 2026-09-29. The previous version (dated HEAD 9ba92c01, 2026-09-24) predates the zone
verification (936330a4), the competition rulings (14890bfa → aeb070cf), the species ruling
(29ef0a20, 38149b92) and `rest` (228b9a7e); its numbers are gone, not carried over.

**Every number below was re-measured on 2026-09-29, and each says where it came from.** The four
sources are:

| short name | file | vintage |
|---|---|---|
| *catalogue* | `data/curated/regulations/entries/catalogue/*.json` (11 files) | working tree; identical in rule count to HEAD |
| *reach report* | `data/generated/reaches/full/report.json` (+ `rule_unresolved.jsonl`, `rule_diagnostic.jsonl`) | run 2026-09-28 16:20, digest `6e4fd99a93de3711` |
| *export* | `data/generated/regs/ui-rules-export.json` (18.6 MB) | 2026-09-28 16:46, same reach digest |
| *bundle* | `data/generated/bundle/bundle.sqlite` (67.0 MB) | 2026-09-28 16:46, same reach digest |

The reach run, bundle and export were built from the code of 228b9a7e. **The working-tree
changes listed in §2 are not in them.** "Printed p. N" is the page number printed on the
synopsis; "PDF p. N" is the index in `data/source/fishing_synopsis.pdf` (R7A is PDF 64, printed 58).

---

## 1. The model on one page

**Entry.** One row of the synopsis: a water (`r<region>:…`, **1,393**), a regional chapter item
(`z<region>:…`, **93**), or a provincial item (`zp:…`, **29**). Total **1,515**, in 11 files
(*catalogue*). An entry carries the printed passage (`regs_verbatim`), the registry items it
covers (`matched`), and three lists: `rules` (**3,317**), `licensing` (**111**) and `see`
(pointers, below). Every rule's `verbatim` is a contiguous substring of the passage, enforced on
load; all 1,515 entries load through the working-tree model with 0 refusals (checked by
validating every entry with `CatalogueEntry.model_validate`).

Entry-level fields beside those: `extents` (narrows the row, never gives a rule its place),
`includes_tributaries` (the first-column asterisk), `symbols`, `scope_note`, and:

- **`see`** — a POINTER is not a rule (user ruling, 3b5c6e9c). *"See Lonzo Creek"* is
  `see: [{verbatim, entry_ids}]` (or `unresolved` with a reason when it points at no row); the
  model refuses an `advisory` that is a pointer. **73 entries carry one pointer each** (*catalogue*);
  the export tags them `alias` 34, `see` 32, `twin` 7 (*export* `entries[].see[].relation`).
  52 water entries hold only a pointer and no rules. A pointer never moves a water to the
  target's region (Mara Lake).
- **`anadromous_rainbow`** — the p.86 steelhead-water flag. **1 entry** sets it
  (`r2:chilliwack_vedder_rivers…@2-4`, *catalogue*); the bundle's `steelhead_water` table covers
  **26 sections** (*export* `about.counts.sections.anadromous_rainbow`). Where it holds, a rainbow
  over 50 cm is a steelhead (`read.as_rainbow`).

Water entries by rule count (*catalogue*): 0 rules 59 (7 licensing-only, 52 pointer-only),
1 rule 642, 2 rules 225, 3 rules 232, 4 rules 129, 5+ rules 106. Water rows hold 2,987 rules and
82 licensing records; zone/provincial entries hold 330 rules and 29 licensing records.

**Rule.** One printed regulation, typed. **13 types in 6 families** (`catalogue.RuleType`,
`catalogue._FAMILY`; counts from *catalogue*, equal to *export* `index.rules_by_family`):

| family | types (rules) | rules |
|---|---|---:|
| retention | retention_limit (1,812), stop_fishing_after_quota (3) | 1,815 |
| gear_and_method | bait_restriction (415), tackle_restriction (361), method_rule (157) | 933 |
| vessel | vessel_rule (389), navigation_duty (1) | 390 |
| information | advisory (67), hazard (26), program_membership (19), facility (13) | 125 |
| access | angler_closure (34) | 34 |
| conduct | handling_rule (20) | 20 |

Retired and refused on load: `angling_from_vessel_prohibited` ("no angling from boats" is a
`method_rule` banning `angling` `when: {angler: in_boat}`), `document_required`,
`access_permission` (licensing is its own list).

### Where a rule applies — `extents`

A list, unioned. **A rule never inherits its entry's extents** (AGENTS 13). Ops, by extent count
(*catalogue*): `whole` 2,575 · `within` 332 · `between` 164 · `downstream_of` 151 ·
`upstream_of` 128 · **`rest` 4**.

- `upstream_of` / `downstream_of` / `between` take cut-point ids from
  `data/curated/waters/splits.json`; `within` takes `area_id` or `area_kind`.
- **`rest`** (228b9a7e) — "Other parts": the rule's water, with the rule's own tributary scope,
  minus every section its named `siblings` bind (`reach.build._build_rest`). A sibling that does
  not draw or bind makes the rest `complement_unknown`, never the whole water. 4 rules: Bull River
  r2, Elk River (upstream of Elko Dam) r3, Findlay Creek r2/r3 (*catalogue*); all 4 bind
  (*reach report* `diagnostics.complement` 4).
- Extent keys beyond the op (rules carrying them, *catalogue*): `splits` 430 · `area_id` 247 ·
  `feature_types` 96 (stream/lake; all-or-none across a rule's extents) · `area_kind` 69 ·
  `item_id` 60 · `within_area` 28 (intersect with a polygon, after the walk) · `outside_area` 15 ·
  **`outside_items` 10** (subtract named waters an area rule only touches — Kootenay Lake from
  the Creston Valley WMA, Goose Lake from Malcolm Knapp) · `item_ids` 7 · **`watershed` 7** ·
  `siblings` 4. `outside_areas` and `outside_area_kind` exist and are unused.
- **`Extent.watershed`** (b460859a) — a watershed PART ("Fraser watershed upstream of X"): sides
  decided by FWA code position, not by a walk. 7 rules: `r5:fraser_river.r5`,
  `z5:white_sturgeon.r1/r2`, `z6:iskut_fraser_closure.r1`, `z6:skeena_nass_winter_closure.r1/r2`,
  `zp:white_sturgeon_licence.r2`. Refused beside `includes_tributaries`.
- A place nothing can draw: the words go in `extent_text` / `unresolved_locators` and the rule
  has **no** extents and stays unbound (**14 rules**, all with `extent_text`; 36 rules carry
  `unresolved_locators`). `{op: whole}` beside `extent_text` is refused.

### Structural fields on a rule (rules carrying each, *catalogue*)

- **`undrawn_part`** (127) — a PART of the rule's own water nothing draws ("on parts", "within
  200 m of Bush-Sullivan Bridge"), held beside `[{op: whole}]`. The rule is shown on the water as
  "not yet mapped" and **never** competes, displaces, lifts or suspends (`read.not_yet_mapped`).
  The export ships these as `binds: sections_in_part` (127). A part naming no place ("on parts")
  exports `identified: false`. Place text may carry no values ("on parts (8 km/h)" is refused).
- **`side`** (1) — `north|south|east|west`: the rule holds on that half of a river's channel
  only (38149b92). `r6:kitimat_river.r1` "No Fishing on the west half of river …" (side west).
  Read as state "beside": shown, never displacing (`read.effective_rules`). Required whenever the
  sentence prints "<side> half of the river" (`HALF_OF_CHANNEL`). A lake's half is `undrawn_part`.
- **`life_stage`** (1) — `adult`, a stage the book defines (p.77 adult chinook). Only
  `zp:salmon_stamp.r1`; species must be `[CH]`; never written as `lengths`.
- **`closure_kind`** (11) — `spring|summer|winter`: the season a zone's blanket closure is named
  for, so an "exempt from spring closure" lift reaches only spring closures in other regions
  (11b7441f). Curator-set; the validator checks it against the printed word.
- **`includes_tributaries`** on the rule (31; `None` inherits the entry's) / **`tributaries_only`**
  (63) / **`tributary_excludes`** (12). "Tributaries" walks STREAMS only (p.86, b460859a);
  "watershed" keeps lakes. `tributary_excludes` names what the walk must not enter; on a
  licensing designation, what it removes goes to the excluded water's own designation
  (`carve_outs_to_owner`).
- `water` (80; stream|lake), `origin` (56), `within` (107; a clause inside its parent quota),
  `exempts` (211), `closed_to` (34) / `closed_to_except` (19), `species_except` (35),
  `record_retention` (5), `condition_of` (4), `authority` (7), `standing` (2), `derived_from` (1),
  `suspended_while` (0 on rules), `notice` (0), `review_reason` (210 rules in 141 entries).

### Species (29ef0a20, 38149b92, aeb070cf)

- **The book's list is the only species set** (p.86, `catalogue.BOOK_FAMILIES`): 22 fish in
  TROUT (RB, ST, CT, GB), CHAR (DV, LT, EB), WHITEFISH (LW, MW), BASS (LMB, SMB) and OTHER (KO,
  GR, BB, WSG, BCB, NP, YP, WP, GE, IN, CRA). Any other code is refused (`species_problems`).
- **A bull trout is a Dolly Varden** (p.86 footnote). `BT` is refused; "bull trout" is `DV`, and
  the label reads "Dolly Varden/bull trout". 105 rules name `DV` (*catalogue*).
- **"Trout" is `TROUT_CHAR`** — trout includes char unless char are specifically excluded (p.86).
  `TROUT` is refused. **When the same row or zone table mentions char apart** (char, Dolly
  Varden/bull trout, lake trout, brook trout — not the group word "trout/char"), its bare "trout"
  lines are `TROUT_CHAR` with `species_except: [CHAR]`; never one char alone
  (`trout_scope_problems`, refused both ways). 394 rules name `TROUT_CHAR`; 25 carry
  `species_except: [CHAR]` (*catalogue*). Such lines label "Trout — …"; the export guide carries a
  user-facing note saying so.
- Groups: `TROUT_CHAR`, `CHAR`, `WHITEFISH`, `BASS`, `ALL_GAME_FISH` (the whole list). **`CHAR` is
  a naming group** (`NAMING_GROUPS`): "All char" names each char, so it beats a water's group
  "Trout daily quota = 2" for a char.
- Open subjects with no members: `ALL_FIN_FISH` (every fish but crayfish), `PROTECTED_SPECIES`,
  `SALMON`. **Chinook `CH`** is the one salmon the book names: a member of SALMON, never a game
  fish, not in `ALL_GAME_FISH` (1 rule, the adult-chinook record duty). Salmon regulations
  proper come with the DFO feed.
- "All other species" / "all species" / bare "catch and release" on a water row = `ALL_GAME_FISH`
  except `CRA`.
- Scientific names for display are `catalogue.SCIENTIFIC_NAMES` (working tree, §2);
  `pipeline/regs/parsing/species.py` and its `bc_species.csv` are deleted.

### Seasons — `when` (576 rules)

`{dates[], hours, weekdays[], unparsed[]}`; absent = all year, every hour, every day. `dates`
inclusive and may wrap the year end, always the days the rule HOLDS (an "except" is inverted when
written). `hours` both ends, each `{at}` or `{solar, offset_min}` (negative = before). `unparsed`
keeps a season nobody could read, never shown as all year. Counts (*catalogue*): dates 566,
hours 7, weekdays 15, unparsed 0. **A date after a `;` scopes only its own clause**; an "and"/comma
chain shares the date. The model refuses a rule whose own quote prints dates it does not carry,
and a dated sentence split into siblings that lost them (1c8dfac8). `windows` is refused.

### Sizes — `lengths` (277 rules)

An ordered list of `{min_cm, max_cm, take}`, inclusive, first match wins; a range without `take`
uses the rule's; a length no range covers is not spoken about. `over_cm`/`under_cm`/`band` are
refused. **Only on `retention_limit`** (working tree, §2); a size a licence depends on is
`Doing.lengths` on the licensing record.

### Gear — `gear` (924 rules), `while` (18), `conduct` (28)

`gear` is an ordered list of clauses, each one `slot` (18 in `catalogue.Slot`; set slots bait,
lure, method, barb; counted/measured slots such as points_per_hook, lines_per_angler,
hook_gap_mm; spec slots set_lining, crayfish_trapping, downrigger, light, ice_hut) and one
bound: `allow` | `only` | `ban` (never empty) for sets, `max`/`min`/`unlimited` for counts,
`must_be` for specs; `when`/`unless` conditions; first match wins within a slot. Slots in use
(clauses, *catalogue*): bait 414, points_per_hook 280, barb 252, method 176, lure 36, others ≤ 7.
`while` = the means during which a rule binds (11 tokens: the methods plus the devices
`downrigger`, `light`). `conduct` = acts from a closed registry (26 in `CONDUCT_ACTS`), named in
the lawful direction. A gear `note` escapes the closed vocabulary and requires a
`review_reason` (working tree, §2; 0 corpus clauses carry one). Every dump uses `by_alias`
(`while_`, `except_` are Python names).

### Exemptions — `exempts` (211 rules)

Exactly one of `{default_id}` (6 registered zone defaults: `spring_stream_closure`,
`summer_stream_closure`, `steelhead_stream_closure`, `trout_char_winter_release`,
`bait_ban_streams`, `single_barbless_hook`) or `{target[, entry_id]}`. A lift-only rule states
nothing else and never competes. The bundle resolves lifts to exact rules, adds DERIVED lifts
(`basis: names_the_fish`: a water row naming a fish its region closes; `equivalent`: another
region's closure of the same `closure_kind`), and carries the lifter's dates and origin.

### Licensing — `licensing` (111 records)

A separate list on the entry, not a rule type. It never decides open or closed, and the angler is
always unknown (answers are conditional). Kinds (*catalogue*, = *export*
`about.counts.licensing_by_kind`): `designation` 74, `requirement` 23, `licence_terms` 10,
`not_classified` 2, `exemption` 1, `alternative` 1. `Who` axes: `residency`, `age`, `guidance`,
`status` (indian_bc_resident, metis, disabled), `role` (companion). Youth/Disabled Accompanied
Waters are two rules — `program_membership` (the notice) and an `angler_closure` closed to
`age: [16_plus]` except disabled residents and companions (`closed_to_except`, 19 rules). A
licensing record with no extents takes its entry's at placement (`reach.licensing.place_record`).
The export lists 14 licences.

---

## 2. In flight — working tree, not committed, not in the reach run / bundle / export

Made 2026-09-29 by another agent; the artifacts in the table above predate them.

1. **`read.effective_rules` step 4b** (`pipeline/deliver/bundle/read.py`,
   `read.released_on_water`; test `pipeline/tests/test_zone_release_by_water.py`, untracked).
   A zone release limited to a kind of water (`water: stream`, every extent `feature_types:
   [stream]`) — Region 3's "Bull trout (Dolly Varden) from streams, Aug 1-Oct 31", Region 4's
   "Trout/char release: in streams from Nov 1-Mar 31" — in force on that water, displaces its
   OWN region's table's quotas and clauses that keep the fish in the same base dimension
   ("daily"), exactly as a no-water release (Region 3's "Lake trout from Oct 15-Jan 31") does.
   Size clauses of another dimension ("none under 60 cm"), closures, water rows and other
   regions' rules are untouched. A water row that LIFTS the release ("EXEMPT from the regional
   Nov 1-Mar 31 trout/char catch and release": Columbia, Lardeau, Upper Arrow drawdown; Duncan
   for bull trout only; Peace River for Zone B kokanee) removes it in step 3, so it displaces
   nothing there. 22 zone releases qualify (all checked against the book). This is read-time
   logic, so it applies to the bundle as shipped once merged; the export `guide` describes it
   (`zone_release_by_water`).
   KNOWN GAPS, not handled (the water-kind release's dimension `daily@water=stream` never meets
   a water row's `daily` in step 4): (a) a water row printing its OWN DATES for the fish never
   overrides a water-kind zone release on the overlap (`water_dates_override`) — 0 instances in
   the current corpus; (b) a water row's GROUP quota ("Trout/char daily quota = 1", Atnarko)
   speaks beside a water-kind zone release NAMING the fish (Region 5's bull trout from streams),
   where a no-water zone release naming the fish would displace it by naming — the release
   still speaks, so the stricter answer is shown, but beside a quota. (c) Kitimat's
   hatchery-only lift of Region 6's winter trout release leaves the release partly lifted, and
   step 4b still displaces the region's trout/char quota for every origin (the water's own
   hatchery quota speaks).
2. **`pipeline/regs/parsing/species.py` is deleted.** Scientific names are
   `catalogue.SCIENTIFIC_NAMES`, which `export_ui_rules.py` reads; `pipeline/tests/test_species.py`
   is deleted; so is `bc_species.csv`, which nothing read (2026-09-29).
3. **Validators** (`catalogue.py`): a gear `note` (`GearWhen`/`GearSpec`) requires a
   `review_reason`; `lengths` is refused off `retention_limit`; a conditional `any_bait` ban
   labels "Bait ban (…)" instead of "No any bait". The corpus still loads clean (0 refusals).
4. **`z2:malcolm_knapp_research_forest`** keeps Goose Lake (`wbk:329291952`) out with
   `outside_items`: only 2.2% of the lake (0.17 of 7.6 ha) lies inside the forest polygon
   (`area_catalog.gpkg`). Rule r1 now carries a `review_reason` saying so. (Peaceful Lake,
   `wbk:329292654`, is 36% inside and still bound — not decided here.)
5. **Catalogue rewrite** (`scratchpad/FIX/migrate.py`, idempotent): every region file re-written
   through `io.write_entryfile` from models, so keys are in canonical order and a save from the
   review app moves no keys (content unchanged except items 4 and 6).
6. **`zp:barbless_single_hook_streams`**: `regs_verbatim` printed the lake note twice, the
   second with a stray ")"; now as PDF p.10 prints it, and r2's verbatim/label lose the ")".
7. **Parse prompt** (`CATALOGUE_PARSE_PROMPT.md`) teaches the lift shape (`exempts`, the six
   `default_id` slugs, a lift-only quota's `species`), Youth/Disabled as two rules, a bare
   "Catch and release" as `ALL_GAME_FISH` except `CRA`, the no-registry envelope (`whole` + a
   `review_reason`), the stamp clause outside Region 4, no list markers in a verbatim, and the
   fields a curator sets; `pipeline/tests/test_parse_prompt_teaches.py` pins each teaching.

---

## 3. What competes — the reference reader

**The ladder is applied in code:** `pipeline/deliver/bundle/read.py::effective_rules(section,
on, fish)` returns the rules that speak for one fish on one section on one day, each with a
state (`speaks`, `beside`, `shown`, `not_yet_mapped`). It is the reference the app must match;
the export `guide` says the same in prose. (The previous version of this file said nothing
applied the ladder; that was wrong from 1c8dfac8 on.) Its steps:

0. Where anadromous rainbow are found (`steelhead_water`), a rainbow over 50 cm is a steelhead.
1. In force on the day (`when`; dormant under `suspended_while`). A `side` rule is "beside".
2. About this fish (`speaks_for`) — **competition is per fish**.
3. Lifts (printed or derived), per fish and per day; a lift scoped to a target, a means, an
   origin or a size leaves the rule standing.
4. Competition on `(type, dimension)`: superior authority first; then **naming** (a rule naming
   the fish beats one naming a group holding it; a `within` clause is named at its parent's
   level); then **place** (water, inherited by the tributary walk, area, region, province).
   Water vs zone quotas: the **same statement** → the water's number wins, larger or smaller;
   **different** statements sit beside; a **larger** water number for a fish is a printed lift of
   the zone's number (Kootenay Lake rainbow 10 over Region 4's 5). A dated zone release/closure
   is not displaced by an undated water quota unless it is the exact same statement (Shuswap
   char 1 vs Region 3's lake trout release Oct 15-Jan 31). **A water row printing its own dates
   for the fish overrides a dated zone release or quota on the overlap** (Cheslatta/Murray lake
   trout Nov 1-30: the lake's 3) — never a closure. A closure is never displaced; only a lift
   removes it, and it speaks for every fish it covers.
   **4b** (working tree, §2): a zone release limited to a kind of water displaces its own
   table's same-dimension quotas on that water.
5. A water's release silences the zone's quotas for that fish, whatever their conditions.
6. A lake on a region line takes both zones' bases; per fish the stricter applies.

Information-family and `standing` rules are "shown" and never compete; `undrawn_part` rules are
"not_yet_mapped". The "(any size)" caution: 7 rows print "(any size)" (*catalogue* grep of
`regs_verbatim`); aeb070cf reports the caution on the 4 where a larger water quota lifts the
zone's size clause — not re-derived here.

---

## 4. Placement and what ships

**Placement** (`pipeline/atlas/reach`, *reach report*): 3,317 rules → **3,277 bound, 40
unresolved**, every unresolved rule with a typed reason: `no_sections_for_items` 23 (water rows
with no matched item: Frog, Hidden, Secret, Square, Redfern, Squirrel lakes, Mackenzie Creek,
Endako River), `no_extents` 14, `locators_unresolved` 1 (Strathcona park waters' carve-out),
`area_scope` 1 (Wood River), `unknown` 1 (Brunette River r1). **All 40 are water-row rules; every
zone and provincial rule binds** (330: 329 `sections`, 1 `sections_in_part`, *export*).

`tributaries_pending` is **8**, and it is not a missing walk: the tributary walk exists (451 rules
carry a `tributaries` diagnostic). The 8 are exactly the unresolved `no_extents` rules whose rule
or entry asks for tributaries (Campbell River r4, Lower Campbell Lake's tributaries, Nanaimo River
r5, Columbia Lake's and Duncan Lake's tributaries, Goat River r3, Fulton River r3, Kitimat River
r6) — measured by applying `reach.build.wants_tributaries` to `rule_unresolved.jsonl`.

Diagnostics (distinct rules, `rule_diagnostic.jsonl`): tributaries 451, outside_bc 282,
region_clip 111, unclassified straddling pieces 108, ambiguous_cut 75, within_area 7,
watershed 7, complement 4, feature_types 3, scope_unclassified 3. Two entries' scope is
unresolved (`scope_unresolved`: `r5:chipmunk_lake@6-1`, `r5:toms_lake@6-1`).

**Bundle** (*bundle*): `entry` 1,515 rows, `rule` 3,317, 2,070 interned rule sets
(`ruleset.set_id`), 151 licensing sets; the `release` table is empty. `schema.sql`'s comment
on `rule.type` still says "one of 14" (another agent's file; not changed here).

**Export** (*export* `about.counts`): entries 1,515, rules 3,317, licensing 111, licences 14,
rulesets 2,070, licensing_sets 151, waters 19,754; sections 1,956,563 (1,956,205 with a ruleset,
286,501 with a licensing set, 63,396 on a named water, 1,831 outside B.C., 10,186 national-park
sections under `province_except`); `unresolved_references: []`. Rules by `binds`: sections 3,150,
sections_in_part 127, nowhere 40. Of the 40, **23 still ship `extents: [{op: whole}]`** — the
rules on entries with no matched item (`provenance.why` = `no_sections_for_items`). Labels: 0
contain `**`; 293 equal their verbatim; 0 duplicate labels within an entry; no rule's `fields`
carries `period` off a counted type; `obligation` appears only where it is `should` (19).

**The app** reads no regulation data (`app/packages/core/src/regulations.ts`: "NOT
INTEGRATED"). **curation-review** edits the model through its validators. At 228b9a7e entry
`see`, `anadromous_rainbow`, `closure_kind` and `Extent.watershed` had no control; the working
tree is adding them (`SeeEditor.tsx` untracked; `EntryDetail`, `ExtentEditor`, `RuleEditor`
modified) — not checked here.

---

## 5. The previous open items — which still stand

From the 2026-09-24 version's §3/§5, checked against the artifacts above:

| item | now | how known |
|---|---|---|
| R2 Malcolm Knapp Research Forest lakes absent | **fixed**: `z2:malcolm_knapp_research_forest.r1` binds (lakes within the forest polygon, Goose Lake out) | *catalogue*, *export* binds |
| R2 bass quota unbound / "no mechanism subtracts one lake" | **fixed**: region-wide rules bind the region and water rows override (ruling); `Extent.outside_items` exists (10 rules). `z2:species_quotas` r1-r7 all bind | *export* |
| R5 white sturgeon ×3, zp sturgeon licence r2 unbound | **fixed**: all bind (`Extent.watershed`) | *export* |
| R6 Skeena/Nass winter closure ×3, Iskut, Fraser-in-R6 unbound | **fixed**: all bind | *export* |
| R6 steelhead self-lift | **fixed** (936330a4): the mainstem exemption is its own rule, `z6:steelhead_stream_closure.r2` | *export* label |
| R7B kokanee stream ban, R4 invasive notice unbound | **fixed**: bind | *export* |
| z2 rubble_creek, z7b thin_ice (no polygon) | **fixed**: bind | *export* |
| Haida Gwaii streams carry R1's all-year bait ban | **fixed**: `z1:bait_ban_streams.r1` is Region 1 streams `outside_area` MUs 6-12/6-13 | *export* extents |
| p.8 livewell/high-grading bullet missing | fixed per 3fa4e5ed | commit message; not re-read against p.8 |
| 6 lost "only one over" sub-limits | fixed per 3fa4e5ed (word numbers accepted) | commit message; not re-measured |
| Reservoir tributary walks swallow neighbours (Lower Arrow) | walk fixed per 3fa4e5ed (stops at the next lake; Lower Arrow 50,722 → 5,352). **Kinbasket's "Does not include Columbia River upstream" is still a rule with no extents** (`kinbasket_lake_tributaries.r2`, unresolved `no_extents`), not a `tributary_excludes` | *export*; walk sizes not re-measured |
| "No trout over N" stored two ways | **still stands**: of retention rules whose every `lengths` band has `take: 0`, 42 also carry `take: 0` and 56 carry no `take` | *catalogue* |
| Zone `source_pages` wrong on 7 of 9 chapters | **fixed**: every quota-box entry carries its printed page (z1 13, z2 21, z3 28, z4 34, z5 42, z6 49, z7a 58, z7b 64, z8 68) | *catalogue* |
| `zp:spear_fishing.r1` label drops the species | **fixed**: "No spear fishing for game fish" | *export* |
| `z6:tagging_program` advisory twice | **fixed**: one rule | *export* |
| Redirect rows are unlinked advisories | **fixed**: `see` pointers (73) | *catalogue* |
| Export defaults leak (`period`, `obligation`), `**` labels, duplicate labels | **fixed** | *export* |
| 92 unbound rules ship `whole` | **23** now (above) | *export* |
| Chehalis River r2 binds 2 Harrison sections | **not re-measured** | — |
| Split-lake parent sections (Williston, Kootenay, Shannon) carry a leftover ruleset | **not re-measured** | — |
| No automated check against the printed book | **still stands** (`quota_print` went with the settling layer on 2026-09-22; 936330a4 was a four-way manual verification) | — |
| Review/repass loop never run on the corpus | **not re-measured** (`reviews/`, the ingest ledger and the parse work dir were not inspected) | — |

Still open beyond that list: the 40 unresolved water rules (§4), ambiguous cuts (75 rules) and
unclassified straddling pieces (108 rules), both reported only in `rule_diagnostic.jsonl`; the
reach build is not deterministic run-to-run (memory note, not re-measured); the steelhead
definitional size is applied only on `steelhead_water` sections (1 entry, 26 sections).

---

## 6. Tests

Collected, not run, on 2026-09-29 against the working tree (`./.venv/bin/python -m pytest
--collect-only -q`, unfiltered): **2,263 tests, 2,066 in the default run, 197 `slow`**
(deselected by `pytest.ini`, `-m "not slow"`). The working tree includes other agents' new,
untracked test files (`test_zone_release_by_water.py`, `test_part_touches.py`), so this moves.
The suite was not run for this document; its pass/fail state is not claimed.

---

## 7. Not re-measured in this rewrite

- Anything that needs the graph or a build: the review-app/builder parity (AGENTS 16), walk
  sizes, determinism, the split-lake leftovers, Chehalis.
- Provenance of the water entries (how many are LLM-parsed vs hand-authored) and the ingest
  ledger's protection set.
- A sample of water rows against the printed page (the 2026-09-24 version sampled 64 entries;
  not repeated).
- Test pass/fail (collected only).
- The export `guide` text, beyond the counts and fields quoted above.

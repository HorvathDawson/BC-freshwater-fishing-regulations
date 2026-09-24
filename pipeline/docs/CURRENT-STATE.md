# Current state — regulations pipeline, as of HEAD 9ba92c01 (2026-09-24)

A read-only audit. Every number here was measured against the files at this commit, not
copied from commit messages. Where a commit message and the data disagree, the data wins and
the gap is noted. "Printed p. N" is the page number printed on the synopsis; "PDF p. N" is the
page index in `data/source/fishing_synopsis.pdf`. Printed = PDF − 2 up to Region 4; the map
pages inserted after that make the offset larger (Region 5 printed 42 is PDF 48).

---

## 1. The model on one page

**Entry.** One row of the synopsis: a water (`r<region>:…`, 1,393), a regional chapter item
(`z<region>:…`, 90), or a provincial item (`zp:…`, 27). Total 1,510, in 11 files under
`data/curated/regulations/entries/catalogue/`. An entry carries the printed passage
(`regs_verbatim`), the registry items it covers (`matched`), and two lists:
`rules` (3,348 in total) and `licensing` (110 in total). Every rule's `verbatim` must be a
contiguous substring of the passage. That substring check is enforced on load.

**Rule.** One printed regulation, typed. There are **14 types in 6 families**:

| family | types | rules |
|---|---|---|
| retention (what you may keep) | retention_limit, stop_fishing_after_quota | 1,801 |
| gear_and_method (how you may fish) | bait_restriction, tackle_restriction, method_rule | 930 |
| vessel | vessel_rule, angling_from_vessel_prohibited, navigation_duty | 405 |
| information (governs nothing) | advisory, hazard, program_membership, facility | 186 |
| access | angler_closure (closed to one kind of angler, e.g. non-guided aliens on weekends) | 15 |
| conduct | handling_rule | 11 |

**What competes.** Two rules compete only when they share `(type, dimension)`. The
dimension is computed from the rule's fields. For retention it is the period
(daily/possession/annual). For tackle it is the set of gear slots. For bait it is which bait
plus the target species. For a method rule it is the methods named. For an angler closure it
is who it closes the water to. Between competitors, the smaller rank wins: water, then
inherited-by-tributary, then area, then region table, then province. A `superior` authority
(federal: national parks, protected species) sits outside the ladder. Information rules and
`standing` rules never compete. A closure is lifted only by an `exempts`, never by a competing
quota. Nothing in the repo applies this ladder today. It is written down in the export guide
and nowhere else.

**Structural fields on a rule.**

- `extents`: where the rule applies, as a list whose members are unioned. Each extent has an
  `op` (`whole`, `upstream_of`, `downstream_of`, `between`, `within`) and cut-point ids
  ("splits") from `data/curated/waters/splits.json`. It may add `item_id`/`item_ids` to scope
  to specific waters, `area_id`/`area_kind` for `within`, `feature_types` (stream/lake), and
  `within_area`/`outside_area(s)` to intersect with or subtract a polygon. **A rule never
  inherits its entry's extents.** A rule whose place cannot be drawn keeps its words in
  `extent_text` / `unresolved_locators`, has no extents, and stays unbound.
- `includes_tributaries` / `tributaries_only` / `tributary_excludes`: whether the tributary
  walk runs, whether the mainstem is left out, and what the walk must not enter.
- `lengths`: an ordered list of length bands `{min_cm, max_cm, take}`, first match wins. It
  replaced `over_cm`/`under_cm`/`band`, which are now refused on load.
- `when`: `{dates[], hours, weekdays[], unparsed[]}`. Empty means all year. `unparsed` holds
  seasons that could not be read and must never be shown as all year. It replaced `windows`
  and four other fields.
- `gear`: an ordered list of clauses. Each clause names a `slot` (bait, lure, method, barb,
  hooks_per_line, points_per_hook, lines_per_angler, …) and one bound: `allow`, `only`, `ban`,
  `max`/`min`, `unlimited`, or `must_be`. First match wins within a slot.
  `while`: the rule binds only while you are doing something (a way of fishing such as
  spear_fishing, or a device such as a downrigger). `conduct`: named duties from a closed
  registry (`do_not_waste_catch`, …).
- `exempts`: what this rule lifts. Either a zone default by slug (`spring_stream_closure`, one
  of 6 registered) or one rule by `target` (+ `entry_id` when the rule is in another entry).
- `suspended_while`: this rule sleeps while another rule in the same entry binds.
- `angler_closure` + `closed_to`: a closure for one kind of angler (15 rules).

**Licensing** is a separate list on the entry, not a rule type. It never decides open or
closed, and it depends on who the angler is. There are six record kinds:
`designation` (Class I/II water, licence unit, steelhead-stamp period or waiver; 74),
`requirement` (who, doing what, must hold which documents; 23), `licence_terms` (how a
licence is sold; 9), `not_classified` (2), `exemption` (1), `alternative` (1).

**Placement** (`pipeline/atlas/reach`). Each rule's extents are resolved to atlas sections by
route measure along the cut's own blue line. The entry's `matched` items are the only water
scope. Then, in order:

1. **Tributary walk.** Runs when the rule, or its entry, includes tributaries (466 rules walked
   in this run).
2. **Feature-type filter.**
3. **`within_area` / `outside_area`.**
4. **Region clip.** A regional row is held to its own region (125 clips). Licensing is exempt
   from this step.
5. **Outside B.C.** Sections past the border are removed (1,824 sections; waters show
   "outside B.C.").
6. **Split lakes.** Williston, Kootenay and Shannon are cut into parts. The parent lake keeps
   one leftover section that carries only area and provincial rules.

Output is `data/generated/reaches/full/`. The result: 3,098 rules bound, 250 unbound, and
every unbound rule has a typed reason.

**What the bundle ships** (`data/generated/bundle/bundle.sqlite`, 64 MB): the `entry` and
`rule` tables (3,348 rules, each with its generated label, verbatim, `when_`, `while_`,
`exempts` and `uncertain`), the interned rulesets and `section_ruleset`, seven licensing
tables plus `licensing_set`/`section_licensing`, `outside_bc`, the items, and the
gauge/stocking/place tables.

**What the export is** (`data/generated/regs/ui-rules-export.json`, 18 MB): every entry,
rule and licensing record, each with its label, verbatim, fields and provenance. It also
carries the interned sets, a `waters` map of 19,757 items with their parts, and a long `guide`
that explains how to read all of it. It deliberately settles nothing: there are no open/closed
verdicts and no quota tables.

**What the app does now:** nothing regulatory. `app/packages/core/src/regulations.ts` is a
typed placeholder. The map draws water in one neutral style, and a water's sheet says
"Regulations are coming". The previous integration was deleted in 30c3e4d1.

**What curation-review does:** a local FastAPI + Vite app (`bash curation-review/run.sh`). It
shows each entry's printed text, its parsed rules, its split bindings and the water on a map.
It can edit every field of the current model and saves through the model's validators, with
no LLM involved. It is the only way a human edits entries.

---

## 2. How big it got

The window is the 12 commits since 83a5dbd3, all written between 2026-09-23 and 2026-09-24.
They changed 244 files, adding 34,451 lines and deleting 17,745. The Python share is 99
files, +11,171/−5,909.

| module | at 83a5dbd3 | now | change |
|---|---:|---:|---:|
| `regs/parsing/catalogue.py` (the model) | 2,137 | 3,072 | **+935** |
| `regs/parsing/entry_models.py` | 736 | 155 | −581 (prose model deleted) |
| `regs/parsing/` whole package | 7,399 (28 files) | 6,919 (19) | −480 |
| `regs/parsing/prompts/` | 572 | 660 | +88 |
| `atlas/reach/` (placement) | 1,701 (10) | 2,485 (12) | **+784** |
| `deliver/bundle/` (incl. schema.sql) | 1,609 (6) | 2,601 (8) | **+992** |
| `tools/export_ui_rules.py` | 782 | 1,612 | **+830** |
| `tools/build_section_data.py` | 1,155 | 1,140 | −15 |
| `regs/table/` | 588 | 644 | +56 |
| `curation-review/` | 7,788 (32) | 8,477 (36) | +689 |
| tests (`pipeline/tests` + added_streams) | 16,512 (79) | 20,226 (88) | **+3,714** |
| test functions | 1,067 | 1,284 | +217 |
| `app/packages/core/src` | 2,108 | 1,350 | −758 (regulations removed) |

**Model size now.** `catalogue.py` holds 27 pydantic classes and 12 enums (29 classes at
83a5dbd3, 39 now). It has 21 `@model_validator`s containing about 88 separate refusal
checks, plus 444 lines of ingest checks in `validate_catalogue.py`. There are 14 rule types,
6 families, 6 licensing kinds, 16 gear slots, 17 registered conduct acts and 6 exemptable
zone defaults.

**How much is prose.** 1,676 of the 3,072 lines in `catalogue.py` are code. The rest is 721
comment lines and 377 docstring lines, most of them the history of the fields that were
removed. `bundle/schema.sql` is 42 KB and mostly comments. `export_ui_rules.py` has 1,393
lines of code, and much of it builds the `guide`, which re-explains the model in prose.

**Does it earn its keep?** Mostly yes for the model itself. The validators are why a string of
real inversions can no longer be written: "barbless not required", "bait ban = allow nothing",
a steelhead-stamp waiver cancelling the provincial stamp, and the 605 closures that read as
catch-and-release. Those were real defects, and the tests pin them (1,841 tests pass). The
costs:

- **One change touches six places.** A field change now means editing the model, the parse
  prompt, the ingest checks, the bundle schema and loader, the export guide, and the
  curation-review editor, and then running a migration over the corpus. That is the
  "circles" feeling. Twelve commits in about 30 hours each reshaped the model and re-migrated
  1,510 entries.
- **Over-built or orphaned:**
  - `tools/build_section_data.py` (1,140 lines) writes `sections.json`, and `regs/table/`
    (644 lines) reads it. **No app, bundle or export reads either.** They fed the settling
    layer that was deleted on 2026-09-22 (759bdebb), yet they are still maintained and
    "promoted as reviewed" in commit messages.
  - `rule_section.jsonl` in the reach run is **16.4 GB** (one JSON line per rule × section,
    about 130 M lines). The bundle interns the same data into 2,307 sets. It is the largest
    artifact in the repo and nothing downstream needs it in that form.
  - Licensing has six kinds for 110 records. Designation (74) and requirement (23) carry the
    weight. `exemption` and `alternative` have one record each, and Bennett Lake's printed
    reciprocity ("B.C. and Yukon licences are valid") is stored as an advisory, not as an
    `alternative`, so even the kind that exists is not used where the book calls for it.
- **Duplicated explanation.** Each rule's meaning is described in the catalogue docstrings,
  the export `guide`, `schema.sql` comments, `pipeline/docs/18-…`, and the parse prompt. They
  already drift: the schema says a rule is "one of 15" types; the model has 14.
- **Stale docs.** `pipeline/docs/NEXT.md` still says 555 rules are `tributaries_pending` and
  24 exemptions are prose-only. Both are fixed: 0 are pending and the exemptions are
  stamped. `AGENTS.md` says 153 slow tests; there are 169.

---

## 3. Are the provincial and regional regulations right?

**How this was checked.** There is **no automated check against the book today.**
`quota_print.py` read each region's printed quota box out of the PDF and matched every line
to a rule (382 of 386 agreed on 2026-09-17). It was deleted with the settling layer in
759bdebb. That commit said the export would carry the last result labelled "NOT
RECOMPUTED". It does not: the export contains no `checked_against_the_book` field at all. So
for this audit I rendered the provincial pages (printed pp. 8–10) and each chapter's
Regional Regulations page, and compared every quota line and every general regulation by hand
with every `zp:` and `z*:` rule.

| chapter | printed p. (PDF) | rules | flagged | unbound |
|---|---|---:|---:|---:|
| provincial | 6–10 (8–12) | 69 + 21 licensing | 11 (+3 licensing) | 1 |
| R1 Vancouver Island | 13 (15) | 28 | 2 | 0 |
| R2 Lower Mainland | 21 (23) | 21 | 4 | 2 |
| R3 Thompson-Nicola | 28 (30) | 22 | 2 | 0 |
| R4 Kootenay | 34 (36) | 26 | 3 (+1 licensing) | 1 |
| R5 Cariboo | 42 (48) | 23 | 4 | 3 |
| R6 Skeena | 49 (55) | 36 | 1 | **6** |
| R7A Omineca | 58 (64) | 27 | 1 | 0 |
| R7B Peace | 64 (70) | 42 | 7 | 3 |
| R8 Okanagan | 68 (74) | 23 | 2 | 0 |
| **total** | | **317** | **37** | **16** |

**The numbers agree.** Every printed quota number, size limit, season and release rule in
the 10 chapters has a matching rule, and none of the numbers disagree. That covers R1's and
Haida Gwaii's tables, R2, R3, R4, R5, R6, 7A, 7B, R8, and the annual Shuswap and Kootenay
quotas. The problem is placement, and a few missing or mis-scoped rules.

**Disagreements with the book:**

1. **R2, printed p. 21 — "No Fishing: in any lake in the UBC Malcolm Knapp Research Forest
   near Maple Ridge"** is missing from the catalogue. It is in no entry's text and no rule.
   (`areas.json` has a polygon for the forest, but only as a permit-access area.)
2. **R2, p. 21 — "Bass: 20 (excluding Mill Lake)"** has a rule, but it is unbound
   (`locators_unresolved`: no mechanism subtracts one lake from a region). **No Region 2 water
   carries a bass quota.**
3. **R5, p. 42 — all three white sturgeon lines** (closed upstream of Williams Lake River;
   catch and release downstream; closed to all fishing Sept 15–July 15 downstream) are
   unbound. The same is true of **provincial p. 6, white sturgeon licence "catch-and-release
   only fishery"**. The reason is structural: a zone entry has no `matched` water, and these
   extents are `upstream_of` / `downstream_of` / `between` on a Fraser cut-point with no
   `item_id`. The resolver has no water to walk, so it returns `no_sections_for_items`.
4. **R6, p. 49 — the Skeena/Nass winter–spring stream closure** (Jan 1–June 15; the Skeena
   mainstem above Cedarvale Jan 1–May 31; the Nass mainstem exempt) is unbound for the same
   reason: 3 rules. **Every Skeena and Nass tributary reads open in winter.**
5. **R6, p. 49 — the Iskut closure above Forrest Kerr Canyon (Apr 1–June 30)** is unbound for
   the same reason. **"Fraser River watershed in Region 6, Apr 1–June 30"** and **"lake trout
   from the Fraser watershed, Sept 15–Nov 30"** are unbound (`unknown`). The extent is
   `whole` on the Fraser item intersected with Region 6, and Region 6 contains no Fraser
   mainstem, so the seed is empty before the tributary walk starts.
6. **R6, p. 49 — the steelhead stream closure May 15–June 15** is stored with
   `exempts: {default_id: steelhead_stream_closure}`, which points at the rule itself. The
   bundle drops self-lifts by design. The printed exemption for the mainstems of the Skeena,
   Nass, Iskut, Stikine and Taku is therefore lost, and **those mainstems read closed to
   steelhead** where the book exempts them. The rule's own `review_reason` says so.
7. **R7B, p. 64 — "Kokanee: 10 (none from streams, except Peace River)"**: the stream half is
   unbound, so **no Zone B stream carries the kokanee stream ban.**
8. **R1, p. 13 — "Bait ban: applies to all streams of Region 1, all year"** is bound to
   `area:region:1` streams. That area contains all 25,772 Haida Gwaii sections (MUs
   6-12/6-13). So Haida Gwaii streams carry an **all-year** bait ban, while the book prints a
   separate Haida Gwaii line, "Nov 1–Apr 30". I read the separate line as meaning the R1
   line excludes Haida Gwaii, which makes this a probable over-restriction. It is an
   interpretation, so it needs your call.
9. **Provincial p. 8 — "Do not keep angled fish alive in a livewell … on stringers … never
   use live fish as bait … High-grading is illegal"** is not in the catalogue.
   `zp:conduct.r4` quotes only the first sentence of that bullet.
10. **R4, p. 34 invasive-species notice** is unbound (a region-minus-listed-waters carve-out).
    It changes no answer, because the four species quota rules say the same thing.

**Minor, no wrong answer:**

- `z6:tagging_program` holds the same advisory twice (r1 = r2).
- `z8:crayfish_traps_turtles.r2` "Crayfish trapping may be used" restates a provincial grant
  that the R8 page does not print.
- **Zone `source_pages` are wrong on 7 of 9 chapters.** z1 says 12 (printed 13), z2 22 (21),
  z4 33 (34), z5 46 (42), z6 52 (49), z7a 54 (58), z7b 62 (64). Only z3 and z8 are right.
  Water entries use PDF page numbers; `zp:` uses printed page numbers.
- R2's list of tidal boundaries on 17 rivers (p. 21) is not stored anywhere. I did not
  check whether the atlas cuts at those boundaries.
- Label bug: `zp:spear_fishing.r1` ("Only non-game fish may be speared") is labelled "No
  fishing by spear fishing". The label drops the species, which reads as a total spear ban.

**Flagged zone rules: 37.** They fall into four groups. Area carve-outs with no mechanism: 3.
Named-water rules recorded as bound, with a curator note: about 12. Provincial rules marked
standing or recovered: 11. Interpretation notes: Haida Gwaii Dolly Varden, the steelhead
self-lift, the Peace/Liard bull trout split. None blocks the numbers above except the
unbound ones.

**The 16 unbound zone rules, and why each one is unbound:**

| rules | reason |
|---|---|
| z5 white_sturgeon r1–r3, z6 skeena_nass_winter_closure r1–r3, z6 iskut_fraser_closure r1, zp white_sturgeon_licence r2 | directional extent on a zone entry with no `item_id`: nothing to walk (`no_sections_for_items`). **A mechanical fix: add the item.** |
| z6 iskut_fraser_closure r2, z6 trout_char_quota r8 | `whole` Fraser ∩ Region 6 is empty before the walk (`unknown`). Needs the walk to start from the tributaries, or a watershed area |
| z2 species_quotas r1, z4 invasive_species_notice r1, z7b species_quotas r5 | area minus a named water (`locators_unresolved`). Needs a subtraction mechanism |
| z2 rubble_creek r1, z7b thin_ice r1–r2 | hazards on places with no polygon (`no_registry`) |

---

## 4. Do the water regulations need a reparse?

**Measurements (1,393 water entries, one per synopsis row; 1,393 of 1,393 rows covered):**

- **Rules per entry:** 3,031 rules, 2.18 per entry. Distribution: 1 rule 692 entries,
  2 rules 231, 3 rules 235, 4 rules 116, 5+ rules 112. 7 entries hold licensing only.
- **Where they came from:** 1,363 (98%) were parsed by the LLM on 2026-09-09/10 with the
  prompt as it stood then. 30 were hand-authored on 2026-09-23: batch 1 of the 09-10 run,
  which was never parsed (Quinn, St. Mary, Skookumchuck, Kilbella, Slocan, Salmo, Sand Creek,
  and 23 Region 5 waters). **No entry was produced by the current prompt.** The prompt was
  rewritten on 09-22, 09-23 and 09-24 and has not been run since. The corpus reached today's
  shape through about 30 code migrations, not by re-parsing.
- **Flagged rules (`review_reason`):** 278 rules in 172 entries.

  | reason | rules |
  |---|---:|
  | no registry match (the entry has no water) | 81 |
  | needs a cut-point | 61 |
  | sign / marker locator ("between boundary signs") | 31 |
  | needs a polygon or area ("on parts", "within 100 m of the outlet") | 26 |
  | tributary carve-out | 12 |
  | other (redirect rows, word-number sub-limits, window-vs-band notes, …) | 67 |

  Nearly all of these are *placement* problems, not wrong readings.
- **Unbound water rules: 234.** `no_extents` 149 (the place is printed but not drawable).
  `no_sections_for_items` 75 plus `no_registry` 6: the 33 entries with an empty `matched`
  hold 81 rules. `unknown` 2 (Brunette r1, Pitt r1). `area_scope` 1 (Wood River: `within`
  with no area). `empty_after_scope` 1 (Peace r6).
- **Rules with `extent_text` but no extents:** 155. A further 67 have both.
  `unresolved_locators` is set on 41.
- **Entries the migrations touched.** The ingest ledger was re-seeded on 2026-09-23 at
  4965df95, recording 1,510 entries. **94 entries have been edited since** (71 water, 23
  zone), all by 9ba92c01. Relative to the 09-10 parse, 74 water entries have added, removed
  or re-quoted rules (mostly the 169 licensing rules moved to `licensing`, Haida Gwaii, and
  the Classified Waters). 16 have re-bound extents. About 15 have field fixes: Wahleach
  take, Fulton type, Rancheria tributaries, Pine River `ALL_FIN_FISH`. Every entry was
  reshaped by the gear, lengths, when and licensing migrations.
- **Known parse defects (scanned across the whole corpus):**
  - **6 lost sub-limits.** The phrase "only one over 50 cm" (written as a word, not a digit)
    could not pass the numbers-must-be-in-the-sentence check, so it was dropped:
    **Okanagan Lake rainbow trout** ("quota = 2 (only one over 50 cm)"), and lake trout
    possession at Cunningham, Indata, Nakinilerak, Tchentlo and Tsayta. All six are flagged.
    The defect is in the validator, not the model.
  - **One fact, two spellings.** "No trout over 50 cm" is written as top-level `take: 0`
    plus a `take: 0` band on 48 rules, and as no `take` plus a `take: 0` band on 50 rules.
    Osoyoos and Skaha print the same sentence and are stored in the two different shapes. A
    consumer that reads `take: 0` literally tells anglers on 48 waters to release every trout.
  - Numbers not in their own sentence: 1. Atnarko r5 carries a 25 cm band, but its quote
    stops before "25 cm"; the words are in its sibling r6. Verbatims that are not substrings:
    0, because it is enforced on load.
  - Garbled source text: e.g. Nahatlatch r2, "except as noted upstream of )". This is an
    extraction defect, not a parse defect.
- **Sample against the printed rows.** I compared 64 entries (207 rules) with their printed
  text: 40 drawn at random (5 per region) plus 24 of the more complex entries (≥ 4 rules).
  - **Wrong: 1.** Okanagan Lake's missing sub-limit, already flagged.
  - **Inconsistent: 1.** The Osoyoos/Skaha shape above.
  - **Ambiguous: 1.** Beaver Lake, "engine power 7.5 kW, no towing on parts": was "on parts"
    meant for both clauses? It is not clear from the text.
  - **Right: the rest,** including dates, species, bands, exemptions, tributaries and
    cut-points on Morice, Clearwater, the Upper Arrow drawdown, Jordan, White and Bennett.

  **Estimated reading-error rate: under 1% of rules, about 1–2% of entries, and the known
  cases are already flagged.**

**Recommendation: no full reparse. Fix a named handful by hand, and run a 2-batch pilot if
you want evidence about the current prompt.**

Why:

1. **The parse is not the problem.** The measured reading errors are about 7 rules corpus-wide,
   all flagged. The real defects are placement (234 unbound water rules, the zone items in
   §3, the tributary walks in §5), and re-parsing changes none of them. The parser would
   face the same missing cut-points and produce the same `extent_text`.
2. **Nobody has measured the current prompt.** A full reparse would be its first real run,
   across 1,393 rows. A 2-batch pilot (60 rows) diffed against the curated entries would
   tell you whether it is better or worse than what you have. Do that before spending 47
   batches.
3. **There is no command for a full reparse.** `parse` skips every row that already has an
   entry. `repass` needs review findings, and `reviews/` is empty. A full run would need a
   hand-built `batch_exporter --force` + `dispatch --force` + `ingest`.
4. **Cost.** A full reparse is 1,393 rows ÷ 30 = **47 parse batches**, plus 47 review batches
   if reviewed. The stale 09-10 prompts were about 120 KB per batch. The pilot is **2 parse
   batches**. The 6 sub-limit rows need **0**: fix them in curation-review, or allow word
   numbers in the validator and reparse them as **1 batch**.

**What a reparse would lose.** Ingest keeps an entry only if it has been edited since the
ledger seed. The seed was taken *after* most hand work, so most hand work counts as
"ingest's own write" and **would be overwritten without `--replace-edited` being needed**:

- **27 of the 30 hand-authored entries** (only 3 are protected).
- **62 of the 74 entries whose rules were restructured by hand** since 09-10. Most are
  Classified-Water licensing migrations that were independently reviewed against the book:
  the Elk, Michel, Wigwam, Skeena and Babine families, Morice, Kispiox, Dean and
  Chilko/Chilcotin.
- **3 of the 16 extent re-bindings.**
- **Curator-only fields that the parser never writes, and are unprotected:**
  - `within_area` on the Fraser upstream of Mission, Pitt River, and Skeena/Kispiox.
  - Cross-entry `exempts` on Dutch Creek, Little Slocan tributaries, and the Upper Arrow
    drawdown.
  - Protected (edited since the seed): the Atnarko (`item_ids` + `tributary_excludes`),
    Squamish tributaries, Slocan, Pend d'Oreille tributaries, Klinaklini, and West Road
    tributaries.
- **Protected:** all 94 entries edited by 9ba92c01 (the part-water fixes), because the
  ledger was deliberately not re-seeded after that commit.

If you do run the pilot, it is human-only and costs credits. Run it into a scratch work dir
so nothing curated is touched. I have not run these commands, so check the flags first:

```bash
W=data/generated/regs/parse-pilot
.venv/bin/python -m pipeline.regs.parsing.batch_exporter --registry data/generated/atlas/full/registry.json --force --out-dir $W
.venv/bin/python -m pipeline.regs.parsing.dispatch --work-dir $W --force --only 0,1 --model sonnet
.venv/bin/python -m pipeline.regs.parsing.ingest_catalogue --batch $W/batches/batch_00{0,1}.json --response $W/responses/batch_00{0,1}.json --dry-run
```

---

## 5. What's broken or unfinished (deduplicated, most important first)

**Known wrong answers in the data**

1. **Zone closures that bind nowhere** (§3, items 3–5, 7). Missing from every water: the
   Skeena/Nass winter closure, the Iskut and Fraser-in-R6 spring closures, all Region 5
   white sturgeon rules, the Region 2 bass quota, and the Zone B kokanee stream ban. **Eight
   of these are one-line fixes: add the Fraser, Skeena, Nass or Iskut `item_id` to the
   extent.**
2. **Malcolm Knapp Research Forest lakes: closed in the book, absent from the data.**
3. **Steelhead self-lift, Region 6.** Skeena, Nass, Iskut, Stikine and Taku mainstems read
   closed to steelhead May 15–June 15. The book exempts them.
4. **The reservoir tributary walks swallow their neighbours.** Lower Arrow Lake's tributaries
   (50,722 sections) contain *every* section of Upper Arrow's (45,369), Lake Revelstoke's
   (35,768) and Kinbasket's (27,618) tributaries. The walk climbs through each reservoir and
   up the whole Columbia. **Kinbasket's row says "Does not include Columbia River upstream of
   Kinbasket Reservoir".** That exclusion is stored only as an advisory, so bull-trout catch
   and release lands on 129 of the 149 Columbia River sections, all of the Kicking Horse and
   Blaeberry, and Columbia Lake. All four rows say the same thing (bull trout catch and
   release), which is why no answer conflicts. The placement is still wrong, and it will
   conflict the day those rows differ. `revelstoke_lake_s_tributaries` and
   `lake_revelstoke_s_tributaries` are the same row twice.
5. **6 lost "only one over" sub-limits**, including Okanagan Lake (§4).
6. **"No trout over N" is stored two ways** (48 vs 50 rules). Pick one and migrate.
7. **Haida Gwaii streams carry R1's all-year bait ban** over their own Nov 1–Apr 30 line
   (§3 item 8). Needs your ruling.
8. **Receiving river bound as a tributary: still there, and small.** Chehalis River r2 ("No
   Fishing downstream of the logging bridge, May 1–31") lands on 2 Harrison River sections.
9. Provincial livewell/stringer/high-grading rule missing (§3 item 9).

**Unplaced rules needing curation (cut-points or polygons)**

- **149 water rules have `no_extents`.** They need cut-points ("between boundary signs…") or
  polygons ("on parts", "within 100 m of the outlet", Shuswap maps A/B/C, Salmon Arm Bay,
  Nation Arm, Osoyoos/Skaha/Okanagan buoyed zones).
- **33 water entries have no matched water (81 rules).** Basalt, Gatcho, Naglico, Pettry and
  Squirrel lakes (in both R5 and R6), Secret, Frog, Hidden, Square, Redfern, the Alouette
  River, the Arrow Lakes, the CVWMA waters, Liard watershed, Chilkoot Trail and others. The
  CVWMA permit requirement is unplaced for the same reason, so it reads "check".
  `reparse_candidates` finds **0** that a reparse could fix. Each needs an item attached by
  hand.
- **Diagnostics.** 63 rules bind through an ambiguous cut (the builder silently picks one of
  two measures). 118 rules have unclassified straddling pieces. Both are listed in
  `rule_diagnostic.jsonl` and nowhere else.
- **Parked in `NEXT.md` §0a:** Fraser side channels (Jesperson's, Herrling, Seabird)
  receiving their parent reach's rules.

**Decisions waiting on you**

1. Reparse or not (§4). My recommendation is a pilot first.
2. A mechanism for "region minus one water" (R2 bass minus Mill Lake, Zone B kokanee minus
   the Peace, R4 invasive notice). It does not exist.
3. What "X Lake's tributaries" means for a chain of reservoirs: should the walk stop at the
   next lake upstream? Also, Kinbasket's printed exclusion needs to become a
   `tributary_excludes`.
4. Haida Gwaii bait-ban scope (§3 item 8).
5. One spelling for partial-release size limits (§4).
6. Whether to keep the orphaned `sections.json` builder and `regs/table` (§2), and whether
   the 16 GB `rule_section.jsonl` should become something smaller.
7. The steelhead definitional size (a steelhead is by definition longer than 50 cm). It is
   recorded in the model but deliberately not applied, because no per-water "steelhead
   present" fact exists.

**Atlas items**

- **Split-lake leftovers.** Williston, Kootenay and Shannon each keep 1 parent section that
  carries a ruleset of area and provincial rules. The export tells consumers to ignore it.
  A consumer that does not read the guide will show it.
- **The Lower Arrow and other reservoir tributary walks** (item 4 above).
- **Fraser-watershed-in-Region-6 rules unbound** (§3 item 5): 2 rules.
- **Ambiguous cuts:** 63 rules.

**The review/repass loop**

- `reviews/` is empty. **The review loop has never been run on the corpus as it stands.**
- The work dir (`data/generated/regs/parse/`) still holds the 09-10 export: 3 batches of 81
  rows, batch 1 never parsed. `run_parse.sh status` says "Next: review", but that would
  review those 3 stale batches, not your entries.
- The reviewer reads a batch plus its raw LLM *response*, never the curated entry. **There is
  no way to machine-review the existing 1,393 entries without re-parsing them.** The only
  review of the current corpus is human (curation-review) and the ad-hoc agent reviews
  recorded in commit messages.
- The ledger (1,510 digests, seeded 09-23) protects 94 entries. See §4 for what it does not
  protect.

**Tests**

- The default run is **1,672 passed, 0 failed, 0 skipped, 169 deselected.** I ran it
  unfiltered, not through rtk.
- The 169 `slow` tests also **all pass**. I ran them with `-m slow`: 169 passed in 3m46s. What
  they are:
  - 153 extraction tests on the PDF (`test_extract_synopsis.py`: row integrity, region
    metadata, first and last rows, no false positives on 48 non-table pages).
  - 7 tributary-walk guards on the real graph: Kootenay doesn't gain Moyie/Yahk, McLennan
    doesn't absorb the Fraser, Koocanusa doesn't swallow the Kootenay, and others.
  - 3 Haida Gwaii quota-area tests.
  - 3 management-unit ↔ region tests.
  - 3 DFO tests: historical snapshots, match report, R6 cascade.

  They need the full build on disk, so CI never runs them.
- **No test checks the rules against the printed book** (§3).

**Things in the export a consumer would trip on**

- **`period: "daily"` ships on 1,549 non-retention rules**, and `obligation: "must"` on all
  3,348. These are defaults leaking into `fields`.
- **92 unbound rules still ship an `extents` of `whole`.** They are the rules on entries with
  no matched water. Only `provenance.uncertain` says they bind nothing.
- **Identical labels for different rules.** Bennett Lake's daily and possession sub-limits
  share a label ("Lake trout (no more than 1, none between 60 cm and 90 cm)") with no
  "possession". Kootenay River r2 and r3 share one.
- **20 labels contain raw `**` markdown.** They are advisories whose label is the verbatim;
  462 labels in total equal the verbatim.
- **The `take: 0` double spelling** (§4).
- **The split-lake parent sections** (above).
- `revelstoke_lake_s_tributaries`, `cedar_lake` ("See Sumallo River") and similar redirect
  rows are advisories. Nothing links them to the row they point at.

---

## 6. What's solid

- **The model refuses whole classes of error.** A rule cannot be stored with its polarity
  inverted, a ban cannot be written as an empty list, and a lift that is scoped to nothing is
  refused. A number or date that is not in its sentence is refused, and so is a verbatim that
  is not in the printed passage. 1,510 entries load through it cleanly, and 1,841 tests pass,
  including the 169 slow ones.
- **The printed numbers are right.** Every regional and provincial quota, size and season
  line matches the book (§3). The sampled water parses are about 99% right, and the known
  misses are flagged.
- **Nothing is silently widened.** Every unbound rule has a typed reason (250 of 250).
  Part-of-a-water rules no longer cover the whole water: 2 flagged `whole` bindings remain,
  and both are intentional. Sections outside B.C. carry no rules.
- **Licensing** was independently checked against the book (169 records at migration), and
  it is placed through the same reach builder as the rules.
- **The provenance chain holds.** Entry facts (name, pages, symbols, matched) come from the
  extraction batch, never from the model. Label text is generated from the fields, never
  authored.
- **Determinism.** The reach digest (`6b9a67c035c27be9`) and the bundle are keyed to one
  build, and the bundle and export say which.

What is **not** solid: placement coverage (§5), the absence of any live check against the
book, and a parse prompt that has not been run since it was rewritten.

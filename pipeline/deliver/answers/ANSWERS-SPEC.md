# ui-rules-answers.json: format `answers/2`

The answers file is the third file beside the UI export pair (`ui-rules-export.json` + `ui-rules-guide.json`). The export
says what the book says; the answers file says what it comes to, for every part of every named water in the export and,
since 2.4, every part of the unnamed water, on every day, for every fish and origin and every angler profile. The app
decides nothing: it looks answers up (AGENTS 56). This page is the contract; the field-by-field dictionary ships inside
the file as `spec` (`encode.SPEC`) and the frame models are `pipeline/deliver/answers/model/`. The history of each
change is `git log -- pipeline/deliver/answers/ANSWERS-SPEC.md`; the old per-round narrative was cut in T1081.

- **Built by** `python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE [--verdicts FILE]
  [--workers N]` (the verdicts default to `verdicts.sqlite` beside the bundle); in a release it is the
  `answers` step of `python -m pipeline.deliver all`. The build refuses to write a file that does not decode back to what
  it built, validates every value against its answers/2 model first, and is deterministic (any worker count, same bytes).
- **Reference decoder** `pipeline/deliver/answers/encode.py` `decode(wire, data)`; one tap is `encode.tap(...)`.
- **One source per fact.** Every rule state and every loser's `reason` and `by`: the verdicts stage's stored answer
  (`verdicts.sqlite`, the reader run ONCE per rule key, reading, fish and origin); nothing in the answers calls the reader.
  Requirements in force: `read.requirements_in_force`. Status: `reading.closed` / `key_meta.own` of the verdicts (the status
  index is the same projection). The parts: the bundle's `part` table. The calendar: `pipeline.deliver.calendar`. The decided
  answer is `rows` alone.
- **No fallbacks.** Anything that cannot be answered stops the build with an `AnswersError` naming it: an export pair from
  another bundle or rule set; a part whose sections disagree on the steelhead rules, the water kind or tidal water; a part
  with no rule set that is not wholly outside B.C.; a reader answer naming a rule outside the set; a loser without a
  reason or `by`; a fish the card asks about that the ladder does not answer; a licence key no section has. States that ARE
  answers are written out: `no_rule`, `by_origin`, a `null` part key for a part outside B.C., a `null` decided answer.
- **Sections and versions today:** `ladder` 1, `rows` 34, `gear` 12, `licence` 7, `display` 20, `trout` 5. A reader
  that does not know a section's version refuses the file; a shape change bumps the section, in `answers.py` and the app's
  reader together. Additive fields leave the version where the app does not yet read them.
- **Generated models:** `python -m pipeline.deliver.answers.model` writes `app/packages/core/src/answers.generated.{json,ts}`;
  `--check` fails CI on drift.

## What changed from answers/1, and the minor versions

Nobody consumed answers/1, so there is no migration. answers/2: the decided answer is `rows` only (no `answer` section);
`no_limit` (was `nolimit`); a part with no run measure has `km: null` (was 1e9); no entry heading is `place: null`; the
ladder holds every fish the verdicts asked; `schemas` holds every frame's JSON Schema (strict). Minor versions keep the
format string `answers/2`: 2.1 moments and `display.closing`; 2.4 the unnamed water (top-level `places` and `unnamed`; an
unnamed part takes the key of its content); later changes are section versions.

## Behaviours a reader of the file must know

- **Moments (2.1).** A rule held on some weekdays or hours DECIDES at those moments (AGENTS 61). A key whose day reads
  differently by moment has slot 10 of its `keys` row set (an index into `segment_moments`); its segments then REPEAT a start
  day, once per moment group, and `segment_moments[...][s]` names each segment's moment in the top-level `moments` table:
  `{weekdays: [names, Monday first], hours: null | {start, end, in}}`. The tap (section 2) picks the segment holding the
  date's weekday; of an hours pair, `in: false` is the answer at every hour but the window, `in: true` inside it. A night or
  hours closure is not in force outside its window; a day is never closed by its night. Model: `schemas["top.moments"]`.
- **`display.closing`** holds every full closure that speaks, unlifted, with the game fish each closes; status is `closed`
  exactly when every game fish is under one. `closers` (closed frames only) is the page's choice of the ones that close the
  water (`display.closers_of`). Side-channel-only parts say where the channels join ("5 side channels, joining 1.1-5.5 km
  from the mouth").
- **Tidal water is a documented state** (ruling 2026-10-06). On a part key with `tidal` 1 the `display` frame is
  `{"status": "tidal", "tidal": true, "note", "see", "licence"}` (`common.TIDAL_STATE`), the `gear` frame is `TIDAL_STATE`
  itself, and the `licence` frame's `holds.tidal` is `TIDAL_STATE` with every profile `{documents: [], none_needed: false,
  requirements: [], tidal: true}`; never "no licence needed". All year, one scope (`common.TIDAL_SCOPE`).
- **Feb 29.** A range printed to Feb 28 runs through Feb 29 (`catalogue.range_days`, the one place dates become days).
- **The year strip** (rows 30): every row's `year` is run-length `[[status, days]]` over the 365 book days, made by `year.py`
  with the page's rule (a row's fish answer by their leading origin; a day takes the most permissive status of its
  non-hours segments). On the wire it is stored ONCE per distinct strip (`rows.strips`; the rows frame's fifth slot names
  each row's strip) because per-row copies broke the release size gate; decoding puts `year` back.
- **Documents** (`stamps.py`): a group tree's kind and a released not-kept line carry `stamps` [{doc, act, name, when}] for the
  angler the Keep tab answers for; `Row.stamps` is the documents a row's fish need by the row's answer; where steelhead are
  closed at the same key and segment the steelhead stamp leaves every profile's `documents` (`stamps.drop_closed_steelhead_stamp`).
  The water file's static `export.pdf_pages` is the printed-page map (printed = PDF - 2 to p.40, PDF - 6 after).
- **`licence.alien_closures`** (additive; `licence` stays 7 until the app reads it): on the frame of a part whose rule set holds an
  `angler_closure` naming the non-resident alien that the ladder leaves standing, `{rule, of, says}` with the whole sentence
  composed here (`licence.alien_line`); the app draws it and composes nothing. Wire: `frames` rows [holds, documents,
  under_16?, alien_closures?] into the interned `alien_closures` table (store `licence_alien`).
- **`gear.bait[].allowances`** (additive; `gear` stays 12): each distinct allowance of an allowed-by-circumstance bait once per
  part, deduped by its words, `by` every clause allowing it.
- **The `trout` section** (built with every answers file; the store requires it, `store.build.ADDED_SECTIONS`): per part key
  and segment a `TroutFrame` {table, dates, covers} or null, composed once per key from the post-pass `rows`, the `display`
  frame and the `licence` answer (`Section.compose_key`). Wire (`encode.TroutCodec`): `frames`, `tables`, `dates`, `sheets`;
  the store adds `cell.trout` and tables `trout_frame`, `trout_table`, `trout_dates`, `trout_sheet`. Contract:
  `handoff/PLAN/design/trout-table-contract.md`. Code: `pipeline/deliver/answers/trout/`.
- **Judgment calls** are `DECISIONS` in `gear.py`, `licence.py`, `display/decisions.py` and `rows/decisions.py` (each with what,
  why, effect, how to undo), except V1-V7, D12, F5b-F5d, T127 and T128, which are in section 9 below; the rulings they implement are `pipeline/docs/RULINGS.md`.

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

Per (key, segment): `[spp, {fish: [hatchery, wild]}, [row], steelhead_line, [strip]]` — the fifth slot (rows 30) names
each row's year strip in `strips` (every distinct strip held once; the stored row has no `year`, decoding puts it back).

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

Per (key, segment): `{status, closing, closers?}` — base | own | closed, the verdicts' code at the
segment's moment; `closing` the full closures that speak, unlifted, with the game fish each closes
(version 3, gap G1); `closers` (version 18, closed frames only) the ones that close the water, page v36
closersAt's choice of `closing`. Static: `rules`
(per export rule: kind, closure, bands, plain sentence) and `waters` (per named water: per export
part its label, runs, place, hint, km, closed_all_year, paper_licence; the picker; unresolved
licensing records).

**Adding a section** takes an `answers.Section` (scope + prepare, or derive_from + derive, optional
static), an `encode.CODECS` entry and its text in `encode.SPEC` and this file; `spec_gaps` refuses a

## 6. Field dictionary

Shipped inside the file as `spec` (`encode.SPEC`); every key of the file and of each section is described there, or the
build stops. Frame shapes: the models in `model/` (`frames.py`, `rows.py`, `gear.py`, `licence.py`, `display.py`, `trout.py`,
`top.py`).

## 9. Decisions kept here (not in a code `DECISIONS` table)

Copied from the old page (T1081 review S3): the undo steps of these live nowhere else. The other judgment calls are the
`DECISIONS` tables in the code (G, L, D1-D6, R, F1-F5a, F6-F17); the rulings they implement are `pipeline/docs/RULINGS.md`.

**Rows fixes** (`rows.DECISIONS` F1-F10, handoff ROWS-REVIEW; where the page contradicts the book
the card follows the book and the port names the page bug, `PAGE_BUGS` in `pipeline/tests/answers_v35_port.py`):
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
- T128 (owner 2026-10-22: "Up to 2 may be hatchery brook, brown or cutthroat trout" read as 2 more on top of the quota) A
  KIND LINE NAMES ITS TOTAL: "Of your 2 trout and char a day in streams, up to 2 may be hatchery brook, brown or cutthroat
  trout". `tree.total_named` picks the tier — the nearest total, stream (lake) share or water's own rule above the kind (a
  count or size tier is part of one) — and `tree.total_words` says it: its number, the group's fish (`written_name` of the
  group's pool), "a day", and where it holds only where limited ("in streams" / "in this stream" for a share, "under this
  river’s rule" for an own tier, "in streams" for a zone total printed for one kind of water). "only N" where the kind is
  held below that total, else "up to N". A kind that is the ONLY kind of that tier at its number names nothing (the header
  or the tier it is folded into says the whole of it). The `sentence` is the same line with the detail and notes; a kind
  naming nothing keeps "Of the 2 a day: …". THE TOTAL IS NAMED ONCE IN FULL — on the header ("4 a day per angler across
  Region 2, lakes included") and the tier lines; a kind line names it SHORT ("your 4 trout and char a day"), never the
  header's words. The card gate proves the named number (a total/share/own tier the kind sits in). Supersedes ROUND UR7 2
  ("(part of the 2 from streams)"). Review: an OWN tier holding only some of the group's fish names no fish
  (`tree.total_fish`): "Of the 3 a day under this lake’s rule, only 1 may be Dolly Varden/bull trout" (Kamloops Lake).
- T127 A POOL THE READER DISPLACED FOR A FISH IS NO CROSS-REFERENCE: `rows.counts_toward` (the pool governs the fish, or
  holds it as role `contains`) decides `build_model`'s xref facts, `group_of`'s kinds and `total_of` — Perry Creek's
  brook trout no longer "count toward" the St. Mary's 1 on its tributaries (displaced, own_row); Perry Creek's own "brook
  trout 20" row is then held by nothing and is a one-kind group of 20 (T80 3) · undo: `A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE`.

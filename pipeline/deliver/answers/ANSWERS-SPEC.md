# ui-rules-answers.json: format `answers/2`

The answers file is the third file beside the UI export pair (`pipeline/docs/06-ui-data-contract.md`). The export says
what the book says; the answers file says what it comes to, for every part of every named water and of the unnamed
water, on every day, for every fish, origin and angler profile. The app decides nothing: it looks answers up (AGENTS
56). This page is the contract. The field dictionary ships inside the file as `spec` (`encode.SPEC`: every key is
described there, or the build stops); the frame models are `pipeline/deliver/answers/model/`.

- **Built by** `python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE [--verdicts FILE]
  [--workers N]` (in a release, the `answers` step of `python -m pipeline.deliver all`). It validates every value,
  refuses to write a file that does not decode back to what it built, and is deterministic at any worker count.
- **Reference decoder** `encode.decode(wire, data)`; one tap is `encode.tap(...)`.
- **One source per fact.** Every rule state and every loser's `reason` and `by` is the verdicts' stored answer; nothing
  here calls the reader. Requirements in force: `read.requirements_in_force`. Status: the verdicts' `reading.closed` /
  `key_meta.own` (the status index is the same projection). Parts: the bundle's `part` table. Days:
  `pipeline.deliver.calendar`. The decided answer is `rows` alone.
- **No fallbacks.** What cannot be answered stops the build with an `AnswersError` naming it (an export pair from another
  bundle or rule set; a part whose sections disagree on the steelhead rules, the water kind or tidal water; a part with
  no rule set that is not wholly outside B.C.; a reader answer naming a rule outside the set; a loser without a reason
  or `by`; a fish the card asks about that the ladder does not answer; a licence key no section has). States that ARE
  answers are written out: `no_rule`, `by_origin`, a `null` part key outside B.C., a `null` decided answer.
- **Section versions** are `answers.SECTIONS`; the app's copy is `app/packages/core/src/regs/versions.ts`
  `KNOWN_VERSIONS`, held equal by `pipeline/tests/test_answers_versions.py`. A reader that does not know a section's
  version refuses the file; a shape change bumps the section in both together; an additive field the app does not yet
  read leaves it.
- **Generated models:** `python -m pipeline.deliver.answers.model` writes `app/packages/core/src/answers.generated.{json,ts}`
  (`--check` fails CI on drift). Minor versions keep `answers/2` (2.1 moments, 2.4 the unnamed water).

## Behaviours a reader of the file must know

- **Moments.** A rule held on some weekdays or hours decides at those moments (AGENTS 61). A key whose day reads
  differently by moment has slot 10 of its `keys` row set; its segments repeat a start day once per moment group, each
  named in the top-level `moments` table (`{weekdays, hours: null | {start, end, in}}`, `schemas["top.moments"]`). Of an
  hours pair, `in: false` is the answer outside the window. A day is never closed by its night.
- **`display.closing`** holds every full closure that speaks, unlifted, with the game fish each closes; the status is
  `closed` exactly when every game fish is under one. `closers` (closed frames only) is the ones that close the water
  (`display.closers_of`).
- **Tidal water is a documented state** (D12, §9): `display` is `{"status": "tidal", "tidal": true, "note", "see",
  "licence"}` (`common.TIDAL_STATE`), `gear` is `TIDAL_STATE` itself, and every licence profile is `{documents: [],
  none_needed: false, requirements: [], tidal: true}`, never "no licence needed".
- **The year strip**: every row's `year` is run-length `[[status, days]]` over the 365 book days (`year.py`), stored
  once per distinct strip (`rows.strips`; the rows frame's fifth slot names it); decoding puts `year` back.
- **Documents** (`stamps.py`): a kind and a released not-kept line carry `stamps` [{doc, act, name, when}];
  `Row.stamps` is the documents a row's fish need; where steelhead are closed at the same key and segment the steelhead
  stamp leaves every profile (`stamps.drop_closed_steelhead_stamp`). The water file's static `export.pdf_pages` is the
  printed-page map.
- **Additive fields:** `licence.alien_closures` (`{rule, of, says}` for a standing `angler_closure` naming the
  non-resident alien, sentence composed here) and `gear.bait[].allowances` (each distinct allowance once, `by` its clauses).
- **The `trout` section** (required by the store): per part key and segment a `TroutFrame` {table, dates, covers} or
  null, composed once per key from the post-pass `rows`, the `display` frame and the `licence` answer
  (`Section.compose_key`). Contract: `handoff/PLAN/design/trout-table-contract.md`. Code: `pipeline/deliver/answers/trout/`.
- **Judgment calls** are the `DECISIONS` tables in `gear.py`, `licence.py`, `display/decisions.py` and
  `rows/decisions.py` (what, why, effect, how to undo), except those in §9; the rulings they implement are
  `pipeline/docs/RULINGS.md`.

## 1. Pairing

| check | how |
|---|---|
| same bundle | `about.bundle` equals `about.bundle` in both export files (`reach_digest`, `section_handles`, …) |
| same rules | `about.export.rule_ids_sha256` is the first 16 hex digits of SHA-256(`rule_ids` joined by `"\n"`) |
| same format | `about.format == "answers/2"` |

Every rule ref is an index into the export's `rules` / `rule_ids`; every licensing ref into its `licensing` /
`licensing_ids`. A reader shows nothing from a set that fails a check.

## 2. How a tap resolves (lookups only)

1. **Part -> key.** `k = parts[item][i]`, `i` the part's index in the export's `waters[item].parts`. `null`: the part has
   no rule set and is wholly outside B.C. (`water.outside_bc` is the COUNT of the water's sections outside B.C., "N
   sections outside B.C.", never a flag). A section of no named water: `k` = its akey - 1 (the regs store's
   `section_akey`), named by `places[p].names[keys[k][7]]`, `p` its place (`section_place`).
2. **Date -> segment.** Day `d` (§3); `starts = segments[keys[k][9]]`; `s` = the last index with `starts[s] <= d`. Where
   `keys[k][10]` is set and that start repeats, the segments sharing it are the day's moments: take the one whose
   `moments[segment_moments[keys[k][10]][s]]` holds the date's weekday (and, of an hours pair, `in` false for the day).
3. **Section -> frame.** `frame = sections[name].frames[sections[name].at[k][s]]`, decoded as the section says.
4. **Fish, origin, profile.** Ladder: the fish (index into `fish`) and origin (none | hatchery | wild); rows: the fish
   code and hatchery | wild; licence: the profile index (mixed radix over `profile_dims`).

Every section uses the same `k` and `s`.

## 3. Calendar and segments

Days are 1..366 on the leap calendar (Feb 29 = 60, Mar 1 = 61 every year; `calendar.day_of`). Segments are the union of
every section's cuts (a rule's, a lift's, a requirement's or a designation's `when`); day 1 always starts one, and a
reading across New Year is two segments sharing one value.

## 4. The keys

`keys` rows: `[ruleset, licensing_set, steelhead_water, steelhead, steelhead_rules, province_except, home_region,
kind, tidal, segments, moments]` (`common.part_keys`); from `unnamed.first` on, one row per unnamed content no named
part has (`common.unnamed_keys`). Each section reads what it needs (`Section.scope`): `ladder`, `gear`, `display` the
rule key (ruleset, steelhead_water, steelhead_rules); `rows` the rule key, the water's kind and the steelhead presence;
`licence` (licensing_set, province_except, tidal, kind, the rule set where a designation can sleep).

## 5. Sections

- **`ladder`**: per fish and origin, every rule the traced reader returned, for every fish the verdicts asked (every
  game fish, ST everywhere, answered as RB where no steelhead rule applies; crayfish, chinook or a protected fish where
  a member rule names it).
- **`rows`**, today's card: per (key, segment) `[spp, {fish: [hatchery, wild]}, [row], steelhead_line, [strip]]`.
  `decided` per fish and origin (`status` keep | no_limit | release | closed, `win`, `daily` = the number after the
  winner's clauses, `narrow`, `lines`, `roles` [rule, role, by], `lift_notes`); `rows`, one per shared limit in card
  order (`kind`, `pool` or `win`, `members`, `daily`, `groups`, `scope` {of: water | area | region | bc, entry, share,
  apart}, `conds`, `items`: ONE item per kind with its keep bands `[[from_cm, to_cm|null, number]]`, `against` where a
  fish with its own row counts toward a total, and `real_daily`); `steelhead_line` possible_with_rules |
  known_with_rules | known_no_rules | null. Every daily limit, a water's own too, counts fish kept elsewhere today.
- **`gear`**: the resolved gear answer (`gear.resolve`); a clause is `[rule, clause index]`; `overruled` the losers.
- **`licence`**: `[holds, documents]`: the requirements in force (held, displaced, wrong_water, waived, …) and the 60
  per-profile answers. Static: `profiles`, `profile_dims`.
- **`display`**: `{status, closing, closers?}` (base | own | closed at the segment's moment); static `rules` (kind,
  closure, bands, plain sentence) and `waters` (per part its label, runs, place, hint, km, closed_all_year,
  paper_licence; the picker).
- **`trout`**: see Behaviours. **Adding a section** takes an `answers.Section`, an `encode.CODECS` entry and its text in `encode.SPEC` and here;
`spec_gaps` refuses an undescribed section or key (`test_answers_v0.py`).

## 9. Decisions kept here (not in a code `DECISIONS` table)

**Rows fixes** (`rows.DECISIONS` F1-F10; where the reference page contradicts the book the card follows the book, the
page bug named in `PAGE_BUGS`, `pipeline/tests/answers_v35_port.py`). F1 one item per kind (Anderson R.); F1b a cap
missing only the steelhead-filtered rainbow is general with `except: [RB]`; F2 origin2 also when the other origin's keep
RANGE differs (Chilliwack Lake); F3 lines about other fish compared by SET (Kitimat, Feb); F4 a fish lifted out of the
narrowing clause counts toward the outer total (`against`; Vedder); F5a slots are 0 bands (Teslin); F5b the zone's
outer size cap caps a water's own bands (Thompson below Kamloops L.); F5c a rainbow's range ends at 50 cm on a
steelhead water; F5d every shared cap (Region 8 "2 over 30"); F6 `possession_cap`, never a yearly limit, decided once by
`display.kind_of`; F7 `scope.apart` only when no outer total is left; F8 `of: area` (Haida Gwaii, Bowron Lake Park,
Liard); F9 a water's own limit still counts fish kept elsewhere; F10 no presence data for any fish but steelhead.
Undo: revert the rows-fixes commit (`rows.py`).

**Keying and reader:**
- V1 Gear and conduct dimensions carry clause condition, means and acts, not the water kind; a water row's barbless
  rule still replaces the zone's. Undo: add `water` to the gear condition in `catalogue.CatalogueRule.dimension`.
- V2 `handling_rule` is keyed by its acts: two duties never replace each other. Undo: return `t.value`.
- V3 G5 order: wrong water is read after the also_printed fold, a superior authority's displacement after wrong water;
  `displaced` lists the superior keys sorted. Undo: reorder in `read.requirements_in_force`.
- V4 The licence frame's `holds` leaves out displaced requirements (they are under `displaced`). Undo: list `rows` in
  the licence section's wire.
- V5 The part key carries `kind` and `tidal` (the licence key is a function of it). V6 Display status per segment is
  the verdicts' projection; a tidal part reads its key's `tidal`. Undo: drop them; map tidal keys.
- D12 Tidal water is a documented state (Behaviours); its ladder lists each fish with only the tidal row's note; its rows
  hold no rule; the export's `waters[].tidal.guide` starts with the same words. Undo: drop the `TIDAL_SCOPE` branches in
  display, gear and licence.
- V7 A range printed to Feb 28 runs through Feb 29 (`catalogue.range_days`, the one place dates become days). Undo:
  drop its `(2, 28)` branch.
- T128 A kind line names its total: "Of your 2 trout and char a day in streams, up to 2 may be hatchery brook, brown or
  cutthroat trout" (`tree.total_named` picks the tier, `tree.total_words` says it; "only N" where the kind is held below
  that total, else "up to N"). The total is named once in full, on the header and the tier lines; a kind line names it
  short. A kind that is the only kind of its tier at its number names nothing.
- T127 A pool the reader displaced for a fish is no cross-reference (`rows.counts_toward`): Perry Creek's brook trout no
  longer count toward the St. Mary's 1. Undo: `A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE`.

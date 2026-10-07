# ui-rules-answers.json — format `answers/1` (v1)

The answers file is a third file beside the UI export pair (`ui-rules-export.json` +
`ui-rules-guide.json`). The export says what the book says; the answers file says what it comes to,
for every part of every named water in the export, on every day, for every fish and origin, and for
every angler profile. The export format is unchanged. The answers file replaces nothing until it is
verified 1-to-1 against the consumer page (COMPARISON.md).

- **Built by** `python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE
  [--workers N]` (about 90 s on 4 processes, 9.3 GB peak). The build refuses to write a file that
  does not decode back to what it built. It is deterministic (4 and 2 workers give the same bytes).
- **Reference decoder** `pipeline/deliver/answers/encode.py` `decode(wire, data)`; one tap is
  `encode.tap(...)`.
- **One source per fact.** Every rule state, every loser's `reason` and `by`: `read.effective_rules_bound(
  trace=True, origin=…)`. Every requirement in force: `read.requirements_in_force` (with the G5 steps:
  wrong water, superior authority). Every status: `status_index.set_profile`. The calendar, the keys
  and the segments: `common.py`, the one keying module. The answers layer arranges what these say;
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

## 1. Pairing

| check | how |
|---|---|
| same bundle | `about.bundle` equals `about.bundle` in both export files (`reach_digest`, `section_handles`, …) |
| same rules | `about.export.rule_ids_sha256` is the first 16 hex digits of SHA-256(`rule_ids` joined by `"\n"`) |
| same format | `about.format == "answers/1"` |

Every rule ref is an index into the export's `rules` / `rule_ids`; every licensing ref an index into
its `licensing` / `licensing_ids`.

## 2. How a tap resolves (lookups only)

1. **Part → key.** `k = parts[item][i]`, `i` the part's index in the export's `waters[item].parts`.
   `null`: the part has no rule set — it is wholly outside B.C. (`water.outside_bc`).
2. **Date → segment.** Day number `d` (§3); `starts = segments[keys[k][9]]`; `s` = the last index with
   `starts[s] <= d`.
3. **Section → frame.** `frame = sections[name].frames[sections[name].at[k][s]]`, decoded as the
   section says (§6).
4. **Fish, origin, profile.** In the ladder and answer frames, the fish (index into `fish`) and the
   origin (none | hatchery | wild); in the rows frame, the fish code and hatchery | wild; in the
   licence frame, the profile index (mixed radix over `profile_dims`).

Every section uses the same `k` and `s`: they join with no remapping.

## 3. Calendar, segments, wrap-around, Feb 29

- **Day numbers** 1..366 on the leap calendar: Jan 1 = 1, Feb 29 = 60, Mar 1 = 61 in every year,
  Dec 31 = 366 (`status_index.day_of`). In a year without Feb 29, day 60 is never asked.
- **Segments** are the union of every section's cuts: a member rule's or a lift's `when` (ladder,
  answer, rows, gear, display), a requirement's or a designation's `when` (licence). Day 1 always
  starts a segment; a reading across New Year is two segments sharing one value.
- **Feb 29 (ruling 2026-10-06).** A range printed to Feb 28 runs through Feb 29: the book is written
  for years without one and means "through the end of February" (`catalogue.range_days`, the one
  place dates become days). The Nicola below the lake is catch and release on Feb 29, as on Feb 28;
  the Feb-29-only segment v0 reported is gone.

## 4. The keys

`keys` rows: `[ruleset, licensing_set, steelhead_water, steelhead, steelhead_rules, province_except,
home_region, kind, tidal, segments]` — the reference harness's `partKey` plus the water's kind and
tidal water (`common.part_keys`). Each section reads what it needs of it (`Section.scope`):

| section | scope (what its answers depend on) |
|---|---|
| ladder, answer, gear, display | the rule key: (ruleset, steelhead_water, steelhead_rules) |
| rows | the rule key, the water's kind, the steelhead presence |
| licence | (licensing_set, province_except, tidal, kind, the rule set where a designation can sleep) |

## 5. Sections

### `ladder` (Stage 4) and `answer` (Stage 5.2 steps 1-4) — unchanged from v0

Per fish and origin, every rule the traced reader returns, and the decided winner (`status`, the
winning pool's own `daily`, `winner`). Field dictionary: the file's `spec`. The number an angler may
keep is `rows`' decided `daily` (after the winner's clauses).

### `rows` (Stages 5.1-5.8): today's card

Per (key, segment): `[spp, {fish: [hatchery, wild]}, [row], steelhead_line]`.

- `spp`: the fish the card asks about (5.1), in the page's order.
- `decided` (per fish and origin, or null): `status` (keep | nolimit | release | closed), `win`,
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

Per (key, segment): `{status}` — base | own | closed, the status index's code. Static: `rules`
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
| Tidal banner + text | export `waters[].tidal.guide`; key `tidal`; the `display`, `gear` and `licence` frames of a tidal key (FIX D12) |
| Closed banner: closures in the chain, ranks | `ladder` (closures that speak) + `display.rules[].plain`; merging back-to-back runs is page |
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
| "At certain times" | `ladder` `beside` + export `when.hours` / `weekdays` |
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
- V6 Display status per segment is the status index's set_profile code (base/own/closed); a tidal
  part reads its key's `tidal`, not a TIDAL code · one definition of closed · undo: map tidal keys.
- D12 (FIX round, user ruling 2026-10-06) TIDAL WATER IS A DOCUMENTED STATE. On a part key with
  `tidal` 1 the `display` frame is `{"status": "tidal", "tidal": true, "note", "see", "licence"}`
  (`common.TIDAL_STATE`: "Tidal water: a different regulation system … see the federal (DFO) tidal
  waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence
  is required"), the `gear` frame is `TIDAL_STATE` itself (never "angling: not allowed here"), and
  the `licence` frame's `holds.tidal` is `TIDAL_STATE` with every profile `{documents: [],
  none_needed: false, requirements: [], tidal: true}` (never "no licence needed"). Its ladder, answer
  and rows hold no rule by definition. All year, one scope (`common.TIDAL_SCOPE`). The export's
  `waters[].tidal.guide` starts with the same words · undo: drop the `TIDAL_SCOPE` branches in
  display/gear/licence.
- V7 Feb 29 (coordinator ruling): a range to Feb 28 runs through Feb 29 · `catalogue.range_days` ·
  3 part-days of the v0 file change (the Nicola below the lake and two lakes), status index day 60
  only · undo: drop the `(2, 28)` branch in range_days.

# ui-rules-answers.json — format `answers/0` (v0)

The answers file is a third file beside the UI export pair (`ui-rules-export.json` +
`ui-rules-guide.json`). The export says what the book says. The answers file says what it comes to,
for every part of every named water in the export, on every day, for every fish and origin. The
export is not changed. The answers file replaces nothing until it is verified 1-to-1 against the
consumer page.

- **Built by** `python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE`
  (about 75 s on 16 processes). The build refuses to write a file that does not decode back to what
  it built.
- **Reference decoder** `pipeline/deliver/answers/encode.py` `decode(wire, data)`. One tap is
  `encode.tap(...)`.
- **One source per fact.** Every state, every loser's `reason` and `by`, comes from
  `read.effective_rules_bound(trace=True, origin=…)`, the reference reader. The answers layer only
  arranges what the reader says, and adds one derivation: the decided answer (§4). It never decides
  a rule's state itself.
- **No fallbacks.** Anything that cannot be answered stops the build with an `AnswersError` naming
  it. This covers:
  - an export pair from another bundle;
  - an export pair whose rule sets or rules differ from the bundle's;
  - a part whose sections disagree on whether the steelhead rules apply;
  - a part with no rule set that is not wholly outside B.C.;
  - a reader answer that names a rule outside the set;
  - a loser without a reason or a `by`.

  States that *are* answers are written out explicitly: `no_rule`, `by_origin`, and a `null` part
  key for a part outside B.C.

## 1. Pairing

| check | how |
|---|---|
| same bundle | `about.bundle` equals `about.bundle` in both export files (`reach_digest`, `section_handles`, …) |
| same rules | `about.export.rule_ids_sha256` is the first 16 hex digits of SHA-256(`rule_ids` joined by `"\n"`) |
| same format | `about.format == "answers/0"` |

Refuse a file that fails any check. Every rule ref in the file is an index into the export's
`rules` / `rule_ids`, in the codec's order (sorted by `entry_id::rule_id`).

## 2. How a tap resolves (lookups only)

1. **Part → key.** Take the water's item id and the part's index `i` in the export's
   `waters[item].parts`. Then `k = parts[item][i]`. A `null` means the part has no rule set: it is
   wholly outside B.C. (`water.outside_bc`). Show "outside B.C.", not "no rules".
2. **Date → segment.** Work out the day number `d` (§3). Then `starts = segments[keys[k][7]]` and
   `s` = the last index with `starts[s] <= d`.
3. **Section → frame.** For each section, `frame = sections[name].frames[sections[name].at[k][s]]`.
4. **Fish → origin.** In the frame, find the fish's row (`row[0]` indexes `fish`). Then pick the
   origin:
   - a row of 2 holds one value for all three origins;
   - a row of 4 holds `[fish, none, hatchery, wild]`.

   `none` is the reader's answer with the origin unknown. `hatchery` and `wild` are the answers for
   a fish of that origin (the page asks these two).

Every section uses the same `k` and `s`, so sections join with no remapping.

## 3. Calendar, segments, wrap-around, Feb 29

- **Day numbers.** Days are 1..366 on the leap calendar: Jan 1 = 1, Feb 29 = 60, Mar 1 = 61 *in
  every year*, Dec 31 = 366 (`catalogue._day_index`). In a year without Feb 29, day 60 is never
  asked.
- **What a segment is.** A run of days on which every section's inputs read the same.
  - In v0 that means every member rule's `when` and every lift's `when`, read through
    `read.in_force` ("yes" / "no" / "part"). This is the status index's `set_profile` signature.
  - So a segment boundary is a day on which a member rule or a lift comes into or goes out of
    force.
  - When v1 sections arrive, the segments become the union of every section's breakpoints. A
    section's own producer code does not change.
- **Wrap-around.** Day 1 always starts a segment. A reading that runs across New Year (Nov 1 to
  Mar 31) is two segments, the last and the first. They point at the same frame, which is stored
  once.
- **Feb 29.** Feb 29 is answered exactly as the reader answers it. A rule printed "Jan 1–Feb 28"
  does not hold on day 60 (`catalogue._days`).
  - One key of the 2,080 has a Feb-29-only segment because of this: the Nicola River below Nicola
    Lake. Its "Trout release all, Jan 1–Feb 28" and the lift of Region 3's spring stream closure
    both end on Feb 28, so on Feb 29 the zone's closure speaks.
  - The 2025–2027 synopsis contains no Feb 29, so no angler meets it.
  - The consumer page reads Feb 29 as Feb 28. That is a reader question (reported), not something
    the answers layer substitutes.

## 4. The decided answer (`answer`), consumer Stage 5.2 steps 1–4

These come from the `hatchery` and `wild` ladder verdicts only. A rule is **in scope** when all of
these hold:

- it speaks;
- it is `type: retention_limit` or of kind `duty`;
- it is not of kind `while`, `exempt` or `standing`;
- its dimension is not `lift`;
- its `origin` is unset or equals the origin asked.

"Kind" is the page's Stage 2.3 test, ported verbatim as `answers.kind_of`.

1. **Winner.** Take the closure (a gate with `may_target` false) of the lowest rank. Failing that,
   take the release (any other gate) of the lowest rank. Failing that, take the pool with the
   smallest number, where unlimited counts as the largest and lower rank wins between equal
   numbers.
   - Rank is `read.source_of(rule).rank`. A water row reached by the tributary walk ranks 1, as the
     reader's `place` reads it.
   - Remaining ties go to the rule earlier in the export's `rules` order. The page keeps its own
     member order; for the ties the corpus holds, the two orders agree.
2. **Status.**

   | winner | status |
   |---|---|
   | closure | `closed` |
   | release | `release` |
   | unlimited pool | `no_limit` |
   | any other pool | `keep` |
   | none | `no_rule` (the page shows no row) |

3. **Daily.** The winning pool's own `take`. It is `null` for every status other than `keep`.
   **This is the number before the pool's clauses.** The page then narrows it by a sub-limit
   covering every fish of the pool ("keep 2 trout and char a day, hatchery only" inside Region 2's
   4), or by an orphan clause. That is step 5 and arrives with `rows` (v1). Until then, do not show
   `daily` as the number an angler may keep.
4. **Origin not known (`none`).** When the hatchery and wild answers are equal, `none` is that
   answer. When they differ, `none` is `["by_origin", null, null]`: read the two.

Roles (governs / agrees / contains / also / moot / narrows / limit / floor / season / duty /
possession) are v1 (`rows`).

## 5. Which fish

A key's frames list every game fish that any member rule names in `species`, with groups expanded
and limited to the book's list (p.80, `catalogue.BOOK_SPECIES`). They add `ST` where the steelhead
rules apply. This is a superset of the page's Stage 5.1 list (DESIGN D6). The page asks only:

- fish named by applying retention rules of kind gate / pool / subcap / sizecap / size / annual;
- minus `ALL_GAME_FISH` rules;
- minus protected species.

Every fish the page asks about is in the file: 188,259 of 188,259 fish asked in the golden outputs.

## 6. Field dictionary

The same text ships inside the file as `spec`. `encode.spec_gaps` refuses a file with an
undescribed key, a section without a codec or spec text, or a reserved section present.

### Top level

| key | what |
|---|---|
| `about` | `what`, `format` (`answers/0`), `bundle` (the export pair's digests), `export` {`rule_ids_sha256`, `rules`}, `sections` {name: version} present, `reserved` {name: what it will hold}, `counts` |
| `spec` | the field dictionary |
| `fish` | [fish code]: the leaf species codes the frames index |
| `keys` | one row per distinct **part key**: `[ruleset, licensing_set, steelhead_water, steelhead, steelhead_rules, province_except, home_region, segments]` (see the next table) |
| `segments` | [[start day]], interned. The first start is always 1, and each segment runs to the day before the next start |
| `parts` | {item_id: [key index or null]}, aligned with the export's `waters[item].parts` |
| `sections` | {name: section} |

The slots of a `keys` row:

| slot | what |
|---|---|
| `ruleset`, `licensing_set` | the export part's set ids; `licensing_set` may be null |
| `steelhead_water` | 0/1: a rainbow over 50 cm is a steelhead here (`anadromous_rainbow`) |
| `steelhead` | "known", "possible" or null |
| `steelhead_rules` | 0/1: the bundle's fact for the part's sections. The export ships it only as `false` on a known part |
| `province_except` | [kind] |
| `home_region` | [region] |
| `segments` | an index into `segments` |

The part key is the reference harness's `partKey` (`reference/compare.py`), with `steelhead_rules`
read from the bundle. The ladder and answer depend only on (ruleset, steelhead_water,
steelhead_rules), the status index's key. The other fields are there so licence and gear (v1) key
the same way.

### `ladder` (v0): Stage 4

The ladder holds, per fish and origin, every rule `read.effective_rules_bound(trace=True,
origin=…)` returns: the speakers and the losers. Its keys:

| key | what |
|---|---|
| `version` | 0 |
| `at` | `[[frame per segment] per key]` |
| `frames` | `[[common, [[fish, v] or [fish, v_none, v_hatchery, v_wild]]]]`. `common` is the verdict of rules whose entry is the same for every fish and origin of the frame. A fish's verdict for an origin is `common` plus its own (they never share a rule) |
| `verdicts` | `[[speaks, beside, shown, not_yet_mapped, partly, lost]]`; see below |
| `reasons` | `read.LOSS_REASONS` keys: the step that removed a rule. `ladder`, `water_dates`, `zone_release`, `closure`, `water_closure`, `size_release`, `water_release`, `same_row_release`, `moot_size_clause`, `stricter_region`, `same_as_peer`, `lifted` |
| `reason_state` | the state each reason gives, parallel to `reasons`: `lifted`, `displaced` or `moot` |

The six lists of a verdict:

- **`speaks`, `beside`, `shown`, `not_yet_mapped`**: sorted rule refs in that state.
- **`partly`**: `[[rule, [lifting rule]]]` for a rule in one of the four states above that a lift
  holds only in part (an origin, a size, a target, a method, some hours).
- **`lost`**: `[[rule, reason, by]]` for a rule that took part and lost. `by` is the rule that beat
  or lifted it.

Rules that are not about the fish, or not in force, are absent, as in the reader.

### `answer` (v0): Stage 5.2 steps 1–4

| key | what |
|---|---|
| `version` | 0 |
| `at` | `[[frame per segment] per key]`, with the same keys and segments as `ladder` |
| `frames` | `[[[fish, d] or [fish, d_none, d_hatchery, d_wild]]]`: the ladder frame's fish |
| `decided` | `[[status, daily, winner]]`, as in §4 |
| `statuses` | `closed`, `release`, `keep`, `no_limit`, `no_rule`, `by_origin` |

### Reserved for v1: absent from a v0 file, never present empty

| section | will hold | producer |
|---|---|---|
| `rows` | the narrowed daily number and every clause line (5.2 steps 5–11), fish grouped by shared limit with go-backs and cross-references (5.3), the row scope and badge (5.5–5.6), the real daily limit (5.7), keep ranges and band numbers (5.8), roles per rule (5.2 step 3, 6.1) | derived from `ladder` + `answer` |
| `gear` | resolved gear slots per part key × segment (7.1) | agent D's `gear.py` |
| `licence` | documents per part key × segment × angler profile (7.7) | agent D's `licence.py` |
| `display` | derived display facts: `kind`, `bands`, `plain` sentences and tags, part labels / place / hint / order / group and closed-all-year, the part's status per segment and the closures that close it, the fish the card asks about (5.1), the steelhead presence line | agent D's `display.py` |

**Adding a section** takes three things and changes nothing else:

1. an `answers.Section` (`scope`, then `prepare`, or `derive_from` + `derive`);
2. an `encode.CODECS` entry (`encode`, `decode_frame`);
3. its text in `encode.SPEC["sections"]` and in this file.

`test_answers_v0.py` fails when any one of the three is missing. The reference harness's streams
map one-to-one onto the sections:

| harness stream | section |
|---|---|
| `ladder` | `ladder` |
| `answer` | `answer` |
| `row` | `rows` |
| `gear` | `gear` |
| `licence` | `licence` |

So v1 verification extends the same way.

## 7. Coverage: everything the consumer page v35 displays → where it comes from

**Status values.**

| status | meaning |
|---|---|
| **v0** | in this file now |
| **export** | in the export pair already |
| **v1 `x`** | the named reserved section |
| **page** | presentation the page keeps (wording, layout, colour, its own drawings, arithmetic over shipped numbers) |
| **GAP** | needed and carried nowhere |

Section references are to `handoff/consumer-pipeline.md`.

### Status, strips, part picker (Stage 3)

| displayed | field(s) | status |
|---|---|---|
| Part label "From the mouth up to …", "Two stretches", side channels | export `waters[].parts[].runs`, `splits[].name`; place/label composition | export + v1 `display` (label) |
| Entry place heading ("Upstream of Kamloops Lake", "Where Region 3 and Region 5 meet") | export `entries[].scope_note` / `full_name`, `home_region` | export + v1 `display` (place) |
| "What sets it apart" hint, " · plus / without …" tie-breaks | rule refs per part | v1 `display` (hint) |
| Picker: "Closed all year · N stretches", order mouth-upward, groups, "N sections", "outside B.C." | export `sections`, `touches`, `outside_bc`; closed-all-year | export + v1 `display` (closed_all_year, order, group) |
| Default part | from the above | page |
| Status line Open / Closed / "Open, except some parts" | the part's status per segment | v1 `display` (status per segment). In v0 it can be derived from `answer` (every fish `closed`) and `ladder` (`not_yet_mapped` closures in force), but the page's exact `isBroad` predicate is a closure naming ALL_GAME_FISH, so it is not shipped as such |
| "today" / "on <date>" | the page's date | page |
| "Opens <date>", "Closed from <date>" (within 21 days) | `segments` + the per-segment status | v1 `display`; the date lookup is page |
| "Trout closed until …" / "Trout release only until …" | `answer` per fish per segment | v0 |
| Water strip (365 days, legend) | `segments` × per-segment status | v1 `display` (status); drawing is page |
| Tidal water banner + text | export `waters[].tidal.guide` | export |
| Closed banner: the closures in the chain, ranks, "opens again" | `ladder` (`speaks` closures) + `segments`; rule sentence | v0 (which closures speak) + v1 `display` (`plain`); merging back-to-back runs is page |
| "Closed all year" | per-segment status | v1 `display` |
| Per-group strip, runs of keeping ≤ 14 days | `answer` per fish per segment, grouped by row | v0 (per fish) + v1 `rows` (grouping) |
| Run sentences "Release only until Dec 31. Then closed …" | `answer` status per segment | v0; the wording is page |
| Date chips (rule start/end dates) | `segments` starts | v0; the choice of chips is page |

### The ladder (Stage 4) and "How this was decided" (Stage 6)

| displayed | field(s) | status |
|---|---|---|
| Every rule's state: speaks / beside / shown / not yet mapped | `ladder.verdicts` | v0 |
| Lifted (with lifter), displaced (with winner) | `ladder` `lost` [rule, reason, by] | v0 |
| "partly lifted", "Lifted only when fishing for …: '<verbatim>'" | `ladder` `partly` [rule, lifters] + export lifter `exempts` / `verbatim` | v0 + export |
| Roles governs / agrees / contains / also / moot / narrows / limit / floor / season / duty / possession / falls | per rule per fish | v1 `rows` (roles). v0 carries `governs` (the `answer` winner) and `replaced` / `lifted` (the `ladder` losers) |
| Tags ("Daily limit", "Release", "Closed", "Lower daily limit", …) | role → words | page over v1 roles |
| Plain sentence per rule (`sayRule`) | `plain` | v1 `display` |
| Place headings (All of B.C., Region 2, a named area, Downstream waters, this water, Federal law) | export `provenance.rank`; trib = export ruleset `trib` | export |
| "Rules that don't apply here (N)" with "Replaced by this river's rule / the Region 2 rule / …" | `ladder` `lost.by` → the beater's rank | v0 + export |
| "Same as another rule", "Not needed here", "Doesn't matter today" | roles agrees / falls / moot | v1 `rows` |
| Answer box "2 a day here, and they count toward 4 a day in Region 2" | narrowed daily, outer pools | v1 `rows` |
| Row sources sheet (verbatim, `who`, role in words, notes, raw fields) | export rule + v1 roles | export + v1 `rows` |
| Rule sheet (label, level tag, verbatim, notes, id, fields) | export `rules[]` | export |

### Today's card (Stage 5)

| displayed | field(s) | status |
|---|---|---|
| Which fish get a row (5.1) | `ladder` frame fish (a superset) + the page's filter | v0 (superset) + v1 `display` (the exact list) |
| Per fish and origin: status, winner | `answer` | v0 |
| The daily number shown | narrowed number (5.2 step 5) | v1 `rows`. v0's `daily` is the pool's own number (§4) |
| Lines: release under / cap over / subcap "only N of them" / outer total / also / orphan / partly / caution / trout note / annual / duty / record / steel | 5.2 steps 5–11 | v1 `rows` |
| Main answer per fish (hatchery if keepable, else wild) | `answer` hatchery / wild | v0 (a lookup over two values) |
| Rows: keep rows by pool, exc (go-backs), xref, origin / origin2 lines, merged lines, everyone / groups, protected species row, row order | 5.3 | v1 `rows` |
| Row title ("Trout and char", "Trout") | pool's species, group names | export `species` (guide) + v1 `rows` (pool) |
| Row badge (Separate from / All Region 2 streams / Region-wide / This water) | scope | v1 `rows` |
| Row value (N / No limit / ↩ Release / ⊘ Closed / real sum) | `answer` status; real daily | v0 (status) + v1 `rows` (number) |
| "N a day in total, all kinds together" | row daily + members | v1 `rows` |
| Scope sentence + worked examples ("Kept 2 elsewhere today? Keep 3 more here") | scope + numbers | v1 `rows`; the arithmetic is page |
| "Really N a day here." / "Only 1 of the 5 can be char or rainbow trout" | `real_daily` + reason | v1 `rows` |
| "For every kind" ✓ lines, "Also" lines, counted-apart rows | row lines | v1 `rows` |
| Steelhead presence line (3 cases) | export part `steelhead`, `steelhead_rules` + whether a steelhead row exists | v1 `display` (`steelhead_line`) |
| Fish items: keep range, band numbers, keep sentence, cap lines, shared cap, steelhead notes, own-row fish per origin, "Release every one until …", stream share | 5.8 | v1 `rows` (+ `display` bands) |
| Duties list ("10 a year", "Record ones over 50 cm …", "Stop fishing for the day after 2") | annual / record / duty rules speaking | v0 (`ladder`, which rules speak) + export fields; grouping into items is v1 `rows` |
| Simple / release / closed / protected rows, "Which fish (13)" | 5.9 | v1 `rows` |
| "What they look like" fish drawings | the page's own art | page |
| Row's own year strip | `answer` per fish per segment | v0 |
| "See every regulation line ›" | row sources | v1 `rows` |
| Spots box (undrawn-part rules): title by what is in force, `parts.what`, dates, "Where: …", "Other dates (N)" | `ladder` `not_yet_mapped` (in force today) + export `undrawn_part`, `parts`, `when` | v0 + export |
| One side of the channel box | `ladder` `beside` + export `side` | v0 + export |
| "Closed to some anglers" | `ladder` speaks + export `type: angler_closure` | v0 + export |
| "At certain times" (hours, weekdays) | `ladder` `beside` + export `when.hours` / `weekdays` | v0 + export |
| "? Check" box (uncertain placements) | export `provenance.uncertain` / `why` | export |
| "No quota rules reach this part of the water" | every fish `no_rule` | v0 |
| "Anywhere in B.C." standing rules | `ladder` `shown` + export `standing`, `verbatim` | v0 + export |
| Possession footnote | `ladder` (a `per_daily` rule speaks) + export `take` | v0 + export |
| Other fish fold, card order | layout | page |

### Gear & licence (Stage 7)

| displayed | field(s) | status |
|---|---|---|
| Closed water: "No gear may go in the water and no licence is needed …" | per-segment status | v1 `display` |
| Line & hooks, Bait, Ways to fish tiles and details (counts, specs, elements, circumstances, units) | resolved slots | v1 `gear` |
| Boats tile (vessel rules active or timed today) | `ladder` (vessel rules `speaks` / `beside`) + export | v0 + export (ordering and the tile are v1 `gear`) |
| Always tile (conduct tokens in four cards) | `ladder` (conduct rules speaking) + `guide.gear.conduct`; `moment` per act | v0 + export + v1 `display` (moment) |
| Licence: who is fishing (profile), records considered, designations in force, requirements, waivers, exemptions, paths, You need, prices, paper licence, folds, flags | per profile | v1 `licence` (prices: export `licence_terms.fees_cad`) |

### Checks (Stage 8)

| displayed | field(s) | status |
|---|---|---|
| Export cases ✓ / ✕ against the ladder | `guide.cases` vs `ladder` | export + v0 (the shipped states are the reader's, so the check becomes a display test) |

**GAPs (carried by neither file, and not named for v1): none.** Every element is in the export, in
v0, in a named v1 section, or presentation. Two items are worth flagging:

- The status line's open / closed is derivable from v0 today, but its exact predicate (the status
  index's `closed`) is shipped only with v1 `display`.
- v0's `daily` is not the number to display (§4.3).

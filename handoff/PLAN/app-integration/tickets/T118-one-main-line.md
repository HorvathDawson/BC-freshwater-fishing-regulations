# T118 — One main line: spans pick a water's stem by length, continuation by section count

Status: DONE — f214beee, review 79221a05 (lane-T118). Ships with the next deliver; the measured bundle output is unchanged, so a
deliver run is only needed to carry the code, not to change data.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding cross 2
(PLAUSIBLE, partly checked on `bundle.sqlite`).

## Decision being unified
"Which sections are this water's main line?"

## Where it is decided today
- `pipeline/deliver/bundle/spans.py:85` `main_stem`: by length.
- `pipeline/deliver/bundle/continuation.py:109` `_main`: by section count; `topology.mouth_downs` reuses it.

How they differ: Ecstall River has 37 of its 42 sections `off_stem`, so the count-based line differs from the
length-based stem. Its exit still reads Skeena, so today the impact is on evidence only, but any water where the two
lines end at different mouths would get a different exit and continuation.

## Work
- `main_line(water)` in a shared `pipeline/deliver/bundle/lines.py`; spans, continuation and `topology.mouth_downs`
  call it. Do it with or after T108 (same modules).
- Pin: a grep test that no other module in `deliver/bundle/` picks a stem.

## Acceptance
Proof: the `water_exit` and `water_continue` diffs before and after; expected empty or each change named.

Rebuild: deliver. Depends: T108 (same files). Size: S. Owner: no.

## As built (2026-10-23, lane-T118)

- `pipeline/deliver/bundle/lines.py` is the one decision: `piece(node)` (a STREAM graph node is a piece of its blue
  line; lakes, wetlands and river polygons are on no line), `main_line(pieces)` (the line carrying the most LENGTH,
  equal to the millimetre is a tie, ties to the lowest line key — exactly spans' old `main_stem`), `main_line_of(nodes,
  ids)`, `handle_lengths(nodes, ids, sids)` and `line_ends(sids, line, length)` → (main line, lowest section, highest
  section).
- Readers: `spans.compute` (`main_line_of`; `spans.main_stem` removed), `continuation.links` / `candidates` (tops and
  mouths via `line_ends`; `continuation._main`, the count, removed) and `topology.mouth_downs` (mouth via `line_ends`).
  `continuation.Flow` carries `length` (`graph_flow` fills it from the graph); `candidates` refuses a Flow without it
  (`ContinuationError`) rather than finding no main lines and so no links, silently.
- Tests: `pipeline/tests/test_lines.py` (whole values; the Ecstall shape run through all three readers; the grep pin),
  fixtures given piece lengths in `test_water_continue.py` / `test_part_geo.py`, the old `main_stem` test moved out of
  `test_part_runs.py`, `test_one_data_flow.py` counts `main_line_of(`. The grep pin is in `smoke.txt` (guard).
- Mutations run by hand, each failing the named test(s): `main_line` weighing every piece 1 (count) →
  `test_the_main_line_is_the_one_carrying_the_most_length_not_the_most_sections`,
  `test_line_ends_are_the_lowest_and_highest_measure_on_the_main_line`,
  `test_spans_continuation_and_the_exit_walk_read_one_line`; tie to the highest key → `test_a_tie_goes_to_the_lowest_line_key`;
  `piece` counting lakes → `test_only_a_stream_node_is_a_piece_of_a_line`, `test_line_ends_…`; `mouth_downs` by count
  → `test_spans_continuation_and_the_exit_walk_read_one_line`, `test_part_geo::test_a_water_exits_where_its_mouth_flows_never_into_its_own_side_channel`;
  `links` by count → `test_spans_continuation_and_the_exit_walk_read_one_line`; `def main_stem` re-added to spans.py
  or `def _main` to continuation.py → `test_no_other_bundle_module_picks_a_main_line`.

### Proof (acceptance)

Scratch bundle built with this code from the live atlas (`atlas/full`) and the batch-4 reach run (`reaches/full`,
report 2026-10-23 06:30), via `scripts/heavy.sh`, diffed table by table against the live bundle of 06:37 (release
r-941dcc149b17 was packaged from it): **every table identical** — `water_exit` 17,730 rows, `water_continue` 17 rows,
`section_span` (with end tokens), `part_geo`, `part_mark`, the panels, all of it. Zero changed row classes.

Why nothing moves although the decision did: on the live bundle **96 of 11,963 stream waters** had a count line
different from their length stem, and in all 96 its lowest piece (the mouth) differs too — Ecstall River (count line
360225600, 6 of 42 sections; length stem 360876170, 5), Chowade River (89 sections), Sutlahine River (71), Schaft Creek
(41), Tsaytis River (40), Park Creek, Thibert Creek, Wilson Slough, Herrling Island Side Channel, Jeperson Side Channel
and 86 more. Spans already measured along the length stem, so `section_span` cannot move; none of the 96 is in a
`water_continue` link either way; and `mouth_downs` is evidence only (`exit_receivers` drops a receiver that flows back
into the water), which on these 96 gave the same exits from either mouth. The unification removes the disagreement
before it reaches a water where the two mouths lead to different receivers.

## Left / follow-ups
- The 96 waters above pair FWA lines in two blk ranges (e.g. 3602xxxxx with 3608xxxxx, 3590xxxxx with 3595xxxxx,
  a `-2200000xxx` added stream with its neighbour): a water carrying two near-parallel lines of its own name may be a
  registry or added-streams duplicate. Not investigated here.

## Adversarial review (2026-10-23)

Re-measured on the live atlas (`atlas/full/graph.pkl`) and live `bundle.sqlite` (read-only) by recomputing the
decisions directly, old count rule vs `lines.line_ends`, for all 11,963 stream waters (no scratch bundle needed: the
review's own code changes are docstrings):

- **One line holds on real data.** For every stream node the section id's line (`continuation.line_of`) equals the
  node's `blk`, and for every water `spans`' stem (`main_line_of` by node) equals `line_ends`' line (by handle): 0
  mismatches. So spans, continuation and the exit walk name the same line on the shipped data, not only in the fixture.
- **96 confirmed** — line, mouth and top all differ on the same 96 waters (Herrling Island / Jeperson side channels,
  Edmond, Clachnacudainn, Beaverfoot, Kearsley, Fowler, Malloch, Chamberlain, Cheewhat, Chuyazega, Geesdale ...).
- **`water_continue`: identical** — `continuation.links` 36 candidates before and after, same 36.
- **The "same exits" claim was imprecise.** `mouth_downs` itself changes on **13** waters, e.g. McKusky Creek (old mouth
  reached Crooked Lake, now Horsefly River), Adsit Creek (Adsit Lake → Canyon Creek), Kakila Creek (Little Lillooet
  Lake → Lillooet Lake), McGillivray Slough (Vedder River → Miller Slough), Washwash River (Owikeno Lake → Tzeo River),
  Ksemamaith Creek (nothing → Nass River), Snehumption Creek (Similkameen River → nothing), Eagle Trib.5. That moves
  the `exit_receivers` candidate set on **3** waters — McKusky Creek drops Crooked Lake, Adsit Creek drops Adsit Lake,
  Eagle Trib.5 drops Eagle Trib.5-3 — but `topology.write` takes the nearest candidate and the live rows already name
  the one left (Horsefly River, Canyon Creek, Eagle Creek). So **`water_exit` rows are unchanged**; zero shipped row
  classes move, as the builder said. The new mouths read as the truer ones (McKusky runs to the Horsefly).
- Mutations re-run: `main_line` by count fails the three `test_lines.py` tests named above plus
  `test_part_geo::test_a_water_exits_where_its_mouth_flows_never_into_its_own_side_channel`; `line_ends` dropping the
  `s in length` guard fails `test_line_ends_are_the_lowest_and_highest_measure_on_the_main_line`.
- Fixed: two stale docstrings still described the old rule (`topology.exit_receivers` "a creek's main line (a count)",
  `spans.compute` "the one place each water's stem is decided"). No behaviour change.
- Not a defect, noted: the grep pin is name-based (`def _main` / `main_stem` / a per-line `Counter`); an inline
  re-implementation under another name would pass it. The three-reader behaviour test is what catches that.

Ships with: the next deliver (no data change).

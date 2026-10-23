# T108 — One "goes on as": continuation and spans disagree on whether water A continues as water B

Status: DONE — fb1b12a5 (code + tests on lane-T108), adversarial review 93d39ad3 (window pins, spec wording). Ships with the next deliver: bundle `section_span` and
`water_continue` change only when `python -m pipeline.deliver` rebuilds the bundle.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding cross 1
(CONFIRMED: `water_continue` joined to the span lower-end tokens in `bundle.sqlite`).

## Decision being unified
"At its lower end, does water A go on as water B, meet B at a confluence, or end?"

## Where it is decided today
- `pipeline/deliver/bundle/continuation.py:132` `_goes_on`: tolerances 100/200/1000 m.
- `pipeline/deliver/bundle/spans.py:143` `lower_end_into`: tolerances 100/250 m plus a magnitude test.

How they differ: 11 of the 17 `water_continue` pairs say "goes on as" while spans say `mouth`. They include Pass→Screw,
Rochester→Holmes, Jr 4-1-2→Jr 4-1 (which the `spans.py` docstring gives as a *not*-continuing example), Finch
Connector→Logger's Lane and Westway Ditch→Magnolia Trib 1. Jeperson Side Channel continues into itself. The "goes on as"
button and the span end words can contradict on one page.

## Work
- `continuation.goes_on(a) -> YES | CONFLUENCE | NO` with one constant set. `spans.lower_end_into` reads it instead of
  deciding again. No water continues into itself.
- Bundle pin: every `water_continue` pair has a `continues:`/`confluence:` end token, and every such token has a pair.
- Grep pin: no other module in `deliver/bundle/` defines continuation tolerances. Tolerance constants also overlap
  T61 2.10; keep them in one place.

## Acceptance
Proof is a reviewed behaviour change: the 17 `water_continue` pairs and every `span_end` token, before and after, each
change named. The bundle pin passes; the self-continuation is gone. The p15 owner ruling stands (keep the 10 municipal
continuations; reversed continuations refused).

Rebuild: deliver. Depends: none (see T118 for the shared main line). Size: M. Owner: no.

## As built (2026-10-23, lane-T108)

One function decides: `continuation.goes_on(a_ord, a, b_ord, b, same_line, flow) -> YES | CONFLUENCE | NO`
(`pipeline/deliver/bundle/continuation.py`), with the one constant set `STUB_M` 100 / `CONFLUENCE_M` 200 / `OWN_M` 1000.
`spans.lower_end_into`, `spans._strahler`, `_BEGINS_M` (100) and `_MEET_M` (250) are deleted. The span end words are
derived from `goes_on`'s answers by `continuation.lower_ends(found, settled, item_of, rank)` -> `{A's mouth section:
token}`, handed to `spans.compute(ends=…)` / `spans.write(ends=…)`; `compute` swaps a plain `mouth` for that word and
decides nothing. `build.py` now writes `section_span` / `split` after `item_down`, `links` and `settle` (still while
only the graph is loaded, before the geometries), instead of inside `_section_touch`.

Folded into `goes_on` from the old span rule (each pinned): a water never continues into one of its own NAME
(Jeperson); a creek whose mouth piece flows into a LAKE ends there; a LAKE draining into B within `CONFLUENCE_M` is
where B begins (A ends; was CONFLUENCE); two rivers meeting a little below B's top — one within `STUB_M`, A within
`CONFLUENCE_M` (Inklin 154 m + Nakina 65 m -> Taku; Sikanni Chief 130 m + Fontas 53 m -> Fort Nelson) — are a
CONFLUENCE, not a tributary. The magnitude ("pure name change") test is gone: p15 kept the continuations it refused.
Kept for the WORDS only (`lower_ends`): a `confluence:<X>` end names X only when it is another candidate into the same
top piece of comparable rank (RULINGS P10: Strahler within 1, together making B's order), largest magnitude first.

### Behaviour change, measured (live bundle `full+reaches` + atlas `full`, old code vs new, same inputs)
The old-code run reproduced the shipped bundle's `section_span` exactly (0 of 55,588 rows differ).
- `water_continue`: 18 -> 17. Removed: Jeperson Side Channel -> Jeperson Side Channel (self). The other 17 are unchanged
  (incl. the 10 municipal ones and Atnarko -> Bella Coola).
- `section_span`: 11 of 55,588 rows change, all a water's natural lower end:
  - `mouth` -> `continues:<B>` (10): Aliceville Trib.1-1 -> Aliceville Trib.1, Beecher Trib.4 -> Beecher Creek,
    Crayfish Creek Trib.3-1 -> Crayfish Creek Trib.3, Eagle Trib.4-1 -> Eagle Trib.4, Finch Connector (Unnamed) ->
    Logger's Lane Connector, Jr 4-1-2 -> Jr 4-1, Noble Creek Trib.1 Trib.1 -> Noble Creek Trib.1, Pass Creek -> Screw
    Creek, Rochester Creek -> Holmes Creek, Westway Avenue Drainage Ditch -> Magnolia Creek Trib 1.
  - `confluence:Talchako River` -> `continues:Bella Coola River` (1): Atnarko River (the book pairs it, T59; its ▲
    already said so). The Talchako keeps `confluence:Atnarko River`.
  - Unchanged: the 6 that already agreed (Chilliwack, Chilqua, Davidson, Hatzis, Marblehill, Page) and all 13 other
    two-river confluence ends (Sproat/Stamp, Inklin/Nakina, Fontas/Sikanni Chief, Nahlin/Sheslay, Hyde/Partington,
    Puntledge/Tsolum, Talchako).
- Candidate links that ship nowhere (CONFLUENCE, unpaired), for the record: 16 dropped (13 Burnaby Lake creeks into the
  Brunette, Cusson -> Mosquito, Poquette -> Quesnel, Sixteen Mile -> Green: a lake at B's top or the creek ends in a
  lake); 3 added (Inklin -> Taku, Sikanni Chief -> Fort Nelson, Stoney Trib.3-1-1-2-4 -> Stoney Trib.3-1-1-2).
- Proof on a scratch copy of the live bundle with these rows applied: the bundle pin and the 17-row live pin pass; on
  the live (old) bundle the bundle pin fails, as it must until the deliver.

### Pins (whole values; mutation that fails each)
- Bundle pin `test_water_continue.py::test_every_continuation_is_a_continues_end_and_every_continues_end_a_continuation`
  (`needs_bundle`): `{(A, A part, B)}` of `water_continue` == `{(A, part, B)}` of every `continues:` span end; none to
  itself or to a water of its own name.
- Grep pin `test_no_other_bundle_module_decides_continuation`: no `*_M` continuation tolerance or `lower_end_into` /
  `goes_on` / `strahler` outside `continuation.py` (re-adding `_MEET_M = 250.0` to spans.py fails it).
- Synthetic, through the build's own path (`links` -> `settle` -> `lower_ends` -> `spans.compute`) in
  `test_part_labels_continue.py`; `goes_on` / `graph_flow` / `lower_ends` units in `test_water_continue.py`. The
  mutation -> failing-test map is in the `test_part_labels_continue.py` docstring (spans ignoring `ends`; no same-name,
  into-lake, lake-outlet refusal; no stub-confluence branch; no rank test) — each run by hand, each caught.
- Live pins updated: 17 rows (Jeperson gone); export pin Atnarko `continues:gnis:11611`, Talchako
  `confluence:gnis:17209`, Jr 4-1-2 `continues:…-225700`.
- RULINGS P10 text updated to say the decision is `continuation.goes_on` and to name the changed examples.

Rebuild: **a deliver is needed to ship** (`python -m pipeline.deliver`, bundle stage onward). No atlas or reach rerun.

Left / follow-ups:
- T118 (one main line) still open: `goes_on`'s A mouth / B top come from `_main` (section count) while `spans` picks its
  stem by length. `lower_ends` is keyed by A's mouth SECTION, so the `continues:` word lands on the linked section
  whatever stem `spans` picks — the bundle pin holds either way.
- `test_part_labels_continue.py::test_lower_ends_on_the_built_export` and the two `needs_bundle` live pins fail until the
  deliver rebuilds the bundle and the regs export.

Owner questions:
- RULINGS P10 (2026-10-13) said a creek alone at the receiver's top is a "Mouth" unless its magnitude is all of the
  receiver's top (the Jr 4-1-2 example); p15 (2026-10-21 late) kept the municipal continuations that rule refused. Built
  default: p15 wins — `goes_on` decides, Jr 4-1-2 and 9 others now read "where it becomes …" instead of "Mouth", and P10's
  wording says so. Confirm, or say which of the 10 should stay a "Mouth" (then they leave `water_continue` too).

## Adversarial review (2026-10-23)

Verdict: the unification holds. No defect in the shipped behaviour; four of `goes_on`'s windows were unpinned, now
pinned. The ANSWERS-SPEC line was still describing the removed rule; it now names `goes_on`.

- **Data effect re-measured independently.** New code (`links` -> `settle(book_pairs)` -> `lower_ends` ->
  `spans.compute`) on the live bundle tables + atlas `full` graph, compared with the shipped `section_span`: 55,588 rows,
  the same keys, **exactly the 11 lower-end changes the As built section names** (10 `mouth` -> `continues:`, Atnarko
  `confluence:Talchako` -> `continues:Bella Coola`), no other column moves. `settle` gives 17 links (Jeperson gone), and
  the `continues:` sections == the settled links' mouth sections, every one on stem. Before: the shipped bundle has 6
  `continues:` ends against 18 `water_continue` rows, which is the disagreement the ticket set out to remove.
- **Second copies.** grep over `pipeline/` and `app/packages`: no other `lower_end_into`, `_BEGINS_M`/`_MEET_M`, or
  span-side Strahler helper. `atlas/gauges/build_panels.goes_on` is a different question (does a side channel's own
  code go on beside it, T101), not a copy. `topology.mouth_downs` answers "what does the mouth flow into" and reads
  `continuation._main`, so it shares the main-line decision. `section_span.sid` is a primary key, so keying `ends` by
  section cannot leak one water's end onto another's row.
- **Build order.** `_places` / `_place_water` moved after `item_down`, `links`, `settle`, `spans.write`. None of those
  read `place`/`place_water`. `spans.write` reads only `item`/`item_section`, the graph and the atlas files.
- **Mutation sweep** (18 mutations of `goes_on`, `graph_flow`, `ranks`, `lower_ends`, `spans.compute`; harness in the
  review scratchpad). The builder's six claimed catches all hold. Four survived before this review and are now pinned:
  - a lake outlet counted only ABOVE A's mouth (dropping the 200 m below it):
    `test_water_continue.py::test_goes_on_reads_each_window_from_the_one_constant_set` (row 2);
  - the two-rivers-past-the-stub branch taking ANY other inflow as the partner (`bool(others)`): same test (row 6).
    Before this review only the no-partner case was pinned;
  - `graph_flow.into_lake` reading only the `lake_in` edge kind (not a `lake` node):
    `test_the_graph_flow_marks_a_creek_into_a_lake_by_the_lakes_kind_too`;
  - `ranks` dropping the FWA magnitude, so a confluence names the first river by id rather than the largest:
    `test_part_labels_continue.py::test_three_rivers_meeting_name_the_largest_by_the_graphs_magnitude`.
- Checks: `test_water_continue.py` + `test_part_labels_continue.py` 37 passed (3 `needs_bundle` deselected; they wait on
  the deliver, as above); the spans/runs/split-name suites have no failures (their bundle tests error only on the
  missing worktree bundle); `pytest -m smoke` 190 passed.

Ship: unchanged. **A deliver is needed** (bundle stage onward). No atlas or reach rerun.

Left / follow-ups (review):
- The bundle pin compares names with `.lower()` while `goes_on` also folds whitespace, so a pair differing only in
  spacing would slip past the pin, though not past `goes_on`. Harmless today (`goes_on` refuses it first).
- The grep pin catches only the known constant names (`BEGINS|MEET|STUB|CONFLUENCE|OWN`). A new tolerance under
  another name in another bundle module would pass it. The pin's real guard is the bundle pin.

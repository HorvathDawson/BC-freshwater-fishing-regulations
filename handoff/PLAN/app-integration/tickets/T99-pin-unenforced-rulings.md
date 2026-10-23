# T99 — Pin the rulings nothing enforces (RULINGS §16) and fix the stale citation (C-4)

Status: DONE — f8742374 + review f3bc2b4b (lane wf_c3f10dea-7b4-5). RULINGS §16 lists only K8 (T98) and K5
(T100); 18 tests in `pipeline/tests/test_rulings_pinned.py` (needs_bundle, ~35 s), each whole-value and
mutation-pinned; C-4 cites C2. Review replaced three tautologies (Z1 Region 4 lake, Li1, E13 side channels — see
"Review" below) and added Z1's no-carve-out check over every section. No rebuild needed (tests and a read-only
tool only). Left: T8's unnamed-tributary placement (below) and Q-T99a.

## Result (2026-10-22, bundle reach_digest dc21329d925d042d)

**Z1** — `pipeline/tools/base_deltas.py` (`python -m pipeline.tools.base_deltas [--per N]`). Per section, day and
fish the reference reader is asked twice (`effective_rules_bound`, traced): over the BASE alone (rules whose
`source_of` scope is region-wide / province-wide) and over everything bound. Every rule whose state, reason or
winner differs is a difference and must be named by ONE entry: its own row; the override at the bottom of the
`by` chain that removed it; the override that removed its base beater (SP-4); its partial lifter. Also: base rules
bound on a section belong to its regions (or the province) and every region it lies in contributes. Sample: 2
per stratum (water row's own reach, tributary walk, lake, base only) in all 9 regions + every region-line place,
spread by a fixed sid hash; 22 fish x 4 days. Live: 0 unattributable (at 3 per stratum: 14,309 differences, all
attributed to 63 rows; at 10 per stratum: 43,272 / 102 rows). Pins: a Region 4 lake with no row answers exactly
Region 4's table (every day, every fish); Kootenay Lake main body's whole difference set (rainbow 10 lifting
Region 4's "Trout/char: 5" and "1 over 50 cm", the row's lines, `z4:kootenay_lake_boundaries`, the Kootenay
rainbow stamp). Mutations: a base rule dropped from the page -> exactly that rule unattributable; another
region's base rule bound -> named out of place; a region's table missing -> named.

**The ◐ rulings** (each reads the bundle/reader/status index/guide; each mutation applies the curated row's
change to what the bundle holds — a seeded rule table or the binding its extents made):
Z2 Mill Lake (bass 4 displaces Region 2's 20; mutation: the row's quota not bass -> 20 speaks) ·
Z10 Peaceful in / Goose out of the Knapp closure (mutation: Goose bound -> closed) ·
Q20 Adams Lake quota every day; Adams River dated quota on its dates, Region 3 otherwise (mutations: lake quota
dated; river quota undated) · L14 all 9 Stellako sections closed Nov 15-Jun 30 (mutation: an exemption lifting both
spring closures -> to May 14) · L16 Chimney spring closure lifted below Brunson Lake only (mutation: lift dropped) ·
T8 Anzac 500 m spot binds the pieces joining inside its window, none outside, and a tributary walk smaller than the
river's; Mitchell 100 m has none (mutations: a piece joining outside bound; the whole river's walk) · G8 Columbia
r13/r14 exactly Fairmont-Donald, Salmon r1/r2 exactly mouth-Hwy 97 bridge, no note (mutation: towing on the whole
river) · Li1 every rule key with designated and undesignated sections reads one status all year (mutation: an index
closing designated sections) · Li10 West Road mainstem Class II Jul 1, none Dec 1, no glyph (mutation: designation
dropped) · E13 Goat C&R on all 10 mainstem sections, no tributary; Fraser night closure on the Fraser and its three
named side channels, no joining slough (mutations: one section left out; a slough bound) · E14 Hays Creek split
130 m from the mouth, closure above it only (mutation: whole creek) · C3 the guide's `species.trout_note` and
`gotchas.trout_includes_char.note` carry the whole sentence.

**Not seen by Z1** (said, not hidden): the attribution sample is 2 per stratum (the CLI runs any size). A base
rule that SHOULD bind a section and binds neither answer is now caught rule by rule wherever the section's peers
(same region place, MU set, water kind, tidal) carry it (`base_sets` / `peer_problems`, every section); a line
missing from a WHOLE peer group is caught only for Region 4 lakes, against the table as its rules are written. T8's "joining within the spot" is proved for the pieces placed on the river (off-stem sections, by their join
measure); the unnamed tributaries above them are checked only as fewer than the river's walk — the bundle's
`section_down` is sparse and the full flow graph is the atlas (T57's sweep reads it through heavy.sh).

Pre-existing gate failures seen (not T99's files, not touched): `test_no_silent_skips` (test_answers_one_kind_group.py:201,
test_synopsis_period.py:57/62/71, test_tidal_gauges.py:137), `test_dataflow_gates::test_gate_4_one_calendar_spec`
(app dayPicker.ts:44, edge.test.ts:100), `test_one_data_flow::test_the_bundle_reads_no_curated_waters_file`
(bundle/build.py `CURATED.gauges.tidal`).

## Review (2026-10-22, f3bc2b4b)
- Z1 `test_z1_a_region_4_lake_…` compared the reader over the base with the reader over the same base (the lake
  was chosen because every bound rule is base) — it could not fail. Now: the lake's binding == Region 4's lake
  table read from the rules' own extents (z4 + zp region-wide lines admitting a lake; unknown extent shapes refused).
- Z1 no carve-out (new): over all 1,956,154 sections, 1,548 (region place, MU set, kind, tidal) groups each carry
  one base; 0 carved out. Nitinat Lake (the one tidal section) binds no base, its own group.
- Li1 compared index codes of sections sharing a rule key; the index profile is per key (`status_index.compute`),
  so they could never disagree. Now: designated sections' shipped code == `status_by_reader` (rules only), 16
  keys x 12 days; mutation: an index closing designated sections.
- E13: each Fraser side channel the night closure touches must be covered whole (was: its name present).
- Mutations that only re-run set arithmetic in the test (E13 Goat, E14, C3, G8) were left: the main assertion of
  each reads the bundle/guide whole-value, so a changed curated row turns it red after a rebuild.

## Owner questions
- Q-T99a (C3): the guide carries the note "Trout includes char … unless the line excludes them or char have their
  own line about the same thing", and the page names the group "Trout and char"; the page does not print the note
  itself. **Default (built with): no change** — "Trout and char" says it where the rule is shown; if the note should
  appear on the page (e.g. beside a trout-only line, the guide's `note_trout_only`), that is a new ticket.

Source: pipeline/docs/RULINGS.md §16 and memory `base-plus-deltas-invariant` ("section == region base + named
overrides, every difference attributable; this is what stops tail-chasing").
- ⚠ NOT ENFORCED: Z1, the base-plus-deltas attribution test (removed with the table in 2026-09). (K8 is T98.)
- ◐ curated data with no test: Z2 Mill Lake; Z10 Peaceful/Goose; Q20 Adams; L14 Stellako dates; L16 Chimney; T8 spot
  closures; G8 Columbia/Salmon; Li1; Li10; E13 Goat and Q12 side channels; E14; C3. (K5 is T100.)
- C-4: `test_zone_decisions.py::test_steelhead_over_50_are_not_in_the_trout_one_over_50` cites the withdrawn 2026-09-28
  "mentions char" ruling; cite C2.

## Work
Z1: a needs_bundle test over the release: for a stratified sample of sections in every region, the page's rules are
the region base plus the rows bound to it, each difference named by one row (T57 does this for unnamed waters; share
the helper). One whole-value test per ◐ ruling, each mutation-pinned (seeded from an output, never from the input —
memory `checks-seeded-from-input-launder`). Update the "Enforced in" column.

## Acceptance
RULINGS §16 lists only K5 (T100) and K8 (T98); each new test fails on a mutation of the curated row it pins.

Depends: none (shares a helper with T57). Size: M. Owner: no.

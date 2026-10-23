# T111 — Every cut reaches the walk: only the first extent's window seeds tributaries

Status: DONE — a9e37a82 (T111), a2d5d62a (review fix). No binding moved.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding reach 5
(CONFIRMED in code; the live list is from the reach agent).

## Decision being unified
"Where does this rule's reach cut the water, and which cuts bound its tributary walk?"

## Where it is decided today
- `pipeline/regs/reach/build.py:434` `window = window or got["window"]`: only the first extent's window survives.
- `pipeline/atlas/graph/tributaries.py:584-592`: once a window is given, the node `lower_bound` fallback is skipped.
- `pipeline/regs/reach/build.py:793` `_cut_places`: re-derives the cuts and ignores `cut_pick` (the ambiguous-cut
  ruling: the cut nearest the rule's other end).

How they differ: in `alouette_river.r2`, `bull_river.r1` and `atnarko_bella_coola_rivers.r2`, creeks at the second
reach's lower cut are never seeded, so their tributary rules are missing.

## Work
- `resolve_extent` returns the `cuts` it actually used (after `cut_pick`). A list of windows goes to the seeding, and
  `confluence_excludes` / `_cut_places` read the same cuts instead of re-deriving them.
- Pin: a test that `_cut_places` (or its successor) has no geometry of its own, and fixtures for a two-extent rule
  (both lower cuts seed) and an ambiguous cut (the picked one is used everywhere).

## Acceptance
Proof is a reviewed per-rule binding diff from a side-dir reach run: the 3 rules gain sections (additions only), plus
every rule with ambiguous or confluence cuts, each move named.

Rebuild: reach + deliver (no atlas). Depends: run in the same reach rerun as T102 if convenient, but diff separately.
Size: M. Owner: no.

## As built (2026-10-23)

One answer to "where does this rule's reach cut the water": `extent.resolve_extent` now returns the `cuts` it
actually used — `{split, blk, m, end: lower|upper, kind, label}`, one per split id, after `cut_pick` and
`AMBIGUOUS_CUT_NEAREST_THE_OTHER_END` (a `between` across two lines learns its lower end from
`_between_across_lines`' own ordering, `chosen["b_below_a"]`). Every reader takes those cuts:

- **Seeding** — `build.seeding_cuts(graph, per)` hands the walk EVERY extent's `lower` cuts (an extent with no cut —
  `whole`, `within` — offers its pieces' lower node bounds, `tributaries.lower_bound_cuts`, as a no-cut rule always
  did; a watershed part seeds nothing). `tributaries.expand / tributaries_of_reach / _mouths_at_lower_bound` take
  `cuts=[(blk, m), …]` instead of one `window`; `None` (a direct caller) derives them from node bounds. The
  `window = window or got["window"]` line is gone. Policy flag `tributaries.EVERY_CUT_REACHES_THE_WALK`.
  (Also renamed the walk's local `cuts` from `code_runs` to `divides`, which the new parameter would have shadowed.)
- **Confluence** — `build.confluence_excludes(..., resolved=per)` reads `resolved[i]["cuts"]` (resolving the
  extents itself when a caller passes none, as the tests and review app do). `_cut_places` is deleted; the
  function holds no geometry (no node bound, no `_cut_at`).
- `cache.POLICY_VERSION` 23 → 24 in the lane, renumbered 24 → 25 at merge (after T117 took 24); the policy pin in `test_reach_builder.py` carries the new flag.

Tests: `pipeline/tests/test_t111_every_cut_reaches_the_walk.py` (whole values) — `resolve_extent`'s `cuts` per op;
a two-reach rule seeds both lower cuts (MUTATION `EVERY_CUT_REACHES_THE_WALK=False` → C Creek lost); extent order
does not matter; a `whole` beside a cut extent seeds its own foot; `downstream_of` has no lower cut; an ambiguous
cut with `cut_pick: lower` tests only 1000 — Y Creek (own row) joining at the unused 3000 candidate is walked as an
ordinary tributary, where the old `_cut_places` kept it and its subtree out (fails if the re-derivation comes
back); the general rule's candidate is the one tested; `confluence_excludes` answers from the cuts it is handed; no
second copy (`_cut_places` absent, no node-bound words in `confluence_excludes`, no `window` in `build_reach`).
Updated: `test_reach_tributaries.py` (`window=` → `cuts=`), `test_complement_rest.py` (fake `expand` signature),
`test_review_reaches.py` (the alias and the id select the same water; `cuts` names the id each was bound by).

**Binding diff (side-dir reach run, code = 530bfc95 on the base, against `data/generated/reaches/full`):
NO BINDING MOVED.** `binding_diff` → rules_changed 0, unresolved_changed 0, licensing_changed 0
(`reach-T111.json` in the lane's scratchpad); the run digest is identical (`81e726a3e6fbad3b`), and
`rule_diagnostic.jsonl`, `rule_extent.jsonl`, `rule_unresolved.jsonl`, `licensing_diagnostic.jsonl` and
`straddle.jsonl` are byte-identical — so no `confluence_cut` row changed either.
Why nothing moved, measured (audit over the live atlas, all 461 rules whose extents carry a split):
- Seeding: the old seeds (first window) and the new (every lower cut) are equal for EVERY walking rule. For the three
  named rules the second reach's lower cut has no creek mouth hung on the piece below it in this atlas:
  `alouette_river.r2` second cut = North Alouette (blk 356337903) @ 4687.3 (216th Street); `bull_river.r1` second
  cut = Bull River @ 2071.2 (Tie Mill Dam); `atnarko_bella_coola_rivers.r2` second reach's lower cut is the South
  Atnarko confluence @ 58205.7, which is the first reach's upper end — inside the rule's reach, so the walk already
  meets it. The ticket's "their tributary rules are missing" was a code reading, not a measured loss; the defect was
  latent (any re-cut that hangs a creek on the second cut would have lost it).
- Confluence: in 89 rules the old `_cut_places` tested 98 places the reach did not use (an ambiguous cut's other
  candidate, or the same split on a side channel); the new cut set is a subset of the old in every rule (0 new
  places). None of the 98 is a confluence cut or a "near … confluence" label with a joiner, so no row changed.

Rebuild that ships it: reach + deliver (no atlas) — though as measured it changes no output today.

## Adversarial review (2026-10-23)

**Defect found and fixed (a2d5d62a).** In the watershed branch of `resolve_extent`, the existing local
`cuts = [(blk, m), …]` (the positions `_watershed_part` takes) rebound T111's name, so a watershed extent
reported bare tuples as its `cuts`: no split, no end, no kind. Both readers skip watershed parts today
(`seeding_cuts` via `got["watershed"]`, `confluence_excludes` via `ex["watershed"]`), so no output changed. Any
new reader indexing `c["end"]` would have crashed on the 8 watershed rules. The positions are now `places`.
Pinned by `test_a_watershed_part_reports_its_cuts_in_the_same_shape`, which fails on the old code.

**Gap found and pinned.** Nothing tested the END a `between` across two lines gives each cut (from
`chosen["b_below_a"]`). New `test_a_between_across_two_lines_knows_its_lower_end` (both split orders) uses a
Chilliwack/Vedder-shaped fixture with a creek at the lower cut on the downstream line.
MUTATION: swapping the two ends in `resolve_extent` fails both parametrisations; the creek is lost.

**Mutations re-run.** Each pin failed as claimed:
- `seeding_cuts` returning the first cut only fails `test_both_lower_cuts_of_a_two_reach_rule_seed`,
  `test_the_order_of_the_extents_does_not_matter` and `test_an_extent_with_no_cut_offers_its_own_lower_bounds`.
- `_used` reporting every candidate of an ambiguous cut (the old `_cut_places` behaviour) fails
  `test_an_ambiguous_cut_is_tested_only_at_the_candidate_the_reach_used` and
  `test_the_general_rule_s_candidate_is_the_one_tested_too`.

**Measurement re-done on the combined branch.** One scratch reach run with the code at T111 + T115 + review
fixes, against `data/generated/reaches/full`:
- Digest `b9be92114323aa22` is identical to the builder's T115-only run.
- `binding_diff` is identical to T115's: 12 rules + 1 licensing record, all on the 4 straddling Fraser pieces.
- So T111 moves nothing in combination either. Diff JSON: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/reach-T111T115rev.json`. The reach dir is deleted.

Checked and found sound:
- `_clip` keeps `cuts`, so a region- or scope-clipped extent still hands over its cuts.
- `per` is the clipped list both readers see.
- The review app / direct callers of `confluence_excludes` resolve their own (unclipped) extents, as before.

Left for later (added below): the carve-out walk (`resolve_carve_outs`: `tributaries_of_reach(graph, secs)`) still
derives its seeds from node bounds rather than the carve-out extent's `cuts`.

## Left / follow-ups
- The 98 dropped places include side channels of a cut across a braid: `cuts` holds the principal line only (the
  line `_cut_at` takes). If a future confluence cut has its joiner on a side channel, it will no longer be found.
  None does today; if one appears, `resolve_extent` should report the side-channel crossings in `cuts` too.
- The carve-out walk (`build.resolve_carve_outs`, "and everything upstream of it") still seeds from the
  carve-out's node bounds (`cuts=None`), not from `seeding_cuts(graph, [got])`. Today the two give the same
  places, except on braid side channels, where node bounds see more. Unifying it is the same decision, but it is
  unmeasured, so it was not changed in review.
- AGENTS rule 16 parity (review app vs builder) not re-run here; the review app calls the same `build_reach`.

## Owner questions
- none.

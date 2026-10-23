# T141 — Carve-out walk seeds from the extent's cuts; a braid cut reports its side-channel crossings

Status: DONE — e69750c0. No binding moved.

Source: T111's leftovers (owner Q100, 2026-10-23: "yes, as T141 with a measured diff").

## Decision being unified
"Which cuts bound a tributary walk?" T111 answered this for the rule's own walk (`build.seeding_cuts` over
`resolve_extent`'s `cuts`). Two places were left over:
- The carve-out walk (`build.resolve_carve_outs`, "and everything upstream of it") and the licensing carve-out
  check (`build_reaches`, per carve-out) still called `tributaries_of_reach(graph, secs)` with `cuts=None`. That
  re-derived the seeds from the carve-out's node bounds.
- A cut held its PRINCIPAL line only. The side channels the same split crosses (one split id, several blue lines
  — `_cut_at`) were dropped. A creek hung on a side channel at the cut was invisible to the seeding, and so was a
  confluence joiner there.

## As built (2026-10-23)
- `extent._crossings` is the one scan of a split's node bounds. `_cut_at` (the principal line) and
  `_side_crossings` (every other line) both read it.
- Each used cut is now `{split, blk, m, end, kind, label, side}`. `side` is `[[blk, m], …]` on the braid's other
  lines. Policy flag `extent.CUTS_REPORT_SIDE_CHANNEL_CROSSINGS`.
- `build.cut_places(cut)` returns the whole cut: the principal line first, then the side crossings.
  - `seeding_cuts` hands the walk every lower cut's places.
  - `confluence_excludes` tests every place (`_joiners_at`). On a side channel it takes only a joiner the label
    NAMES; the unnamed fallback of a curated confluence cut stays with the principal line.
- `build.carve_out_cuts(graph, got)` = `seeding_cuts(graph, [got])`.
  - `resolve_carve_outs` seeds each carve-out's walk from it and reports it as the detail row's `cuts` (the review
    app shows the row).
  - The licensing carve-out check in `build_reaches` walks from the same `cuts`.
  - Policy flag `build.CARVE_OUT_WALK_SEEDS_FROM_ITS_CUTS`.
- No `cache.POLICY_VERSION` bump: no outcome moved (below). The policy pin in `test_reach_builder.py` is
  unchanged.

Tests: `pipeline/tests/test_t141_carve_out_walk_seeds_from_cuts.py`. They are whole-value tests on a braid
fixture: main river M is cut at 1000 by `a`, and side channel S is cut by `a` at its 500 (or bounded there by
another split `z`). X Creek joins S at 500 on the piece below.
- The cut reports `side: [["S", 500]]`, and `seeding_cuts` gives both places.
- The rule's walk takes X Creek.
- A carve-out blocks X Creek: its `cuts` detail is pinned.
- Under a `z` bound, the carve-out and the rule's walk both leave X Creek alone.
- A side-channel confluence joiner is found only by name.

Mutations, each run:
- `CUTS_REPORT_SIDE_CHANNEL_CROSSINGS=False` fails 4 tests: side crossing, rule walk, carve-out, confluence
  joiner.
- `CARVE_OUT_WALK_SEEDS_FROM_ITS_CUTS=False` (node bounds) fails
  `test_a_carve_out_does_not_seed_at_a_bound_that_is_not_its_cut`.

Updated: `test_t111_every_cut_reaches_the_walk.py`, because the whole-value `cuts` now carry `"side": []`.

**Binding diff.** One combined scratch reach run with T141 + T121, against `data/generated/reaches/full`
(digest `b9be92114323aa22`, the batch-4 rebuild of 2026-10-23 06:30). **No binding moved.**
- `binding_diff`: rules_changed 0, unresolved_changed 0, licensing_changed 0.
- Diff JSON: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/reach-T141T121.json`.
- The run digest is identical (`b9be92114323aa22`).
- These files are byte-identical: `rule_diagnostic`, `rule_extent`, `rule_unresolved`, `licensing_diagnostic`,
  `licensing_placement`, `straddle`, `tidal` and `outside_bc`.
- The scratch reach dir is deleted.

Why nothing moved — an audit over the live atlas and corpus, run through heavy.sh:
- 654 cuts in 478 split extents. Only **5 cuts carry a side crossing**:
  - `ash_river.r1` at Elsie Lake, upper end.
  - `sproat_river.r1` at Sproat Lake, upper end.
  - `pitt_river.r3` / `r4` and `zp:bait` `bait.r3` at Pitt Lake: blk 356001043 @ 1842.5.
- Only one of those is a LOWER cut (`pitt_river.r3`), and no creek mouth sits at its side crossing. So the seeding
  gained nothing.
- None of the 5 is a confluence cut, so no `confluence_cut` row changed.
- All **66 carve-outs** walk exactly the same set from their cuts as from their node bounds.

The change closes a latent defect: a re-cut that hangs a creek at a braid's side crossing would have been lost to
the walk, and a carve-out's walk could have seeded at a bound that is not its cut.

Rebuild that ships it: reach (+ deliver, which follows any reach rerun). It changes no output today.

## Left / follow-ups
- The review app (`curation-review/backend/reuse.py`) shows `resolve_carve_outs` rows. Each row now carries
  `cuts`, and the review app does not display it yet. A UI line ("walked from …") would be a curation-review
  ticket.
- AGENTS rule 16 parity (review app vs builder) was not re-run. The review app calls the same functions.

## Owner questions
- none. Default built: on a side channel, a curated confluence cut takes only a joiner its label names, never the
  unnamed "anything joining here" fallback.

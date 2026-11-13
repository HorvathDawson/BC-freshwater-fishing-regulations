# T669 — One "strictest of both regions" for salmon and freshwater straddlers

Status: IN PROGRESS — item 1 done (91398f801); left (Claude): items 2 and 3 (feed format change: worker + app + live.compose together); the shared comparison landed in T668 (dfb4ea7c1)

Source: owner 2026-11-11 (T668): "The strictest of the rules is the same logic the fresh water regulations use I
believe; we should double check that and if possible unify that logic." The side-by-side is in
`handoff/PLAN/app-integration/T668-audit.md`, section "Freshwater (Z4 / T183) vs salmon straddlers".

## Today
- Shared: region membership (`registry.regions.section_regions`). Count and size caps are compared with the
  freshwater primitives: `deliver.salmon._caps` / `_both_bind` call `ladder.stricter` and `rules.statement`.
- Not shared: salmon still ranks per-day VERDICTS (`salmon.STRICT`: closed > release > check > open). The freshwater
  side runs `read.effective_rules` step 6 over RULES (`closure_grade`, `release_origins`, `keeps`). The DFO feed
  (worker → app) carries only verdicts and their quota trees, so the feed path cannot hand rules to the freshwater
  reader.
- Differences that are on purpose (keep them unless the owner says otherwise): a DFO region with nothing to say is
  closed (owner 2026-10-27, closed unless DFO opens it); the DFO header's daylight line (`h`).
- Not covered by either side: a pooled count (aggregate tier) and hatchery/wild kinds as caps (salmon shows both
  regions' words); comparing two rules' hour windows.

## Work
1. One table of cases (closure vs open, release vs open, two counts, two size caps, pooled, hatchery/wild, gear,
   hours) that BOTH the freshwater reader and `salmon.strictest` run, each case's expected winner stated once.
2. Move the closure / release ranking onto `closure_grade` / `release_origins` by keeping the answering rule's statement
   beside each DFO verdict (a verdict field, in both the release and the feed). This is a feed format change: the worker
   and the app move with it.
3. Pooled and kind caps as freshwater caps (the aggregate tier is `total` with `whose`), so a mix shows numbers, not
   only words.

## Blocks full unification
The feed composes chains from verdict indices in the app (`withSalmonFeed`), with no rules. Any rule-level comparison
needs its inputs in the feed format (worker + app + `live.compose` together).

Depends: T668. Size: M. Owner: no.

## Item 1 done 2026-11-12 (91398f801)
`pipeline/tests/test_strictest_cases.py`: eleven cases, each outcome stated once; the freshwater step-6 reading
(`ladder.stricter`, two regions' identical statements once) and `salmon.strictest` / `both_gear` / `tighter_hours` must
both reach it. Three differ on purpose, each with its reason in the table: a pooled count (salmon shows both words,
item 3), hours (only the salmon header compares them), a region with nothing to say (DFO: closed, `NOT_LISTED`).
Mutation: a freshwater reader that never displaces fails four cases. Test only: no output can move.

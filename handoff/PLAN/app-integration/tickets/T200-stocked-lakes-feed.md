# T200 — stocked lakes: feed the prepared Stocked mode

Status: OPEN — NEXT AFTER CLOSED TESTING (first). Was POSTPONED (deferred, owner 2026-11-05).

Source: owner 2026-10-25 ("at the end, alongside the stocked and bathy maps"); decisions.md "OWNER 2026-10-21 — lake
Stocked / Depth modes: prepared, not fed yet".

## State
The release's optional per-lake values slot (contract + reader) and the colouring engine's stocked ramp exist; the
Stocked toggle is disabled with "no data yet"; the bundle's stocking tables are empty (stock_water, stock_code, release).
T158: the cvd stocking ramp (navy → pale yellow) does not hold against the no-access line colour.

## Work
Pick the source (FFSBC stocking reports / BC open data), its licence and refresh cadence (owner); a fetch + parse into
curated data; bind stocked waters to lake items at delivery; fill the slot; enable the toggle; fix the cvd ramp
conflict (T158); tests + size diff; owner look.

## Next after closed testing (owner 2026-11-13)
Owner, 2026-11-13, verbatim: "after the closed release we will do stocking and depth maps next. so add that to next after
closed tickets." Starts once Play closed testing is running; stocking first, then T201. Carried with T634 (items 4-5).
Feasibility note: [`handoff/PLAN/research/stocking-feasibility.md`](../../research/stocking-feasibility.md).

**The note supersedes the Work above and main's name-first matcher** (name picks the nearest same-named lake with no
distance cap; code only a late fallback; 550 unnamed rows matched unchecked). Here the lake code picks and a name never does.
- **First step:** extend `pipeline.stocking.propose` with the open FISS release point (OGL layer "Known BC Fish
  Observations", 74,879 release points, 2,317 of 2,318 FIDQ lakes) as evidence; regenerate proposals; show the 154
  review rows on a page. No credits, publishes nothing.
- **Matcher:** FIDQ `wbody_id` -> WBID -> FWA polygons by `WATERBODY_KEY_GROUP_CODE_50K`; the FISS point must be inside or
  within 100 m; name (and variants) only confirms. Measured: 2,164 of 2,318 auto (93.4 %: A 1,674 named, B 490
  unnamed), 154 to review. Frozen reviewed file `data/curated/stocking/matches.json` (`stocking-matches/1`, keyed by
  `fidq:<id>`, value a `wbk:` item or a stream place), one writer (`accept`); the owner ticks proposals.
- **Data shapes:** feed `feeds/stocking/index.json` (`stocking-feed/1`, keyed by item id, no section id); `rel` rows
  `[date, species, stage, count, grams, strain]`; retire the bundle's `release` / `stock_water`. Size: 5 years 35 KB gzip,
  full history 354 KB gzip, last-stocked index 12 KB. Lake tab Stocking card; Stocked colouring reads the feed.
- **Sizes:** matching M, stream places M, feed publish M, app card M (the Lake tab itself L, shared with T201). Data is
  4.5 months behind (to 2026-06-29); licence of FIDQ counts still "Access Only" in the catalogue.
- **Owner questions (in the note):** (1) OGL link for FIDQ, or ask the Province / FFSBC; (2) open-only FISS first
  version (no counts/strain) if the licence stays open; (3) class B bulk-accept with a sample or tick each; (4) streams
  in the first version or lakes only; (5) history depth 5 years or all on tap; (6) is a 4-month lag acceptable.
  Defaults to build with: ask for the link, FISS-only first, class B sample-tick, lakes only, 5 years, lag accepted.

# T200 — stocked lakes: feed the prepared Stocked mode

Status: OPEN — NEXT AFTER CLOSED TESTING (first); in the next version, after a careful review of the stocking matching with the owner (owner 2026-11-27). Was POSTPONED (deferred, owner 2026-11-05).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T200 | stocked lakes: feed the prepared Stocked mode; NEXT AFTER CLOSED TESTING, first (plan: docs/research/stocking.md; matcher = lake code picks, FISS point + name confirm, frozen reviewed file) | OPEN | — | next version (owner 2026-11-27): the stocking matching reviewed with him first; pick 1 (card look) after the T1189 prototype; Credits and NOTICE name GoFishBC (his permission, 2026-11-26) |

Source: owner 2026-10-25 ("at the end, alongside the stocked and bathy maps"); decisions.md "OWNER 2026-10-21 — lake
Stocked / Depth modes: prepared, not fed yet".

## State
The release's optional per-lake values slot (contract + reader) and the colouring engine's stocked ramp exist; the
Stocked toggle is disabled with "no data yet"; the bundle's stocking tables are empty (stock_water, stock_code, release).
T158: the cvd stocking ramp (navy → pale yellow) does not hold against the no-access line colour.

## Next after closed testing (owner 2026-11-13)
Owner, 2026-11-13, verbatim: "after the closed release we will do stocking and depth maps next. so add that to next after
closed tickets." Starts once Play closed testing is running; stocking first, then T201. Carried with T634 (items 4-5).
Feasibility note: [`docs/research/stocking.md`](../docs/research/stocking.md).

**Work: the note supersedes main's name-first matcher** (name picks the nearest same-named lake with no
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
- Also: enable the Stocked toggle; fix the cvd ramp conflict (T158); tests + size diff; owner look.
- **Sizes:** matching M, stream places M, feed publish M, app card M (the Lake tab itself L, shared with T201). Data is
  4.5 months behind (to 2026-06-29); licence of FIDQ counts still "Access Only" in the catalogue.
- **Owner questions (in the note):** (1) OGL link for FIDQ, or ask the Province / FFSBC; (2) open-only FISS first
  version (no counts/strain) if the licence stays open; (3) class B bulk-accept with a sample or tick each; (4) streams
  in the first version or lakes only; (5) history depth 5 years or all on tap; (6) is a 4-month lag acceptable.
  Defaults to build with: ask for the link, FISS-only first, class B sample-tick, lakes only, 5 years, lag accepted.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-13: "after the closed release we will do stocking and depth maps next"; starts once Play closed testing is running; owner: licence link, class B, streams, history depth, lag (questions in the ticket); with T634 items 4-5

## Plan 2026-11-25 night (read-only; both features)
Notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `stocking-depth-plan.md`.
- What ships today: the Stocked and Depth colour modes, legends and the `lakes/1` reader exist (`layers.ts:150-160`,
  `lakeFacts.ts:1-101`, `mapModes.ts:126`) but NOTHING feeds them: both toggles show "Coming soon / No data yet".
  There is no Lake tab, stocking card or depth card (the old site had both). The credits page already claims
  stocking and depth under OGL (`menu/licences.ts:144`, `sources.ts:121`) and the About page lists "stocking
  records" (`html.ts:211`); the owner chose on 2026-11-25 to claim both features now (T1152).
- Licences read 2026-11-25: bathymetric PDFs are OGL-BC (free to ship now); Stocking Reports and placed contours
  are still "Access Only" (the owner's OGL link is still owed); open fallback: FISS release points (OGL, no counts
  or strain); the GoFishBC report is richest and freshest but "All Rights Reserved".
- Competitors: River Atlas has stocking reports with "follow"; BetterFish a stocked-lakes layer (third-party
  listing only); depth contours in B.C. are paid (Fishbrain Pro, Navionics); nobody confirmed stocking alerts.
- First slices: stocking = FISS only, lakes, 5 years, a Stocking card plus the Stocked colour through the T412
  feed pattern; depth = the OGL PDF as a "Depth chart" card plus a tint, contours later; build the Lake tab once,
  in the stocking release.

## Owner

- [x] 2026-11-25 — Picks (defaults first): stocking v1 from open FISS records only / wait for the licence / ask the Society; the look: a card plus the map colour; alerts: later with T234 / now / never; a source lag is fine if the card says "records to <date>": yes / no. → "i got permission to use go fish bc data we are ok" (2026-11-26 00:50: GoFishBC data may be used, so the richer GoFishBC report, not only FISS); the first slice → "Full report on the lake page (Recommended)" (2026-11-26 00:54)
- [x] 2026-11-26 — The 11 picks of the stocking and depth build plan (the planning chat's notes `stocking-depth-build-plan`; mock-ups of the stocking and depth cards to be published for you). Pick 10: a scheduled fetch of GoFishBC's JSON rests on your permission (their robots.txt asks robots to stay off /wp-json/). Default: none built before you pick.
  → pick 10: "i got permission to use go fish bc data we are ok" (2026-11-26; Credits and NOTICE must say so). Pick 2 (depth card = a picture): "the bathy map pdfs we would convert to iamges if shipping with app to compress them best" (2026-11-26). Pick 3 (which lakes get a Lake tab): superseded by T1189's Info tab (2026-11-27). Picks 4-9 and 11, the recommended set: "everything is good except the stocking matching we need to spend some time with to make sure everything is matched correctly. and that is the reason this is in next version" (2026-11-27). Reading: the recommended set stands; the lake matching (the 132 unnamed lakes with it) is reviewed with him before it ships.
- [ ] 2026-11-27 — Pick 1, the stocking card look (A release list / B one line a year / C latest + calendar), asked with a picture after the T1189 prototype (the cards now live in the Info view).

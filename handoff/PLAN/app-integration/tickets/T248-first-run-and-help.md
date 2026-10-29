# T248 — first-run walkthrough and help

Status: DONE — 5436081e6 (web walked desktop + phone; native not walked on a device)

Source: owner 2026-10-28 — approved proposal P14 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
A short first-open walkthrough (3–4 cards: modes Regs/Conditions/Salmon, the date pill, tap a water, offline) after the
disclaimer (T236 first-run notice, which stays first and separate); "Help" page in More explaining colours, badges,
hatches and dots (from the legend tokens, no hand-copied colours); re-openable from More.
## Acceptance
Shows once per device (stored), skippable, re-openable; render tests; desktop + phone shots.
Depends: T236 (first-run notice). Size: S. Owner: no.

## Built (2026-10-29)
- Words + logic: `core/src/help.ts` — `WALKTHROUGH_WORDS` (4 cards: modes, date bar, tap a water, offline + sync/bell),
  `WALKTHROUGH_VERSION`/`walkthroughSeen`, and `helpVM()` (the Help page).
- Help is GENERATED: every row is a swatch token over `WATER_STATUSES`, `GEAR_LEGEND`, `SALMON_MAP_STATUSES`, `RUN_STATES`,
  freshness kinds and `syncVM`'s own states; no hex; `help.test.ts` fails if a status/mark/state lacks a sentence.
  Surface: `ui-native/src/menu/HelpPage.tsx` (swatches through the map's resolvers; `SyncGlyph`/`BellGlyph` are now exported
  from TopBar so the page draws the very glyphs). `map` gained `tidalColours(theme)`.
- Walkthrough: `ui-native/src/Walkthrough.tsx` (`useWalkthrough(store, blocked)`, `WalkthroughSheet`); store
  `openWalkthroughStore` (localStorage `canifishthis.walkthrough.v1` / `spots/walkthrough.txt`). SEQUENCE: first-run notice ->
  walkthrough -> in-season notice; the walkthrough is held while the notice is up, the in-season notice while either is.
  Skip or finish = seen. More -> Help (new first row, `MenuPageKey` "help") has "Show the walkthrough again".
- Tests: `core/src/help.test.ts`, `ui-native/src/Walkthrough.test.tsx`; More row pins updated. Shots: `T248-shots/`.

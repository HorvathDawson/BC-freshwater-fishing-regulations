# T247 — search upgrades

Status: DONE — 96abc2b58 (web walked desktop + phone on the real release; native not walked on a device)

Source: owner 2026-10-28 — approved proposal P13 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
Fuzzy / typo-tolerant search (edit distance on names + variants, T224), search by management unit ("2-4"), region
("Region 2"), nearby town ("near Chilliwack": nearest-town data in core, T182), recent searches (device-only, clearable).
## Acceptance
Golden queries (typos, MU, town, region) return the right water first; recents survive a restart and clear; search stays
under its time budget on the phone.
Depends: T224 (variants). Size: M. Owner: no.

## Built (2026-10-29)
- **Index, built once** (`core/src/searchIndex.ts`): `WaterSource.searchIndex()` reads core whole (waters, aliases, towns, `item_near`,
  `item_area`, `name_source`); `buildSearchIndex` holds it; `queryIndex` answers each keystroke in memory (`useSearch` falls back to
  the SQL searches only if the read fails; the index warms when search opens). Name ladder == `matchTier` (pinned).
- **Typos**: bounded Damerau-Levenshtein per word (none to 3 letters, 1 to 6, 2 beyond), last word may be half typed; runs only when
  < 8 waters match as typed, ranks below every exact match; the group says "some by close spelling". Towns fuzzy too.
- **MU / region / town**: "2-8", "mu 2-8", "region 2", "r4", "zone 7a", "elk 4-2" (name inside an area), "4-" (a region's units),
  "near Hope" and a plain town name (waters it describes, below name matches). Heading says "40 waters in MU 2-4".
- **Where**: every water row says `Region 4 · MUs 4-2, 4-22 +1`; same-named waters sit together (`clusterByName`).
- **Recents**: last 6 searches a result was picked from, device-only (`openRecentSearchStore`: localStorage / file), shown before two
  letters are typed with Clear; tapping one searches it again.
- **Pipeline**: `build.core_area` -> core `item_area (ord, regions, mus)` from the bundle's `section_ruleset.place/.mus`; ADDITIVE (core/1).
  core gz 1,271,866 -> ~1,381,000 on r-0fa7927b16e5's tables: core gate 1.3 -> 1.45 MB, `first_search` 1.4 -> 1.5 MB. **The live
  release has no `item_area` yet: regions/units appear (and "2-8" / "region 2" work) once a release is cut from this code** (owner/release
  lane; nothing else to do). Name/typo/town/recents work on the live release now.
- **Measured** (laptop, real index 19,811 waters / 22,723 names / 2,805 towns; `tools/search-golden.test.ts`): per query 0.4-9 ms
  (worst: a two-word typo), build 33 ms (synthetic) / 17 ms real; browser: index read 131 ms after core is in memory (the cold first
  search is core's load, as before T247), warm result 245 ms of which 200 is the owner's debounce (kept; could drop to ~50, owner call).
  Native not timed on a device (expo-sqlite read is the same 6 queries; the index is plain JS).
- Tests: `searchIndex.test.ts` (23), `Search.upgrades.shell.test.tsx`, conformance (index vs SQL, fixture + release), `test_release.py`
  (core_area), `tools/search-golden.test.ts` (Tier 2: `SEARCH_CORE=<core.sqlite>`); a typed-query-not-yet-answered no longer flashes
  "Nothing by that name" (`SearchResults.query`). Shots: `T247-shots/` (desktop + phone, on a core with `item_area` served locally).

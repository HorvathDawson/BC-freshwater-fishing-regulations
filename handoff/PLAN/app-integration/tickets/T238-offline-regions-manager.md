# T238 — offline regions manager (phone)

Status: DONE (b55c20c7b, 342454f7b, 685197125) — what does not depend on per-region tiles; the per-region map is T259 (deferred by owner 2026-10-29 to after this APK).

Source: owner 2026-10-28 — approved proposal P4 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
1. A "Offline data" page (More tab): each region (1–8) with its size (tiles + basemap + release slices), on/off, and the
   total storage used; delete a region's files; "keep everything" stays the default.
2. Tiles: per-region pmtiles extracts (or byte ranges of the province archive) — decide with T85/T202 (install size).
3. Map: a region not kept draws online-only (online) or the outside-BC paper with a "not downloaded" note (offline).
4. Update service (T231) downloads only kept regions.
## Acceptance
On a phone: turn off 3 regions → storage drops by their size; the map there says "not downloaded" offline; turn one back
on → it downloads; tests over the in-memory phone store.
Depends: T231, T85/T202 (tile split). Size: M.
Owner: yes — default region set on first install (default: all, ~434 MB, with the choice offered on first run, T248).

## Outcome (2026-10-29, overnight lane; b55c20c7b, reworked — see the commit after 6f3fc912f)
Owner change (2026-10-29, relayed): REGULATIONS ARE NEVER PER REGION; only map data is a choice by region.
- **More → Offline maps** (first of the More pages). Core `offline.ts` `offlineVM` (whole strings pinned);
  `@app/data` `release/offline.ts` (`offlineSizes`, `regsKept`); `@app/ui` `useOfflineMaps`; `ui-native`
  `menu/OfflinePage.tsx` (Shell → `ShellSheets` → `MenuHost`, Shell passes `sync={syncState}`).
- **Fixed first row "Regulations: all of B.C." — "Always available offline" (N MB)**, N = every data file the manifest
  names (regs, salmon, answers, core, water …; ≈ 39 MB on the live release). No action on it, none anywhere that drops
  regulations (the earlier per-region Save / Remove / Remove-everything were removed, with the loader/cache drop path).
  Web adds what the release cache holds so far ("… · 12.5 MB of 41.0 MB kept in this browser so far" / "All kept").
- **Map:** "Map of B.C." one row (tiles + mask + basemap ≈ 460 MB); "Map by region" lists the nine regions as "Part of
  the province map" with NO size and NO action — there is no per-region tile split (T85/T202), and a per-region size
  would be invented. The rows become choices when the split lands.
- **Phone:** storage used = map + regulations of the installed set; the T231 release download (progress bar; waiting for
  Wi-Fi → "Resume now" = a check); "Download large updates on Wi-Fi only" = T231's mobile-data setting inverted.
- **Install keeps every region's regs** (item 3): verified — `install`/`diff` iterate every manifest file, no region
  filter; pinned by `data/src/release/install.regs.test.ts` (all nine regions' regs + salmon on first install and through an
  update that changes one region; the rest copied, none dropped).
- **Web prefetch walks every region's regs** (item 4): `background.ts` `regionOrder` is now view → neighbours → every
  other region (replaces the 10-28 view + neighbours cap); only data files (the tap list) — no tiles; low-bandwidth mode,
  Save-Data, metered, hidden, battery still hold it (`prefetchHold`). Test: `background.test.ts` "every region's regs over
  time" (14 files, all nine ready, regs order view-first).
- The bell next to the sync button is untouched.
- Tests: `core/src/offline.test.ts`, `data/src/release/offline.test.ts`, `install.regs.test.ts`, `background.test.ts`,
  `ui-native/src/menu/OfflinePage.test.tsx` (no buttons on the web page; phone switch + Resume; pressAudit clean).
- Shots `../T238-shots/`: phone-web-offline.png (390×844), desktop-web-offline.png (1280×900), from :39218. Native
  layout not shot (no emulator this lane).

## Calls made (owner asleep)
- Map by region shown as the province map's parts with no size/action until the tile split (rather than one row only),
  so the shape of the future choice is visible without stating a size the data does not have.
- No Wi-Fi toggle on the web (no metered setting there).
- The web's "always available offline" is literal on a phone; on the web the row says how much the browser holds so far,
  filled by the background prefetch (which now walks the whole province on a good connection).

## Left (for new tickets)
1. Per-region map tiles (pmtiles extracts by region, T85/T202), then the region rows become Save / Remove with sizes;
   install / update service fetch only kept regions' MAP (never regs); the map says "not downloaded" offline there.
2. Owner: default map regions on first install (Default: all) — ties to T248 first run.
3. The whole-province web prefetch adds the remaining regions' regs + salmon slices (not measured) over the 10-28 cap's 6.15 MB; measure with
   `tools/prefetch-measure.mjs` and the request walk (background off there) if the budget matters.

## Finish (2026-10-29)
Owner: the per-region map (T259) waits for after this APK; regulations are always all of B.C. The page stays honest that
the map is all-or-nothing for now (one "Map of B.C." row; the regions listed as part of it, no size, no action; the note
says choosing map regions comes with the split). Added (685197125): the RULE BOOK row — the synopsis PDF is now part of a
phone's first download (fa9e82b57, owner 2026-10-29): "Rule book (synopsis PDF): 25.0 MB. On this phone · opens offline."
(counted in storage used); web: "Downloaded when you open it". Tests: `core/src/offline.test.ts`,
`data/src/release/offline.test.ts`. The web regs prefetch vs `WEB_BUDGET` (T238 left 3) moves with T259.
Emulator (test bed, n1_pixel, staging r-164ca35636ea, 2026-10-29): install PASS. The first run streamed the rule book with
the release (`synopsis-08f58537261f.pdf`, 25,041,728 bytes, native transport, followed sha256 443 ms after the last byte;
up in 77 s, peak heap Java 16 MB). The offline scenario's cold start in airplane mode, its banner and the offline search
PASS. Its later steps FAIL on a stale Maestro selector ("Lake · near Lindell Beach", but the row now reads "Lake · Region 2 ·
MU 2-3 · near Lindell Beach", T247), not on the data, so opening the book offline was not driven through the UI. The test-bed
APK is a release build (`run-as` refused), so the file was proven by the app's own download log.


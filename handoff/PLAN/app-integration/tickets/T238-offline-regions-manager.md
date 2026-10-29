# T238 — offline regions manager (phone)

Status: PARTIAL — b55c20c7b. Page + web per-region save/remove built; per-region TILES and per-region removal on a phone wait on a tile split (T85/T202).

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

## Outcome (2026-10-29, overnight lane; b55c20c7b)
- **More → Offline maps** (first row of the More pages; `MenuPageKey "offline"`). Core `offline.ts` `offlineVM` (words +
  view model, whole strings pinned in `offline.test.ts`); `@app/data` `release/offline.ts` (`offlineSizes`,
  `createOfflineStore`); `@app/ui` `useOfflineMaps`; `ui-native` `menu/OfflinePage.tsx` (wired through `ShellSheets` →
  `MenuHost`; Shell passes `sync={syncState}`).
- **What a region is:** its own slices `regs-<r>` + `salmon-<r>` (0.3–1.6 MB each on r-0fa7927b16e5). Every other data
  file is "Province-wide data" (≈33 MB) and the map is "Map of BC" (tiles + mask + basemap ≈ 460 MB, ONE file each for the
  province) — both shown as what they are, never as a region's.
- **Web:** state read from the release cache (`ByteCache.has/remove`, `ReleaseSource.stored/drop`,
  `ReleaseLoader.stored/drop` — new, optional). Save a region = its slices + any missing shared file through
  `ReleaseLoader.prefetch` (the same verified path as T253/T231), one file at a time with a progress bar; a failure stops
  with what was saved kept and the row offers Resume (fetches only the rest). Remove = the region's slices (shared go with
  the last region); "Remove everything saved". Province-wide data gets its own Save when partly kept. Storage used = what
  the cache holds. A region reads "Saved" when its own slices are kept (otherwise the background prefetch's regions never
  read as saved). Tests: `data/src/release/offline.test.ts` (Cache Storage stand-in, real web source),
  `ui-native/src/menu/OfflinePage.test.tsx` (drawn, pressAudit clean: every press named, ≥ 44×44).
- **Phone:** the whole set is installed, so every region reads "On this phone", no per-region action; storage used = the
  held manifest (map + data); the T231 release download shows on the page (progress bar; "waiting for Wi-Fi" → Resume
  now = a check); **Wi-Fi only** switch = T231's large-updates-on-mobile-data setting inverted (same store). Tested over the
  hook with an UpdateSnapshot; NOT run on a device (no emulator this lane).
- Shots `../T238-shots/`: phone-web-offline (all saved), -removed (7B removed), -saved-shared (bottom: Remove everything,
  note), desktop-web-offline. Phone-native layout not shot (no emulator).

## Calls made (owner asleep)
- Region slices only per region; tiles honest as province-wide. Removing a region on a phone NOT offered (it would save
  ≤ 1.6 MB of 490 MB and needs install/boot to tolerate a partial set) — the page says so.
- No Wi-Fi toggle on the web (the web has no metered setting; a Save is the reader's own tap).
- Removed regions on the web can come back: the background prefetch (T253) still fetches the view + neighbours.

## Left (for new tickets)
1. Per-region tile extracts (pmtiles by region) so a phone can keep/drop regions and storage really drops (T85/T202);
   then install/boot/update-service download only kept regions and the map draws "not downloaded" offline (Work 2–4).
2. Owner: default region set on first install (Default: all) — still open, ties to T248 first run.
3. Web: the background prefetch ignores the page (a removed region in view comes back); decide whether a removal should
   exclude it from the prefetch.

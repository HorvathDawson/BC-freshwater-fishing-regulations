# T259 — offline map by region (T238 leftovers)

Status: DROPPED (owner 2026-11-05: "doesn't save much space and I think it is not necessary") — was: OPEN — deferred by owner 2026-10-29 to after this APK.

Source: T238 (2026-10-29 overnight) "Left"; owner 2026-10-29: regulations are never per region — only the MAP may be
chosen per region.
## Work
1. Per-region map tiles: pmtiles extracts by region (or byte ranges of the province archive) — decide with T85 / T202.
   The Offline maps page's region rows (today "Part of the province map") become Save / Remove with real sizes.
2. Install and the update service (T231) fetch only the kept regions' MAP files; regs, answers and salmon stay whole-
   province (`install.regs.test.ts` keeps pinning that).
3. Offline, a region not kept draws the outside-BC paper with a "not downloaded" note.
4. Measure the whole-province web regs prefetch (T238 rework: every region's regs + salmon after the view's) against
   `WEB_BUDGET` (`tools/prefetch-measure.mjs`, the request walk).
## Acceptance
On a phone: drop 3 regions' maps → storage falls by their size, regs still answer there offline, the map says "not
downloaded"; take one back → it downloads. Tests over the in-memory phone store.
Depends: T85/T202 (tile split). Size: M. Owner: yes — default map regions on first install. Default: all (T248 first run).

## Note (2026-10-29)
Deferred by owner 2026-10-29 to after this APK; nothing built. A design sketched while scoping it (not decided, for when it
is picked up): keep ONE province archive per map file, re-laid so each region's tile data is one contiguous byte range
(a tile touching two regions, and the low zooms, in an always-kept shared range); the manifest names each range (offset,
bytes, sha256); a phone downloads the header + directories and its kept ranges and rewrites the directory offsets into a
compact local archive (a missing tile is then simply absent). The web reads the same archive unchanged.


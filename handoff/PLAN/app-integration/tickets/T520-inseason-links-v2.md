# T520 — In-season links/2: section-exact FWA ranges, content ids, one check place, GitHub sync fixed

Status: OPEN — designed 2026-11-08 from a fresh audit (coordinator; owner asked for the audit: "it feels fragile").
Carries T518 (exact sections + ends on the page).

## Why (audit 2026-11-08, verified on code + live staging)
- **Real today:** `feeds-sync-inputs.yml` has failed all 4 runs since 11-05: `publish.main` always runs
  `compile_places`, which needs `data/release/LATEST` + the atlas (not on the runner). It also aborts the dfo-entries /
  wording / roster push-syncs. Staging is current only through the Mac `sync-inputs.sh`.
- **Latent:** the feed row id includes the row's position among a water's rows (worker inseason.ts:360): a DFO row
  inserted above another (Heber Creek ×2, Fraser ×2) moves a curated match onto another notice's text, undetected.
- **Latent:** a device off `links.release` walks part marks (`inSeasonParts`); derived pick-points are passed as fake
  labels → always "on part of this water"; T517 renumbering makes any lagging links wrong. Second load path (async marks).
- **Silent:** publish skips an unresolved extent / missing section / handles mismatch and exits 0; the device keeps an
  old capture on a format mismatch with no report. The worker reports `unmatched: 32` with all 32 matched.

## Design (owner 2026-11-08: accurate, not overkill; no old-release compatibility)
- **Resolve where the atlas is.** The review app (Mac) resolves a match to FWA ranges on save and writes them into
  `data/curated/inseason/notice_waters.json`; the release promote (`upload.sh`) re-resolves. GitHub publish only formats
  committed data — no atlas — so the push-sync works again.
- **Link `inseason-links/2`:** `{release, notices: {id: {from?, to?, waters: {item: {all: true} | {runs: [[blk, m0, m1]…],
  ends: "From A to B"}}}}}`. No part indexes, labels or extents. Lakes/wetlands are whole-item only.
- **Row id = hash(region, water, normalised change)** (not `effective`, not position). An amended notice gets a new id →
  shows as not-on-the-map + urgent email until re-matched. One-time re-key of the 32 matches.
- **Device:** one pure core `inSeasonCover(notice, item, pageSections)` via `section_measure` lines (lazy, only partial
  items). Banner, KeepTab, map highlight (T518) and screen reader read it. Delete `inSeasonParts`, `InSeasonMarks`, the
  marks fetch, `places`, `partial` wording. A range landing on nothing → banner on the whole water + `reportFailure`.
  Format mismatch → reported. Re-fetch links when the device's release changes.
- **Checks in ONE place, `publish.py`, non-zero exit on:** unresolved extent, empty coverage, a range not landing on
  the release it names. Worker: fill true matched/unmatched counts from the curated file it already gets. Keep the
  worker's empty-page/drop guard + heartbeat.
- **Delete:** `auto.py` (unused) + its tests, `--no-places`; review backend `_in_effect` uses core's rule once.
- Size: links ~½ of today (≈3 KB gz either way).

## Proof
- All 32 notices cover exactly the same sections as today's `places` (diff); partial ones (Fraser, Pitt ×2, Kaslo,
  Snehumption) show their own ends and highlight only their sections (shots light/dark).
- `gh workflow run feeds-sync-inputs.yml … dry_run=true` passes; a push run passes.
- Tests: publish refusals, content id stability under reorder, `inSeasonCover` all/some/none/off-release.

# T520 — In-season links/2: section-exact FWA ranges, content ids, one check place, GitHub sync fixed

Status: IN REVIEW — ece44d1f3, 46ce5a22d, fd09e65af, 9ef7e7600, 28c0c7869, in build 22 (3d807829d); staging 32/32 matched; owner: OWNER-TODO "Look at" (`shots/T520-*.png`) + owner: OWNER-TODO "Phone check with build 22"; GitHub R2 secrets: OWNER-TODO.
Earlier: IN REVIEW — ece44d1f3, 46ce5a22d, fd09e65af, 9ef7e7600, 28c0c7869 (review fixes: end tokens said by core, stamps, per-input sync, ids with typography folded; staging re-synced, 32/32 matched). Proof pending: the owner's look at shots/T520-*.png and the next APK (the device half ships with it). The 32 notices cover exactly today's sections (diff against the live places, r-6dce77a24e6a: 0 differences); staging: links/2 synced, feeds worker deployed, feed reads matched 32 / unmatched 0. Owner 2026-11-08 change of plan: publish from the Mac only — the GitHub workflow is DELETED (not fixed); the review app runs deploy/staging/sync-inputs.sh after a save. Notes: a river's lake pieces (no line) ride as `lakes` places (Fraser, a Similkameen trib); the map outlines covered sections in the attention ink. Carries T518 (exact sections + ends on the page).

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
- ~~`gh workflow run feeds-sync-inputs.yml … dry_run=true` passes~~ — superseded (owner 2026-11-08): publish from the
  Mac only; the workflow is deleted. Proof instead: `deploy/staging/sync-inputs.sh` (by hand and from the review app)
  puts links/2 and the staging feed reads matched 32 / unmatched 0.
- Tests: publish refusals, content id stability under reorder, `inSeasonCover` all/some/none/off-release.

## Ends: tokens, said by core (review 2026-11-08) — WHAT T517 MUST PROVIDE
- `resolve` stores each covered stretch of a water's main line as two end TOKENS, mouth upward (a split id, a pick-point
  id — `lake:<wbk>:inlet|outlet`, `region:A|B`, `mu:A|B`, `trib:<item>` — or `mouth` / `source`), never words.
  Core composes the sentence (`inSeasonWhere`: "From A to B", "Upstream of A", "Downstream of B"), AGENTS 33. One
  geometric rule for every case: the Hell's Gate double extent is one stretch; a partly covered tributary says where on
  IT (its own tokens), never the parent's name.
- INTERIM words: `notice_waters.json` / links `words` (token -> a place's NAME, `resolve.interim_words`: the curated
  split label or the atlas point's label). **T517 must ship a token -> words table in the release** (the words
  `answers.display.part.end_words` / `end_variants` give each run-end token, the same ones the page boundaries say), keyed
  by these tokens (split ids as cuts.json names them; the derived point ids above). Then core reads the release's
  table instead of the links' `words`, and the links stop carrying words — so "Where" and the part boundaries can never
  disagree.
- Weak words today come from the curated LABELS, not the composition: `lodgepole_creek__falls` is labelled "Falls",
  `alexander_creek__easternmost_hwy_3` "easternmost Hwy 3" (the province says "the easternmost Hwy 3 bridge"). Fixing
  those labels is curation (owner: ask-before-changing content), or T517's end words when they say more.

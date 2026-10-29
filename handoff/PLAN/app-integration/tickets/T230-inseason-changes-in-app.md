# T230 — provincial in-season changes: curated matches, banners, unmatched notice, review tab

Status: IN PROGRESS — main checkout lane (redesign/stream-sections).

Source: owner 2026-10-28 design for provincial IN-SEASON changes; the feed is T225's `feeds/inseason/index.json`
(`inseason-feed/1`, `app/workers/feeds/src/inseason.ts`).

## Work
1. `data/curated/inseason/matches.json` (AUTHORED, `CURATED.inseason.matches`): row id → extents (`whole` /
   `upstream_of` / `downstream_of` / `between` on an item, by curated split ids), optional `effective` {from,to}, note;
   append-only `history`. Loader + validation `pipeline/regs/inseason/matches.py`; app file
   `pipeline.regs.inseason.publish` → `data/generated/inseason/feed/matches.json` (`inseason-links/1`, split LABELS,
   no section ids).
2. App: feed + links joined (`@app/core` inseason), banner on every matching water/part while in effect;
   informational only (never changes an answer; future option: a full closure could set "Check"). Offline: last capture.
3. App-wide notice for unmatched rows in effect: once per row per device, plus a badge icon in the chrome.
4. Review app "In-season" tab: rows + match state, pick the extent on the map, save → matches.json (one route).
5. Tests + shots in `T230-shots/`. Dev: `/feeds/inseason/` from the local tile server.

## Acceptance
Tests for the loader/validation, the join/in-effect/sentence (whole strings), part resolution, the route; shots.

Depends: T225 feed. Size: L. Owner: no. Default: rows with only a `from` stay in effect until the page drops them
(or a curated `to`); a part whose split is not a part boundary in the release falls back to the whole water, said so.

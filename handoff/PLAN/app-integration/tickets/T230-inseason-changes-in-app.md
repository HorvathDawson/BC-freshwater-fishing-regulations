# T230 — provincial in-season changes: curated matches, banners, unmatched notice, review tab

Status: DONE — 03fc2eb2 (curated file, loader, auto-matcher, review tab) + the app commit.

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

## As built (2026-10-28)
- `pipeline/regs/inseason/{matches,auto,publish}.py`; curated `data/curated/inseason/matches.json` (`source` owner|auto,
  evidence, confidence; `guesses` pre-fill the review editor; owner matches are never overwritten by auto).
- AUTO (owner add-on): 22 of today's 32 rows auto-matched (water resolves to one item, every boundary an existing split
  on it); 10 left with a guess: Fraser r2 ×2 (13 candidate waters), "Chilliwack Lake, Upper Pitt River" (two waters),
  Stave/Northrop channel (another water, "the intake" no split), Fraser r5 ("Region 5 boundary" no split), Kaslo /
  Keen / Lardeau (boundaries with no split), "Granby River's Tributaries" (tributaries only), Okanagan River Oxbows
  (41 waters). Re-run: `python -m pipeline.regs.inseason.auto` then `python -m pipeline.regs.inseason.publish`.
- App: `@app/core` inseason.ts (join, in-effect, part resolution on `part_mark` labels, every sentence), `@app/data`
  `httpInSeason` (index.json + matches.json, last capture kept), `@app/ui` `useInSeason` / `useInSeasonBanners` /
  `useInSeasonNotice`, `regs/InSeasonBanner.tsx` (under the access notice), `InSeasonNotice.tsx` + the top bar's bell.
- Review app: In-season tab, `POST /api/inseason/matches` (AGENTS 66 exception).
- PRODUCTION (owner): upload `data/generated/inseason/feed/matches.json` to R2 `feeds/inseason/matches.json` after each
  match change (the T225 worker writes index.json beside it). Not automated.
- Known limits: a cut label that is not a part boundary in the release shows the banner on the whole water with "On
  part of this water"; "including tributaries" is not walked (the text says it); only the Map/Conditions top bar carries
  the bell (the notice itself opens on any screen at load).
- Shots: `T230-shots/` desktop + phone notice, bell badge, part banner (Fording below Josephine Falls; part above shows
  none), partial banner (Peace), review tab.

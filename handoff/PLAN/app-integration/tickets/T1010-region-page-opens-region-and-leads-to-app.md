# T1010 — Website: a region page's "Open the map" opens on that region; the page leads people into the real app

Status: IN REVIEW — owner: see ## Owner. Worker: crashes.ts EXPECTED gains the region names-file 404 — STAGING feeds-worker deploy (Claude, with the next staging release); PRODUCTION feeds-worker deploy is the owner's (his production command list, with the 0.9.0 release)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1010 | Website: a region page's "Open the map" opens framed on the region (`?fit=w,s,e,n` start link, box from the release); redesign the page to lead into the app (water search, crawlable links kept below) | IN REVIEW — owner: see ## Owner | a088c9cd0, 54778147e | option B built (dot picture, button, search); PR open |

Source: owner 2026-11-22, on https://canifishthis.ca/region/4, verbatim: "if i click open map it doesnt center on region 4
in the map. also that page should be designed better to lead users to go to the real app/map. it could have a search
there to get to a specific waterbody and keep the linking somehow but if a user ends up here we want them going to the
real pages."

Owner pick 2026-11-22 23:20 (dialog with the phone pictures A/B/C): "B: map preview" — the dot picture of the region's
waters, then the "Open the map" button and the search. Build it (self-hosted, ~10 KB per page as measured), phone +
desktop, light + dark. (Lane L2 had recommended A — App first; C was Search first. Pictures of all three:
`handoff/PLAN/app-integration/T1010-shots/` `option-A-*`, `option-B-*`, `option-C-*`.)

Done:
- Start link key `?fit=w,s,e,n` (a088c9cd0), not `?region=n`: the box is worked out by the SEO build from the release
  (`tools/seo` `regionBox`), so the app needs no region table and the same key frames any area a page names. Read once
  like `show`; a bad box, one off the globe, or one beside `regs=` / `show=` is refused out loud (`startLink.test.ts`);
  the Shell frames the browse map on it (`landOnBox`, `FitStart.shell.test.tsx`). Android's App Links take
  `canifishthis.ca/`, so the installed app reads `fit` too (28e68fb69, T605, `fitOf`).
- Region, letter and topic pages (54778147e) link `https://canifishthis.ca/?fit=…` ("Open the map of Region N"; no
  `date`). r-741815c6090d: Region 4 `?fit=-118.82,48.999,-114.074,52.412`; Region 1's box reaches Haida Gwaii.
  `deploy/staging/site.sh` keeps the query when it rewrites the hub link to staging.
- Option B (lane L16): region hub = dot picture (inline SVG at build time, `<title>` text alternative, tokens), then
  "Open the map of Region N" (`?fit=`), then a water search, then the crawlable lists as before (`tools/seo/regionMap.ts`,
  `site.ts`, `html.ts`). Search: hidden until its self-hosted script runs; loads `/assets/region-N.<hash>.json` on first
  use, up to 8 matches ranked (accents folded; "Name (formerly X)"); a stale file (404 after a release) says "reload it"
  and is reported once as "names file out of date (HTTP 404)" (classed expected in the worker `crashes.ts EXPECTED`);
  any other failed load reported once as app "region-page", route "other". Region hubs only (letter and topic pages keep
  the plain card). Tests: `site.test.ts`, `regionFind.test.ts`. Shots: `T1010-shots/after-B-*.png`. Region 4 (2823
  waters): picture 26.8 KB raw / 7.8 KB gzip; page 45 KB / 11.5 KB; names file 124 KB / 23 KB.

## Work (left)
- The owner's look (## Owner).
- STAGING feeds-worker deploy for the names-file 404 class (Claude, with the next staging release); PRODUCTION
  feeds-worker deploy is the owner's (with the 0.9.0 release).
- Not run: Lighthouse (SEO and accessibility scores no lower than now), the staging check (the link opens the staging
  map framed on the region, screenshot), the real MapLibre camera on that box (the render test checks the map is asked to
  frame exactly the box).

## Acceptance
- https://canifishthis.ca/region/4 → "Open the map" opens the map framed on Region 4 (web, and the app on a phone if
  the link opens the app).
- The region page has a water search that lands in the app; the crawlable links are still on the page.
- Tests pin both; Lighthouse SEO/accessibility not lower.

Risk: low (a new start-link key, read once). Size: M. Owner: picks the look.

## Owner

- [ ] 2026-11-22 — Look at the region page's option B (`handoff/PLAN/app-integration/T1010-shots/after-B-*.png`, phone and desktop, light and dark) and say if the look is right (A, B or C; A was recommended).

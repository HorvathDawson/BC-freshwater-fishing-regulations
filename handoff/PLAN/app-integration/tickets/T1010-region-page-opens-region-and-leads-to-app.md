# T1010 — Website: a region page's "Open the map" opens on that region; the page leads people into the real app

Status: IN REVIEW — owner: OWNER-TODO "T1010 region page option B: look at the after shots"

Source: owner 2026-11-22, on https://canifishthis.ca/region/4, verbatim: "if i click open map it doesnt center on region 4
in the map. also that page should be designed better to lead users to go to the real app/map. it could have a search
there to get to a specific waterbody and keep the linking somehow but if a user ends up here we want them going to the
real pages."

## Why the map does not move
The region page's button is `mapLink(site)` = `${site.origin}/` (`app/tools/seo/site.ts:87`, used at `:217`), a bare
link to the home map. The comment above it says why: "the app ... takes no region or bbox in a link yet". The start
link (`app/packages/ui/src/startLink.ts`, `ROUTE_KEYS`) knows a water (`show`, `regs`), a point (`here`) and a day, but
no area. `site.test.ts:302-307` pins the bare link, so the test has to change with it.

## Work
1. **A start link for an area.** Add one key to the start link, read once at start like `show` (and gone at the first
   write), that opens the map framed on a region: `?region=<n>` (or one general `?fit=w,s,e,n` box, which topic and
   letter pages can use too; pick one and say why in the ticket). The map already frames a box (`fit: {key, bbox}`, on
   web and native). The region boxes come from the release (the region polygons the bundle already has), not a table
   copied into the app. A bad value is refused out loud (`bad(...)` like `here`), never guessed. Unit tests in
   `startLink.test.ts`; a render test that the web map is framed on the box.
2. **The region page links with it.** `mapLink` takes the region; every region page (and its topic and letter pages)
   opens the map on that region. Update the `site.test.ts` pin: the link names the region and still carries no `date`.
3. **The page leads into the app.** Redesign the region hub (and the topic / letter pages built from the same
   template) so a visitor's next step is the live app:
   - At the top: one clear primary button "Open the map of Region N", and a **search box for a water in this region**.
     Choosing a water opens the app on that water (the same `/?show=<item>` link a water page's ‹ and × use, or its
     `/water/<slug>` page, which is the app itself). It works with no JavaScript as a plain list filter or a GET to
     the app; with JavaScript it suggests names as you type (the region's names are already on the page).
   - Below it: the crawlable links (topics, A-Z, every water) stay as they are, so search engines still reach every
     water page. They are kept, just not the first thing.
   - Text says what the app does that this page does not (open or closed today, flows, the map), in one line.
4. **Proof:** screenshots of the new page on phone and desktop width (sent to the owner before merge; T1010 is a
   look change, so he picks between at most two looks); the site build's tests; Lighthouse (SEO and accessibility
   scores no lower than now); the link opens the staging map framed on the region (screenshot).

## Acceptance
- https://canifishthis.ca/region/4 → "Open the map" opens the map framed on Region 4 (web, and the app on a phone if
  the link opens the app).
- The region page has a water search that lands in the app; the crawlable links are still on the page.
- Tests pin both; Lighthouse SEO/accessibility not lower.

Risk: low (a new start-link key, read once). Size: M. Owner: picks the look.

## Done 2026-11-22 (lane L2)
- **Start link key: `?fit=w,s,e,n`** (a088c9cd0), not `?region=n`: the box is worked out by the SEO build from the release
  (`tools/seo` `regionBox`: around the middles of the parts each region's keys hold), so the app needs no region table and
  the same key frames any area a page names (topic, letter, later a town). Read once like `show`, gone at the first route
  write; a bad box, one off the globe, or one beside `regs=` / `show=` is refused out loud (`startLink.test.ts`). The Shell
  frames the browse map on it (`landOnBox`) and does not open at the reader (`FitStart.shell.test.tsx`, mutation-checked).
  Native has no start link (D1), but Android's App Links take `canifishthis.ca/`, so the installed app IS reached by the link:
  it read `fit` as unread and reported it until 28e68fb69 (T605), which reads it with the same `fitOf` and frames the map.
- **Region, letter and topic pages** (54778147e) link `https://canifishthis.ca/?fit=…` ("Open the map of Region N"; no `date`).
  r-741815c6090d: Region 4 `?fit=-118.82,48.999,-114.074,52.412`. Region 1's box reaches Haida Gwaii (its MU 6-12 / 6-13 waters'
  keys are region 1 in the release). `deploy/staging/site.sh` now keeps the query when it rewrites the hub link to staging.
- NOT checked: the real MapLibre camera on that box (the render test checks the map is asked to frame exactly the box), and
  the staging screenshot (no deploy from a lane).

## The look: three options (pictures in `handoff/PLAN/app-integration/T1010-shots/`, phone 390 + desktop 1280, light + dark)
Mock-ups on the real Region 4 page and release (the search shows suggestions for "Koo" as JavaScript would draw them):
- **A — App first** (`option-A-*`): "Fishing in Region 4", one line on what the app does, a full-width "Open the map of
  Region 4" button, the water search under it; then "Browse Region 4's 2785 waters" (topics, A-Z, year card) as now.
- **B — Map preview** (`option-B-*`): a picture of the region made of its waters (each dot a part's middle, from the release)
  with the button under it; the search beside it on desktop, below on a phone. Costs ~29 KB of SVG per region page
  (~10 KB gzip) unless drawn as a file.
- **C — Search first** (`option-C-*`): the search is the first thing, the map button second (outlined), three cards for
  what the app does (open or closed today, river flows, every rule on the map), topics as chips.
Recommendation (lane L2): **A** — one obvious next step on a phone's first screen, the search right under it, smallest page;
B is the most inviting on desktop but heavier and the picture is not the real map.
The search without script: a GET form; where it lands (a filtered list page per region, or the app with a query key) is
part of building the picked look.

## Built 2026-11-22 (lane L16): option B (owner pick 2026-11-22 23:20)
- Region hub: dot picture of the region's waters (inline SVG drawn at build time, one dot per water, `<title>` text alternative, light and
  dark through the page's tokens), then the real link "Open the map of Region N" (`?fit=`), then a water search, then the crawlable
  lists as before (`tools/seo/regionMap.ts`, `site.ts`, `html.ts`).
- Search: hidden until its self-hosted script runs (no script: the lists below are the way in); loads the region's names file
  (`/assets/region-N.<hash>.json`) on first use, lists up to 8 matches as links to the water; a failed load is said on the page.
  Not reported to the crash endpoint (a hub page has no reporter config): left as is, say if wanted.
- Region hubs only. Letter and topic-by-region pages keep the plain "Open the map" card (a picture on each would add ~10 KB gzip to
  thousands of pages).
- Tests: `site.test.ts` (picture, text alternative, button after the picture and before the lists, names file, no image host),
  `regionFind.test.ts` (the shipped script). Shots: `T1010-shots/after-B-*.png`.

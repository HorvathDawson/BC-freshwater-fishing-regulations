# T1010 — Website: a region page's "Open the map" opens on that region; the page leads people into the real app

Status: OPEN, next release batch.

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

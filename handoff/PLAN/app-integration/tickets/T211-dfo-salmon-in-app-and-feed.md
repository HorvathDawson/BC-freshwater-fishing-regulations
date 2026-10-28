# T211 — DFO salmon regulations in the app, and the DFO feed (plan + build)

Status: DONE (parts 1, 2, 4) — 7dd3828c, afce6842, 28e7bfe9, 4b18a372; part 3 (historic matching) moved to the DFO-locations lane (coordinator 2026-10-27); owner: the feed cron + R2 sync (OWNER-TODO)

Source: owner 2026-10-27.

## 1. Salmon regulations in the UI — OPTIONS for the owner
Lay out 2–4 designs (with mock shots) for showing DFO tidal/freshwater salmon rules alongside the provincial regs: e.g.
a separate "Salmon (DFO)" section on the water page, merged into the day's answer with a DFO source badge, a salmon
map layer, or a per-species sheet. For each: what an angler sees on a sample day, how DFO's frequent changes and
effective dates show, how conflicts with provincial rules read, data/release size. Owner picks.

## 2. The feed — plan how it works
DFO notices change often (memory `dfo-salmon-churn-asymmetry`: waters ~never move, rules turn over ~50%/yr, scope text
drifts). Plan: fetch cadence, diffing, where the feed lives (like the gauge feed: local + R2 sync, owner-run upload),
how a rule change reaches the app without a full rebuild, failure handling, and the owner's role.

## 3. Extents must match every historic location
Build the DFO locator → extent table from ALL historic DFO scrapes (not only today's page), so a location that comes back
or reappears with drifted wording is already matched. Normalise scope text (the 99 dormant locations are part of this);
report coverage: historic locations matched / total.

## 4. New-location alert
When the feed sees a DFO location (or scope wording) that matches no known extent, it must NOT silently drop its rules:
hold them unplaced and NOTIFY the owner (FEED_NOTIFY_URL, like HYDAT) so the new locator → extent can be added to the
matches and the rules propagate. Include a test feed with an unseen location.

Order: 1 and 2 are plans for the owner; 3 and 4 are code once the plan is accepted.


## Owner pick (2026-10-27)
Option A. All salmon closed → "Salmon — closed"; else species grouped. Main-map toggle Standard / Salmon in standard regulation mode (salmon paints differently). Next: build A + the toggle, then the feed (parts 3–4).

Owner, later the same day: the Salmon paint must be FAST (toggle instant: no re-fetch, no re-tile; precomputed at
deliver time, read by the style through feature-state / a match); and every water DFO does not list for salmon is
CLOSED to salmon fishing — an explicit default in the data, not an absence.

## Built (2026-10-27, option A + the toggle)
- **Reach:** `pipeline/atlas/reach/dfo_salmon.py` places every curated DFO location with an extent through the same
  `build_reach` (`regional=False`; a variant takes its target's extent) → `<run>/dfo_salmon.jsonl.gz`, outside the
  rules' digest. The reach stage key now includes the DFO entry files; its outputs the new file.
- **Bundle → release:** `pipeline/deliver/salmon.py` joins the typed feed by fingerprint, builds CHAINS (the locations
  on a section, narrowest first: the Skeena cascade) and precomputes per chain and day of the year each species'
  verdict, the gear lines and the map status (runs). Chain 0 is the UNLISTED water, closed every day; a species
  nothing opens that day is closed (`NOT_LISTED`). Tables `salmon_doc` + `salmon_section`; release file `salmon`
  (`salmon/1`: JSON half + sid runs → chain), gate 600 kB (provisional, unmeasured). A location with no extent ships
  under its water's item ids and is never placed. Pack contract regenerated (`pack.generated.ts`).
- **App:** core `salmon.ts` (decode, day lookups, `salmonCard`, words); `loader.salmon()`; the "Salmon · DFO" card
  under the Keep tab (`SalmonCard`, `useSalmonCard`) with its own source + as-of line, "Salmon — closed" when every
  species is closed, species grouped otherwise, "Location not yet mapped" for unplaced DFO places on the water. Map:
  the Standard / Salmon toggle in the Map tab's regulations legend; `salmon` colour mode (profile, label `sp` =
  chain, day in the style) + `salmon_status` (pushed, native); tokens `color.salmon.*` (aliases of status hues);
  the salmon channel is installed on the Map tab whatever the toggle says, so a toggle is a mode change only.
- **Tests:** `pipeline/tests/test_salmon_file.py`; core `salmon.test.ts`; map `salmon-paint.test.ts` (paint, the
  unlisted chain paints closed, toggle = zero feature-state calls, cost independent of section count, < 16 ms over
  20k sections); ui `salmonColour.render.test.tsx` (file asked once, before any toggle; feed identity unchanged by
  the toggle); ui-native `SalmonCard.test.tsx` (card, collapse, unlisted → closed, unmapped tag, toggle).

**Deliver output changes** (new reach file, bundle tables, release file). Not built here: the owner runs
`python -m pipeline build` (reach reruns: its code and key moved; then deliver). Left: the live feed (part 2 —
the app reads only the release's snapshot), parts 3–4; `request-walk.pin.json` may need a re-cut (the salmon file
is fetched on the Map tab after the day's status is in); `salmon` gate to set from the first measured release.

## Built (2026-10-27, parts 2 + 4: the live feed and the new-location alert)
Owner corrections taken: offline = the LAST FEED CAPTURED on the device (its time on the card), the release's file only
when none was ever captured; part 3 dropped here (another lane owns the 346 historic locations); part 4 feeds the same
DFO mode queue.
- **Feed job:** `pipeline/regs/dfo_salmon/live.py` (`python -m pipeline.regs.dfo_salmon.live [--offline]`): fetch →
  parse → untangle/verify → typed → `entries.reconcile` → join by fingerprint (entry files read only) → per LOCATION and
  day each species' verdict index (-1: says nothing) + gear → `<feed>/index.json` (`dfo-feed/1`; 247 locations, 45 KB raw)
  + `unplaced.json`. Holds: a failed fetch keeps the region's last answers (`stale`; 2 in a row notify); a page that will
  not parse, or a structural change, holds the region; `section_moved` keeps that location's last answers. Changed pages
  kept in `cache/dfo_salmon/history/`. Dev: `serve-tiles.mjs` serves `/feeds/dfo/*.json`; prod: R2 `feeds/dfo/`.
- **Part 4:** a fingerprint no entry knows is UNPLACED (drift / new / new_water), its rules held in `unplaced.json`
  (never dropped), the owner told once per (region, fingerprint) via `FEED_NOTIFY_URL` (`gauges.feed.notify.send_due`,
  now with `key_of` / `message_of`). `live queue` seeds them into the entry files (`entries seed` + `group`: a rewording
  proposed as a variant) so the review app's DFO mode lists them like any location; the next run joins them.
- **Deliver:** every location is now in its chains (dormant ones say nothing) so the feed paints a revival without a
  rebuild; an unmapped place dormant at build carries `d: 1` (off the card until the feed lists it). Takes effect at the
  next deliver run (release `salmon` grows a little; no format change).
- **App:** core `salmonFeed.ts` (`parseSalmonFeed`, `composeChain` == the pipeline's `live.compose` ==
  `deliver.salmon.chain_calendar`, pinned by `salmonFeed.golden.ts`; `pickSalmon`: the capture unless the release was
  cut from newer pages; `salmonFeedLine` "Checked with DFO …"). data `feed/salmon.ts` (`httpSalmonFeed`: fetch hourly at
  most, capture kept by `openSalmonFeedStore` — a file on the phone, a storage key on the web; `withLiveSalmon` lays it
  over `loader.salmon()` once, so the card and the map read one answer and the toggle never re-fetches). App wires
  `ENDPOINTS.dfoFeed`. Card: `salmon-checked` line under the page date.
- **Tests:** `pipeline/tests/test_dfo_live.py` (compose == chain calendar; today's pages join all 247; the unseen-location
  test feed: drift + new_water held, told once, everything else publishes, not told again; fetch failure / parse hold;
  `queue` → seeded → next run joins; the app golden is current); core `salmonFeed.test.ts`; data `feed/salmon.test.ts`;
  ui-native `SalmonCard.test.tsx` (checked line).
- Not built: a `changes.json` diff log (the card has no "recent changes" fold in option A); a feed staleness line in
  `staleness.ts` (the card shows the capture time instead).

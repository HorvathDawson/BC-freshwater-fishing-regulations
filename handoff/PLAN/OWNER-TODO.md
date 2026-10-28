# Owner to-do

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **Staging is live (2026-10-28): https://staging.canifishthis.ca** (app + SEO pages, release r-46be3c135e68; data from
  data-staging.canifishthis.ca). Check: the map paints and a tap opens regs; a few `/water/<slug>` pages (e.g.
  `/water/vedder-river`), `/region/2`, `/sitemap.xml`, and an old link such as `/waterbody/930-055749/` (one 301 to
  `/water/san-juan-river`). Staging sends `X-Robots-Tag: noindex`. Decide on the zone: (a) Cloudflare **managed robots.txt**
  is prepended to ours (it says `Allow: /` for everyone and blocks AI crawlers); (b) **Bot Fight Mode / JS detections + Web
  Analytics** inject ~2.6 s of script into every page (Lighthouse Performance 69-76 instead of >= 95; our pages have no
  JS). Prod's data worker `bc-fishing-r2` double-gzips release files: use `deploy/staging/data-worker.js` for prod (T86).
  Details: handoff/PLAN/seo/staging.md.
- **SEO / domain (T188 plan, T198):** confirm you own canifishthis.ca and canifishthis.com; at the registrar/DNS set
  .com and www to 301 to https://canifishthis.ca. Once hosting (T86) is live: add Domain properties in Google Search
  Console (DNS TXT), import to Bing Webmaster, submit /sitemap.xml, and give Claude the tokens. Later, send the
  outreach by hand (T197). Plan: handoff/PLAN/seo/PLAN.md sections 4 and 7.

## Reviews
- **T227 DFO places not on the map (2026-10-28): review app → DFO, open each, "accept suggestion", Save extent.**
  - accept in review app → DFO: Cowichan — Sandy Pool sign → tidal boundary (= FWA mouth; active, high); weir/Cowichan Lake → 66 Mile Trestle (high); CNR mile 70.2 → Skutz Falls (high, new Skutz cut); "incl. tributaries and Cowichan Lake" = whole river + lake + tributaries (medium: lake's inflows counted). Last three dormant.
  - accept in review app → DFO: Capilano — "including tributaries" = whole + tributaries (high).
  - accept in review app → DFO: Kitimat — downstream / upstream of Hwy 37 bridge (high, OSM-checked); hatchery outfall signs = your 08-10 pins (high; west-bank-only width not modelled).
- **Release r-a871447c48ba verification follow-ups (2026-10-28):** (1) run `node app/design/regs-v11/port/sync.mjs`
  (Claude may not): the snapshot + golden carry the Q62/Q63 words (9517ff8e); sync should reproduce golden.json's text
  exactly; pin geometry may move where the longer words wrap (then re-pin harness.parity / exclusions and re-run
  `pnpm -C app exec vitest run conformance/src/regs`). (Whonnock 08MH100 and Beattie "Trout" answered 10-28: T228.)
- **T228 re-cuts on release r-e01dc8f3719f (2026-10-28):** Claude cannot run `node app/design/regs-v11/port/sync.mjs`.
  Run it, then `pnpm -C app regs-fixture`, and commit what they write (taps, golden, hydrate, dev regs-release fixture).
  This release changes 34 waters' parts (no-access split, e.g. Coquitlam is now 4 parts). It adds salmon-chain pieces to
  `part_stretch` / `part_mark`, and drops 1,618 dormant gauges from Conditions (148 lake gauges among them). Look at
  Coquitlam and Cowichan on the Map tab: shots in `app-integration/T228-shots/`.
- **T224 name variants (2026-10-28):** local sources name FWA lines the app has no name for (Kilgard Creek and more):
  ticket T224 "Findings" (data: `handoff/PLAN/curation-review/name-variants/candidates.json`). Two calls: (1) does an accepted
  variant become a SEARCH ALIAS only, or also the DISPLAY NAME of an FWA-unnamed water (today any alias on an unnamed
  line silently becomes its name)? Default: alias only, display only by your per-name yes. (2) May high-confidence
  ones auto-accept (≥ 80 % line overlap both ways or a reviewed/bound gauge, one candidate, no rival name, the line
  FWA-unnamed)? Default: no, you confirm each. Also 08MH079's blk has TWO names: Kilgard Creek (Abbotsford) vs
  Lonzo Creek (North Fork) (the ECCC station name) — which one, or both?
- **T202 basemap (2026-10-27):** the basemap is now z13 (918 → 274.5 MB). Look at the app at your spots, z10–z18,
  light and dark: trail and FSR names should show from z15. Then say "delete the old basemap", and Claude removes
  `data/generated/scratch/basemap-pre-T202-20260925.pmtiles`. Grid coarsening shipped too: the basemap is now 274.5 MB. Shots
  and numbers: `handoff/PLAN/tiles/T202-basemap-audit.md`.
- **"Needs a split" follow-ups (2026-10-27, live in r-8b43fb267199):** (1) the three guessed `low` cuts are in the
  review app, **Low-confidence cuts** queue (top bar button; or http://127.0.0.1:5183/?lowcuts) — each entry opens on
  satellite, flown to the cut, with why it is a guess: Chapman lower falls + 100 m below (SCRD survey point, ~576 m below
  the power line), Deena "second bridge" (no bridge mapped: a point 5 km above the tidal boundary), Mamin "third bridge"
  (Mamin Main road, 9.1 km above tidal). Right = Place y; wrong = Place n, "split misplaced", pick the right point on
  the map in the extent fix. (2) Columbia
  r11 "from Mud Lake": FWA names no Mud Lake, so it runs Fairmont → Columbia Lake (the outlet wetland, wbk 329093907, is
  the likely Mud Lake) — say if Mud Lake is elsewhere; (3) Stave r1/r2 (Ruskin/Northrop spawning channels) stay notes:
  the channels have no sections in the atlas — adding them is Claude's work (T222), nothing for you here.
- **T159 08MH079 (2026-10-28):** the station sits on Kilgard Creek (FWA-unnamed, blk 356314888, a Marshall/Lonzo
  tributary, 69 m), not on Lonzo/Marshall itself. Say yes and Claude adds the `review.json` bind (ticket T159 has it);
  the matcher rerun + `promote --only 08MH079` follow.
- **T204 phone check:** Kanaka Creek's map now has two marks (112 Avenue bridge, Cliff Park Falls) and its lower part is
  two separately selectable stretches ("… · stretch 1 of 2"); say if the stretch wording should differ.
- **Tidal boundaries (T212, 2026-10-27, critical):** open the curation review's **Tidal** tab. 1,163 proposed tidal
  cuts (every named stream your tidal polygon crosses + every named ocean-entering FWA stream); start with
  "words name it" (31). 19 agree with the book / DFO words; look first at Nimpkish (polygon 5.7 km up vs "100 m above the Hwy 19 bridge" ≈ 1.2 km), Tlell (polygon
  misses it; synopsis says 1.5 km above Hwy 16, DFO says ~1 km) and Squamish (the dyke line is not located).
  "accept proposal" (a) pre-fills Place y + the cut; Record mark is the only write. Summary:
  handoff/PLAN/curation-review/tidal/tidal-candidates.md. (Capilano = source text and Cowichan = river mouth are
  already yours, 10-27, a86fddbf; 3 tidal marks recorded 10-28.)
- **Cranberry canyon cuts:** the bottom is a DEM estimate (low). In the review app, **Low-confidence cuts** queue (with
  White River and Chuckwalla River, the other `low` cuts).
- **Map look (T64, T85, T113, T152; all built and live):** look at the map (wetland-heavy Fort Nelson and the
  Chilcotin without unnamed wetlands; z13–z18 bends on the Fraser/Vedder now overzoomed from z13; Vancouver's dense z13
  tiles; no OSM creeks under ours at z14+) and say if the look suffers — the basemap's OSM wetland fill is one style layer
  away. Best done in the same sitting as the T202 basemap look above (basemap z13 shipped 10-27).
- **Q4 / T55 + T149 — slough and canal candidates: review app, Sloughs tab only** (:5183, `#sloughs/`). All 2,424
  candidates with their pre-marks; your 150 review-page verdicts show as "page: …" and your 8 free-text notes as
  "your note" (incl. 329097416 Cruickshank, the one whose verdict you cleared: "similar this can be on moat lake").
  Confirm the pre-marks (one, or accept all visible); then Claude folds them in, dry run first.
  The old slough page (artifact Es3Mokt7Fs2yNvm7JgQua8) is retired — do not mark there; its whole database (151 verdicts + 58 spot checks)
  is saved in `handoff/PLAN/sloughs/marks-final-2026-10-28.json`; nothing else reads it.

Cleared 2026-10-28 (already settled, stale-item sweep): Q114 pilot (you dropped it 10-25; you do the real review),
T211 feed plan (built 10-27 with your corrections; only the cron + R2 sync below is left), Q145 (= that feed: 6-hourly
in season), Q32 (T95 dropped; salmon is T164/T211 now), Q142 (lake splits deferred, your 10-28 "lake split later"),
Q150 (answered 10-23: own keeps amber), Q119 (Claude's own audit — Claude re-derives it, T125), T185 decide (your
marks are the decisions; applied, Hatzic left alone), DFO-salmon build + "on parts" rebuild (both live in
r-46be3c135e68), run-timing go (you said go 10-25 → T199; Q144 stays).
Answered 2026-10-24: Q125 (muted fill), T179 (400 m; a curated cut never moves; 25 m lake-anchor proposals; merged
splits keep every name and id), PSF permission (T162; their reply is not stored), braids marked (T67 promoted), Q7 (delete old releases — pruned to live + previous; `python -m pipeline.deliver.release.prune`).
Answered 2026-10-23: Q5 + Q34–Q41 (curation plan approved, defaults; splits mid-review must work; salmon pass later = T132), Q13 (Claude runs the reader gate), Q44 (no Level from a side channel; Flow only if it carries a large share), Q53 (as built), Q45 (leave unbound).

## Your open questions (31 listed below — each built with a default; the table in tickets/README.md has the wording)

Triaged 2026-10-23: 64 smaller calls were decided by Claude and 12 were already answered or obsolete — all recorded in
decisions.md "CLAUDE 2026-10-23 — owner-question triage" (read and reverse any). What is left needs you:

- **Readings of the book (a wrong call misleads anglers):** Q124 a river's rules on unnamed widenings (456 polygons) ·
  (Q87, Q140, Q62, Q63 answered 2026-10-28.)
- **Which gauge speaks (your policy):** Q31 the gauge rule · Q103 diversion
  gauges stop at their canal (24 sections lose every gauge) · Q113 cuts for 38 silent stations (atlas rebuild).
- **How it looks (taste):** Q130 each model's line under Change · Q132 day chips may still use ELF · Q79 "river" vs "stream" on the page ·
  Q91 "no gauge close enough" wording · Q123 how the Map view marks the selected part (built: dim the others; thin halo in
  `app-integration/T151-shots/alt-halo--*`) ·
  Q2 / Q139 / Q47 no-access and tidal notes on Conditions.
- **Vessel rules (T209):** the 10 close-wording undrawn parts kept ("various locations", "swimming areas") in
  on-parts.md: whole water too, or leave as notes? (`max_power_kw` 7.5 is stored on the 162 electric-only rules, 10-28.)
- **Salmon (T164):** confirm the proposed DFO cut points in T164's DFO review mode (96 bridges/signs/dams).
- **User accounts (T223, new 2026-10-28):** one account for web + phone. Questions Q-T223a–f in the ticket: auth
  provider, what syncs (spots / photos / pins / settings), privacy of spot locations, offline merge rule, optional or
  required, before or after first release. Nothing is built until you answer.
- **Batch 7:** Q146 re-centre the map when the parts list opens · Q147 steelhead lake wording on newly taken ponds ·
  Q148 dams → "lake" for the slough candidates · Q149 Bonaparte by hand.
- **Gauge gaps on main rivers (T174, shipped r-bda3fea28838):** Q153 (side piece adopts the largest ANSWERED river section) and
  Q155 (a water's own lines between alike pieces) shipped: +407 Flow / +327 Level sections, 0 answers changed — veto =
  one switch each in panels.py. **Q154 is yours:** the "double-line polygons" are small FWA lakes; walking them as the
  river moves 96,590 sections (+95k Flow) — kept off. Numbers in tickets/T174. Your "how small?": examples +
  a proposed pool-only cut (< 1 ha and ≤ 2× the river's width: 10,650 of 136,080, 11,423 sections) in
  curation-review/q154-examples.md. **See them in the review app:** Sloughs tab → "Small ponds (Q154)"
  (`#sloughs/q154/`): the 20 md examples + a 60-pond sample (caught / just missed / clearly not, all regions), each
  on the satellite map with its outline and river; mark each pool / real lake / unsure (keys p / l / ?) and the tally
  shows how your calls line up with the cut — then say yes / no / another cut.
- **T89 six waters:** Frog/Hidden/Secret/Square (5-6), Redfern (5-15), Squirrel (6-1) — nothing of those names near
  the printed MU; if you know where any is, the proposal file `handoff/PLAN/curation-review/t89-awaiting-water.json`
  lists the leads and the fix path.
- **Run timing (T199, last band; you said go 10-25):** Q144 (steelhead first, passage on a paler ramp, its own layer).
  The release total no longer blocks it (T182: per-file gates + a web cold-load gate; 20 MB offline backstop).
- **T182 caps (built with defaults, veto if wrong):** offline total 20 MB (was 15); cold-load gate 3.35 MB (salmon on
  the first Salmon press, as you decided 10-28).
- **Scope and big rebuilds:** Q10 imagery corridor (offline corridor + pools go/no-go; online Esri is settled).
- **Things only you can do or know:** Q4 slough marks — now in the review app's **Sloughs** tab (`#sloughs/`): 936 pre-marked stream to confirm (801 narrow, 135 wide marsh; "accept all visible" after checking), 324 on your list (113 intermittent) · Q126 were you
  on Level? · Q9 phone testing · Q26 hosting/app name ·
  Q28 iOS timing · Q27 sign-off walk.

## Things only you can run or provide
- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **R2 upload** of a new release and of the gauge feed, with your credentials:
  `python -m pipeline.deliver.release.publish --release data/release/$(cat data/release/LATEST) --yes` (records
  data/release/PUBLISHED so the prune keeps it). When you set up the feed sync, upload ONLY `data/generated/gauges/feeds/`,
  not `feeds.runs/` (T177).
  T215: a release now also carries OUR synopsis PDF (`synopsis-<sha12>.pdf`, ~24 MB, raw, immutable; named in the manifest's
  `synopsis`): the same publish command uploads it, and the bucket's CORS must allow GET from the web origin (the app fetches it
  once per release to draw the highlight). Until a release with it is uploaded, "open the PDF" stays the plain gov.bc.ca link.
- **`FEED_NOTIFY_URL`**: set it where the feed cron runs to get one message per new HYDAT release
  (ntfy / Discord / Slack). Off until set.
- **`scripts/update-hydat.sh`**: run it when that message arrives.
- **DFO salmon feed cron + R2 sync (T211 part 2):** on the feed host, every 6 h Jun 1–Oct 31 and daily otherwise:
  `.venv/bin/python -m pipeline.regs.dfo_salmon.live` (no credits, no secrets; ~15 s; writes
  `data/generated/regs/dfo_salmon/feed/index.json` + `unplaced.json`), then sync ONLY that `feed/` dir to R2
  `feeds/dfo/` (never `feed.notified.json`, which sits beside it). Same `FEED_NOTIFY_URL` as the gauges: one message
  per held new / reworded DFO place, per region held (structural change, page that will not parse, two failed fetches).
  When one arrives: `python -m pipeline.regs.dfo_salmon.live queue --regions <n>` (or ask Claude), commit the entry
  files, and the place shows in the review app's DFO mode. Local dry run: `... live --offline` (cached pages).
- **Phone testing** (T77, Q9), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
  - Gesture/navigation parity on the phone (T50, T77; owner 2026-10-27; same RNGH code as web, not seen on a device):
    Android edge-swipe back and the back button through the back table (HANDOFF-ui "Back, everywhere");
    `android:enableOnBackInvokedCallback="false"` in the manifest (predictive back off) is deliberate until judged;
    sideways view swipe on a water (Regulations/Conditions/Map) incl. not starting in the 24 px edge zones and not
    fighting the map pan / chart / minimap shields; sheet drag-to-close and sheet scroll-vs-drag hand-off; iOS
    left-edge back (`EdgeBack`); Spots/Conditions/More tab back goes to the Map tab; part step and upstream/downstream
    (▲/▼) keep one back; section highlight while scrolling the regs tabs; keyboard (search) vs sheets.
  - Native colouring (T74, 2026-10-27; emulator: 0 grey frames in 49 sampled pan frames): on the phone, pan and fling
    at z9–z13 and say whether any water shows plain/grey before its colour (`EXPO_PUBLIC_COLOUR_PROBE=1` logs
    `T74PROBE` lines per pan for the numbers); the five hatches;
    "At my location" (asks on the tap, then drops the pin where you stand); a theme switch keeps the hatches.
  - iPhone (T217, 2026-10-27; simulator: 0 grey frames in 49 sampled pan frames, pill/hatches/At my location seen):
    the same colouring checks on a real iPhone, plus the location system prompt on the tap (pre-granted on the
    simulator) and whether the gauge pill's slightly wider padding on iOS is acceptable.
  - Android on a real phone (T50, 2026-10-27; emulator-verified): the rule-book link opens the highlighted PDF in your
    PDF app with the entry's page FIRST and the whole book after it — say if that reads right; nothing sits under the
    status bar or the gesture / 3-button bar; changing the system font size keeps the water you had open.
- **Sign-off walk** at the end (T87) before app/design is deleted.

- **SEO, v1 pages (2026-10-27):** keep your existing Search Console property; when the new site is live add the new sitemap
  beside the old one, and for two months watch Pages > Not found (404) and Redirect error (the 19,331 old `/waterbody/` URLs
  should show as Page with redirect). Deploy the generated `_worker.js` with `run_worker_first = ["/waterbody/*"]` (T86 hosting).

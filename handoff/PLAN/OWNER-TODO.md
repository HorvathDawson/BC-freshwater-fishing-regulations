# Owner to-do

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **0.4.0 on Play internal (versionCode 7, 2026-10-31, `89ebd94bf`; release r-4e709418ebd8 on staging, smoke 18/18):**
  install from the tester link and look at (1) the T316 spot marks on the map (Noto Sans names); (2) a spot saved
  offline on a river fills by itself back online (T314). Prod stays yours:
  `pipeline.deliver.release.publish --release data/release/r-4e709418ebd8 --min-app-version 0.3.0 --yes`.
  New in this release: the 08MH044 cut moved to Albion. The build's braid rule carried it across Bedford Channel and 2
  of its tributaries. So Bedford Channel now has 2 sections and no gauge; before, it was answered "fair" by 08KA007
  Fraser at Red Pass. On the Fraser, the length cut at Bedford Channel moved to the gauge. The cut is still labelled
  "Fraser River At Whonock" (old HYDAT name). Say if you want any of that different.
  Still red, not from this batch (other lanes, in flight): pytest 7 fast / 2 slow failures (T322 slough-fold gate
  allowlists, T325 lock/write-through + DFO lock tests, review export after the Hatzic mark = `entries.json` not
  re-exported, status_index skip, T57 sweep fixture, a flaky browser test); release-perf A5 search p95 ~9 ms vs its 8 ms
  ceiling (same on r-038e6: the laptop, not the release); knip on the untracked `app/tools/build-play-graphics.mjs`
  (pngjs).
- **DFO wording registry (T321, 2026-10-31):** review the 85 PROPOSED entries in `data/curated/dfo_salmon/wording.json` (one a line: kind structure/note/provenance + what reads it; mark `accepted` or `rejected`), esp. the 15 notes. Then the feeds worker deploy (`npx --prefix app wrangler deploy -c app/workers/feeds/wrangler.toml`) so new wording emails you and the card shows DFO's words. After edits: `deploy/staging/sync-inputs.sh` (or ask Claude) — no worker redeploy needed.
- **Build pass (T266/T295/T297, 2026-10-31):** review `handoff/PLAN/app-integration/build-pass-2026-10-31.md`. The proof side run matched r-51d80e68f056 byte for byte, except the tile_version (T305) and the publish stamps. Nothing was promoted. Yours: (1) confirm the decided-data split, one dir `data/curated/decided/`; (2) `mined_names.json` lacks your 10-29 Sheldens Creek accept, so run `python -m pipeline.tools.mined_names` with the next atlas rebuild. (3) The slow suite has 8 failures on live data that predate the pass (T306; status_index 198/6000 disagree is the one to look at).
- **App test bed (T255, 2026-10-29):** Android emulator scenarios for downloads/sync/offline — `app/tools/testbed/README.md`; last report `handoff/PLAN/app-integration/T255-shots/run-final/index.html`. Found and ticketed: the first install crash-loops on Android (T262, high), Wi-Fi return does not start a held update (T265).
- **ODbL offer (T235, 2026-10-28):** done as you asked (`data/LICENSE-ODbL.md`, `_licence` in each file). Yours: (1) the repo has no LICENSE of its own — the notice says the rest is "under its own terms"; pick one if you want it stated; (2) the app links files on the `redesign/stream-sections` branch (`licences.ts` `REPO_BLOB`) — flip to `main` when it merges; (3) the no-access land (incl. Malcolm Knapp) and reserve outlines are OSM, not the Province — now credited as OSM.
- **Update testing on your phone (T251, 2026-10-28):** dev build → About → Updates log → channel Test → reopen; then `node deploy/staging/test-updates.mjs reset | feed-bump gauges | release 1 | corrupt | missing | feed-404 dfo | min-app 99.0.0` and Check now (steps in tickets/T251). Before prod (T86/T240): the production data worker must allow the `X-App-Version` header (copy staging's data-worker.js CORS line), or web update checks fail.
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
- **Tidal close-out (2026-10-29), one yes/no proposal:** Mamquam Blind Channel — the DFO 2015 tidal guide calls it
  tidal; make `wsc:900-103611` "Mamquam Blind Channel Wetland" whole tidal water (its "Trib 1" stays fresh)?
- **T227 DFO places not on the map (2026-10-28): review app → DFO, open each, "accept suggestion", Save extent.**
  - accept in review app → DFO: Cowichan — Sandy Pool sign → tidal boundary (= FWA mouth; active, high); weir/Cowichan Lake → 66 Mile Trestle (high); CNR mile 70.2 → Skutz Falls (high, new Skutz cut); "incl. tributaries and Cowichan Lake" = whole river + lake + tributaries (medium: lake's inflows counted). Last three dormant.
  - accept in review app → DFO: Capilano — "including tributaries" = whole + tributaries (high).
  - accept in review app → DFO: Kitimat — downstream / upstream of Hwy 37 bridge (high, OSM-checked); hatchery outfall signs = your 08-10 pins (high; west-bank-only width not modelled).
- **Release r-a871447c48ba verification follow-ups (2026-10-28):** (1) run `node app/tools/regs-recut/sync.mjs`
  (Claude may not). It re-cuts taps.json + hydrate.json only: golden.json is frozen since T87 deleted the prototype
  (2026-10-28), so the golden re-extraction of the Q62/Q63 words is moot. (Whonnock 08MH100 and Beattie "Trout"
  answered 10-28: T228.)
- **T228 (2026-10-28):** two DFO queue items from T228. (1) Klinaklini: place "Management Units 5-6 to 5-11 boundary"
  as a split (pre-filled point). (2) Bella Coola: exclude the Atnarko (gnis:17209) from its tributaries. The re-cut
  commands come after the next release (one more rebuild is coming).
- **T224 names (2026-10-29, live in r-62e2f3a8c81e):** review app **Names** tab (:5183, `#names/`), reject any that
  are wrong among these 20 auto-accepted names (list + why in tickets/T224 "Owner spot-check"): Cat Stream (applied: a 3 km OSM way
  matches 95 %; within your rule), The Cut Coulee, Cohen Creek, Benes Creek, College Creek, North McNaughton Creek, Brigade Bay Creek,
  Problem Creek, Coho Creek (OSM); Corinne Brook, Jantz Stream, Marshall Creek, Gill Brook, Mckay Creek, Shamrock Creek,
  Smith Brook (Abbotsford); Inkaneep Creek, Meadow Creek Diversion, Quinsam River Diversion Headpond, Noble Creek (ECCC).
  No rejects = done. The rest of the review list (36 high, 552 medium, 8,061 low) is optional.
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
  the printed MU; if you know where any is, the proposal file `data/generated/review/t89-awaiting-water.json`
  lists the leads and the fix path.
- **Run timing (T199, built 10-28 on staging; Q144 answered with the defaults):** (1) run
  `gh workflow run feeds-runtiming.yml --ref redesign/stream-sections -f upload=true` once (uses the repo's R2 secrets;
  staging only) and check its report; (2) after every atlas promote, rebuild the binding base on the Mac:
  `python -m pipeline.runtiming.feed.base --upload --yes`, then run the workflow; (3) when happy, uncomment its
  monthly schedule; (4) look at Run timing on the phone (T199-shots/).
- **T182 caps (built with defaults, veto if wrong):** offline total 20 MB (was 15); cold-load gate 3.35 MB (salmon on
  the first Salmon press, as you decided 10-28).
- **Scope and big rebuilds:** Q10 imagery corridor (offline corridor + pools go/no-go; online Esri is settled).
- **Things only you can do or know:** Q4 slough marks — now in the review app's **Sloughs** tab (`#sloughs/`): 936 pre-marked stream to confirm (801 narrow, 135 wide marsh; "accept all visible" after checking), 324 on your list (113 intermittent) · Q126 were you
  on Level? · Q9 phone testing · Q26 hosting/app name ·
  Q28 iOS timing · Q27 sign-off walk.

## Things only you can run or provide
- **Store listing sign-off (T244, 2026-10-29)**: read `app-integration/store/listing-draft.md` and `store/privacy-labels-draft.md`
  (nothing is submitted). Decide: the **app icon / adaptive icon / feature graphic** (DONE from the favicon, per your answer: `T244-shots/icons-preview.png`; add words to the feature graphic if you want), the support URL, categories. Contact = support@canifishthis.ca and the Location row (declared: collected + shared with Open-Meteo) are decided.
  Look at the new splash: `app-integration/T244-shots/` (web boot screen light/dark, native art); it shows on the next native build.
- **Production run (T240 section 11, 2026-10-29)**: the prod commands for map-assets (T270), HYDAT day flow (T274), the report-purge deploy (T271),
  and the SEO build against the prod app export (`--app`, T269/T279) are written in `tickets/T240-production-launch-checklist.md`.
- **Google Play setup (T268), when Google finishes verifying your Play account**: create the app ca.canifishthis.app,
  accept Play App Signing, upload the first AAB `app/dist/aab/latest.aab` by hand to Internal testing, add your Gmail as
  tester (opt-in link into `~/.canifishthis/play-test-url`), App content (privacy policy, data safety, content rating;
  our answers in the ticket), service-account JSON to `~/.canifishthis/play-service-account.json`. Unblocks T267
  (`deploy/staging/build-apk.sh --play`). Back up `~/.canifishthis/` (upload key + password).
- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **R2 upload** of a new release and of the gauge feed, with your credentials:
  `python -m pipeline.deliver.release.publish --release data/release/$(cat data/release/LATEST) --yes` (records
  data/release/PUBLISHED so the prune keeps it). When you set up the feed sync, upload ONLY `data/generated/gauges/feeds/`,
  not `feeds.runs/` (T177).
  T215: a release now also carries OUR synopsis PDF (`synopsis-<sha12>.pdf`, ~24 MB, raw, immutable; named in the manifest's
  `synopsis`): the same publish command uploads it, and the bucket's CORS must allow GET from the web origin (the app fetches it
  once per release to draw the highlight). Until a release with it is uploaded, "open the PDF" stays the plain gov.bc.ca link.
- **Feeds (T225)**: the gauges, DFO salmon and in-season feeds now run on the Cloudflare Worker `bc-fishing-feeds-staging`
  on a schedule (STAGING, switched on 2026-10-28; status: https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status).
  Emails go to your verified address: DFO places / in-season changes / a whole feed failing at once, the rest in one daily
  digest. When a DFO email arrives: `python -m pipeline.regs.dfo_salmon.live queue --regions <n>`, commit the entry files,
  then sync them (below). Production switch-on (worker + prod bucket) is yours.
- **Feed sync workflows** (`feeds-sync-inputs.yml`: DFO entries + roster + in-season matches; `feeds-hydat.yml`): NO new
  secrets — they use the repo's existing `CLOUDFLARE_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and refuse any
  bucket but `bc-fishing-regulations-staging`. Token scope: owner 2026-10-29 says it covers staging + prod (the workflows'
  exact-bucket guard keeps them on staging). Manual (`workflow_dispatch`); listed in the Actions tab
  only once on `main` — until then `gh workflow run feeds-sync-inputs.yml --ref redesign/stream-sections -f what=both -f
  dry_run=true`. The Mac twin: `deploy/staging/sync-inputs.sh`.
- **`scripts/update-hydat.sh`**: run it when the "New HYDAT" email arrives (after the HYDAT action).
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
  - Accessibility (T243 → T258, 2026-10-29): a TalkBack / VoiceOver walk (map → water → regs → conditions → salmon →
    menu) and the largest system text; the bottom tab labels stop growing at 1.3× (Q160) — say if that is wrong.
  - Regs scroll (T50, 2026-10-29): scroll a water's Regulations, switch to Conditions / another tab / change the font
    size, come back — it should be where you left it.
  - Offline maps (T238/T259): More → Offline maps; which map regions a first install keeps (Q161, default all).
- **Sign-off walk** at the end (T87) before app/design is deleted.

- **SEO, v1 pages (2026-10-27):** keep your existing Search Console property; when the new site is live add the new sitemap
  beside the old one, and for two months watch Pages > Not found (404) and Redirect error (the 19,331 old `/waterbody/` URLs
  should show as Page with redirect). Deploy the generated `_worker.js` with `run_worker_first = ["/waterbody/*"]` (T86 hosting).
- **Gauge 08MH044 moved (HYDAT 2026-10-19):** ECCC renamed FRASER RIVER AT WHONOCK → AT ALBION and moved it ~6 km
  downstream. DECIDED 2026-10-31 (owner: use HYDAT's point): matches.json row moved (1e72e203e); lands with the next
  `--atlas` rebuild (it moves a gauge cut).

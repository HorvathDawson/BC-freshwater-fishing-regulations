# Owner to-do

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **DFO salmon feed plan (T211 part 2):** approve the feed plan in handoff/PLAN/dfo-salmon-ui/PLAN.md §2 (6-hourly in
  season, R2 `feeds/dfo/`, rule changes with no rebuild). Option A + the map toggle are built (you picked A, 10-27).

- **SEO / domain (T188 plan, T198):** confirm you own canifishthis.ca and canifishthis.com; at the registrar/DNS set
  .com and www to 301 to https://canifishthis.ca. Once hosting (T86) is live: add Domain properties in Google Search
  Console (DNS TXT), import to Bing Webmaster, submit /sitemap.xml, and give Claude the tokens. Later, send the
  outreach by hand (T197). Plan: handoff/PLAN/seo/PLAN.md sections 4 and 7.

## Reviews
- **"Needs a split" follow-ups (2026-10-27, live in r-8b43fb267199):** (1) check on the map the three guessed `low` cuts —
  Chapman lower falls + 100 m below (SCRD survey point, ~576 m below the power line), Deena "second bridge" (no bridge
  mapped: a point 5 km above the tidal boundary), Mamin "third bridge" (Mamin Main road, 9.1 km above tidal); (2) Columbia
  r11 "from Mud Lake": FWA names no Mud Lake, so it runs Fairmont → Columbia Lake (the outlet wetland, wbk 329093907, is
  the likely Mud Lake) — say if Mud Lake is elsewhere; (3) Stave r1/r2 (Ruskin/Northrop spawning channels) stay notes:
  the channels have no sections in the atlas — they need the channels added (added_streams) before a cut can draw them.
- **T204 phone check:** Kanaka Creek's map now has two marks (112 Avenue bridge, Cliff Park Falls) and its lower part is
  two separately selectable stretches ("… · stretch 1 of 2"); say if the stretch wording should differ.
- **Tidal boundaries (T212, 2026-10-27, critical):** open the curation review's **Tidal** tab. 1,163 proposed tidal
  cuts (every named stream your tidal polygon crosses + every named ocean-entering FWA stream); start with
  "words name it" (31). 19 agree with the book / DFO words; look first at Capilano (polygon 1 km up, the book's CN
  bridge is at the mouth), Nimpkish (polygon 5.7 km up vs "100 m above the Hwy 19 bridge" ≈ 1.2 km), Tlell (polygon
  misses it; synopsis says 1.5 km above Hwy 16, DFO says ~1 km) and Squamish (the dyke line is not located).
  "accept proposal" (a) pre-fills Place y + the cut; Record mark is the only write. Summary:
  handoff/PLAN/curation-review/tidal/tidal-candidates.md.
- **Cranberry canyon cuts:** the bottom is a DEM estimate (low). Check it on the map in the review tool.
- **Map look (T64, T85, T113, T152) — after the split rebuild (Claude runs it):** look at the map (wetland-heavy Fort Nelson and the
  Chilcotin without unnamed wetlands; z13–z18 bends on the Fraser/Vedder now overzoomed from z13; Vancouver's dense z13
  tiles; no OSM creeks under ours at z14+) and say if the look suffers — the basemap's OSM wetland fill is one style layer
  away. Basemap z13 is parked (T85 "Parked").
- **Q114 / T136 — the 30-entry curation pilot: ON HOLD until the split rebuild** (owner 2026-10-24: the splits change
  what you review). Claude then regenerates the pilot and starts the app; you open http://127.0.0.1:5183 (the pilot's 30
  open first). Fix extents in the mark (T191); nothing writes the catalogue.
- **Q4 / T55 + T149 — slough and canal candidates (page v2).** https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8 — your 98
  marks are kept. New: revised classes with feature-code evidence (2,424 candidates), a **Complexes** view (one mark sets
  every unmarked piece of a slough area on one river; your own marks are never overwritten), and a **Spot check** tab
  (spot checks done). Finish the marks; then Claude folds them in (Okanagan oxbows → streams), dry run first.

Answered 2026-10-24: Q125 (muted fill), T179 (400 m; a curated cut never moves; 25 m lake-anchor proposals; merged
splits keep every name and id), PSF permission (T162; their reply is not stored), braids marked (T67 promoted), Q7 (delete old releases — pruned to live + previous; `python -m pipeline.deliver.release.prune`).
Answered 2026-10-23: Q5 + Q34–Q41 (curation plan approved, defaults; splits mid-review must work; salmon pass later = T132), Q13 (Claude runs the reader gate), Q44 (no Level from a side channel; Flow only if it carries a large share), Q53 (as built), Q45 (leave unbound).

## Your open questions (31 — each built with a default; the table in tickets/README.md has the wording)

Triaged 2026-10-23: 64 smaller calls were decided by Claude and 12 were already answered or obsolete — all recorded in
decisions.md "CLAUDE 2026-10-23 — owner-question triage" (read and reverse any). What is left needs you:

- **Readings of the book (a wrong call misleads anglers):** Q124 a river's rules on unnamed widenings (456 polygons) ·
  Q87 Beattie Lake bull trout beside or inside the trout quota · Q140 no-access banner on a part's open stretch ·
  Q42 tidal Fraser below Mission (postponed) · Q62, Q63 size-slot and "keep only" wording.
- **Which gauge speaks (your policy):** Q31 the gauge rule · Q103 diversion
  gauges stop at their canal (24 sections lose every gauge) · Q113 cuts for 38 silent stations (atlas rebuild).
- **How it looks (taste):** Q150 the new "own regulations" colour (no access now amber, T165) ·
  Q130 each model's line under Change · Q132 day chips may still use ELF · Q79 "river" vs "stream" on the page ·
  Q91 "no gauge close enough" wording · Q123 how the Map view marks the selected part (built: dim the others; thin halo in
  `app-integration/T151-shots/alt-halo--*`) ·
  Q2 / Q139 / Q47 no-access and tidal notes on Conditions.
- **Salmon (T164):** confirm the proposed DFO cut points in T164's DFO review mode when it lands (96 bridges/signs/dams);
  Q145 where and how often the DFO re-fetch runs.
- **Vessel rules (T209):** should the parser fill `max_power_kw` (7.5) on the 162 "Electric motor only - max 7.5 kW"
  rules, or does the label stay word-only? Also the 10 close-wording undrawn parts kept ("various locations", "swimming
  areas") in on-parts.md: whole water too, or leave as notes?
- **Batch 7:** Q146 re-centre the map when the parts list opens · Q147 steelhead lake wording on newly taken ponds ·
  Q148 dams → "lake" for the slough candidates · Q149 Bonaparte by hand.
- **No-access look (T165, built):** answered 2026-10-24 (as built).
- **Gauge gaps on main rivers (T174):** Q153–Q155, measured before anything changes.
- **Run timing (T162, data + plan):** read `handoff/PLAN/runtiming/PLAN.md` and say go or not for the app; Q144
  (steelhead first, passage on a paler ramp, its own layer) and the release total (~14.92 of 15 MB with `runs`).
- **Scope and big rebuilds:** Q10 imagery corridor · Q32 salmon plan · Q142 Shuswap lake rings.
- **Things only you can do or know:** Q114 the 30-entry curation pilot · Q4 slough marks (review page) · Q126 were you
  on Level? · Q119 audit references · Q30 Atnarko campsite signs · Q9 phone testing · Q26 hosting/app name ·
  Q28 iOS timing · Q27 sign-off walk.

## Things only you can run or provide

- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **T185 proposals (2026-10-27): decide.** 8 proposals from your marks
  (handoff/PLAN/curation-review/proposals/): Hatzic r2/r3 tributaries (in memory: 4 -> 92 sections, 8 -> 124 km
  each), 2 new splits (Quinsam confluence, shared by two Campbell locations; Cowichan Sandy Pool sign), 4 DFO extents,
  Dragon Lake listed for a lake split later. Open the app's **Proposals** tab and accept / reject each (a mark).
  That is your only step; Claude runs the side chains and applies them.
- **Build for the DFO salmon (T211, 2026-10-27):** `python -m pipeline build` — the reach step reruns (it now places
  the DFO locations: `dfo_salmon.jsonl.gz`), then deliver (bundle `salmon_*` tables, release file `salmon`). Needs the
  typed feed (`python -m pipeline.regs.dfo_salmon.feed`, no credits). Check the `salmon` size against its 600 kB gate.
- **Reach + deliver rebuild for the "on parts" change (2026-10-26):** 33 vessel rules now hold on the whole water
  (list: handoff/PLAN/curation-review/on-parts.md); the bundle shows them as notes until the rebuild.
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
- **Phone testing** (T77, Q9), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
  - Gesture/navigation parity on the phone (T50, T77; owner 2026-10-27; same RNGH code as web, not seen on a device):
    Android edge-swipe back and the back button through the back table (HANDOFF-ui "Back, everywhere");
    `android:enableOnBackInvokedCallback="false"` in the manifest (predictive back off) is deliberate until judged;
    sideways view swipe on a water (Regulations/Conditions/Map) incl. not starting in the 24 px edge zones and not
    fighting the map pan / chart / minimap shields; sheet drag-to-close and sheet scroll-vs-drag hand-off; iOS
    left-edge back (`EdgeBack`); Spots/Conditions/More tab back goes to the Map tab; part step and upstream/downstream
    (▲/▼) keep one back; section highlight while scrolling the regs tabs; keyboard (search) vs sheets.
- **Sign-off walk** at the end (T87) before app/design is deleted.

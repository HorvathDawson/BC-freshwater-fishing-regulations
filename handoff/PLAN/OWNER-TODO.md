# Owner to-do

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **SEO / domain (T188 plan, T198):** confirm you own canifishthis.ca and canifishthis.com; at the registrar/DNS set
  .com and www to 301 to https://canifishthis.ca. Once hosting (T86) is live: add Domain properties in Google Search
  Console (DNS TXT), import to Bing Webmaster, submit /sitemap.xml, and give Claude the tokens. Later, send the
  outreach by hand (T197). Plan: handoff/PLAN/seo/PLAN.md sections 4 and 7.

## Reviews
- **Two cuts to re-place (2026-10-26):** binding them failed the reach run, so both stay unused (high severity in the
  review tool). `burton_creek__hwy_6_bridge` does not resolve on the Woden reach (its note doubts the FWA Burton Creek),
  and `kootenay_river__navigation_dolphin_2000_m_downstream` lies in Kootenay Lake, off the river, so Kootenay lower r1
  still ends at the lake. Details: handoff/PLAN/curation-review/split-name-audit.md.
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
- **Reach + deliver rebuild for the "on parts" change (2026-10-26):** 33 vessel rules now hold on the whole water
  (list: handoff/PLAN/curation-review/on-parts.md); the bundle shows them as notes until the rebuild.
- **R2 upload** of a new release and of the gauge feed, with your credentials:
  `python -m pipeline.deliver.release.publish --release data/release/$(cat data/release/LATEST) --yes` (records
  data/release/PUBLISHED so the prune keeps it). When you set up the feed sync, upload ONLY `data/generated/gauges/feeds/`,
  not `feeds.runs/` (T177).
- **`FEED_NOTIFY_URL`**: set it where the feed cron runs to get one message per new HYDAT release
  (ntfy / Discord / Slack). Off until set.
- **`scripts/update-hydat.sh`**: run it when that message arrives.
- **Phone testing** (T77, Q9), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
- **Sign-off walk** at the end (T87) before app/design is deleted.

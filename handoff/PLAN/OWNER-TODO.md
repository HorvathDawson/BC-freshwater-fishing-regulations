# Owner to-do

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **T162 run timing — PSF's terms forbid redistribution.** salmonwatersheds.ca/terms: personal non-commercial use only;
  no copying, publishing, mirroring or redistributing (modified or not) without PSF's prior written permission. T162 is
  stopped before any fetch. Ask PSF (info@salmonwatersheds.ca) for written permission to ship derived run timing in the
  app, or pick another source. Also yours: the three Explorer pages under `data/source/runtiming/*.html` are tracked
  in this PUBLIC repo — untrack/purge them or not. Details: the T162 ticket's Outcome.

- **T67 braids — check every cut (phone-friendly):** https://claude.ai/artifact/Et8B4Z6ZbKNNw9mMkJWzFH
  Round 2 (2026-10-24): 28 proposed cuts (Right / Wrong / Unsure + note), 2 re-homed, 19 braids left alone, 45
  dropped ditches, and three yes/no calls on the Questions tab (a gauge on a named side channel cutting its mainstem;
  a moved length cut changing a 1 km Fraser piece's region 5 → 7; the Exchamsiks curated-split fix). Nothing is promoted until you have marked them. (Push is off in /config, so no
  notification was sent.)

## Reviews
- **Curation pilot (Q114) — now an artifact, works on a phone:** https://claude.ai/artifact/DSnzYbemqcTLbUXJud3DLp
  30 targets on release r-0ed3c2b2a0a7. Per target: the book crop (tap to zoom), the map, the rules, the angler's answer,
  then T / R / P / A with y / n / ? and Record. Marks save per person with timing; share it from the Share menu to let
  helpers mark too. About an hour. Claude imports the marks and runs the pilot report.

- **Q114 / T136 — the 30-entry curation pilot** (about an hour): `handoff/PLAN/curation-review/PILOT.md`, then
  `python -m pipeline.regs.review.gate pilot-report --pilot handoff/PLAN/curation-review/pilot.json`.
- **Q4 / T55 + T149 — slough and canal candidates (page v2).** https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8 — your 98
  marks are kept. New: revised classes with feature-code evidence (2,424 candidates), a **Complexes** view (one mark sets
  every unmarked piece of a slough area on one river; your own marks are never overwritten), and a **Spot check** tab
  (40 intermittent "lakes" on a stream's course + 10 isolated ones). If the spot check says the on-course ones are not
  lakes, say so and Claude runs a dry run of folding them (10,676) before anything changes.

Answered 2026-10-24: Q7 (delete old releases — pruned to live + previous; `python -m pipeline.deliver.release.prune`).
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
- **Salmon (T164, new):** confirm the proposed DFO cut points in the review tool when they land in S0 (96 bridges/signs/dams);
  Q145 where and how often the DFO re-fetch runs.
- **Batch 7:** Q146 re-centre the map when the parts list opens · Q147 steelhead lake wording on newly taken ponds ·
  Q148 dams → "lake" for the slough candidates · Q149 Bonaparte by hand.
- **No-access look (T165, built):** answered 2026-10-24 (as built).
- **Gauge gaps on main rivers (T174):** Q153–Q155, measured before anything changes.
- **Run timing (T162, new):** Q144 default species, passage reaches shown dashed, its own map layer.
- **Scope and big rebuilds:** Q10 imagery corridor · Q32 salmon plan · Q125 sewage/tailings/dumps drawn
  as lakes · Q142 Shuswap lake rings.
- **Things only you can do or know:** Q114 the 30-entry curation pilot · Q4 slough marks (review page) · Q126 were you
  on Level? · Q119 audit references · Q30 Atnarko campsite signs · Q9 phone testing · Q26 hosting/app name ·
  Q28 iOS timing · Q27 sign-off walk.

## Things only you can run or provide

- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **R2 upload** of a new release and of the gauge feed (`pipeline.deliver.release.publish`), with your credentials: `python -m pipeline.deliver.release.publish --release data/release/$(cat data/release/LATEST) --yes`
  (moved from pipeline.tools.publish by T61; it records data/release/PUBLISHED so the prune keeps it). The feed now also carries
  T143 (each quantity ranked by its own first model: Level at +1/+3/+5 answers) and T144 (Change = CLEVER + COFFEE,
  ELF only with the low-water caveat); Claude republished the LOCAL feed (data/generated/gauges/feeds) for :39217. When you set up the feed sync to
  R2, upload ONLY `data/generated/gauges/feeds/` — not the run archive `data/generated/gauges/feeds.runs/` beside it
  (T177; it stays local so T178 can score the models in ~6 weeks). A new release is cut
  locally (latest: T128 wording, T114 search keys, T56 Twin Lakes/sloughs/"See X"). The gauge feed also needs a
  republish for T126 (forecast clock 8 h fix, CLEVER summary) and T106 (declared quantity) — no re-fetch needed.
- **`FEED_NOTIFY_URL`**: set it where the feed cron runs to get one message per new HYDAT release
  (ntfy / Discord / Slack). Off until set.
- **`scripts/update-hydat.sh`**: run it when that message arrives.
- **Phone testing** (T77, Q9), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
- **Sign-off walk** at the end (T87) before app/design is deleted.

# Owner to-do

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **Q6 / T67 — braid splitting at build** (2026-10-23: "not yet", you read it first). Read `handoff/PLAN/braids/T67-assessment.md`, then answer R14 Q1 (keep
  ditch-only lines as the river), Q2 (leave automatic confluence cuts out), Q4 (cap the cut-line length; suggested no).

## Reviews
- **Q114 / T136 — the 30-entry curation pilot** (about an hour): `handoff/PLAN/curation-review/PILOT.md`, then
  `python -m pipeline.regs.review.gate pilot-report --pilot handoff/PLAN/curation-review/pilot.json`.
- **Q4 / T55 + T149 — slough and canal candidates (page v2).** https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8 — your 98
  marks are kept. New: revised classes with feature-code evidence (2,424 candidates), a **Complexes** view (one mark sets
  every unmarked piece of a slough area on one river; your own marks are never overwritten), and a **Spot check** tab
  (40 intermittent "lakes" on a stream's course + 10 isolated ones). If the spot check says the on-course ones are not
  lakes, say so and Claude runs a dry run of folding them (10,676) before anything changes.

Answered 2026-10-23: Q5 + Q34–Q41 (curation plan approved, defaults; splits mid-review must work; salmon pass later = T132), Q13 (Claude runs the reader gate), Q44 (no Level from a side channel; Flow only if it carries a large share), Q53 (as built), Q45 (leave unbound).

## Your open questions (37 — each built with a default; the table in tickets/README.md has the wording)

Triaged 2026-10-23: 64 smaller calls were decided by Claude and 12 were already answered or obsolete — all recorded in
decisions.md "CLAUDE 2026-10-23 — owner-question triage" (read and reverse any). What is left needs you:

- **Readings of the book (a wrong call misleads anglers):** Q124 a river's rules on unnamed widenings (456 polygons) ·
  Q87 Beattie Lake bull trout beside or inside the trout quota · Q140 no-access banner on a part's open stretch ·
  Q42 tidal Fraser below Mission (postponed) · Q62, Q63 size-slot and "keep only" wording.
- **Which gauge speaks (your policy):** Q31 the gauge rule · Q84 side channel and its river's Level · Q103 diversion
  gauges stop at their canal (24 sections lose every gauge) · Q113 cuts for 38 silent stations (atlas rebuild).
- **How it looks (taste):** Q134 square spot pins · Q150 the new "own regulations" colour (no access now amber, T165) ·
  Q130 each model's line under Change · Q132 day chips may still use ELF · Q79 "river" vs "stream" on the page ·
  Q91 "no gauge close enough" wording · Q69 spot pin colour · Q123 how the Map view marks the selected part (built: dim the others; thin halo in
  `app-integration/T151-shots/alt-halo--*`) ·
  Q2 / Q139 / Q47 no-access and tidal notes on Conditions.
- **Salmon (T164, new):** confirm the proposed DFO cut points in the review tool when they land in S0 (96 bridges/signs/dams);
  Q145 where and how often the DFO re-fetch runs.
- **Batch 7:** Q146 re-centre the map when the parts list opens · Q147 steelhead lake wording on newly taken ponds ·
  Q148 dams → "lake" for the slough candidates · Q149 Bonaparte by hand.
- **Run timing (T162, new):** Q144 default species, passage reaches shown dashed, its own map layer.
- **Scope and big rebuilds:** Q6 braids · Q10 imagery corridor · Q32 salmon plan · Q125 sewage/tailings/dumps drawn
  as lakes · Q142 Shuswap lake rings · Q7 which old releases to delete.
- **Things only you can do or know:** Q114 the 30-entry curation pilot · Q4 slough marks (review page) · Q126 were you
  on Level? · Q119 audit references · Q30 Atnarko campsite signs · Q9 phone testing · Q26 hosting/app name ·
  Q28 iOS timing · Q27 sign-off walk.

## Things only you can run or provide

- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **R2 upload** of a new release and of the gauge feed (`publish.py`), with your credentials. The feed now also carries
  T143 (each quantity ranked by its own first model: Level at +1/+3/+5 answers) and T144 (Change = CLEVER + COFFEE,
  ELF only with the low-water caveat); Claude republished the LOCAL feed (data/generated/gauges/feeds) for :39217. A new release is cut
  locally (latest: T128 wording, T114 search keys, T56 Twin Lakes/sloughs/"See X"). The gauge feed also needs a
  republish for T126 (forecast clock 8 h fix, CLEVER summary) and T106 (declared quantity) — no re-fetch needed.
- **`FEED_NOTIFY_URL`**: set it where the feed cron runs to get one message per new HYDAT release
  (ntfy / Discord / Slack). Off until set.
- **`scripts/update-hydat.sh`**: run it when that message arrives.
- **Phone testing** (T77, Q9), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
- **Sign-off walk** at the end (T87) before app/design is deleted.

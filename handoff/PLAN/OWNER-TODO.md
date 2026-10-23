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
- **Q4 / T55 — slough and canal candidates.** Mark them on https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8
  (stream by the book / not a stream / unsure). Start with the 65 "stream by book" and 236 "likely". Includes the 40
  Okanagan River oxbows (p.70 says isolated oxbows are exempt from the spring closure).

Answered 2026-10-23: Q5 + Q34–Q41 (curation plan approved, defaults; splits mid-review must work; salmon pass later = T132), Q13 (Claude runs the reader gate), Q44 (no Level from a side channel; Flow only if it carries a large share), Q53 (as built), Q45 (leave unbound).

## Questions answered with a default (change any you disagree with)

New: Q114–Q121 (curation pilot; rebuild with tiles after a split; TOOL: notes; steelhead twins in the review app; station-name promote fix; two audit references; store version; reason renames); Q105–Q113 (review queue defaults: pre-check names, undrawn part as a risk, review order, a new split stales its water, hand-picked cuts; "no forecast" map label; wetland gauges; Fraser at Mission speaks weakly for the Vedder Canal polygon; 38 active-but-silent stations have no cut); Q94–Q104 (review tool: 150 split points in S0, the A check as a deep link, a partial mark while awaiting build; Fraser 7A piece loses its steelhead twins; bundle regions and carve-out walk follow-ups; edge type 1150, Burnaby Lake's two outlets, diversion gauges no longer speak above their canal, 24 sections lose every gauge); Q87–Q93 (Beattie Lake bull trout beside or inside, P10 vs p15 mouths, 1,302 Level answers go silent, spot station, no-gauge and lake wording); Q85–Q86 (review tool: preview binding while awaiting build, moved splits seen after next atlas build); Q83–Q84 (side-channel share threshold; should a channel also not take the river's Level); Q80–Q82 (curation marks: km not sections, a split stales its whole water, "unsure" verdict); Q74–Q79 (gauge dots follow the forecast day, Change dot label, undeclared quantity, p10/p90 wording, search keys need the new release, "river" vs "stream" on the page); Q67–Q73 (heat map zoom and arrows, spot pin colour, stacked spots, spot names); Q62–Q66 (size and time wording choices, the app's own origin pick); Q59–Q61 (Expected change: model order, COFFEE in the day chips, colour by ratio); Q46–Q52, Q54, Q56–Q58 (no-access colours, "Like your visit?" days and weights, HYDAT --stations)
and Q46–Q52 (unnamed-lake walk stop, tidal sentence placement and flags, whole Kootenay Lake in
search, pointer pages, photo picker). See the "Owner questions" table in `handoff/PLAN/app-integration/tickets/README.md` (Q1–Q121). The ones most likely
to matter:
- Q1–Q3 / T54 no-access streams: permit-only areas get the gentle note; Conditions keeps the flow colour plus a
  banner; lakes inside no-access areas (Coquitlam Lake) get the fourth colour.
- Decided by Claude under your call 11 (record in decisions.md "CLAUDE 2026-10-22 (T81)"; read and overrule any):
  Q8, Q11, Q12, Q14–Q23, Q25 — incl. boundaries under the water status lines, the warm temperature dot's own hue,
  and dark-mode water names near the "base regulations" blue.

## Things only you can run or provide

- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **R2 upload** of a new release and of the gauge feed (`publish.py`), with your credentials. A new release is cut
  locally (latest: T128 wording, T114 search keys, T56 Twin Lakes/sloughs/"See X"). The gauge feed also needs a
  republish for T126 (forecast clock 8 h fix, CLEVER summary) and T106 (declared quantity) — no re-fetch needed.
- **`FEED_NOTIFY_URL`**: set it where the feed cron runs to get one message per new HYDAT release
  (ntfy / Discord / Slack). Off until set.
- **`scripts/update-hydat.sh`**: run it when that message arrives.
- **Phone testing** (T77, Q9), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
- **Sign-off walk** at the end (T87) before app/design is deleted.

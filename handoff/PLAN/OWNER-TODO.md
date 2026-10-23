# Owner to-do

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **Q44 / T101 — side-channel gauges.** May a gauge on a side channel or slough speak for the river it branched from?
  If yes, two Nicomen Slough level gauges answer Level for about 71,668 Fraser sections. Suggested: no for Level;
  Flow only when the side channel carries a large share of the river. Built and reviewed, held unmerged.
- **Q5 / T66, T97 — curation review plan.** Read `handoff/PLAN/curation-review/PLAN.md` (verdict: the tool is not
  ready for a person yet) and approve it or change it. Its 9 questions are Q33–Q41.
- **Q6 / T67 — braid splitting at build.** Read `handoff/PLAN/braids/T67-assessment.md`, then answer R14 Q1 (keep
  ditch-only lines as the river), Q2 (leave automatic confluence cuts out), Q4 (cap the cut-line length; suggested no).

## Reviews
- **Q53 / T102 — tributary walk order guard.** Your "order only where a code is missing" would switch it off (every live
  edge has a code; 28,359 same-code floodplain/braid edges rely on it). Default built: code decides parent/child, order
  still decides equal or unrelated codes.
- **Q45 / T89 — eight waters the book prints where the registry has no water of that name** (incl. Squirrel Lake 6-1).
  Their rules bind nothing until each is located; the list is in T89 ("Q-A").
- **Q4 / T55 — slough and canal candidates.** Mark them on https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8
  (stream by the book / not a stream / unsure). Start with the 65 "stream by book" and 236 "likely". Includes the 40
  Okanagan River oxbows (p.70 says isolated oxbows are exempt from the spring closure).
- **Q13 / T82 — launch the reader-gate agent run.** Ready now (T56 landed).

## Questions answered with a default (change any you disagree with)

New: Q62–Q66 (size and time wording choices, the app's own origin pick); Q59–Q61 (Expected change: model order, COFFEE in the day chips, colour by ratio); Q46–Q52, Q54, Q56–Q58 (no-access colours, "Like your visit?" days and weights, HYDAT --stations)
and Q46–Q52 (unnamed-lake walk stop, tidal sentence placement and flags, whole Kootenay Lake in
search, pointer pages, photo picker). See the "Owner questions" table in `handoff/PLAN/app-integration/tickets/README.md` (Q1–Q66). The ones most likely
to matter:
- Q1–Q3 / T54 no-access streams: permit-only areas get the gentle note; Conditions keeps the flow colour plus a
  banner; lakes inside no-access areas (Coquitlam Lake) get the fourth colour.
- Decided by Claude under your call 11 (record in decisions.md "CLAUDE 2026-10-22 (T81)"; read and overrule any):
  Q8, Q11, Q12, Q14–Q23, Q25 — incl. boundaries under the water status lines, the warm temperature dot's own hue,
  and dark-mode water names near the "base regulations" blue.

## Things only you can run or provide

- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **R2 upload** of a new release and of the gauge feed (`publish.py`), with your credentials. A new release is cut
  locally by T56 (Twin Lakes, sloughs, "See X", the 2027-03-31 regulations date).
- **`FEED_NOTIFY_URL`**: set it where the feed cron runs to get one message per new HYDAT release
  (ntfy / Discord / Slack). Off until set.
- **`scripts/update-hydat.sh`**: run it when that message arrives.
- **Phone testing** (T77, Q9), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
- **Sign-off walk** at the end (T87) before app/design is deleted.

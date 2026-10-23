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

- **Q45 / T89 — eight waters the book prints where the registry has no water of that name** (incl. Squirrel Lake 6-1).
  Their rules bind nothing until each is located; the list is in T89 ("Q-A").

- **Q4 / T55 — slough and canal candidates.** Mark them on https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8
  (stream by the book / not a stream / unsure). Start with the 65 "stream by book" and 236 "likely". Includes the 40
  Okanagan River oxbows (p.70 says isolated oxbows are exempt from the spring closure).
- **Q13 / T82 — launch the reader-gate agent run** after T56 lands.

## Questions answered with a default (change any you disagree with)

New this round: Q46–Q52 (unnamed-lake walk stop, tidal sentence placement and flags, whole Kootenay Lake in
search, pointer pages, photo picker). See the "Owner questions" table in `handoff/PLAN/app-integration/tickets/README.md` (Q1–Q44). The ones most likely
to matter:
- Q1–Q3 / T54 no-access streams: permit-only areas get the gentle note; Conditions keeps the flow colour plus a
  banner; lakes inside no-access areas (Coquitlam Lake) get the fourth colour.
- Q14–Q16 / T83 map colours: boundaries vs water lines, the temperature dot hue, low-contrast pairs.
- Q23: dark-mode water names now sit close to the "base regulations" blue and the "above normal" flow colour.

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

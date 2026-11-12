# T627 — Digest email: an easy-to-read summary of reports and errors

Status: IN REVIEW — efe94f572; look approved; DEPLOYED to staging 2026-11-11; owner: OWNER-TODO "T627 read one staging digest on the phone"

Source: owner 2026-11-10 ("we want a nice easy to read summary"), answering T609's question whether a report's words
belong in the digest email or only a count.

## Work
1. The daily digest opens with one plain line per kind: how many problem reports, how many new error kinds, how many
   repeats, and whether any feed run failed.
2. Under it, each problem report as a short block: when (B.C. time), build, the screen or water, and the reporter's
   words trimmed to a few lines. Each new error kind as one line: message, build, count.
3. Keep T609's caps ("and N more"); the full text stays behind GET /reports and /crashes.
4. Plain text first, readable on a phone; no raw JSON, ids only at the end of a block.
5. A test pins the layout from a fixture day (two reports, three error kinds, one failed run).

## Acceptance
- The owner reads one staging digest on his phone and says it is clear.

Depends: T609. Size: S. Owner: one look.

## Owner 2026-11-11: the look
Summary then short blocks: one line of counts, the feeds line, each report as a short block with trimmed words, new errors
one per line, repeats as a count. Build it.

## Built (2026-11-11) — efe94f572, NOT deployed (the owner picks the look from samples first)
`app/workers/feeds/src/digest.ts` lays out the digest; `events.compose(…, digest)` calls it. The other emails are unchanged.
- Opens with: one line of counts ("In this digest: 2 problem reports · 3 new error kinds · 3 known error kinds seen
  again"), the feeds line ("Feeds: Gauge feed failing; In-season check recovered", or "no change of state in this digest":
  it never says "all fine", only what the digest holds), and "Also:" naming every other section by its subject.
- A problem report: what and where (bold), the words in quotes cut at a word within 240 characters, then when (B.C.
  time, `core/day.bcStampWords`), the day looked at, map mode, build, platform, release; last, small: what is not shown
  ("32 more words, gauge, map position, install id. Full report: Review app → Reports (Sync)"), reply allowed or not, id.
- A new error kind: message (cut within 140 characters, said when cut), screen, build(s), platform, count and installs;
  last, small: first seen (B.C. time), environment, release, id. The stack is not in the digest: the section says
  GET /crashes holds it.
- Repeats as a count: `crashEvents` counts the defect kinds already told that were seen again yesterday (UTC), once per
  day, as one `fine` event (it rides in a digest, never causes one). The "not defects" count line stays.
- Feed health, DFO, in-season, HYDAT, run timing, roster, model scores, gauge cards: their own sections, word for word.
  Order: reports, errors, feeds, then the rest.
- T609's cap stays (50 per kind, "…and N more: <where>."). Subject line unchanged.
- Report and crash events now carry `data` (the fields above; never the email address). An event queued before this
  (no fields) is still shown: title, trimmed text, id.
- HTML: system fonts, inline styles, no image / font / stylesheet / script; the only link is the status page.

Found on the way, fixed here: a gauge-card event (`kind: "card"`) was missing from `events.ORDER`, so it was in no email
and a digest recorded it as told. It now has its section (digest) and its own email (outside one).
Side effect to know: a crash event now has `data`, so OUTSIDE a digest (NOTIFY_DIGEST off) the crash email goes through
its T609 section (cap, "What to do") instead of the bare list it fell to before.

Tests: `test/digest.test.ts` 8 (the fixture day pinned whole: two reports, three error kinds, one failed run; opening
lines; every report / error fact kept or named; cap; every kind present; no remote asset; trimming),
`test/crashes.test.ts` +1 (the repeat count, once per day, no extra reads once told). Worker suite 29 files / 362 green.
Fixtures (`test/digest.fixture.ts`) are built by the worker's own event builders.

Left: the owner's look; then deploy to staging (`deploy/staging/feeds.sh`) and one real digest read on the phone
(the acceptance). Not checked: how the HTML renders in the owner's mail app at phone width.

## Deployed (2026-11-11)
Owner approved the look. `deploy/staging/feeds.sh` (worker suite 30 files / 429 green, unfiltered) deployed staging worker version
912dfcf6-3a7a-45f2-943b-f7e0e2720a45 and ran gauges / dfo / inseason (all HTTP 200). Verified: /status.json answers with the fresh runs;
`POST /notify/preview` composes the new opening ("In this digest: no problem reports · 1 new error kind" + "Feeds: ..." lines,
HTML counts table). The one pending crash event was queued before the deploy (no `data`), so it shows in the old-event form, as designed.
No test email sent: the next daily digest (NOTIFY_DIGEST=daily) is the acceptance read. Production not touched.
Left: the owner reads one real staging digest on his phone; the mail-app rendering at phone width is still unchecked.

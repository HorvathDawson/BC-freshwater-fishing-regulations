# T585 — Web and phone answer from the same feed and release, and say which

Status: OPEN — F8 and F14 built in data (5502ba714); F12, F13 need the screens and the boot lane

Source: T580 audit 2026-11-10 (owner: "stuff like this should never happen … look for other things like that").
Findings, with file:line, the state that shows each and the proposed fix: `handoff/PLAN/app-integration/T580-findings.md`.

## Work
- F8 (plausible): the card's figure and percentile come from a different file than the rows, dots and estimate.
- F12: the phone can run an older release than the web for hours or days.
- F13 (plausible): a switchable phone build can keep another environment or channel.
- F14: offline, only the phone answers from a kept week, and its "now" has no standing.
- Goes with T550 (gauge readings age / datum / day): do them together.

Acceptance: each finding named above no longer reproduces, and the test or gate the findings file proposes for it is in.

Depends: T567. Size: M. Owner: no.

## Done 2026-11-10
- F8 (5502ba714): `feed.now()` takes the rank from the index row, and only for the reading that row names (same `at`);
  otherwise the figure comes with no rank and `Reading.rankedAt` names the ranked reading. Test: format.test.ts.
- F14 (5502ba714): offline the kept index answers the same way: the kept reading keeps its rank when it is the ranked
  one, no standing (with `rankedAt`) when a newer kept row is the reading now. Test: history.test.ts.
- F13: `resolveEnv` / `resolveChannel` ignoring the stores when not switchable is already pinned (env.test.ts); with F8
  the test channel's index and the live station files can no longer give the card another rank than the rows.
## Left (screens; boot is T555's lane)
- The card must adopt `rankedAt` ("ranked at <time>; this reading <time>") and `kept` words instead of "no record".
- F12: release id (and env/channel when overridden) on the Conditions "as of" line in non-production builds; apply a
  ready staged set at cold start (data/src/release/boot.ts); boot test that web and phone end on one data_release.
- The gauge feed names its HYDAT (`hydat.release`); the app should say when it differs from its release's HYDAT (the
  ranks and the chart bands then come from two envelopes).

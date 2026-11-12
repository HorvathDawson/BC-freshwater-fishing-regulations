# T585 — Web and phone answer from the same feed and release, and say which

Status: IN PROGRESS — F8, F12, F13, F14 built (5502ba714, 04316bd91, c58640b86, e4add6e9a); left (Claude): the emulator check and the owner's yes on the new words, both listed under "Done 2026-11-11".

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

## Done 2026-11-11 (the screens and the boot)
- The card's words (04316bd91, c58640b86). `readingStanding` (core): a figure the index did not rank (`rankedAt`)
  says which reading was ranked and which this is, never "no record to compare it against"; the standing dot is not
  drawn for it. A kept figure (offline) leads its age line with the kept words. Tests: core `conditions.test.ts`,
  `freshness.test.ts`; `ui/src/hooks/stationReading.test.ts`; the rendered card in `ConditionsPanel.test.tsx`.
- F12 / F13, the "as of" line (e4add6e9a). The gauge card's foot names the data release in a development or staging
  build, and the data source / update channel when they are not the build's own (`dataProvenanceLine`, core; provided
  once by App.tsx through `DataProvenanceContext`, read by the one Conditions body). Nothing in a production build.
  It sits in the card's foot, so a water with no gauge card shows none.
- F12, the cold start (e4add6e9a). A staged set was already OPENED by the next cold start (the pointer is swapped at
  staging); what was missing is that it was applied without a word: no "Updated to", no "applied" in the Updates log,
  the Updates row keeping the old time. Boot now knows the first boot to open a set (`Booted.applied`, a small
  `release/opened` record) and one rule (`bootSaid`) tells the service and the reader for a cold start and an in-session
  apply alike; `applied()` leaves a later pending release's state alone. Not built: installing a newer set BEFORE the
  map on a cold start (it would hold the start for a download; the service still stages it and the Shell applies it
  at the first still moment). Boot test: `tools/release-boot.test.ts` "(T585 F12) …" (cold start says it once; an
  in-session apply is not said again; web and phone end on one data_release).
- HYDAT differs: built in T633 item 5 (9995fcf13).

### New words, for the owner to confirm (each pinned in a test)
- `Ranked at <time>; this reading <time>.` (times as everywhere on Conditions: `2026-09-03 at 22:00 PT`)
- `Ranked at <time>; this reading is a different one.` (this reading's own time unknown)
- `This reading has not been ranked yet.` (the index names no ranked time)
- `Offline — reading saved on this phone` (the age line's lead for a kept figure; from the chart's agreed
  "Offline — measured line as of …, saved on this phone")
- `Data release r-… · Data source Staging · Channel test` (staging / development builds only; the labels are About's
  and the Developer card's own)

### Not device-tested (the emulator run should check)
- Staging build: the gauge card's foot shows the release line; after an update is applied it shows the new id.
- Stage an update, kill the app before the still moment, open it: "Updated to …" shows once, the Updates log has
  "applied", a second open says nothing.
- Airplane mode on a gauge opened before: the card's age line leads with the kept words; the standing is the kept
  rank or the two times.


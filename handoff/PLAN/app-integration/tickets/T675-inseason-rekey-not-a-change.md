# T675 — An in-season row whose id moved while its content stayed is a re-key, not a change and not a removal

Status: DONE — 35339591e (built, tested, staging feeds worker deployed 2026-11-11; production takes it with its next worker deploy, the owner's).

Source: found 2026-11-11 reading staging's `feeds/inseason/index.json`. In-season notices are BC Fish and Wildlife's.

## Today (before the fix)
- The feeds worker's in-season job (`app/workers/feeds/src/inseason.ts` `buildFeed`, called by `publishFeed`) compared the
  page with the last published feed BY ROW ID. When the id scheme moved (T520, 2026-11-08: position → content; then the
  date joining the id for rows sharing region + water + change), every row arrived under a new id, so each was read as an
  AMENDED notice of the same region + water, and its old id as a REMOVAL.
- Staging, read 2026-11-11: all 32 live rows `status: "changed"` with `previous` equal to their own text and `firstSeen`
  reset to 2026-11-08; `removed` held 38 entries (32 + 6, the two re-keys) identical in region, water, change and
  effective date to rows still live.
- No angler effect: `joinInSeason` only drops ids in `removed` and no live id was there; nothing in the app reads
  status / previous / firstSeen. But the bookkeeping was false, and production's first run with the content ids would
  have written the same.

## Built
- A row gone under one id whose content (region, water, change, effective, as the id reads them: `changeKey`) equals a
  live row under another id is the SAME notice: it carries `firstSeen`, `changedAt` and its status to the new id, is
  never the "amended" source of another row, and adds nothing to `removed`. Also covers a sibling row leaving (the date
  drops out of the survivor's id).
- A feed the old rule wrote is put right by the next run: a `previous` equal to the row's own text is no change
  (status `unchanged`, `previous` dropped); a `removed` entry whose content is live is dropped. A notice that really
  left stays in `removed`.
- Tests (`app/workers/feeds/test/events.test.ts`, two `(T675)` cases): a re-key alone, a re-key beside a real amendment,
  removal and new notice, the sibling case, and staging's own feed as a fixture
  (`test/fixtures/inseason/feed-staging-rekeyed-2026-11-11.json`). Mutation-checked: 5 mutations of the rule, each fails.

## Checked on staging (2026-11-11, after `deploy/staging/feeds.sh`)
- `/status.json` in-season: ok, rows 32, new 0, changed 0, removed 0, unmatched 0. The feed: 32 rows `unchanged`, no
  `previous`, `removed` empty.

## Not recoverable
- Staging's `firstSeen` stays 2026-11-08 (the re-key reset it and nothing recorded the earlier value). Production has
  not run the content ids yet, so its `firstSeen` values carry over when it does.

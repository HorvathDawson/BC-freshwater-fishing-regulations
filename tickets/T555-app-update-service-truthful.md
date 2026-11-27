# T555 — Update service: a refused release never reads "Updated"; one byte count; every format bump is gated; saved ids follow aliases

Status: IN PROGRESS — items 1, 3, 4, 5 built (9312354, ed637de, ab6c897, 138f1a5e0, 5018a81d8, d707dcdfb); item 2 is its own ticket, T673; left (Claude): the emulator check under "Not device-tested" below.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T555 | Update service: a refused release never reads "Updated"; one byte count; every format bump is gated; saved ids follow aliases | IN PROGRESS (items 1, 3, 4, 5 built; item 2 split to T673; left (Claude): emulator check of the saved-id rewrite) | 9312354, ed637de, ab6c897, 138f1a5e0, 5018a81d8, d707dcdfb | data pipeline review 2026-11-09 (verified); rank 11, severity 3, 5 findings; item 4: gate derived from `app_reads.json` (every contract file per app version, `pnpm app-reads`) … (detail: ticket file) |

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 11 of 17; highest severity 3 of 5.

## Done
- 1. A refused (or retrying) release made the update service say "ready" then "Updated to ..." while the phone stayed on
  the old set (app/packages/data/src/release/boot.ts:420-424, App.tsx:343-398, update/service.ts): built (commits in Status);
  the rest with T585 F12 (e4add6e9a): a re-boot onto the set already in use is never "Updated to" (`bootSaid`).
- 2. Release-bound and atlas-bound feeds at one unversioned key (`feeds/inseason/matches.json`, `runtiming/index.json`)
  → T673 (feeds keyed by release and atlas); not built here.
- 3. Two byte-count functions (update/service.ts releaseNeed vs release/install.ts installBytes, which also counts the
  synopsis, 11,722,042 B) and four "did the set change" tests: built (commits in Status).
- 4. App-version gate (d707dcdfb): `pipeline/deliver/release/app_reads.json` per app version, the formats it reads for
  every contract file (the contract's own + the app's `ALSO_READS`), written by `python -m
  pipeline.tools.emit.app_reads`; its `--check` is in `pnpm check` (`app-reads`) and `scripts/fast-fail.sh`.
  `app_gate.problems` derives the oldest app that reads ALL of a manifest's formats and refuses a stamp below it;
  `site_gate.required` asks the same; `PAINT_NEEDS` / `SALMON_NEEDS` are gone. NEW STOP for every lane: a version bump
  in app.json needs `python -m pipeline.tools.emit.app_reads` (one line, like the changelog entry; `--replace "<why>"`
  only when no build of that version left the machine). Tests: `test_app_reads.py`.
- 5. Saved ids follow `core.item_alias` (ab6c897, 138f1a5e0, 5018a81d8): every by-id read in `@app/data` (`water`,
  `regulationKeys`, `locate`, `waterBox`, `waterPoint`, `itemOrd`; `WaterSource.currentItem` the one resolver); a KEPT
  id is rewritten once per release opened (`followSavedItems`, `packages/data/src/savedIds.ts`): a spot's `item`, a
  gauge alert's `water.item`. Recent searches hold text, never an id. Release check
  `pipeline/deliver/release/gone_ids.py` (`checks/<r>/gone_ids.txt|json`, lists, never stops): r-a178299f6005 →
  r-303b4007a70a 19,788 of 19,806 kept, 18 absorbed and aliased, 0 gone — absorbed ids HAVE shipped. Tests:
  `release/alias.test.ts`, `regs/reads.test.ts`.

## Work (left): the emulator check, "Not device-tested" below

## Acceptance
A refused set shows a failure state, not "ready/Updated" (test with a bad sha). One install-size function, used by the mobile-data gate. The app-version gate is generated from every format constant. Saved ids resolve through core.item_alias (test with an absorbed id).

Depends: —. Size: M. Owner: no.

## Not device-tested (the emulator run should check)
- A spot and a gauge alert saved on an absorbed id (e.g. one of the 18 above, saved on build 22's release) open their
  water after the update, and the spot's water link and "this water's spots" list hold the new id.
- The app starts and updates as before with the new `release/opened` record (T585 F12) on a phone that already holds a set.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
data pipeline review 2026-11-09 (verified); rank 11, severity 3, 5 findings; item 4: gate derived from `app_reads.json` (every contract file per app version, `pnpm app-reads`); item 5: every by-id read follows item_alias, kept ids rewritten once, release lists ids gone with no alias (18 absorbed ids have shipped, all aliased)

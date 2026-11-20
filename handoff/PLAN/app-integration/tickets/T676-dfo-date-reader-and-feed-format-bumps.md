# T676 — The release and the live feed read a DFO date cell the same way; a feed format bump does not strand an older app

Status: IN PROGRESS (2026-11-20, b1f6f23dc, review fixes 719fd569c 243b46352 2c316de53) — items 1, 3 and 4 done (4f591c31a, 4bc03ac14); item 2 built on the owner's 2026-11-20 pick (per-feed `minAppVersion`), branch lane/l1-feeds. left (Claude): main chat review + merge, then the staging feeds worker deploy (the worker stamps the dfo / inseason / live indexes) and the next app build (the gate ships in the app; an app older than it ignores the field) 2026-11-20 07:18: merged (27e98e4eb), staging feeds worker deployed (stamps live). 2026-11-20 10:35: leftovers L2-L4 built on lane/la-feeds-leftovers (31b04ca5c: a refused gauge file asked again only by its validator, the More row shows any gauge-file hold steadily, the foreground obs sync reports); left (Claude): main chat review + merge, then the next app build.

Source: T556 (data pipeline review 2026-11-09, findings 1 and 3). T556 pinned the two languages with shared fixtures and
generated the format strings; these two are changes of behaviour, not pins.

## Today
1. **Two date readers, on purpose.** The Python reader (`pipeline/regs/dfo_salmon/dates.py`, what the RELEASE's salmon
   file is typed with) refuses date cells the worker's robust reader (`app/workers/feeds/src/dfo/dates.ts`, what the
   LIVE feed is typed with) reads: "Aug 2 To Aug 31", "1 Oct to 31 Dec", "Oct 1st to Dec 31st"
   (`app/workers/feeds/test/dfo.dates.test.ts`), and "<day> until further notice". The same page is then a dated rule in
   the live feed and an unparsed advisory (or an undated rule) in the release file; which one a phone shows depends on
   whether it holds a feed capture (`pickSalmon`). On the saved pages the only case is "until further notice": six
   locators, the same days closed, different words (`dfo.vectors.test.ts` names it and counts it).
2. **A feed format bump strands older apps.** Every live feed sits at one unversioned key
   (`feeds/dfo/index.json`, `feeds/inseason/…`, `feeds/live/…`, `runtiming/…`) and its reader refuses any format but its
   own. The strings are now one table (`pipeline/common/feed_formats.py`), so the two sides cannot disagree at build
   time — but an INSTALLED app still reads "no feed" (reported since T556 for DFO, T548 for in-season) from the day a
   bumped feed is published. `minAppVersion` gates releases only (`update/service.ts`).
3. The DFO index answering an HTTP error (not 200) is still silent in the app (`app/packages/data/src/feed/salmon.ts`
   `get`); in-season reports it (T368). Left alone in T556: production serves no DFO feed yet, and a 404 reported every
   hour needs the report's dedupe checked first.
4. The vectors cover parse → untangle → locations → typed → calendar and the name key. NOT covered: the join to the
   curated entries (`entries.ts` `reconcile` / `byFingerprint` against `entries.py`), `header.ts` and `quota.ts` against
   their Python (their own tests run on the same pages, one side each).

## Work
1. One date reader's reading: port the robust reader to Python (or generate both from one table of shapes), so the
   release file and the feed type a date cell the same; then the "until further notice" difference in
   `dfo.vectors.test.ts` goes and robust mode equals the vectors with no exception. Needs the owner's nod on reading
   "until further notice" as open-ended in the release (it is a reading of DFO's words).
2. A bump that does not strand: publish a bumped format at a versioned key and keep writing the old one for a stated
   time, or add `minAppVersion` to each feed index and reuse the release gate's update prompt. Decide with T673 (the
   same keys move there).
3. Report a refused DFO index (HTTP error) once the report's dedupe is confirmed.
4. Add entries-join, header and quota rows to the vector file.

## Acceptance
A date cell reads the same in the release file and the live feed (one vector file, no named exception). A feed format
bump on staging leaves the previous app version showing its last capture AND an update prompt, by test.

Depends: T556 (done), T673 (item 2). Size: M. Owner: item 1's reading; item 2's choice.

## Done (2026-11-11, 4f591c31a)
- **Item 4.** The DFO vectors (`python -m pipeline.tools.emit.dfo_vectors`) now hold, per saved page: the fixture entry
  file joined to it (`entries.by_fingerprint` → location_id, `entries.reconcile`), and the region header
  (`header.parse_header`, `digest`, `resolve`, `deliver.salmon.header_answers` — bucket / shared / sizes / hours / folds —
  and `quota.header_closure`); plus `joins.page_default` (a curated page default added to region 8's file — the input is
  in the vectors, the worker loads the same). `dfo.vectors.test.ts` reproduces every value; `test_dfo_vectors.py` pins
  that a reconcile / header / date-reader change on the Python side alone fails `--check`. The fixture's `scopes` are
  left out of the join (the worker reads none; region 6's fixture predates the residual rule `entries.load` enforces).
- **Drift found and fixed:** the worker's `reconcile` called every curated page default `dormant` ("absent from the
  page") — Python's T349 guard was never ported. Six of the nine curated region files hold one. No feed output moved:
  the worker never publishes a page-default place (`page:*` fingerprints are no table row), so it never reached `gone`
  or `reworded`.
- **Item 3.** `httpSalmonFeed` reports an index HTTP refusal (`dfo index.json: HTTP <n>`), offline still not. Dedupe
  confirmed in `ui/crashReport.ts`: one error is sent once a run (its id folds digits, so every status is one report),
  at most `maxPerRun` (5) a run. Production serving no DFO feed yet means one report per app start there until it does.
- **Item 1, the reader.** `pipeline/regs/dfo_salmon/dates.py` `read_dates` is `dates.ts` `readDates` ported line for
  line (JS `\s`, ASCII `\b`, first-match `replace` kept). The vectors' `dates` hold its reading of 1,822 cells — every
  recorded cell (`date-cells.json`), the named shapes and a seeded fuzz (seed 676) — and the worker must read each the
  same: no exception, no difference.

## Done (2026-11-12, 4bc03ac14) — item 1, on the owner's answer
Owner 2026-11-12: "it is from that day to no end. If the thing is for a closure then it closes etc" (RULINGS D5).
- `typed.to_rules` reads the date cell with `dates.read_dates` (`typed.FORMAT` 3; the release refuses rows typed by
  older code). "<day> until further notice" is `When.open_ended` on the range from that day: it holds from its start to
  Dec 31 and runs on past the year end; before its start it speaks only where no dated row of its place does
  (`deliver.salmon.holds_kind` / `_held`, the worker's `holdsKind`). A cell the reader cannot read keeps the row's limits
  with `when.unparsed` and `CatalogueRule.check` ("dates unparsed — check DFO"): the species reads "check" every day, a
  gear line shows every day flagged. T358's "non-retention until further notice reads closed" stands.
- Words: `answers.common.when_words` says the last range "from <day> until further notice" ("until further notice" from
  Jan 1), as the worker does. `When.open_ended` and `CatalogueRule.check` are new model fields (DFO only; the export
  guide's `WHEN_TEXT` names `open_ended`).
- The worker reads a cell one way in both its modes (typed.ts lost the Python-parity date path; `toRules` /
  `locationCalendar` lost their `robust` argument). The vectors' named difference is gone: both modes equal the
  vectors with no exception; the six open-ended locators are counted and must say "until further notice".
- Effect on release `r-483b295de57c`, every chain recomputed in memory: NO open/closed status moves (the same days were
  closed before, all year); 6 undrawn places change their words (Nanaimo, the Fraser above the CPR bridge, Osoyoos Lake,
  the lower and middle Shuswap: "… (from <day> until further notice)"). The three other cells the reader now reads
  ("Aug 2 To Aug 31", "July 1 until Aug 8", "June 1 until Aug 8") are in the recorded archive only, on no page today.
- The scraped rows were typed again (`python -m pipeline.regs.dfo_salmon.feed`, 2026-11-12). Not changed: the scrape's
  health report (`validate.py`) still warns with the old `interpret_dates` (warnings only, read by no answer).
- Tests: shared rows "until further notice, a closure / an opening" and "date cells the old reader refused" in
  `dfo-rows.json` (both languages); `test_t671_salmon_readings.py` `test_t676_*`; `test_dfo_salmon.py`'s unreadable
  season test now pins the kept limit + `check`; `dfo.dates.test.ts`, `dfo.vectors.test.ts`.

## Held
- **Item 2** — owner's choice (versioned key vs per-feed `minAppVersion`), decided with T673. Not started.

## Owner answer 2026-11-20

Owner 2026-11-20 ("Couldn't we just assume most of these are done already?"), item 2 pick (versioned feed key, or a minimum app version per feed): the default is taken: a per-feed `minAppVersion`. The ticket stated no default; this one is chosen because the acceptance asks for an older app to keep its last capture AND show an update prompt. NOT built yet; Claude's next step.

Status line before this answer: PARTIAL (2026-11-12, 4bc03ac14) — items 1, 3 and 4 done (4f591c31a, 4bc03ac14): the release reads every DFO date cell with the live feed's reader on the owner's answer of 2026-11-12; left (Claude): item 2 (a feed format bump that does not strand an older app) — put the choice to the owner (versioned key vs per-feed `minAppVersion`; T673 closed as DONE 785fc9914 without deciding it), then build it. Split from T556 (2026-11-11): what its findings 1 and 3 proposed and T556 did not build.

## Done 2026-11-20 — item 2 (b1f6f23dc, lane/l1-feeds)
- **One table.** `pipeline/common/feed_formats.py` `FEED_MIN_APP`: per index feed (dfo_feed, inseason_feed, gauge_feed,
  runtiming_index) the oldest app that reads its format; emitted as TypeScript `FEED_MIN_APP`. Today every value is
  `0.0.0` (every app that carries the gate reads today's formats). `test_feed_formats.py` pins (format, minimum) pairs:
  a format bump without a new minimum fails, saying what to set.
- **Every writer stamps it** as the index's `minAppVersion`: the worker's `dfo/live.ts`, `inseason.ts`,
  `gauges/publish.ts`; the Python `regs/dfo_salmon/live.py`, `gauges/feed/publish.py`, `runtiming/feed/build.py` (run
  timing's index is written by Python only, not the worker as the plan said). Every reader takes extra keys, so today's
  installed apps read the stamped indexes as before (the gauge contract's key list gained it).
- **The gate** (core `feedGate.ts`): `createFeedGate(appVersion)` with the release gate's rule (`releaseAllowed`): an
  index for a newer app throws `FeedNeedsNewerApp`; the need is held per feed and cleared by a readable index. No app
  version (the web) passes everything, as the release gate does. App.tsx makes one and hands it to every reader
  (`httpSalmonFeed`, `httpInSeason`, `httpFeed`, `httpRunTiming`) and to the update service.
- **The readers**: an index for a newer app is refused, NOT reported (a known state), the last capture answers (DFO,
  in-season, run timing) or what the session/phone holds (gauges); the in-season inbox shows no "unreadable" line for it.
- **The prompt**: the update service treats a loader's `FeedNeedsNewerApp` as checked, not seen (the next check reads it
  again), logs `app-too-old` with "needs app ≥ x", and its snapshot carries `needsApp` (the gate's highest, whoever
  fetched). core `appTooOld(updates)` (a release's `app-too-old` or a feed's `needsApp`) is what Shell hands
  `useAppUpdate`: the same strong prompt, "New data needs the latest app".
- **Tests**: `app/packages/data/src/feed/gate.test.ts` is the acceptance — a staging format bump (`dfo-feed/4`,
  `minAppVersion` 0.9.0) on app 0.8.4: the capture keeps answering, the strong prompt shows, nothing is reported, the
  feed is read again next check, a readable index clears it; the mutation (no gate) reads as a refused, reported feed with
  no prompt; one case per reader. `core/src/feedGate.test.ts` (the rule, today's stamps pass an unstamped build, the web,
  `appTooOld`). Worker: dfo / in-season / gauge index stamp pinned (`dfo.worker.test.ts`, `events.test.ts`,
  `gauges.contract.test.ts`).
- **Not covered** (by design): `feeds/inseason/matches.json` (links carry their own format and are refused per T673) and
  the files an index names (fetched from the index, verified). An app older than this build ignores `minAppVersion`, so
  a bump still strands those until they update.
- Owner decision recorded in the Status (2026-11-20 pick) — no question left.

## Review fixes 2026-11-20 (243b46352; 719fd569c)
An independent review of lane/l1-feeds said "merge after fixes":
- **Every gauge file is gated, not only the index.** Station files and `obs.json` carry `minAppVersion` too (worker
  `gauges/publish.ts`, Python `publish.py` station files; the contract's key lists). The gate's `checkFile` refuses a file
  stamped for a newer app (held, never clearing: only the index clears) and the session's copy / the kept week answers,
  unreported. (The "a hold excuses any unreadable gauge file" rule this first built was removed by the second review.)
  `fetchObs` takes the gate; the background check has its own (`notify.native.ts`), so a bumped `obs.json` logs "gauge
  alerts wait for the app update" and reports nothing (alerts' states stand).
- **A 304 clears a held need**: the index in use answered unchanged → the feed's `needsApp` and the gate's need clear
  (`gate.note(feed, null)`), e.g. after a reader's own fetch met a bump the origin then took back.
- **A refused index is remembered**: its ETag, sha and need are kept (`refused` in the update store); the next check sends
  that validator, so a 304 or the same bytes re-emits `app-too-old` ("(unchanged)") with nothing downloaded; a restart
  holds the need at once; an app new enough for it drops the refusal and reads the index whole.
- **More says why**: a feed's row and the Updates line read "New data needs the latest app; the last copy is shown"
  (`UPDATE_WORDS.row.feedTooOld`; a release too old for the app is said first).
- **The Python stamps are pinned on the written files** (`test_dfo_live.py`, `test_feed_gaps.py` index + station file,
  `test_runtiming_feed.py`), no longer by grepping the source.
- 719fd569c: a gate listener that throws is reported, not swallowed (the T368 scan).
- Not done here (the main chat tickets it): the quota quote dropping other trailing conditions ("only 2 over 35 cm").

## Second review 2026-11-20 07:00 MST (2c316de53)
- **M1:** only a gauge file's OWN `minAppVersion` excuses it. The gate's hold no longer excuses a file this app cannot
  read (`holds()` removed): a broken file in this app's own format, or another format with no stamp, is reported (T557).
  The background check makes its gate per run (`notify.native.ts`), so no need outlives a run.
- **M2:** the update store's kept refusals are read back through `keptRefusals` inside the start-clean guard: plain-object
  entries with a semver `need` and a string-or-null etag / sha256; anything else dropped, the field dropped when not a
  plain object (`refused: "ab"` crashed every start; `{dfo:{need:5}}` gave needsApp 5).
- **M3:** `gate.test.ts` proves a broken station file and `obs.json` are reported with a gate present, held or not (fails
  under the reviewer's `why && opts.gate` mutation; checked); the M2 repros and seven more shapes start clean (fails with
  the guard removed; checked).
- **L1:** `gauges.contract.test.ts` pins `minAppVersion` on `obs.json` and every station file the worker writes.
- **Leftovers (noted, not fixed here; L2–L4 built 2026-11-20, at the end):**
  - L2: a gauge station file or `obs.json` refused as newer than the app is downloaded again on every read (only the
    index's refusal is remembered).
  - L3: the gauges row on More shows a need only from the index; one set by a station file or `obs.json` reaches the
    prompt (the gate) but not the row.
  - L4: `syncObservations` swallows every failure (`catch {}`), a broken `obs.json` included; the T368 scan lists it.


## Leftovers L2–L4 built 2026-11-20 (lane LA, `lane/la-feeds-leftovers`, 31b04ca5c)
The owner, 2026-11-20 10:00: "Make sure every left over goes on this release when we cut it".
- **L2, a refused gauge file is remembered.** Background (`fetchObs` with its kept copy, so `createCheckSources`' `HELD.obs`):
  a file refused as newer than the app is kept beside the good copy as `refused: {etag, need}`; the next run sends THAT
  validator, so an unchanged refused file is a 304 and the same `FeedNeedsNewerApp` (alerts wait), nothing downloaded. The
  gate is asked first whether the need still refuses (an app updated since reads it whole); a refusal not in its shape is
  dropped. Foreground (`httpFeed`): each refused file is remembered (validator, need, when); inside FEED_TTL_MS nothing is
  asked, after it a 304 by its validator; the sync icon (`refresh`) asks at once; a 404 drops it and its hold. The
  foreground obs sync now has its own in-memory kept copy, so an unchanged `obs.json` (refused or not) is a 304 too.
- **L3, the gauges row shows any gauge-file hold, steadily.** core `feedGate`: a file's need is held under its own name
  (`checkFile(doc, feed, url)`), cleared only by that file reading (or `clearFile` on a 404); the index passing and
  `note(feed, null)` (a 304 of the index) clear only the index's need. The update service's rows read the gate's word per
  feed (`refused()[id]`, index and files together) and publish when it moves, so the More row and the prompt no longer
  flicker (a 304 cleared them until the next station read set them again).
- **L4, the foreground obs sync reports.** `syncObservations`' `catch {}` is gone: a stamp (`FeedNeedsNewerApp`) is a known
  state (held, not reported), offline is the environment's (not reported), anything else is reported once per kind a run
  (`reportOnce`: the sync runs on every feed update). `tools/failures.test.ts` FINE for http.ts 3 → 2.
- **Tests** (`app/packages/data/src/feed/refusedFiles.test.ts`, 9; `core/src/feedGate.test.ts` file holds): background
  304 on the second and later runs, the origin taking the bump back, an updated app, through `createCheckSources`, bad kept
  refusals; foreground no request inside the TTL, 304 after, refresh, take-back, 404; the row held across two index 304s
  and cleared by the file; obs.json's hold on the row; a 500 / a broken file reported once each, offline and a stamp not.
  Mutation-checked (each fails the tests named): background refusal memory off; foreground TTL skip off; foreground
  validator off; rows from the service only; the index clearing file holds; `catch {}` back.

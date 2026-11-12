# T676 — The release and the live feed read a DFO date cell the same way; a feed format bump does not strand an older app

Status: PARTIAL (2026-11-12, 4bc03ac14) — items 1, 3 and 4 done: the release reads every DFO date cell with the live feed's reader on the owner's answer of 2026-11-12; item 2 waits on the owner's choice, decided with T673; left (Claude): item 2 with T673. Split from T556 (2026-11-11): what its findings 1 and 3 proposed and T556 did not build.

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

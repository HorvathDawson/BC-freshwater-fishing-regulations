# T676 — The release and the live feed read a DFO date cell the same way; a feed format bump does not strand an older app

Status: PARTIAL (2026-11-11, 4f591c31a) — items 3 and 4 done; item 1's reader ported and pinned, its use in the release waits on the owner's nod; item 2 waits on the owner's choice and T673; owner: OWNER-TODO "T676 reading to rule on"; left (Claude): wire read_dates into the release after the nod, item 2 with T673. Split from T556 (2026-11-11): what its findings 1 and 3 proposed and T556 did not build.

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

## Held
- **Item 1, wiring** (`typed.to_rules` reading with `read_dates`, so the named "until further notice" difference in
  `dfo.vectors.test.ts` goes). Changes what an angler is shown; the owner's nod is in OWNER-TODO.md. Measured on the
  281 recorded cells: 15 "until further notice" cells (the release types them all year with no window; the feed keeps
  the start day and says "from <day> until further notice"), and 3 the release keeps as an unparsed window that the
  robust reader reads ("Aug 2 To Aug 31", "July 1 until Aug 8", "June 1 until Aug 8"). Wiring also brings robust mode's
  `check` for an unreadable cell (limits kept, flagged) and `open_ended` on `When`, which the release model and the app's
  release reader do not carry yet — size M with the bundle side.
- **Item 2** — owner's choice (versioned key vs per-feed `minAppVersion`), decided with T673. Not started.

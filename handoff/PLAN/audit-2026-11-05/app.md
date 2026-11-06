# T333 audit — `app/` (packages, apps/mobile, workers, tools)

Finder: app area. Read-only; grep/read only, no builds or suites run. Paths are repo-relative under `app/` unless
they start with `deploy/` or `.github/`. Ranked most harmful first. Confidence = how sure I am the finding is real
and matters, not how sure the fix is.

---

1. **`counts()` answers 0 for a count it never read** · silent fallback + contradicts AGENTS 32 ·
   `packages/data/src/release/source.ts:282-286` ·
   `return { waters: num(r?.waters) ?? 0, reaches: num(r?.reaches) ?? 0, surveyed: null, stations: paint.stations.length }`
   — `get(CROSS.COUNTS)` returns `row | null` (`source.ts:80-81`); a release whose `counts` row is missing or
   unreadable yields `waters: 0, reaches: 0`, typed non-null in `BundleCounts` (`packages/data/src/types.ts:65-74`).
   AGENTS 32 says exactly this case must render as NOTHING ("we have not asked" and "there are none" are different
   claims; `surveyed: null` on the next field follows the rule, the two before it do not). The Layers / About sheet
   would print "0 waters" over a 19,699-water release.
   **Fix:** `waters: num(r?.waters) ?? null` (type `number | null`, as `surveyed`), or throw a typed `ReadError` when
   the row is absent — a missing counts row is a defective release, not an empty province. · confidence: high

2. **The feeds Worker's tests run nowhere but by hand** · dead pathway / mirrored Python with unexercised parity ·
   `workers/feeds/package.json:7-10` (`test: vitest run --root .`), `workers/feeds/test/*.test.ts` (22 suites incl.
   `dfo.parity.test.ts`, `gauges.parity.test.ts`, `gauges.py.test.ts`), `vitest.config.ts:108-123` (projects list
   `packages/*`, `conformance`, `tools`, `apps/mobile` — not `workers`), `deploy/staging/feeds.sh:10` (wrangler
   deploy straight away, no test step), `.github/workflows/*.yml` (no mention of `workers/feeds`). The Worker is
   outside the pnpm workspace by design (`package.json` `$comment`), so `pnpm check` never reaches it. The DFO
   parser (`workers/feeds/src/dfo/{typed,untangle,parse,header,quota}.ts`) and the gauge publisher
   (`gauges/publish.ts`, `gauges/change.ts`) are line-for-line ports of `pipeline/regs/dfo_salmon/*.py` and
   `pipeline/gauges/feed/publish.py`; their parity to Python is proved only by those suites, which nothing runs
   before a deploy. AGENTS 40 ("a value on both sides is generated, not typed twice") is satisfied in spirit only
   while the parity suite is green — and nobody is checking.
   **Fix:** add `workers/feeds` to `app-ci.yml` (`cd app/workers/feeds && npx vitest run`) and make
   `deploy/staging/feeds.sh` run it first (`set -e` already there); or list the suite in `scripts.check`. · high

3. **"logged below" catches that log nothing: a notification that fails to send is lost** · silent fallback / worker
   job failure not reported · `workers/feeds/src/index.ts:388, 406, 437, 458` (`catch { /* logged below */ }` around
   `deliver(...)` in `backup`, `purge`, `skill`, `runTimingCheck`) — the `console.log` that follows prints the JOB's
   result (`r.ok`, `copied`…), never the delivery error, so a failed "Backup failed (urgent)" email vanishes with no
   trace. Same shape at `index.ts:262` (the reports/crash digest read fails → one line in the run's private log,
   `log: logs.slice(-40)`, that no human reads unless they POST the job by hand) and `backup.ts:95`
   (`/* the result is also logged by the caller */` — the caller logs `r`, not the swallowed error). The owner's rule
   is "no silent fallbacks; a failure not shown is reported"; the Worker has no `reportFailure`, so "reported" here
   means the status page / an event — and these are the one class that cannot become an event because the event
   path is what failed.
   **Fix:** in each catch, `console.error(JSON.stringify({ job, notifyError: String(e) }))` and surface it on
   `/status` (a `lastNotifyError` field in `private/feeds/status.json`, shown red in `statusHtml`). · high

4. **The no-silent-fallback net stops at `packages/` + `apps/mobile`** · silent fallback (coverage) ·
   `tools/failures.test.ts:24-25` (`ROOTS = [core, data, ui, ui-native, map, apps/mobile]`) — `workers/feeds/src`
   (17 comment-only catches: `reports.ts:144,152,241,270,328`, `crashes.ts:186,218,272`, `dispatch.ts:94`,
   `index.ts:388-458`, `backup.ts:95`) and `tools/*.mjs` are unscanned. T368's header says "app-wide" and the owner
   named the sweep T333; the Worker is the one place a swallowed catch costs a data refresh or an owner email, not a
   screen. Several of those catches ARE fine (a broken R2 object skipped, "fresh" state), but nothing lists them with
   a reason, so a new swallow there is invisible.
   **Fix:** add `workers/feeds/src` to `ROOTS` (the worker is TS; the scanner needs no runtime), give it a
   `FINE` block with the ~12 defensible sites, fix #3's four. · high

5. **Crash and report wire shapes are typed twice, with no parity test, and the Worker refuses anything it does not
   expect** · duplicate data type / parallel enums · app: `packages/ui/src/crashReport.ts:24-60` (`CRASH_FORMAT`,
   `CrashKind`, `RouteKind`, `CrashSheet`, `CRASH_LIMITS = {60,200,2000,100}`, `CrashPayload`),
   `packages/core/src/menu/report.ts:189-240` (`ReportContext`, `reportPayload`); Worker:
   `workers/feeds/src/crashes.ts:33-68` (`CRASH_KINDS`, `ROUTE_KINDS`, `VIEWS`, `SHEETS`, `CRASH_LIMITS` retyped,
   `CrashPayloadV1/V2`), `reports.ts:43-47` (`STR_FIELDS`, `INT_FIELDS`, `V2_STR_FIELDS`, `V2_NUM_FIELDS`). The
   only link is a comment (`crashes.ts:5`, `reports.ts:4`). `crashes.ts:100,113`: "every key known to THIS version …
   any unknown key is refused; every crash/2 field is required". So the day a `RouteKind` or a context field is added
   on the app side, every crash and report is answered 400 — and the crash reporter cannot report its own refusal
   (`crashReport.ts` FINE: 14 guards). The Worker's tests (`test/crashes.test.ts:4-7`, `test/reports.test.ts:5-9`)
   build payloads by hand, never from the app's `crashPayload()` / `reportPayload()`.
   **Fix:** one of: (a) a `tools/wire.test.ts` in the app project that imports both sides (tools may import
   anything) and asserts `validateCrash(crashPayload(sample)).ok` and `validateReport(reportPayload(sample)).ok`
   plus enum-set equality; (b) move the four enums + limits into a tiny shared file the Worker reads by relative path
   (it already resolves `vitest`/`typescript` from `app/node_modules`). · high

6. **`InSeasonFeed` is two shapes already drifting** · duplicate data type ·
   `workers/feeds/src/inseason.ts:286-295` (writes `format, fetchedAt, changedAt, source, pageHash, matching, rows,
   removed[{id, region, water, change, effective, removedAt}]`) vs `packages/core/src/inseason.ts:41-47` (reads
   `format, fetchedAt, source, rows, removed[{id}]`). `parseInSeasonFeed` (`core/inseason.ts:66-72`) is lenient
   (`removed ?? []`, `source` → `""` when not a string), so the drift is masked rather than caught; `FeedRow` vs
   `InSeasonRow` likewise. Same class as #5, lower stakes (informational banner).
   **Fix:** same mechanism as #5: a test that feeds the Worker's `FEED_FORMAT` output into
   `parseInSeasonFeed` and refuses `null`; drop the `?? []` / `""` defaults so a half-feed is `null`. · medium

7. **A default secret: the report rate-limit salt** · silent fallback (config) ·
   `workers/feeds/src/reports.ts:146` `env.REPORT_SALT ?? "bc-fishing-report"` — the per-IP key hashed with a
   constant in the repo if the wrangler secret is absent. A deploy without the secret silently works and every IP
   hash is pre-computable. Sibling: `index.ts:65` `env.FEEDS_ENV || "staging"` and `reports.ts:183` /
   `crashes.ts:176` `isTest = … && env.FEEDS_ENV === "staging"` — a production worker deployed without the var
   tags every email "[staging]" and honours client-claimed `test: true` (test reports are purged after 7 days,
   `index.ts:397`). Both are "works anyway" defaults on values that should be required.
   **Fix:** refuse to start the job / answer 500 when `REPORT_SALT` or `FEEDS_ENV` is unset
   (`if (!env.REPORT_SALT) throw new Error("REPORT_SALT not set")`), and pin them in `wrangler.toml` vars /
   secrets per environment. · medium

8. **`PanelMember` / `BasinMember` typed twice, in core and data** · duplicate data type ·
   `packages/core/src/paint.ts:24-33` (`PanelMember {panel, ord, station, role, areaKm2, years, regulated,
   sameRiver, lead, lake}`, `BasinMember {basin, station, areaKm2, years}`) vs `packages/data/src/types.ts:145-180,
   506-512` (`PanelMember {station, role, areaKm2, years, regulated?, sameRiver?, throughLake?}`, `BasinMember
   {station, areaKm2, years}`). `data` imports `core` and `release/source.ts:201-220` builds `data.PanelMember[]`
   from `loader.paint()` rows typed as `core.PanelMember` — the same donor, two field sets, two spellings of the
   lake flag (`lake` vs `throughLake`). The "SEPARATE … ON PURPOSE" note at `types.ts:225` is about `PanelRoute`,
   not this pair. Nothing pins that every field the sheet reads exists on the paint row.
   **Fix:** in `data/types.ts`, `export type PanelMember = Pick<CorePanelMember, "station"|"role"|"areaKm2"|"years"|
   "regulated"|"sameRiver"> & { throughLake?: true }` (or rename core's `lake` → `throughLake` and `Omit` the
   indices); same for `BasinMember`. · medium

9. **`GearMark` is two enums** · parallel enum · `packages/map/src/paint.ts:150` `type GearMark = "fly" | "bait"`
   vs `packages/core/src/method.ts:147-148` `type GearMark = Exclude<MethodClass, "none">; GEAR_LEGEND = ["fly",
   "bait"]`. `map` may import `core` (`layers.json`); the legend (`ui-native/shell/MapLegend.tsx:49`) iterates
   core's while the layers (`map/paint.ts:146-149` `GEAR_LAYERS`) are keyed by map's. A third method class
   (e.g. "artificial") would type-check on one side and have no layer on the other, and no test says so.
   **Fix:** `import type { GearMark } from "@app/core"` in `map/paint.ts`; make `GEAR_LAYERS:
   Record<"stream"|"lake", Record<GearMark, string>>` so a new class fails to compile until it has layers. · medium

10. **The platform line of a report is composed in four places** · duplicate pathway (one value, four code paths) ·
    `apps/mobile/src/App.tsx:98, 117, 408`, `apps/mobile/src/notify.native.ts:48`,
    `packages/ui-native/src/menu/MenuHost.tsx:101` — each writes `WEB ? "web" : \`${Platform.OS} ${Platform.Version}\``
    (or without the web branch at `:98` / `notify.native.ts:48`, where `ua: true` headers get `"web undefined"`-style
    strings if ever reached on web). That is three of `check-platform.mjs`'s `INLINE_ALLOWED` lines
    (`App.tsx` 5, `MenuHost.tsx` 2) spent on one word. A crash report, a problem report, the data-request header
    and the notification check can disagree about where the reader is.
    **Fix:** one `platformLine()` in `apps/mobile/src/platformLine.web.ts` / `.native.ts` (+ `.d.ts`), passed down;
    lower the three `INLINE_ALLOWED` counts. · medium

11. **`tile-ids.native.ts` is dead; the map package has a second, unguarded platform mechanism** · dead code +
    platform mixing the T410 guard misses · `packages/map/src/tile-ids.{web,native}.ts` with no `tile-ids.d.ts`
    (the only pair without one; README "Web and native code" says a pair is "typed by one foo.d.ts");
    `packages/map/src/web/useMapInstance.ts:16` and `web/useChannels.ts:15` import `../tile-ids.web` BY ITS PLATFORM
    NAME, so nothing ever resolves `./tile-ids` and the `.native` half (`tile-ids.native.ts:2` "NOTHING TO READ")
    exists only to satisfy `check-platform`'s pair rule. Beneath that: `map/src/web/` (15 imports from
    `Map.web.tsx`) and `map/src/native/` (5 from `Map.native.tsx`) are a FOLDER split; `check-platform.mjs:168-185`
    scans those files as "shared" (they are not `.web.ts`), and nothing refuses a shared file importing from
    `./web/` or `./native/` — `web/useMe.ts` already imports a sibling. knip cannot see the dead half either
    (`knip.jsonc` makes every `*.{native,web}.ts` an entry).
    **Fix:** delete `tile-ids.native.ts` and rename `tile-ids.web.ts` → `web/tileIds.ts` (it is web-only by
    nature); teach `check-platform` that `packages/map/src/{web,native}/` are platform folders: only `Map.<platform>`
    and their own siblings may import them. · medium

12. **A section's standing has two pathways: the map's classes and the sheet's per-section read** · duplicate
    pathway ("one load path per screen") · map: `packages/data/src/standing.ts:117-139` (`speakerStanding` →
    `panelStanding(speaker.panel, …)`, the panel as the release interns it); sheet:
    `packages/ui/src/panel/standings.ts:166-173` (`answerFrom(withThroughLake(panels.get(section), lakes), …)`).
    `withThroughLake` (`data/panel.ts:342-345`) is a no-op when `panel.lakeKnown` (paint/5) but on a paint/4 release
    it marks members from the ROUTE walk — a factor (`THROUGH_LAKE_FACTOR`, `trust.ts:22`) the map never applies.
    So on paint/4 the map colour and the sheet number for one section come from different arithmetic; `standing.ts:19`
    says "ONE ARITHMETIC" and `standings.ts:167-170` says "one door now" about the fetch, not the lake factor.
    Whether a paint/4 release can still boot is the question; nothing refuses one, and the fixture releases are
    paint/4 (`types.ts:216` "Absent on a paint/4 release").
    **Fix:** either refuse paint/4 at `loader.paint()` (then delete `withThroughLake` + `throughLakeStations` and
    the route fallback) or apply the same `withThroughLake` in `speakerStanding`. One path, asserted by a test that
    colours and reads the same section over the fixture release. · medium-low

13. **A malformed station file reads as "no readings"** · silent fallback · `packages/data/src/feed/http.ts:433-448`
    `observations()`: `f?.recent ?? []` → `null` (fine: not published), but a file WITH `recent` and a missing or
    mistyped `now` / `daily` / `forecasts` / `priorYears` becomes `{}` / `[]` — the card shows no forecast and no
    prior years with no failure shown or reported. The publisher (Worker) writes all of them; their absence is a
    defect in the feed, not the environment. Same at `history.ts:150-169` (`obs.stations ?? {}`, `base.daily ?? []`).
    **Fix:** a `parseStationFile(x): StationFile | null` (as `parseSalmonFeed` / `parseInSeasonFeed` do) that
    returns `null` for a half file, and the caller throws a typed `FeedError` that `useAsync` reports. · medium-low

14. **`/status` is public and prints error detail** · worker: unvalidated exposure ·
    `workers/feeds/src/index.ts:495-506` (no auth, by design) → `status.ts:245,282,220` print
    `lastError.error.slice(0, 300)` / `.detail.slice(0, 160)` / `sk.error.slice(0, 300)`; `index.ts:335` stores
    `String(e.stack ?? e).slice(0, 2000)` as the error, so the first 300 chars of a stack (file paths, R2 keys,
    source URLs with query strings) are on a no-auth page. `servedStatus` (`status.ts:117-124`) sends the whole
    `lastError` in `/status.json`.
    **Fix:** store `message` and `cls` for display and keep the stack under `private/feeds/status.json` only, or
    gate `/status` behind the bearer (the owner is its only reader). · low-medium

15. **`style.json` theme order comes from the file system** · non-determinism in a committed generated file ·
    `tools/build-style.mjs:38-40` `readdirSync(dir + "themes").filter(.json).map(parse)` — no `.sort()`; themes are
    then iterated in that order (`:62, :76, :167, :206`) into `style.json` / `style.meta.json`, and `pnpm style`
    fails CI when the committed output differs. Two themes today and APFS/ext4 happen to agree; a third theme or a
    different FS makes `pnpm style` flap between machines.
    **Fix:** `.sort()` the listing (and iterate `tokens` by sorted key where its order lands in output). · low-medium

16. **The data Worker throws on a malformed path** · worker: unvalidated input ·
    `deploy/staging/data-worker.js:31` `decodeURIComponent(new URL(request.url).pathname.slice(1))` — a request
    like `/%E0%A4%A` raises `URIError` outside any try, so the worker answers a 500 (a Cloudflare error page, no
    CORS headers) instead of 404. `limited()` at `:20` fails OPEN when the RL binding throws (documented, fine).
    **Fix:** `try { key = decodeURIComponent(…) } catch { return 404 with CORS }`. · low

17. **A client-claimed `env` decides storage** · worker: trust of input · `workers/feeds/src/crashes.ts:171`
    `if (v.crash.env === "local") return 202 not stored` and `apps/mobile/src/env.ts:36` (local builds post to the
    STAGING worker). Anyone can post `env: "local"` to be answered 202 and skip the rate limiter (`:173` runs after);
    conversely a mislabeled local build drops real crashes. Low stakes on staging; worth a line because the same
    `env` string is #7's `isTest` gate.
    **Fix:** rate-limit before the `local` short-circuit; consider an allow-list of `env` values. · low

18. **Dead / stale pieces** · dead code / doc drift ·
    - `tools/testbed/heapprof.py`: referenced only from untracked `tools/testbed/out/runs/*/report.json`; not in
      `tools/testbed/README.md`; outside knip (Python). Either document under the test bed's "tools" or delete.
    - `workers/feeds/src/inseason.ts:2` and `index.ts:2` cite `archive/pipeline/recurring/in_season` as the source
      of the port; T335 deletes `archive/` — the comment becomes a dangling reference (same in
      `tools/build-style.mjs:291,543,602`, `tools/build-spinner.mjs:6`, `tools/build-icons.mjs:8`,
      `tools/seo/legacy-crosswalk.mjs:5`, `map/src/runtime-style.ts:468`, `map/src/hatch.ts:8`,
      `ui-native/src/FishSpinner.native.tsx:6`, `map/style/layers.source.json:2166`): nine "see archive/…" pointers.
    - `knip.jsonc` allowance `packages/data/src/sql/queries.ts: ["exports"]` ("PREPARED, NOT FED") — every export
      has a consumer now (`release/queries.ts` wraps them; `STOCKED_ON_ITEM` is read by `source.ts`); the reason
      names three queries, the allowance covers 25 exports.
    **Fix:** delete or document `heapprof.py`; rewrite the nine comments as "v1 (git: archive/… @ <sha>)" when
    T335 lands; narrow the knip allowance to the three named exports. · low

19. **Two `SalmonFeed`s, two `paint.ts`, two `Held`s, two `Notice`s** · naming collisions (not duplicates, listed
    so the verifier does not re-find them) · `core/salmonFeed.ts:46` (the `dfo-feed/3` file) vs `map/colouring.ts:82`
    (the map's salmon input); `core/paint.ts` (the paint file) vs `map/paint.ts` (map paint); `data/manifest.ts:20`
    vs `workers/.../status.ts:11`; `data/notify/check.ts:39` vs three Worker `Notice`s. Different ideas, one word
    each — a reader grepping for the type lands on the wrong one. No fix needed beyond a rename if touched. · n/a

---

Not found (checked, clean): a TS re-run of the regulation cascade (every reader in `core/src/regs/*` and
`core/src/salmon.ts` looks up precomputed verdicts; `salmon.ts:5-10` "NOTHING HERE READS A RULE"); device-clock
reads outside `core/clock.ts` (every `new Date(x)` in packages constructs from a value); DOM globals or
`Platform.OS` in shared files beyond `INLINE_ALLOWED`; external assets beyond Esri imagery + Open-Meteo (the
remaining `https://` are licence/credit links and one Google Maps directions link out of the app); curated files
read at run time; retired field names (`windows`, `size_*`, `barbless` as a field) in app code — `min_size` in
`salmon.ts:403` / `dfo/quota.ts:170` is DFO's own key; `schedule:` in any workflow; committed `dist/`,
`graphify-out/`, `tools/testbed/out/` (all untracked); timestamps in `*.generated.ts`, `thumbs.json`, `fonts.json`
(sha-pinned); the status vocabulary (one function in core, no literals elsewhere).

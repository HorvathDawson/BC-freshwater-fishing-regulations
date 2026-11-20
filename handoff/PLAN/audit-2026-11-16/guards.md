Saved by the main chat from the checker's hand-back (the checker could not write files).

# General audit, checker "guards": do the guards still guard, and what do the checks cost?

Read-only pass on `redesign/stream-sections`. Evidence: last night's logs (`scratchpad/logs/`), the code, GitHub's run list, the crash reports, and a few small test runs.

## 1. Inventory of exceptions and allow-lists in force

### Flag these

| Exception | Where | Who / when | Finding |
|---|---|---|---|
| Closed golden parts are held only to "the part at that index is closed" | `app/tools/regs-recut/taps.py` lines 399-406, 433-437, 478 | T278 / T517 mechanism | **Hides a real fault.** This is the hole that put golden state `x15p9` on the wrong closed stretch last night. The fix (53d8e74c5) corrected the number only. Four golden states are still checked this weakly: `x15p9`, `x16p7`, `chw0`, `x17p5`. `PART_GROWN_IS` pins label and section count for grown parts; closed parts have no such pin. |
| Golden declarations as a whole | `taps.py` `PART_MOVED` 17, `PART_GROWN` 11, `PART_GONE` 11, `PART_RECOUNTED` 20, `PART_TIDAL` 5 | Claude, release by release, citing owner tickets | 64 of the 134 golden states (48%) carry a hand declaration, and 20 waters are excluded from the Parts-sheet comparison (`taps.json` `recut`). Each release that moves a part needs hand edits; last night they cost four attempts. Still needed while the frozen 134 states stay the oracle. |
| `taps.py` stops at the first stale declaration | `raise SystemExit` in `resolve()` | - | Attempts 3, 4 and 6 each found one more. It should list all of them (the T666 rule already applied to preflight). |
| `exclusions.ts` | cited by `taps.py` lines 41 and 47, `app/packages/core/src/regs/testing/golden.ts` lines 30 and 37, `core/src/regs/tree.ts` line 54 | removed by T439 (9fc2a3953) | **The file no longer exists.** The exclusions now live in `taps.json` (`gone` 11, `tidal` 2, `recut` 20). `tree.ts` says it "counts every golden line it changes" and nothing does. |
| Held continuation links | `data/generated/bundle/continuations_held.json` (10 rows) | owner gate of 2026-10-29 | 5 of the owner's 17 reviewed links are now `stale` (the rule no longer proposes them, so they silently stopped shipping). 4 new links wait as `label` with no owner item; `OWNER-TODO.md` does not mention them. Only the build log prints this. |
| Data tier and slow tier | `-m` deselection | P00, 2026-10-20 | See 1b. No automatic runner. |
| `release-perf.test.ts` (14 tests: "the release answers like the bundle") | `describe.skipIf(!live)`; needs `RELEASE_DIR` and `BUNDLE` | Tier 2 | Skipped even in the release's own strict run (the release sets `RELEASE_DIR`, not `BUNDLE`). Only `scripts/test-full.sh` runs it. |
| `fetch_status` swallows every error | `pipeline/deliver/release/pieces_gate.py` (`# noqa: BLE001`) | 2026-11-15 | Fails closed, so it hides no fault, but it throws the cause away. A 403 read as "the worker cannot be asked". The user agent was added; the message still cannot say "HTTP 403". `deploy/lib/input-formats.sh` line 35 has the same shape. |
| Release ledger lost its acceptance record | `data/generated/release/ledger.json`, `r-46e4355a7277` `binding_diff` | attempt 9 | Now reads "same reach run": attempt 9 re-took the baseline from the already-promoted release and overwrote the record that a 407-rule diff was accepted. The diff file is still on disk. |

### In force and still needed (no action)

| Exception | Count | Who / when | Note |
|---|---|---|---|
| `NOT_LANDED_OK` (`pipeline/regs/review/apply.py`) | 2 tidal marks | 2026-11-09, T359 | Both wait on one owner decision ("does a confirmed DEM point become a cut?"), open since 2026-11-03. The check passes and names them. |
| Closed review domains (`data/curated/review/closed.json`) | ponds, proposals | owner 2026-10-30 (T329); Claude with owner approval (T331) | The landing check prints "not checked: ponds, proposals". Correct while nothing reads them. |
| Accepted unplaced pieces (`unplaced_piece_calls.json`) | 2 accepted, 0 proposed | owner 2026-11-11 (Noons Creek, Seymour River) | City-stream pieces far from the named water. A stale row stops the run. |
| Rules allowed to bind nothing (`test_t89_rules_that_bind_no_water.py`) | 8 by design + 10 awaiting a water | owner T89, T708, T713 | Exact both ways on the live bundle. |
| `QUOTA_UNDER_CLOSURE_KNOWN` / `RELEASE_UNDER_CLOSURE_KNOWN` | 19 / 1 | rulings of 2026-10-05/06 | The export refuses anything outside them. |
| Pipeline skip allow-list (`test_no_silent_skips.py`) | 5 sites | 2026-10-09 | No skip or xfail mark on any of the 7,974 collected tests. |
| App skips | 36 test files use `skipIf` | T432 | On GitHub every release-backed suite skips. Locally 37 tests skip; in the release's strict run 17 (two screenshot tools, one overzoom test, `release-perf`). |
| `INLINE_ALLOWED` (`check-platform.mjs`) | 7 files | owner rule T410 | Each with a reason. |
| `OWNER_APPROVED_FWA_EXCLUDE` | empty | - | Nothing excused. |
| Release escapes (`--no-fast-fail`, `--no-fresh-copy`, `--no-follow-tests`, `--no-ci-check`, `--ship-stale-atlas`) | none used | - | None is remembered by the ledger (`REMEMBERED` = baseline, accept-diff, accept-fixtures, min-app, push); none of the nine attempts used one. |
| `SITE_DIRTY_OK`, `CI_CHECK_SKIP`, `SITE_CHECK_SKIP`, `NO_WORKER_YET`, `HEAVY_NO_FAST_FAIL` | not used last night | - | The staging site reports `commit 5c5078b2a, dirty: false`. `SITE_DIRTY_OK` also exists in `deploy/prod/site.sh`. |
| Suppressions added since 2026-11-10 | 18 eslint-disable, 6 ts-expect-error, 57 noqa, 15 `|| true` | - | Read each one outside tests. Nothing hides a fault except the two "cause thrown away" cases above. |

### 1b. What each tier never runs (pipeline suite, 7,974 tests collected)

| Tier | Runs | Never runs |
|---|---|---|
| GitHub `pipeline-ci`, fast-fail's whole tier, the fresh copy (same expression) | 6,490 | 1,208 data tests (115 files) and 276 slow tests (30 files) |
| Smoke (`-m smoke`) | 442 (measured 17.6 s) | the rest |
| Release "follows" stage | 7 pipeline files + the whole app check, on the candidate | 108 of the 115 data-test files |
| `scripts/test-full.sh` | everything | run by hand only; the last STATUS notes about it (2026-11-11) say "killed twice at ~70 %" |

**Run by no automatic tier:** every `needs_*` test outside the 7 release-follow files, every `slow` test (the determinism and full-build guards AGENTS rule 18 says CI must run), and `app/tools/release-perf.test.ts`. Last night about 700 tests of the review files were run by hand after the build (17 failures and 2 errors, nearly all test faults; the first attempt in one process was killed at 50 GB).

## 2. Gaps

| # | Stop | Cheap check | Exists now? |
|---|---|---|---|
| 1 | Reach gate refused a piece 14 min into the build (T778) | The save-time side check reports withheld pieces and applies the gate's rule | **No.** T779 is open. |
| 2 | Disk filled from test temp folders (T764) | A free-disk line in `scripts/heavy.sh` (it checks memory only) | **No.** The leak is smaller, not gone: 702 entries and 2.8 GB left in the system temp between 00:40 and 04:20 (a 1.6 GB `pytest-of-…` tree, five `regs-web-*` Chrome profiles of about 200 MB each, 25 `gauges-*` folders from the worker tests made after 03:50). 3,265 old `fixture-release-*` folders are still there. |
| 3 | Committed mini release stale; CI red for three pushes (runs 43821025943, 43822994971, 43829506741, all on "The committed mini release matches a rebuild") | `python -m pipeline.tools.mini_release --check` (24 s) | **Exists in fast-fail and would have fired** (the causing commits d65d4e15d, 7958b3c60, e3d6d5f26, a2ba7396b all touched `pipeline/atlas` or `pipeline/deliver`). It was not run before those pushes: the check after landing was the pytest tier by hand (six runs, 5 to 6 min each), which does not include this step. No git hook exists, and CI's colour is read only at a release, an upload or an APK build. |
| 4 | Binding diff, 407 rules | 1 s once a candidate exists | **Exists** (`--dry-run`). In the real run it sat behind an 18-minute preflight. |
| 5 | Two stale golden declarations, one silent wrong closed part | Rehearse the re-cut on the candidate | **Exists** (`--dry-run --rehearse-follows`), not run. The closed-part hole is **open**. `taps.py` reports one stale declaration per run. |
| 6 | 12 app tests pinned old part counts | Same rehearsal runs the whole app check on the candidate | **Exists**, not run. The tests still pin counts and names. |
| 7 | Upload gate refused: host answered 403 to Python's default client | Ask the worker in preflight and in the rehearsal (1 s) | **No.** `pieces_gate` runs only inside `deploy/staging/upload.sh` (line 22), after the promote. `staging_prechecks` asks about the run-timing base, `gh` and `rclone` only. The gate was new that day and had never run against the real worker. |
| 8 | A pytest chunk crashed on the fresh copy | Section 5 | **No.** Two bugs, both open. |

Also seen:
- **The release rebuilt itself for 9 minutes.** Attempt 4's build stage reran `deliver` (545 s, "inputs changed: curated") because the release's own in-season commit (fade4f689, `data/curated/inseason/notice_waters.json`) changed the stage key. Same release id came out.
- **A false stop after a stopped re-cut.** After attempt 4 stopped on changed pages, the fixtures were put back by hand while the ledger said they were cut for the candidate, so attempt 5's "goldens cut from LATEST" step failed for that reason alone.
- **AGENTS' timings are out of date.** It says `pnpm check` ≈ 17 s and Tier 1 ≈ 75 s. Measured: the app check takes 3 to 4 minutes (vitest 198 s for 7,743 tests); GitHub's app run 16 to 21 minutes, the pipeline run 10 to 12. "Fast-fail, target 2 min" took 733 to 751 s with a release base.

### Time per attempt (seconds, from the logs)

| Attempt | Wall | fast-fail | fresh copy | build | follows | upload | Stopped by |
|---|---|---|---|---|---|---|---|
| 1 | 18 min | 751 | 370 | - | - | - | not pushed; CI red; mini release stale |
| 2 | 19 min | 733 | 360 | 3 | - | - | binding diff to read |
| 3 | 1 min | 44 | not rerun | 3 | ~10 | - | first stale golden declaration |
| 4 | 16 min | 46 | 384 | 545 | ~20 | - | 2 golden pages changed |
| 5 | 6 min | 48 | ~300, crashed | - | - | - | PDF crash; false goldens stop |
| 6 | 7 min | 54 | 381 | 3 | ~20 | - | 1 golden page changed (wrong closed part) |
| 7 | 6 min | not rerun | not rerun | 3 | ~330 | - | 12 app tests |
| 8 | 13 min | 61 | 363 | 3 | 344 | seconds | upload gate, 403 |
| 9 | 19 min | 550 | 358 | 3 | 1 | 253 | done |
| Total | 105 min | 2,287 | 2,516 | 563 | ~725 | 253 | |

Repeat work inside those 105 minutes:

| Repeat | Seconds | Why it told nothing new |
|---|---|---|
| Whole no-data tier in place, inside fast-fail (attempts 1, 2, 9: 402 + 389 + 402) | 1,193 | The fresh copy runs the same 6,490 tests on the same commit minutes later, and GitHub runs them a third time. |
| "Every source scanner" and "every review test" steps (attempts 1, 2, 9) | 450 | Subsets of the whole tier that runs next in the same fast-fail. |
| Fresh copy rerun after commits to `app/tools/regs-recut/taps.py` or app tests only (attempts 4, 5, 6, 8) | 1,428 | GitHub's own path filter does not start the pipeline run for those commits. About 30 pipeline test files name app paths; only those can be reached. |
| Deliver rerun caused by the release's own commit (attempt 4) | 545 | Same release id. |
| **Sum** | **3,616 (60 min)** | |

Before the release command: the no-data tier also ran by hand in place six times (2,075 s, 35 min), and three builds were started (2 min stopped, 16 min refused, 33 min good). Across the night the same 6,490 tests ran 16 times on this machine (96 min) plus once per push on GitHub.

## 3. A smaller check plan (a proposal; nothing here is approved or changed)

Each kind of fault is checked once, at the cheapest place that can see it, and the release finds all its stops in one rehearsal.

**While working (seconds)**
- The one test file touched.
- `scripts/fast-fail.sh` on its normal base: 45 to 60 s measured. It already holds the mini-release check (24 s), the scanners, the ticket guard (0.5 s) and the app gates (23 s).
- Not the whole app check per commit (3 to 4 minutes now).

**Once before a push (minutes, once)**
- One command that runs what GitHub runs: every step of `pipeline-ci.yml` read from the file (fast-fail already does this for five), the no-data tier once if pipeline code changed (6 min), `pnpm check` once if the app changed (4 min). This replaces the hand "CI tier" runs.
- Make it automatic at push time (a pre-push hook running fast-fail is about a minute). Owner's call.
- One line of CI status at the next pause after a push.

**Once in the release**
1. Build.
2. Rehearse on the candidate (`--dry-run --rehearse-follows`, about 8 minutes). Fix everything it lists. Then one real run.
3. Keep the fresh-copy run; drop the in-place whole tier and its two subset steps when the fresh copy will run in the same preflight.
4. Rerun the fresh copy only when a commit since its pass touches something the pipeline tests can read (`pipeline/`, `curation-review/`, `scripts/`, `data/`, config, the app files pipeline tests name). For an app-only commit run the ~30 test files that name app paths.
5. `taps.py` lists every stale declaration in one run; closed parts get a named pin.
6. Ask the feeds worker the upload gate's question in preflight and in the rehearsal, and print the HTTP error.
7. The release's own in-season commit does not rerun deliver (after checking that deliver does not read that file).
8. Add one thing: the data tier once on the candidate, in chunks. The only item that adds time; not measured (last night's partial hand run: about 7 minutes for 700 tests).

**Only production needs**
- `--prod` re-uploads the release staging already checked: the gates that ask the production environment (site version, feeds worker, input formats) and the smoke test after upload. No test tier again.

**What I would not cut, and why**
- The reach gate, the binding-diff stop, the changed-golden-page stop: each caught a real change last night that only a person can judge.
- The fresh-copy run once per release: the only check that the commit works without this machine's files; it caught the PDF crash.
- The whole app check on the candidate: it found the 12 stale tests in one list.
- The scanners and the ticket guard: seconds.
- GitHub's runs: no local time.

**Expected time saved**
- A release with no stops: preflight from about 18 minutes to about 8.5 (592 s). Roughly 10 minutes per release.
- Last night: about 60 of the 105 minutes. With the rehearsal, I would expect four or five attempts, not nine (a judgement).
- Before the push: three or four of the six hand tier runs (about 20 minutes).

## 4. Weak tests (30 sampled, added or changed since 2026-11-10)

Delegated pass; I re-read five of the "weak" verdicts and they hold. 18 strong, 6 weak, 4 never run on GitHub, 2 strong but costly pins.

| Test | Why it cannot fail | Fix |
|---|---|---|
| `pipeline/tests/test_review_closed_areas.py:235` (changed last night, T777) | Asserts `near is (metres <= 200)` at a point 0 m from a built cut. A threshold of 2,000 m, or `near` always true, passes. The "far" case is tested nowhere. Reads the live tree. | Add a point at least 1 km from any cut on data the test builds; assert `near is False`. |
| `pipeline/tests/test_review_followup_1115.py:241` | Both sides of `cannot_change == why` call the same function with the same arguments. | Pin the reasons against made-up shapes. |
| `pipeline/tests/test_release_live_counts.py:68-79` | Searches the module's source text; a comment satisfies it. | Call `review_counts()` on made-up input and compare the whole result. |
| `pipeline/tests/test_release_command.py:947-949` | The command list is pinned exactly (good); the script half is a substring search, so dropping `-m "$MARKERS"` still passes. | Assert the whole line, or stub and check the `-m` received. |
| `pipeline/tests/test_t89_rules_that_bind_no_water.py:112-114` | Counts the test's own sets. | Delete; lines 117-130 pin the helper both ways. |
| `app/packages/ui-native/src/WaterScreen.unnamed.test.tsx:110-111` (changed last night) | Picks the stream by "closed all year by tributary rules", then asserts exactly that. | Delete the two lines. |
| `app/packages/ui-native/src/WaterMode.test.tsx:227` (changed last night) | Name checked only against `/^Unnamed stream · Regions? \d/`. | Compare with the store page's header name. |
| `app/packages/data/src/sql/drivers/sectionNames.test.ts:68-76` (changed last night) | Checks exclusion and order only. | Compute the expected set and compare the whole list. |
| `app/tools/r8-config.test.ts:79-81` | `envFor` is defined in the test file; those lines run no shipped code. | Move `envFor` into `tools/`, or label it a harness test. |

Costly pins: the tests of commit 39918037e that type "of 21", "Part 5 of 6", "Part 7 of 7" and whole part-name lists (StepInPlace T750, WaterMapView, `waterGroup.fixture`, `ui/waterMap`), and `test_dfo_closures.py`, which copies the live DFO places and pins Babine wordings. Fix: build "of N" from the number of parts; compose expected names from the part-name helper.

Never run on GitHub (skip without a release): `stretch-names.test.ts` (2 tests), `salmonLazy.release.test.ts`; `test_review_closed_areas` above is `needs_atlas`.

Scanner blind spots (confirmed by reading):
- `pipeline/tests/anchors.py` excuses a whole file once any test in it uses a sandbox: `test_atlas_inputs.py:102` and `test_review_proposals.py:203` read live curated files and pass.
- `sandbox_tree` copies the whole live curated tree, so a "sandboxed" test can still pin live content.
- Neither scanner looks at counts or names. The app scanner's header says "Not scanned: literal counts on live data" and allows labels such as "part 1 of 5".
- The app scanner knows only `RELEASE_DIR`. `SEARCH_CORE`, `SLICED_RELEASE`, `BUNDLE`, `TILES_DIR` also choose a release or build.
- Tests that pass on an empty list: `test_answers_25.py:162`, `test_answers_23.py:218`, the `all(... if live)` in `test_dfo_closures.py`.
- The app's skip guard allows any skip built from `liveRelease()`, with no count.

## 5. The crashes: two different bugs

13 crash reports dated 2026-11-15 and 11-16, in two families.

### A. The PDF library called outside its lock (what stopped attempt 5)
- Report `python3.13-2026-11-16-024652.ips` is the fresh copy's chunk 31 itself (pid 44614, child of the `time` wrapper 44613 that the chunk runner named). Not a forked child.
- Test: `pipeline/tests/test_review_screen.py::test_pages_render_from_many_threads_at_once` (8 threads).
- Crashed thread: `gc_collect_main` -> ctypes -> `CPDF_Page::~CPDF_Page` -> `CPDF_ImageObject::MaybePurgeCache`, while another thread was inside `FPDF_LoadDocument`. Python's own dump agrees: one thread "Garbage-collecting" in `pypdfium2/_helpers/page.py` `_close_impl`, another in `document.py` `_close_impl`.
- Cause: `curation-review/backend/synopsis_pages.py::page_png` and `evidence.py::_render` hold `PDFIUM_LOCK` around `doc[n - 1].render(scale=…).to_pil()` and `doc.close()`, but never close the page or the bitmap. Every pypdfium2 object refers to itself (`self._fin_obj = self` in `pypdfium2/internal/bases.py`, version 5.11.0), so those temporaries are freed only by the cyclic garbage collector, in whichever thread it runs, without the lock.
- Smallest fix: inside the lock keep `page` and `bitmap` in names, copy the pixels out (`bitmap.to_pil().copy()`; `.convert("RGB")` already copies in `evidence._render`), then `bitmap.close()`, `page.close()`, `doc.close()`.
- The review app serves these routes from a thread pool, so the same race can crash the review app.
- The other six PDF reports (aborts in `CFX_FontMapper::AddInstalledFont`, one at interpreter exit) are child processes and fit the mutation test in the same file, which runs unlocked renders in a child on purpose. Not matched report by report.

### B. A child process dies between fork and exec (the crash T777 describes)
- Six reports: 20:57 to 20:58 on 11-15 and 00:59 on 11-16 (`data-tests-2c/2d`). All say "crashed on child side of fork pre-exec".
- The chain, identical in all six: `subprocess_fork_exec` -> `do_fork_exec` -> `fork` -> `_pthread_atfork_child_handlers` -> `osgeo::proj::io::SQLiteHandleCache` (PROJ's after-fork handler closing its cached `proj.db` handle) -> `sqlite3Close` -> `osgeo::proj::VFSClose` -> `unixClose` -> `verifyDbFile` -> `sqlite3_log` -> `os_log_type_enabled` -> `_os_log_preferences_refresh` -> SIGSEGV.
- Libraries: pyproj's bundled `libproj.25.9.5.1.dylib`, which links Apple's `/usr/lib/libsqlite3.dylib`. Apple's SQLite logs through the system log, which cannot be used in a forked child. Linux (GitHub) has no such path.
- So the forking calls are ordinary `subprocess` calls (`git` in `test_review_queue.py`, the helper processes in `test_review_marks.py`, the `node` child in `test_review_dfo.py`) in a Python process where an earlier test file used pyproj. None is `os.fork` or a multiprocessing fork.
- The ticket's proposed fix ("spawn, never fork") would not help: spawn is already the macOS default and starts its child through the same `fork_exec`.
- Measured: each of the three files alone never imports pyproj (`test_review_queue` 25 passed in 1.6 s, `test_review_dfo` 23 in 1.1 s, `test_review_marks` 61 in 20 s; no `proj.db` open at the end).
- Smallest fix: in `scripts/pytest-chunked.sh`, a test file that starts a child process gets a process of its own, found by grep as `test_curation_review*` already is. 34 files match `subprocess|multiprocessing|Popen`; under a minute of extra start-up. Never group such files by hand with files that load the atlas.
- Not a fix: changing each call to avoid fork needs `close_fds=False`, no `cwd` and an absolute program path at every call site.

## What I did not verify
- The data tier and the slow tier were not run. Command: `scripts/heavy.sh data-tier 40 -- bash scripts/pytest-chunked.sh -- -m "not slow and (needs_bundle or needs_atlas or needs_reach_run or needs_tiles or needs_source or needs_cache)"`.
- The release command, its rehearsal, an upload or a deploy were not run. What the rehearsal lists is read from `release.dry_run` / `rehearse_follows`.
- The plan's savings are sums of logged step times; "four or five attempts" is a judgement.
- Whether `deliver` reads the in-season matches file.
- Why Apple's SQLite logs on that close (no symbols).
- Weak-test verdicts: five re-read by me; the delegated pass ran three app test files and some pipeline files and proved four mutations by monkeypatch; the rest are reasoned from code.
- Which process the four 20:57 fork crashes belonged to.
- Temp-folder figures are a snapshot at about 04:20.

---

# The checker's own summary

**The report file was not written.** The harness refused the write of `…/audit-general/guards/report.md` ("subagents return findings as text"), and I did not route around it. The full report text is below the summary, ready to save as that file. Nothing in the repo was changed; my scratch folder is empty. (`handoff/PLAN/OWNER-TODO.md` was already modified by another session, and head moved from d0d584159 to 0c9988033 while I worked.)

# Summary

The release command ran for 105 minutes over nine attempts, and about 60 of those minutes were repeat work. Three of last night's holes are still open. The tests that read the newly built data are the ones the release does not run.

## Exceptions that should go or be re-checked
- **Closed golden parts:** the check only asks "is the part at that index closed". This is the hole that put golden state `x15p9` on the wrong closed stretch; last night's fix corrected the number, not the check. Four states are still held this weakly (`x15p9`, `x16p7`, `chw0`, `x17p5`).
- **Golden declarations:** 64 of the 134 golden states (48%) now carry a hand declaration, and `taps.py` stops at the first stale one, which cost three attempts.
- **`exclusions.ts`:** the file was removed by T439. Five comments still cite it, in `taps.py`, `golden.ts` and `tree.ts`.
- **Held continuation links:** 5 of the owner's 17 reviewed links are now "stale" and silently stopped shipping. 4 new ones wait with no owner item.
- **`NOT_LANDED_OK`:** 2 tidal marks, both waiting on T359 since 2026-11-03. Still valid; re-check when T359 is answered.
- **Upload gate:** it still throws away the cause of a failed fetch, so a 403 reads as "the worker cannot be asked".
- **Release ledger:** the record that the 407-rule diff was accepted was overwritten in attempt 9 with "same reach run". The diff file is still on disk.
- **Fine as they are:** the 2 accepted unplaced pieces (owner 2026-11-11), the closed review domains, the 18 rules allowed to bind nothing, and the 5 pipeline skip sites. No release escape flag was used last night and none is remembered. The staging site reports `dirty: false`.

## Tests no tier runs
The pipeline suite has 7,974 tests. The 6,490 no-data tests ran 16 times on this machine last night (96 minutes), plus once per push on GitHub.
- **Data tests:** 1,208 tests in 115 files have no automatic runner. The release runs 7 of those files.
- **Slow tests:** 276 tests, also with no automatic runner.
- **`release-perf.test.ts`:** 14 tests, skipped even in the release's own strict run.

About 700 review tests were run by hand after the build. I found no record that the rest of the data tier ran on the release now on staging.

## Gaps still open
1. **Save-time check (T779):** it still hides withheld pieces, so the next one stops a build 14 minutes in.
2. **Disk:** no free-disk check anywhere. Temp folders still leak, less than before: 2.8 GB was left between 00:40 and 04:20.
3. **Push time:** nothing runs fast-fail before a push, and nobody sees a red CI until a release. The 24-second mini-release check exists and would have fired. The hand "CI tier" run after landing does not include it.
4. **Upload gate:** its question to the feeds worker is asked only at the last stage, after the promote.
5. **Rehearsal:** `python -m pipeline release --staging --dry-run --rehearse-follows` exists and was not run. By its code, it lists the binding diff, stale golden declarations, changed pages and failing app tests in one pass.
6. **Self-inflicted rebuild:** the release's own in-season commit reran the deliver stage for 9 minutes and produced the same release id.
7. **Two crash bugs:** both open (below).

## Proposed check plan (for the owner to say yes or no; nothing is changed)
- **While working:** the one test file touched, plus fast-fail (about a minute). Not the whole app check, which now takes 3 to 4 minutes, not the 17 seconds the docs say.
- **Once before a push:** one command that runs exactly what GitHub runs, made automatic at push time. Then one look at CI's colour afterwards.
- **Once in the release:**
  - Build, then rehearse on the new release and fix everything it lists, then one real run.
  - Stop running the same 6,490 tests in place when the fresh copy runs them minutes later.
  - Rerun the fresh copy only when a commit could affect it.
  - Ask the upload gate's question at the start.
  - Add one thing: run the data tests once on the new release. This is the only item that adds time, and I did not measure how long it takes.
- **Production only:** the gates that ask the production environment, and the smoke test. No test tier again.
- **Not cut:** the reach gate, the binding-diff stop, the changed-page stop, the fresh-copy run once per release, the whole app check on the new release, the scanners, and GitHub's runs.
- **Time saved:** about 10 minutes on a release with no stops. Last night, about 60 of the 105 minutes. My judgement, not a measurement, is that the rehearsal would have made it four or five attempts.

## Weak tests worth fixing first
A delegated pass sampled 30 tests: 18 strong, 6 weak, 4 never run on GitHub, 2 costly pins. I re-read five of the weak verdicts and they hold.
- `test_review_closed_areas.py:235` (changed last night): a wrong threshold passes.
- `test_review_followup_1115.py:241`: both sides call the same function.
- `test_release_live_counts.py:68-79`: searches source text.
- `test_t89_rules_that_bind_no_water.py:112-114`: counts its own sets.
- `WaterScreen.unnamed.test.tsx:110-111`, `WaterMode.test.tsx:227`, `sectionNames.test.ts:68-76` (all changed last night): each got weaker.
- The 12 app tests edited last night still type "of 21" and "Part 5 of 6", so they will need hand edits again at the next re-cut.

## The crash
There are two different bugs, and T777 describes only one.
- **What stopped attempt 5** was not a forked child. The review app's page renderer (`synopsis_pages.page_png`, `evidence._render`) never closes its page and bitmap objects, so the garbage collector frees them in another thread, outside the PDF lock. This can crash the review app itself. Fix: close the bitmap, page and document explicitly inside the lock.
- **The fork crash** comes from ordinary `subprocess` calls (git, node, helper processes) made in a test process where an earlier test file used pyproj. All six reports show the same chain, ending in Apple's SQLite logging inside the forked child. The ticket's "use spawn" fix would not help, because spawn goes through the same call. Smallest fix: each test file that starts a child process gets a process of its own in `pytest-chunked.sh` (34 files, under a minute).

## Not verified
- I did not run the data tier, the slow tier, the release, its rehearsal, an upload or a deploy.
- What the rehearsal would have listed is read from its code, not from a run.
- Whether deliver actually reads the in-season file.
- Most of the weak-test verdicts are the delegated pass's.
- Which run the four 20:57 fork crashes belonged to.

---


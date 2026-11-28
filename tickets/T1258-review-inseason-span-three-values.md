# T1258 — The review tool's In-season tab unpacks two values from `span`, which gives three since T1167

Status: IN PROGRESS — lane wt-T1258 (branch fix/T1258-review-inseason-span).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1258 | The review tool's In-season tab unpacks two values from `span`, which gives three since T1167 | IN PROGRESS | — | independent review, before_pr, PR; the memory and fork-crash lines under Not done here |

Source: the planning chat, 2026-11-27: a data-tier run of the review tests on PR #367's branch failed four In-season
tests and two screen tests with `ValueError: too many values to unpack (expected 2)` at
`tools/curation-review/backend/inseason.py` `_row_view` (`frm, to = M.span(...)`); `pipeline/regs/inseason/matches.py`
`span` returns `(from, to, dated)` since T1167. On origin/main too. The same run grew past 48 GB with the five review
files together and `scripts/heavy.sh` stopped it twice.

## Work

1. `_row_view` takes all three; `dated` is shown, not dropped (W46): `dates.dated`, and `banner` = the app's own word
   for today (`matches.on`, core's `inSeasonOn`: in force | upcoming | none; none on a removed or stale row). The tab's
   effect chip is NOT changed here (a new look in the owner's tool): see "Not done here".
2. A plain-tier test of the call shape: `pipeline/tests/test_curation_review_inseason_row.py` (no atlas, no feed).
3. Run the five review data-tier files one at a time; find what grows memory.

## Acceptance

- The new test fails with the two-value unpack, passes with the fix (plain tier, CI).
- `test_curation_review_inseason`, `_screen`, `_tidal`, `_tidal_unregistered`, `test_review_closed_areas` green file by
  file with `--with-data`, or each failure named with its cause.
- Per-file peak memory recorded here.

Depends: —. Size: S. Owner: no.

## Results (2026-11-27, lane wt-T1258, data APFS-cloned from the main checkout, one file at a time, lane 3)

- inseason 13 passed; tidal 2 passed; tidal_unregistered 2 passed (before the rebase).
- screen 21 passed (after: `cache/dfo_salmon` cloned; the Re-resolve test asserts writes' burst rule). This machine's
  resolution names r-71d09ad82e81 while LATEST is r-0aec88ed3edb, so the first Re-resolve rewrites the copy and the
  stale write reuses that snapshot (BURST_S, product behaviour): the test now asserts that snapshot holds the file
  from before the burst; when the resolution is current it asserts a new backup as before.
- closed_areas 10 passed on origin/main 2d4b8cb2f and on this branch rebased onto b7e56036c; 8 passed + 2 errors
  in two earlier branch runs (the fork crash below).
- Memory (probe: resident size per test, gc at each teardown): inseason peak 11.3 GB; screen 7.1 GB after its first
  test, 12.6 GB peak, 12.4 GB at the end after gc; closed_areas: footprint 12 -> 20 -> 27 GB over its three
  needs_atlas tests. LEADING CAUSE: every fixture that imports the backend pops `app`/`reuse`/... and re-imports them,
  so the atlas caches (registry, graph ~0.7 GB, geometry ~2.9 GB, the rest of reuse's lru caches) load again — once
  per FILE in four files, once per TEST in `test_review_write_through.wt` (function scoped) — and the old copies are
  not given back (rss after module teardown + gc stays at the file's peak). Five files in one process: ~11 + 12.6 +
  27 + tidal's ≈ 50 GB, the 49-53 GB heavy.sh stopped. No fixture scope changed (wt's writes are per-test sandboxes).

## Not done here

- Show "upcoming" in the In-season tab's effect chip (`InSeasonReview.tsx` `EffectChip`) from the row's `banner`: a new
  look in the owner's review tool, so a shot for the owner first. The backend field and its type are in.
- MEMORY: one backend import per pytest process for the review data tier (a session fixture that loads app/reuse
  once and points each test's sandbox at it), or `cache_clear()` on reuse's caches + gc before the modules are
  popped, then measure that rss comes back. Numbers above.
- FORK CRASH (intermittent, 2 of 4 closed_areas runs): `git init` in `test_review_write_through.wt` died with SIGSEGV.
  ~/Library/Logs/DiagnosticReports/python3.13-2026-11-27-155239.ips: "crashed on child side of fork pre-exec";
  fork -> _pthread_atfork_child_handlers -> libproj 9.5.1 `SQLiteHandleCache::getHandle` atfork lambda -> lru cache
  clear -> sqlite3Close -> unixClose -> verifyDbFile -> sqlite3_log -> os_log_type_enabled -> EXC_BAD_ACCESS.
  Smallest candidate fixes: run git through posix_spawn, which runs no atfork handlers (absolute `/usr/bin/git`,
  `-C <repo>` instead of cwd=, close_fds=False: `subprocess` then uses posix_spawn here, `_USE_POSIX_SPAWN` True);
  or make the temp repo once per module before the app (and PROJ) is imported.

## Follow-ups

- `tools/curation-review/backend/inseason.py:89-93` `_in_effect` re-implements the in-force rule and disagrees with `M.on` on a row with no dates (`in_effect: None` but banner "in force"): derive it from the banner; not done here.
- The intermittent fork crash and the fixture memory growth in the review data tests are T1265.

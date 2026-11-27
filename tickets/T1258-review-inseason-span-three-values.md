# T1258 — The review tool's In-season tab unpacks two values from `span`, which gives three since T1167

Status: IN PROGRESS — lane wt-T1258 (branch fix/T1258-review-inseason-span).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1258 | The review tool's In-season tab unpacks two values from `span`, which gives three since T1167 | IN PROGRESS | — | rerun screen + closed_areas after review; closed_areas git SIGSEGV (PROJ atfork) and per-file memory probe unfinished |

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

## Results (2026-11-27, lane wt-T1258, data cloned from the main checkout)

- inseason 13 passed (peak rss 11.3 GB, probe); tidal 2 passed; tidal_unregistered 2 passed.
- screen 19 passed, 2 failed BEFORE the two lane fixes: `cache/dfo_salmon` not in the worktree (environment), and the
  Re-resolve backup count (writes' burst rule; the machine's resolution names r-71d09ad82e81, LATEST is
  r-0aec88ed3edb) — the test now sets `writes.BURST_S = 0`. Rerun after the fixes: owed.
- closed_areas 8 passed, 2 errors: `git init` in the `wt` fixture died with SIGSEGV — macOS crash report: "crashed on
  child side of fork pre-exec", PROJ 9.5.1's atfork child handler (`SQLiteHandleCache`) closing sqlite, sqlite3_log ->
  os_log in the child. Native fork-safety, not this change; needs its own line.
- Memory: footprint beats 10 GB (inseason), 11 GB (screen), 9 GB (closed_areas); the `wt` fixture is FUNCTION scoped
  and re-imports app/reuse (graph + geometry reload) per needs_atlas test. Per-file probe beyond inseason: owed.

## Not done here

- Show "upcoming" in the In-season tab's effect chip (`InSeasonReview.tsx` `EffectChip`) from the row's `banner`: a new
  look in the owner's review tool, so a shot for the owner first. The backend field and its type are in.

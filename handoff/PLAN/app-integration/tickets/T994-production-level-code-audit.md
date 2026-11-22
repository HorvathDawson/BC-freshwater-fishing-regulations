# T994 — Production-level audit: non-standard practices and data-flow issues across the code

Status: IN PROGRESS (main chat 2026-11-21: read-only audit agent first; findings become numbered lines here or new tickets).

Source: owner 2026-11-21, verbatim: "You could also get an agent to do a basic audit for non standard practices or
issues with the flow of data etc. we want to make our code production level. Also clean up gha workflows and make sure
we just waste less time with bad ci/cd of failed runs." (The GitHub Actions half is in T993's scope.)

## Work
1. Read-only audit of `pipeline/`, `app/`, `curation-review/`, `deploy/`, `scripts/`: non-standard practices
   (error handling, config, typing, logging, dependency use, dead code, copy-pasted logic), and data-flow issues
   (a fact computed twice, a reader of curated files at run time, a hidden fallback, an untyped boundary).
   Start from the 2026-11-16 audit (`handoff/PLAN/audit-2026-11-16/`) and report only what it and the open tickets
   do not already hold.
2. Each finding: file:line, what is wrong, why it matters in production, the fix, its size. Ranked.
3. The owner sees the list; fixes go into this chat's lanes or new tickets.

## Acceptance
A ranked findings list with no item already in an open ticket; each kept finding fixed or ticketed.

Depends: —. Size: M. Owner: reads the ranked list.

## Audit done 2026-11-21 (read-only agent): 30 findings, none already ticketed
Report: `handoff/PLAN/audit-2026-11-21/t994-audit.md` (ranked, file:line, fix, size; "(sub)" = sub-auditor reading not
re-opened by the lead). Top 5: `update-hydat.sh --upload` publishes ungated to any bucket; review backend CSRF on
`POST /api/inseason/resolve`; worker backups keep report text 400 days (privacy promise is 180); a failed in-season feed
read resets history as a first run; one failed read wipes the water-temperature history (worker + Python twin).

Fix lanes (disjoint files):
- **F1 feeds worker + its Python twins:** findings 3, 4, 5, 23, 24, 25, 26, 30.
- **F2 pipeline + review backend:** 1, 2, 14, 15, 16, 17, 21, 27.
- **F3 app data seam:** 8, 11, 12, 13, 22.
- **With T993 (workflows, deploy scripts):** 6, 7, 9, 10, 18, 19, 20, 28, 29.

## Lane F3 (app data seam): all five confirmed against the code and fixed
- **8** (no timeout on salmon / in-season / run-timing / HYDAT-day / weather reads): ONE helper `timedFetch` (`app/packages/data/src/net/timeout.ts`, 30 s, headers and body, an environment error so it is shown and not reported); `App.tsx` wraps those five reads and the manifest recovery. The gauge feed and release files keep their own limits. `conditionalJson` rides the injected fetch, so in-season/run-timing/salmon conditional reads are covered. Commit: see git log "T994 F3: timed fetch".
- **11** (synopsis-PDF recovery swallowed): the alternate reads through `loadManifest` (ok check, parse, timed); the native catch reports (`reportFailure`); `tools/failures.test.ts` FINE count for `synopsisPdf.native.ts` 4 to 3. Same commit as 8.
- **12** (`storedHydat` format): `parseHydatDaily` checks `hydat-daily/1`, an integer `from`, `q` as arrays of arrays; named throw. Commit: "T994 F3: validate at the seam".
- **13** (`parseManifest`): each file entry checked for url, bytes, sha256, format; `parseManifestText` makes a non-JSON body a `ManifestError`; the four JSON.parse-then-parse call sites use it. Same commit as 12.
- **22** (`release/previous`, `release/refused`): `readHeld` shape-checks the files map; both boot reads get the OPENED pattern (report, then absent). Same commit as 12. The boot catches have no test (boot has no harness for these reads); `readHeld` is tested.
- `pnpm check` stops at `build-coho` here (no `.venv` in the lane worktree); `tsc -b`, the data package and `tools/` vitest run pass, apart from `regs-fixture` (also needs `.venv`).

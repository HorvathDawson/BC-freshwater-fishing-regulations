# T537 — `release --staging`: resolve in-season before the promote, resume from the smoke, name the after-chores

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the code). Rank 5.

## Today
- **In-season re-resolve runs mid-upload, after the promote.** `deploy/staging/upload.sh:90` → `sync-inputs.sh:37-45`
  runs `pipeline.regs.inseason.resolve` (through heavy.sh, so it can also wait on the lock mid-upload). LATEST has already
  moved (`pipeline/release.py:472-485`). A failed match stops the upload after the build and promote; a good one leaves
  `data/curated/inseason/notice_waters.json` uncommitted, which dirties the tree for the next preflight. Both recent
  releases needed the separate commit.
- **A smoke failure reruns the upload.** `upload.sh:107` writes the pointer, `:112-114` clears the old release, `:120`
  runs the smoke; `release.py:496-503` records `staged_at` only on exit 0 and has one skip (`:490`), so a rerun redoes
  gates, sync-inputs, clear and smoke (minutes; rclone skips present files). `:117` test/ reset failure is only a WARNING
  (breaks the no-silent-fallbacks rule).
- **After-promote chores are prose.** `release.py:542-545` names only the production command. Goldens re-cut
  (`node app/tools/regs-recut/sync.mjs` + commit, `runbooks/release.md:83-93`) is manual, and every lane's fast-fail is
  red (goldens check) until it is done.
- **A failed upload leaves mixed vintages** (lower): LATEST, run timing (`upload.sh:54`) and worker inputs (`:90`) move
  before the pointer (`:107`). T485 covers the run-timing half.
- **Prod promote needs staging to still serve that release** (`deploy/prod/upload.sh:38-42`), but `release --prod`
  (`release.py:613-627`) does not check it and a later `release --staging` replaces it silently. Fails early, not late;
  prod is on hold.

## Work
1. Resolve in-season against the candidate in stage 4 (checks), before the promote; stop there on a failed match; commit
   `notice_waters.json` with explicit paths (or stop with the exact commit line). `sync-inputs.sh` then only verifies.
2. Ledger records "uploaded" and "smoked" separately; a rerun after a smoke-only failure runs only the smoke. Pointer-free
   smoke parts (feeds status, backup age, private/ not served) run before any upload. test/ reset failure stops.
3. Last stage "after": re-cut goldens and show the diff to commit; report whether site and feeds worker are behind. At
   minimum the success message prints the exact commands.
4. Pointer-coupled objects go up as one last group (workflow done → inputs → pointer).
5. `release --prod` and its dry run compare staging's live manifest first; `release --staging` says when it replaces a
   release the owner was handed a prod command for.

## Acceptance
- Tests on the stage runner with faked commands: failed resolve stops before promote; smoke-only rerun skips the upload;
  success message lists the chores. One real `release --staging` ends with a clean tree and green fast-fail.

Depends: T485 (run timing). Size: M. Owner: no.

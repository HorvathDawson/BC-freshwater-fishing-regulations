# T537 — `release --staging`: resolve in-season before the promote, resume from the smoke, name the after-chores

Status: IN PROGRESS — b3b950e5e (built + unit-proved); left (Claude): one real `python -m pipeline release --staging` ending with a clean tree and green fast-fail, and the pointer-free smoke group in app/tools/smoke-staging.mjs (tools lane) — see Built.

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

## Built (2026-11-10, b3b950e5e)
1. Stage 4 (checks) resolves the in-season matches on the CANDIDATE (`regs.inseason.resolve --release <candidate>`
   through heavy.sh, told fast-fail already ran), before the promote; ledger `inseason` (once per release). A failed match
   stops at checks, LATEST untouched. A rewritten `notice_waters.json` is committed alone (explicit path) and the run
   stops naming the push. `sync-inputs.sh` only verifies: matches on another release than LATEST are refused, nothing
   put, the resolve + commit commands printed (never re-resolves, from any caller).
2. The release runs `upload.sh` with SMOKE=0, records `uploaded_at`, then runs the smoke itself and records
   `smoked_at` (= `staged_at`). A rerun after a smoke-only failure runs only the smoke. `upload.sh`: a failed test/
   reset STOPS (was a WARNING).
3. The success message lists the after-chores with exact commands (`after_chores`): goldens (checked by
   `scripts/check-goldens-paired.py`; "RE-CUT NEEDED" + the sync.mjs / commit lines), site, feeds worker, prod.
4. `upload.sh`: run timing (workflow done) → `sync-inputs.sh` → the pointer, as one last group.
5. `release --prod` and `--prod --dry-run` read staging's live `release/manifest.json` first and refuse unless it serves
   that release (unreadable = refused). `release --staging` prints when it replaces a staged release never published
   and marks it `replaced_by` in the ledger.

Proof: `test_release_command.py` (+6: failed resolve stops before the promote; rewritten file committed alone, stop for
the push, not resolved again; smoke-only rerun = no second upload, chores printed; replacing a prod-handed release is
said; prod refused when staging serves another / is unreadable, dry run says WOULD STOP; after-chores name the commands)
and the stage order now `build, inseason, upload, smoke`. `test_staging_scripts_fail_safe.py` (sync-inputs only verifies;
upload order and the reset stop pinned). Real: `release --prod r-a178299f6005 --dry-run` → "staging serves: ok" (the
manifest needs a User-Agent: python's default gets 403); `after_chores` on this tree → "goldens: cut from LATEST already".

Not verified / left: the acceptance's one real `release --staging` (clean tree + green fast-fail at the end) — not run
(no releases in this lane). The pointer-free smoke parts BEFORE any upload need a group in `app/tools/smoke-staging.mjs`
(`--only` has http / browser / desktop; http mixes pointer-free and pointer checks) — app/tools is another lane's. The
goldens re-cut is named, not run, by the release. On a resume after the in-season commit, `pipeline build` reruns
deliver (notice_waters.json is a deliver input), so the candidate may be rebuilt once.

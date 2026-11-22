# T987 — Repo README and docs brought up to date; clean-up pass

Status: DONE — ffba227f6, 5090e2f84, fef1e1973 (PR #27, merged 2026-11-22). Independent review: one defect (a still-tracked
file described as removed), fixed in fef1e1973.

Source: owner 2026-11-21: "add deferred next release ticket to update readme and docs for repo and clean up stuff. (not
for this release".

## Work
- **README and docs:** bring the repo's README and the docs up to date with what exists now: the top-level README,
  `app/README.md`, `app/MAP.md`, `pipeline/docs/MAP.md`, `pipeline/README.md`, the deploy/release runbooks, and
  `handoff/PLAN` pages that are history rather than live. Fix or remove stale paths, commands and numbers (AGENTS 42:
  every number reproducible or gone).
- **Clean-up:** dead scripts, stale generated files and folders, leftover worktrees and branches already merged, old
  handoff pages (git keeps them), unused tools. Quick importance check per item, no approval list (owner 10-24); git is
  the safety net. Never touch curated data, marks, or the owner's review files.
- Keep the guards green (`pnpm check`, `node app/tools/check-tickets.mjs`, the docs guard).

## Acceptance
A new reader can follow the README to build, test and release; no doc cites a path that does not exist; the clean-up is
listed in the commit messages.

## Done (2026-11-22, branch `docs/T987-docs-and-cleanup`)
- **README:** Getting started on `scripts/bootstrap.sh` + the hooks + the app install; the stage diagram in the build's run
  order (clim, reach, tiles, then deliver); a Contributing section: branches, Conventional Commits (T998), the hooks,
  PRs with auto-merge, the four required checks per workflow, the SemVer cut. Layout lists deploy/, scripts/, handoff/.
- **Maps and READMEs:** `app/README.md` (commands: check:fast, test, tiles, the feeds worker suite; stale "ten rules" and
  doc-10 references), `app/MAP.md` (the full `check:fast` order; package-qualified data paths; no unmeasured timing),
  `pipeline/README.md` (unreproducible durations out, pointing at `build --status`; the review app writes marks only;
  `--cut`), `pipeline/docs/00-README.md` (index complete), `data/README.md` (was v1's, citing scripts that no longer
  exist), module READMEs under `pipeline/atlas/waters/`, the DFO salmon design docs (paths moved to the curated layout,
  the two plans marked historical), and single dead paths in RULINGS, GOTCHAS, 04/05/06/15/19, ANSWERS-SPEC, app docs.
  AGENTS W47 lists `perf/` and `test/` branches; AGENTS 37 names the CI steps that stand for the removed handoff's gate.
- **Dead paths** (backticked paths, fenced command tokens and relative links over the 57 repo docs outside `handoff/`
  and the data trees, checked against the tree and git-ignore): 74 root-relative dead paths before, 4 after, each a
  file that is made later by design (the review server's `inputs-sync.jsonl` log, `data/curated/bathymetry/matches.json`
  on the first accept, the stocking roster and folder the unbuilt fetch makes). Folder-relative tokens: 52 unresolved
  before, 47 after, all generated or remote outputs named relative to their folder (R2 keys, `batches/batch_NNN.json`),
  placeholders, or npm package paths. One dead relative link fixed (CURATION.md).
- **Removed (git keeps them):** listed with reasons in the chore commit's message.
- **Left:** `pipeline/regs/dfo_salmon/{DESIGN,PLAN}.md` kept with a historical banner (code and handoff pages cite
  them); `ticket-audit-2026-11-09.md`, the pages OWNER-TODO cites (`INFLIGHT-chat14.md`, `OWNER-RETURN-2026-11-15.md`)
  and the evidence images stay (owner rows in `CLEANUP-CANDIDATES.md`); branches and worktrees are the main chat's.

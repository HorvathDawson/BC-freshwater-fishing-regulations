# T1193 — Scripts and repo usability clean-up: one obvious way in for a human

Status: OPEN — lane: the repo clean chat, after T1192 PR 1; the audit (`repo-usability-audit.md`) is still being written.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1193 | Scripts and repo usability clean-up: one obvious way in for a human | OPEN | ae94d6546, ed3931146, 2e3d25588, 51acf1842 | PR 1 (deletes) and PR 2 (one ledger-hook script) merged; PR 3: a plain `pytest` is the no-data tier, loudly (in review, Claude); PR 4: a root Makefile, one verb per task (after PR 3, Claude) |

Source: the owner, 2026-11-26 (planning chat), verbatim: "same with scripts too and a few others that feel like they
have way too many things that it would be hard for a human to use the repo themselves." and "also these scripts feel
obsolete? (build_tidal_boundary.py, process_tidal_boundary.py — the DFO tidal polygon, processed once and stored on R2".
The audit: `~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/night-notes-2026-11-26/repo-usability-audit.md`
(being written).

## Work
1. Read the audit; rank the crowded places (`scripts/` first; the owner did not name the "few others").
2. Each processed-once-stored-on-R2 script (the two tidal-boundary scripts first): keep, pin its output, or delete;
   the audit's evidence in the PR.
3. One PR per place; every removed or moved runnable says where its job went.

## Acceptance
A developer who has never seen the repo can, from the README alone, set up, run the tests, build the data, run the app
and cut a release, each by one obvious command or page.

Depends: the usability audit. Size: M-L. Owner: yes (which places, after the audit). Model: Opus.

## Done (PR 1, delete what ran)
Deleted: `data/process_tidal_boundary.py`, `deploy/staging/{play-listing-images.py,flood-test.mjs}`, `app/tools/perfetto/q1-q3.py`,
`app/tools/testbed/{lab-shots,tap-glide}.mjs`, `app/tools/polish-shots.mjs` (their stray doc/comment references fixed).
Kept: `deploy/prod/drop-v1.sh` (docs/release/PRODUCTION.md, upload.sh and test_prod_prune.py name it; the doc is a
release page), `deploy/prod/feeds-placeholder*` (feeds.sh tells a person to run it, deploy/lib/input-formats.sh names it, a test pins its worker.js), `app/tools/perfetto/pinch.py` (the zoom-pinch scenario's reader).
Also deleted (owner: "I do not run them; delete"): `deploy/staging/backup.sh`, `restore-test.sh` and the one test that only read restore-test.sh; the feeds worker's own backup.ts stays (its comments no longer name them).

## PR 3, a plain pytest is the no-data tier (owner 2026-11-26: "yes with loud default and maybe we should clean up pytests too")
With no `-m`, the root `conftest.py` deselects by CI's "Tests" step expression (`pipeline/tests/tiers.py` `ci_markers`,
which `scripts/test-env.sh` `no_data_markers` now calls: one reader of the one home) and the run's last line says
`N data tests NOT RUN (they need built data): add --with-data`. `--with-data` is `-m "not slow"`, the old default; an
explicit `-m` is untouched; both together is a usage error. Every caller that leaned on the old default now says
`--with-data`: `scripts/test-full.sh`'s fast step, fast-fail's named-file steps, the release follows' whole files
(`release_follows.pipeline_tests_cmd`), the re-cut's pins (`pins.mjs`), the annual-synopsis runbook. The pytest folder
clean-up the owner mentioned is a later ticket. Task runner (owner, same day): "A Makefile (Recommended)".

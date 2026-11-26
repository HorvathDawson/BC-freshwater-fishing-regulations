# T1062 — Release candidates: tag `vX.Y.Z-rc.N` and promote, instead of moving the version tag after late fixes

Status: IN PROGRESS — the build is on main (PR #257, 7139d1ae2); done when 0.10.0 has shipped through an rc with no moved tag

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1062 | Release candidates: tag `vX.Y.Z-rc.N` and promote, instead of moving the version tag after late fixes | IN PROGRESS — the build is on main (PR #257, 7139d1ae2); done when 0.10.0 has shipped through an rc with no moved tag | 5ac3d67f3 | flip to DONE with the commits after 0.10.0; two review finds are T1147 |

Source: owner idea during the 0.9.0 release, relayed by the 0.9.0 chat 2026-11-24: 0.9.0's tag had to move after late fixes (T1027, T1034, T1049 cherry-picks).

## Work
1. Plan first (docs): how rc tags fit `pipeline release --cut`, release-please / the version files (T998), the staging release, Play Internal build numbers and the app_reads row (T1048).
2. Then build: the cut makes `-rc.N`; the final tag is put on the rc that passed, never moved.

## Acceptance
a written plan the owner agreed to; then one release done with rc tags and no moved tag.

Severity: process. Release: with chat 3 (repo move + merge queue), or before 0.10. Model: Sonnet for the plan, Opus for the cut change. Size: M. Owner: yes (agree the plan).

Owner 2026-11-24 (planning chat question, picked "With the repo-move chat"): the plan is written in chat 3; nothing is
built before the owner agrees it.

Owner 2026-11-25 ~13:58 (repo-move chat, dialog). The plan is `handoff/PLAN/repo-move/RC-TAGS-PLAN.md`; his picks,
verbatim labels: visible version in an rc build: "Plain 0.10.1 (Recommended)"; where the rc is tagged: "At the
staging ship (Recommended)"; who runs `release --final`: "Claude, on your word (Recommended)"; the tag ruleset:
"After the first rc release (Recommended)"; data-only releases get rcs: "No (Recommended)"; the plan as written:
"Agreed". Step 1 is done; step 2 (the build, about 150 lines with tests and five doc pages) waits for a chat.

## Built (2026-11-25, `feat/T1062-rc-tags`)

- `pipeline/release.py` `tag_release`: at the staging ship, `data/<r-id>` and the next rc `v<x.y.z>-rc.N` (annotated,
  N one past the newest on origin) on the uploaded commit; a rerun tags the pushed HEAD the next rc, or nothing when
  HEAD is the newest rc apart from app.json's `versionCode` / `buildNumber` (`beyond_build_numbers`). A version tagged
  final already gets no rc, said. Data-only releases unchanged. Ledger `tags` now lists the rcs too; no new field.
- `release --final <x.y.z>`: `v<x.y.z>` on the newest rc's commit, pushed; refused with no rc, another version,
  HEAD or `origin/release/<x.y.z>` beyond the rc, or the tag existing (origin or local). No `--dry-run` (refused).
- `tag_problems` (production): with rcs, `v<x.y.z>` AT the newest rc and the ship commit at or before it; with none
  (tagged before T1062), the old rule.
- `release --rc-check` + `deploy/lib/build-links.sh` `build_check_rc`: on `release/x.y.z` both store builds' precheck
  wants HEAD = the newest rc apart from the build numbers ("tag a new rc first"); `--no-rc-check=<reason>`, printed.
- Found while building: 0.9.0 passed `tag_problems` because production ran (ledger `prod_at` 2026-11-24 12:00) while
  `v0.9.0` was still at the ship commit a98dd68f9; the tag was re-made at 65f84020c at 14:22, after production. A
  `--prod --dry-run` of r-0aec88ed3edb now would refuse it.
- Review fixes (2026-11-25, PR #257): production takes a data release shipped from a versionCode commit after the rc
  (the rc apart from the build numbers); `--final` after a failed push pushes its local tag when it is on the newest
  rc and refuses one elsewhere (`git tag -d`), and the staging ship stops on a local-only final; a failed comparison
  at the ship is a Stop; both store builds' inner run refuses a HEAD other than the one the precheck checked
  (`BUILD_PRECHECK_SHA`, `build-links.sh build_same_head`).
- 0.10.0: origin had no `v0.10.0` on 2026-11-25, so its next staging ship (this change cherry-picked) tags
  `v0.10.0-rc.1`. Had the old code made `v0.10.0` first: no rc is made, production takes the old rule.

## 2026-11-25: built (repo-move chat, PR #257)
On main through the merge queue as 7139d1ae2; commits 5ac3d67f3 (`build(T1062)`: code and tests) and fad6911e8
(runbooks), cherry-picked onto `release/0.10.0` after a85dec393 (the repo's new name). What changed:
`release --staging --push` tags `v<x.y.z>-rc.N` (a rerun at a new HEAD is the next rc; a HEAD that differs from
the newest rc only by `versionCode` / `buildNumber` tags nothing); both phone build scripts refuse a tree that
is not the newest rc; `python -m pipeline release --final <version>` (Claude, on the owner's word) puts
`v<x.y.z>` on the newest rc; `--prod` refuses until that tag is there. One independent review; its four finds
fixed with real-git tests (235 passed, 20 mutations killed); two more are T1147.

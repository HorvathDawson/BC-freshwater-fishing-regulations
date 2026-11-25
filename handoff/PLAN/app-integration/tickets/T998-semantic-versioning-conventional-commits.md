# T998 — Meaningful versions: SemVer for the app, the bump derived from Conventional Commits

Status: DONE — merged by PR #14 (6a16738db, 2026-11-22 18:06Z). On GitHub, docs-ci's `commits` job passed every later PR and refused one (#17: a 111-character header; replaced by #19). Follow-up: #22 exempts the 5 trunk commits that merged before the check was on.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T998 | Meaningful versions: SemVer for the app, the bump derived from Conventional Commits (1.0.0 at public launch) | DONE | 6a16738db | owner 2026-11-21; 2026-11-22: next release is 0.9.0 (owner accepted the computed bump; it waits for the next chat's work) |

Source: owner 2026-11-21, verbatim: "We should make a standard versioning system too (when major minor patch happens)
like to make versions meaningful". Dialog answers: 1.0.0 "At public launch (Recommended)"; who picks the bump:
"Conventional Commits" (not the recommended ticket-derived option). Standing rule: industry best practice.

## Today (measured 2026-11-21)
35 app versions in 17 days (0.6.5 → 0.8.5), each bumped by hand in `app/apps/mobile/app.json` + `package.json`.
Most are patch bumps even when they added features (0.7.1–0.7.12, 0.8.1–0.8.5). The only minor bumps were 0.7.0 and
0.8.0. No release tags. Commit subjects are free-form (`T994 F1 …`, `handoff: …`, `Merge …`): none of the last 300
commits uses a Conventional Commits type. Data releases are content ids (`r-<hash>`). Data that old apps can't read is
gated by the derived `minAppVersion` (T993 G2). Build numbers (`versionCode` / `buildNumber`, now 43) only go up.

## The rule (SemVer 2.0.0, as an app applies it)
- **MAJOR:** a change an installed app can't take without updating (the data's `minAppVersion` passes the installed
  major), or a redesign that changes how the app works. Commit marker: `!` after the type, or a `BREAKING CHANGE:`
  footer.
- **MINOR:** a new feature or a change anglers notice (`feat`).
- **PATCH:** bug fixes only, no new behaviour (`fix`, `perf`).
- **No bump:** `docs`, `chore`, `ci`, `test`, `refactor`, `build`, `style`. A data-only release never bumps the app:
  it gets the tag `data/<r-id>` (T995), and its own id stays the content hash.
- **Before 1.0.0** (closed testing): a breaking change bumps MINOR, the standard 0.x reading (release-please's
  `bump-minor-pre-major`). **1.0.0 = the first open Play / App Store release** (owner). From then on, the full rule.
- Build numbers: +1 on every store build, never reset, independent of the version.
- The website version matches the app version (unchanged).

## Commit format (Conventional Commits 1.0.0)
`<type>(<scope>)!: <subject>`. The scope is the ticket id when there is one: `fix(T983): …`, `feat(T985): …`,
`ci(T993): …`. The ticket guard's subject regex (`check-tickets.mjs` `T_IN_SUBJECT`) already matches `(T983)`, so the
"ticket named in a commit exists" rule keeps working. Merge commits (T995, merge-commit-only) take the PR title, and
the PR title follows the same format. Handoff-only commits use `docs(handoff): …`.

## Work
1. **commitlint** (`@commitlint/config-conventional`, pinned) with a repo config: allowed types as above; scope is
   optional; a `Tnnn` scope must exist in the ledger (reuse check-tickets). Subject max 100 chars (the repo's subjects
   run long).
2. **Enforcement, the standard three layers:** a `commit-msg` hook in T995's versioned hooks dir (`core.hooksPath`);
   a CI job checking every commit in a PR plus the PR title (a required check, T995); and `before_push.py` running
   the same check over the commits about to be pushed. History before the cutover commit is exempt (recorded as a
   commit id in the config, not a date).
3. **The bump:** the release command reads the commits since the last `v*` tag (first run: since the cutover commit,
   0.8.5 as the base) and computes the next version. It prints the commits that decided it, sets `app.json` +
   `package.json`, opens the changelog entry, and asks the owner to confirm. A version set by hand that disagrees with
   the computed one is refused, naming both.
4. **Tags:** the release command tags `v<version>` on the release branch at ship (T995). The production steps check
   the tag exists and points at the built commit.
5. **Changelogs:** the in-app angler notes stay hand-written (`changelog.ts`, owner-cleared, unchanged). A generated
   developer changelog (`CHANGELOG.md`, grouped by type, from the commits) is written by the release command.
6. **A data-only release's branch** (open from T995's lane, 2026-11-22): after `release/<v>` merges back, app.json
   still names that version, so preflight's "HEAD on `origin/release/<app version>`" points at a finished branch.
   Default: `--cut --data-only` makes `release/data-<YYYY-MM-DD>` from origin/main, and preflight accepts it when no
   app file changed since the last `v*` tag (the computed bump is none). It tags `data/<r-id>` and merges back by PR like
   any release branch.
7. **Docs:** AGENTS.md rule for the commit format and the bump rule; the human runbook (T993 plan 11) says how a
   version is chosen; agent briefs carry the format; memory updated.

## Acceptance
- A commit `T994 fix x` is refused by the hook and by CI; `fix(T994): x` passes; `fix(T9999): x` is refused (not in
  the ledger).
- Fixture histories: fix only → 0.8.6; one feat → 0.9.0; `feat!` before 1.0 → 0.9.0; after 1.0, `feat!` → 2.0.0;
  docs/ci only → no app release (the command says so).
- A hand-set version that disagrees with the computed one is refused (mutation: delete the comparison, the test fails).
- The release rehearsal (`--dry-run`) prints the computed version and the commits that decided it.

Depends: T995 (tags, release branches, versioned hooks, required checks). Size: M. Owner: no (rules answered; he
confirms each computed version at release). Default: as above.

## Lane notes (2026-11-22, feat/T998-versioning)

Commits: a512a5165 (`ci`: commitlint, the hook, before_push, docs-ci `commits`), 2c2b85977 (`feat`: the bump, `--cut`,
preflight `version`, data-only branches), 7acffa1b8 (`fix`: the commits our tools make carry the format), and the docs
commit (AGENTS W48, the release runbook, the maps, this ticket).

- **Item 1, commitlint:** `@commitlint/config-conventional`, `lint`, `load` 21.2.4 and `parse`, `is-ignored` 21.2.3 (the
  version `lint` takes: one copy), exact, in `app/package.json` (`app/deps.md` says why each). The CLI package is not
  used: one script, `app/tools/check-commits.mjs`, does the hook (`--edit`), ranges (`--range a..b`) and the PR title
  (`--title t --number n`, checked as the merge commit `t (#n)`). Config `app/tools/commitlint.config.mjs`: the types of
  `app/tools/versioning.json` (the release command reads the same file), header ≤ 100, a ticket id in the HEADER (scope
  or subject, the guard's own `T_IN_SUBJECT`, through check-tickets' new `ledgerIds` / `ticketsIn`) must have a ledger
  row. Off, with why: subject-case (acronyms, code names), body / footer line length (prose wraps at 120). commitlint's
  default ignores cover `Merge pull request #N from …`, `Merge branch …`, `Merge remote-tracking branch …` (verified by
  test), `Revert "…"`, `fixup!`. Not covered (history only, all exempt or merged already): hand-typed `merge origin/main
  into …` (lowercase) and `Merge review/T995 (…)` (no "into") — write git's own merge subject.
- **Item 2, three layers:** `.githooks/commit-msg` (strips git's repository variables as pre-push does; sub-second);
  `before_push.py`'s first step, always (the hook: each pushed ref's `<base>..<sha>`; by hand: `origin/main..HEAD`);
  docs-ci's `commits` job on EVERY pull request (no `changes` gate; `ok` = `docs ok` needs it; `pull_request` now also
  on `edited`, since a re-run reads the old title; pinned by test_workflow_pins, `ci_paths.EVERY_PR` / `PR_TYPES`).
  The cutover b5dd8ade3 and the T995 switch merge ca7c0cce7 (the old main's d6a5ca528 under it: a `fix(in-season)` from
  August that would have counted) are exempt by id, with their ancestors (`versioning.json`).
- **Item 3, the bump:** `pipeline/release_version.py`. ONLY THE APP'S COMMITS COUNT (a path under `app/` a store build or
  the site ships: not the feeds worker, the dev tools / gates, the conformance suite, the fixtures, test files, docs —
  `NOT_APP`, `APP_AGAIN` for the SEO builder), as release-please's per-package paths; otherwise every pipeline `fix`
  would force an app release and "data-only" (item 6) could never happen. Measured on origin/main today: no app commit
  bumps since the cutover (5 app commits: chores and merges): `--cut` says "no app release", `--cut --data-only` would
  cut `release/data-2026-11-22`. A merge commit whose own subject is not in the format counts by what it merged; a
  non-merge app commit not in the format stops the release, named (add it to `exempt` with why). `--cut` prints the
  decision, asks the owner to type the version (no terminal: prints the command), refuses a typed version that
  disagrees naming both (`RV.accepts`; mutation-tested), accepts 1.0.0 from a 0.x base (the owner's launch call), and
  makes the branch's one commit WITHOUT a checkout (a temporary index): app.json + package.json, an EMPTY draft entry in
  changelog.ts (check-changelog and the build refuse it until the anglers' bullets are written), CHANGELOG.md.
  Preflight's new `version` check holds app.json to the same computation; the rehearsal prints it with its commits.
- **Item 4, tags:** T995's (`tag_release`, `tag_problems`), unchanged for an app release.
- **Item 5:** `CHANGELOG.md` (developers, grouped by type) is written at the cut; a fix cherry-picked onto the branch
  after the cut is not in it (the next cut lists it).
- **Item 6, data-only:** `--cut --data-only` makes `release/data-<YYYY-MM-DD>` from origin/main, refused when an app
  commit bumps the app; `ci_paths.shipping_branch` (preflight and `ci_paths.py release-branch`, which upload.sh reads)
  takes a checked-out data branch; preflight's `version` check wants no app bump and app.json = the shipped version; the
  ship tags only `data/<r-id>` (ledger `data_only`); `--prod` asks no `v` tag of it; it merges back by PR as any.
- **The commits our tools make** now carry the format (they reach PRs): the fixture re-cut `test(T666): …`, review
  marks `chore(review): …`, the writers `fix(T325): …`, proposals `fix(T185): apply <id>: <title>` (cut to 100),
  build-apk.sh's `chore(release): versionCode N …` (its last-Play-build lookup reads both subjects).
- **Item 7:** AGENTS W48 (the format, the PR title ≤ 93 so ` (#N)` fits, the bump), W47's hook line, the release
  runbook §0 (how the version is chosen; T993 item 11's one human runbook folds it in), app + pipeline maps. NOT done
  here: the memory line (the main chat's).
- **Found:** `core.hooksPath` on this machine was the absolute path of the main checkout's `.githooks`, so a worktree
  ran the main checkout's hooks, not its own (made relative 2026-11-22, see the review section). `test_review_queue`'s two corpus tests
  need the live atlas (missing in a fresh worktree; unrelated).


## Independent review (2026-11-22), landing
- Fixed (311ee72d5): the commit-msg hook found the repo from its own file, so with an absolute `core.hooksPath` every
  worktree was checked against the main checkout's ledger (a lane's new ticket refused; reproduced). It now uses
  `git rev-parse --show-toplevel`, pinned. `release_version` counted `app/tools/maplibre-patch/` as not-app; the
  MapLibre build files now bump the app (test case).
- **`core.hooksPath` set to the relative `.githooks` on this machine (2026-11-22)**, the documented form: each worktree
  runs its own branch's hooks, so old branches (no `check-commits.mjs`) keep committing and the hook is live only
  where it exists.
- Landing step: after local main syncs past this merge, run `pnpm -C app install --frozen-lockfile --prefer-offline` in
  the main checkout at once (the hook needs `@commitlint`; the review app's write-through, build-apk's versionCode
  commit and release_follows' re-cut commit all commit there). Every lane worktree needs its own real install.
- Left (low, not blocking): 24 of the 72 post-cutover commits on main fail the linter (long headers, old merge
  subjects); no range reads them unless a PR's `base.sha` is stale: make the `commits` job use the merge ref's parents
  (`HEAD^1..HEAD^2`). The merge commit's body is the PR body (`merge_commit_message=PR_BODY`), never linted: a body line
  `BREAKING CHANGE:` makes that merge breaking. `release_version.last_tag` takes the highest `v*` tag in the clone, not
  only those reachable from HEAD (preflight does not fetch tags). commitlint's merge-ignore regex uses `/m`, so a body
  line "Merge branch x" skips the commit (the Python reads it the same way).

## Owner answers 2026-11-22 (dialog)
- The next version: the tool computes **0.9.0**, decided by #14 itself (`feat(T998)`, which touched `app/package.json`
  and the lockfile for commitlint). Owner, verbatim: "let it be 0.9 but we wont release yet until we have more stuff
  rom next chat in it". So: no tool change; the next release is 0.9.0 and waits for the next chat's work.
- Left (low, not built): the pre-push range could leave out commits already on origin/main, as CI does (#22's exempt
  entries cover today's five); a commit that only changes dev tooling under `app/` still counts as an app commit.

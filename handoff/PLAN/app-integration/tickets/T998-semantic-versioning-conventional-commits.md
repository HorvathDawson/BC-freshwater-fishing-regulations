# T998 — Meaningful versions: SemVer for the app, the bump derived from Conventional Commits

Status: OPEN (this chat; builds on T995's tags and release branches, so it lands with or after T995).

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

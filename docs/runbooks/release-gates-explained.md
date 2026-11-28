# Why the release stopped: every gate, in plain words

Written 2026-11-22 for the owner (T992). The release runbook is `docs/runbooks/release-by-hand.md`; the flags
are `docs/runbooks/release.md`. The code is `pipeline/release.py` (its `PREFLIGHT` table and its stages); this
page follows the code as it is on this date.

Each gate below says what it protects,
why it stops, what to do, and whether going past it is ever safe. The 0.8.4 and 0.8.5 stops were traced in the T993 inventory (cut in T1081; git history).

## The shape of the release command

`python -m pipeline release --staging` runs seven stages, in order. Each stage is recorded in the release ledger
(`data/generated/release/ledger.json`), so a rerun skips what is done.

1. **preflight** — fourteen checks before anything is built. Every one runs, and every failure is listed together.
2. **baseline** — keeps a copy of the live release's rule run, so there is a "before" to compare with.
3. **build** — builds the new release. It is the **candidate**: written to `data/release/<r-id>/` and named in
   `data/release/CANDIDATE`. The live release, **LATEST**, does not move.
4. **checks** — the candidate's files, the app it needs, and the binding diff against LATEST.
5. **follows** — refreshes what is cut from a release (the in-season matches, the test fixtures), runs every test on
   the candidate, and makes sure the release's own commits are pushed.
6. **promote** — LATEST becomes the candidate. Only here does the live pointer move.
7. **staging** — uploads to the staging bucket, runs the smoke test, tags the commit.

A stop before stage 6 leaves LATEST, staging and the phones exactly as they were.

## Words used below

- **Release under test**: the release the tests read. During a release it is the candidate (`RELEASE_DIR`).
- **Golden state**: one question a tester would ask the app: a water, a part of it, and a date. There are 134 of them
  (`app/fixtures/golden/states.json`, `states`; count with
  `node -e 'console.log(Object.keys(require("./app/fixtures/golden/states.json").states).length)'`).
- **Golden page**: the words the app shows for a golden state: the rules, the season, the sources. They are stored as
  files: `app/packages/ui-native/src/regs/fixtures/*.json` (the page view) and `app/fixtures/golden/page-oracle.json`
  (the page code's answer for every golden state).
- **Fixtures**: test data cut from a real release: the golden pages, the tap answers (`app/fixtures/golden/taps.json`),
  and a small copy of the release for the app tests (`app/packages/data/dev/regs-release/`).
- **Reach run**: the step that decides which sections of which waters each rule applies to (each rule's **bindings**).
- **Accept / escape**: a flag that lets the run go past a gate. An **accept** (`--accept-diff`, `--accept-fixtures`)
  names exactly the one result you read; it never passes a different one. An **escape** (`--no-fast-fail`,
  `--no-ci-check`, …) skips a check and needs a reason, which is printed every time.

## What a golden page and a fixture re-cut are, and why they exist

**The problem they solve.** The app's tests must check the app against real rules. If they checked against a frozen
copy, the copy would drift from the data, and the tests would pass on rules no angler sees. If they checked against
whatever release happens to be on the machine, a new release would silently change what they check.

**The answer.** The tests check against fixtures cut from one named release. When a new release is built, the fixtures
are **re-cut** from it (`node app/tools/regs-recut/sync.mjs --release <candidate>`, which the release runs itself in
stage 5). The re-cut writes:

1. the tap answers for every golden state (`taps.json`);
2. the page code's answer for every golden state (`page-oracle.json`);
3. the small release copy and its section lines;
4. the golden pages (`app/packages/ui-native/src/regs/fixtures/`);
5. the trout table's screen-reader labels (`app/fixtures/golden/trout-a11y.json`).

**Two kinds of change come out of a re-cut** (`pipeline/release_follows.py`, the `answers` column):

- **Bookkeeping only**: ids, handles, the release copy. Nothing an angler reads changed. The release commits these by
  itself with the release id.
- **A golden page changed**: what an angler is told for one of the 134 questions is different. The release stops and
  shows you the pages, because this is exactly the kind of change a person should read before it ships.

**A pure renumber passes by itself** (built 2026-11-21, T993 item 8). A golden page lists its sources by number
(`src`: rows of the release's source table). One rule added early in the table moves every number after it, so a page
can "change" with nothing new on it. That was 0.8.5's stop 8: nine pages, all renumbered. Now the release reads the
old and new page through one shared number mapping, checks that each old number names the same rule as its new one,
and commits it as `renumber only (source indexes)`, printing the mapping. Anything else (a word, a date, a source
added or dropped, two sources swapping places) still stops.

## What the binding diff checks

**What it protects.** The rules are written about waters ("the Vedder River downstream of the Vedder Crossing
bridge"). The reach run turns those words into the exact map sections each rule covers. A change anywhere in the
pipeline (the code, the atlas, a name file, a curated row) can quietly move a rule onto, or off, a stretch of water.
The binding diff compares every rule's sections in the candidate with the live release's, so no such move ships
unseen.

**Why it stops.** Any difference at all. It writes a summary (`data/generated/release/checks/<r-id>/binding_diff.txt`)
and the full list (`binding_diff.json` beside it).

**When it passes by itself** (T993 item 8, deliberately strict). Only when the binding code and the atlas did not
change since the live release's commit, and every change is explained by its own input, one way: a rule added (it only
gains sections), a rule removed (it only loses), its extents only added or only removed, its entry's matched waters
likewise, or a name / match row for those waters added or removed. Then it prints `binding diff AUTO-ACCEPTED` with
each change and its commit. Wording edits, a change that moves both ways, or any change of the binding code never pass
by themselves. (0.8.5's stop 7, Lost Lagoon +1, would still stop today: that batch also changed reach code.)

**When an accept is safe.** Read the summary. If every changed rule is one the batch meant to move, rerun with
`--accept-diff data/generated/release/checks/<r-id>/binding_diff.json`. If even one change surprises you, do not
accept: find its cause, fix it, rerun.

## Every gate, stage by stage

Each gate: **protects** (what would go wrong without it), **stops when**, **do** (the fix), **go past** (the escape,
and when it is safe). "Never" means there is no escape, or you should not use it.

### Stage 1: preflight (fourteen checks, all listed together)

**committed**
- Protects: a release built from code or curated data that is not in git cannot be rebuilt or traced.
- Stops when: a path the build reads (`pipeline/`, `scripts/`, `deploy/`, `data/curated/`, the config) has uncommitted
  changes. The review log `marks.jsonl` and `closed.json` may stay dirty.
- Do: commit them (on `main`, then cherry-pick onto the release branch).
- Go past: never.

**atlas** (the atlas is current)
- Protects: the map was built from its curated inputs (cuts, names, areas, added waters, tidal limits). An input edited
  after the atlas build would not be in the map.
- Stops when: such an input changed after the atlas build. It names each one.
- Do: rebuild the atlas (`python -m pipeline build --atlas`, read its parity, then `--promote`).
- Go past: `--ship-stale-atlas "<reason>"`, only when the edit cannot change geometry (a wording-only change) and you
  agree to ship the old map.

**marks** (review marks have landed)
- Protects: a review mark you saved is in its curated file, so the release ships what you reviewed.
- Stops when: a mark in force is not yet in its file.
- Do: `python -m pipeline.regs.review.apply`, then commit the file it names.
- Go past: a mark that must wait goes on the dated `NOT_LANDED_OK` list in `pipeline/regs/review/apply.py`. Your call.

**wrong-saves**
- Protects: no place ships while its latest review save says "wrong" or "unsure" with no fix (owner 2026-11-17).
- Stops when: such a save exists.
- Do: fix the place and re-save it in the review app.
- Go past: a dated row on `WRONG_SAVE_OK` in `pipeline/regs/review/wrong_saves.py`, when you say it ships as it is.

**classified** (classified waters, also asked again in stage 4)
- Protects: every Classified Water the book prints is in the data as printed (its classes, tributaries, licence).
- Stops when: one is missing or different.
- Do: read the printed page. A parse defect is fixed in the corpus (you re-run the parser); a changed book is fixed in
  `BOOK` (`pipeline/regs/classified_waters.py`).
- Go past: never.

**pushed** (on the release branch)
- Protects: the release ships exactly what its branch on GitHub holds; the staging upload starts a GitHub workflow that
  runs GitHub's copy.
- Stops when: there is no `release/<version>` branch yet, HEAD is not on it, or HEAD has commits GitHub does not.
- Do: what it prints: `python -m pipeline release --cut`, or `git switch release/<version> && git pull --ff-only`, or
  `git push origin release/<version>` (after a Play build, which commits its versionCode).
- Go past: never.

**version**
- Protects: the app version means something (SemVer from the commit messages, T998).
- Stops when: `app.json`'s version is not the one the commits compute, or a commit not in the commit format joined the
  history before the format check existed, or a data-only branch carries an app change.
- Do: cut the computed version (`python -m pipeline release --cut`). An old commit not in the format goes into
  `app/tools/versioning.json` `exempt` with why (history is never rewritten).
- Go past: never.

**ci** (T992's list: "CI")
- Protects: GitHub's checks on this exact commit. 0.8.4's stops and 0.8.5's stops 1 and 4 were all red on GitHub hours
  before the release found them.
- Stops when: a workflow is red on HEAD, or HEAD has no run and the newest finished run is red
  (`scripts/ci_status.py`). A run still going only warns.
- Do: read the run it names, fix it, push, let CI finish. A flaky run: `gh run rerun <id> --failed`.
- Go past: `--no-ci-check "<reason>"`, only when the red is a known flake you reran green elsewhere, or a red in an area
  this release does not ship. Never for a real failure.

**live**
- Protects: there is a live release to compare with.
- Stops when: `data/release/LATEST` is missing. Do: build the first release. Go past: never.

**min-app** (T992's list: "the app-version gate", asked here before the build)
- Protects: a release in a data format an app cannot read would break that app. The release is stamped with the oldest
  app allowed to apply it; older apps keep what they hold and say "Update the app to get new regulations".
- Stops when: the code writes a format no recorded app reads, or a `--min-app-version` you gave is lower than the
  formats need.
- What it does for you: it derives the number (T993 item 2): the live release's stamp, raised to what the code's
  formats need, and says which older apps a rise leaves behind. You no longer have to remember the flag (0.8.5's stop 3
  was a forgotten `--min-app-version 0.8.4`).
- Do: drop a too-low flag; for a new format, the app that reads it must be recorded first
  (`python -m pipeline.tools.emit.app_reads`).
- Go past: never lower. `--min-app-version X.Y.Z` may only raise it.

**site** (T992's list: "the staging site's app version")
- Protects: the staging website runs an app too. If it is older than the release's minimum app, the website refuses the
  new data ("The map data could not be loaded"), found only after the upload.
- Stops when: the live staging site's version is below the minimum app (0.8.5's stop 5: a 0.8.3 site under a 0.8.4
  release).
- Do: deploy the staging site from the release branch first (`deploy/staging/site-all.sh`), then rerun.
- Go past: `SITE_CHECK_SKIP=<reason>` in front of the command (printed; the upload honours it too). Never for a
  release the site will serve.

**pieces** (also asked again in stage 4)
- Protects: closed areas inside a salmon place are drawn by the feeds worker. A release that binds a piece the worker
  does not serve would show it open.
- Stops when: the staging feeds worker does not serve every piece the release binds (asked of LATEST before a build,
  of the candidate after).
- Do: `deploy/staging/feeds.sh`, then rerun. Go past: never.

**fast-fail** (T992's list)
- Protects: the cheap checks (a minute or two: conflict markers, the ticket ledger, typecheck, changed tests, the
  committed mini release, …; the list is `scripts/fast-fail.sh`) catch a doomed run before the long build.
- Stops when: any step fails. It lists every failed step (0.8.5's stop 6 was a fast-fail step itself handing worker
  files to the app's tests).
- Its pass is remembered by the content of the files, so a rerun on the same files skips it.
- Do: read each failed step; one alone: `FAST_FAIL_ONLY='<part of its name>' scripts/fast-fail.sh`.
- Go past: `--no-fast-fail "<reason>"`, only for a failure in something this release does not ship, and say so.

**fresh-copy** (T992's list)
- Protects: a test that passes here only because this folder holds something git does not (a build, a cache, an
  uncommitted file). It clones HEAD into a clean folder and runs the pipeline's no-data tests there, as CI does
  (`scripts/fresh-copy-tests.sh`).
- Stops when: a test fails on the clean copy (0.8.5's stop 1: the committed mini release had been cut before a format
  switch flipped).
- It runs once per release; a later commit that touches only what the release refreshes (fixtures, matches) does not
  rerun it.
- Do: fix the test, or commit the file it needed. `scripts/fresh-copy-tests.sh --only <regex>` reruns a few.
- Go past: `--no-fresh-copy "<reason>"`, when the commits since its last pass touched only docs.

### Stage 2: baseline

- Protects: the binding diff needs the live release's rule run as its "before".
- Stops when: the rule run on disk is not LATEST's and no copy was kept.
- Do: give one with `--baseline DIR` (its digest must match; checked). Go past: never.

### Stage 3: build

- Stops when: a build stage fails. `python -m pipeline build --status` names the stage and its log.
- Do: fix, commit, rerun. Finished stages are skipped. A rerun does not rebuild a candidate this release built when
  nothing a build reads was committed since.

### Stage 4: checks (on the candidate)

**manifest sanity and the size gates** (T992's list: "a size gate")
- Protects: every file the release lists is there at its size, and each file stays under its size limit
  (`SIZE_GATES` in `pipeline/deliver/release/build.py`, plus the web budgets and the offline total a phone holds). A
  phone on a slow line downloads these.
- Stops when: a file is missing, a different size than listed, or over its limit (0.8.5's stop 2: `water` 85 KB over,
  after a 25-minute build, from the river-mouth column).
- Do: a measured, intended growth gets its limit raised in `SIZE_GATES` with the measurement in a comment (as T985
  did); otherwise shrink the file.
- Earlier warning: the rehearsal prints each file's headroom, tightest first, `WARN` under 5 %.
- Go past: never by flag.

**app-version gate** (T992's list; the same gate the upload runs, `pipeline/deliver/release/app_gate.py`)
- Protects: as min-app above, now on the real files.
- Stops when: a file is in a format the oldest allowed app cannot read. (If preflight derived the number, this should
  not happen; if it does, the derivation has a gap to fix.)
- Do: `--min-app-version X.Y.Z` (the oldest app that reads these formats). Go past: never lower.

**pieces, classified**: as in preflight, now on the candidate.

**binding diff** (T992's list): see [What the binding diff checks](#what-the-binding-diff-checks).

The checks also list, without stopping: item ids a phone may have kept that the new release lost, the live counts, moved
gauge records, and a WARNING per one-bank place keeping a creek whose side is unread (T1029): look at each on the map.

### Stage 5: follows (on the candidate)

**in-season matches**
- Protects: every in-season notice you matched to a water still lands on a real stretch of the new release.
- Stops when: a match does not resolve. Do: re-match it in the review app's In-season tab, commit
  `data/curated/inseason/notice_waters.json`, rerun. Go past: never.

**fixture re-cut and golden pages** (T992's list): see
[What a golden page and a fixture re-cut are](#what-a-golden-page-and-a-fixture-re-cut-are-and-why-they-exist).
- Stops when: a golden page changed in what an angler reads. Nothing is committed.
- Do: read `git diff -- app/packages/ui-native/src/regs/fixtures app/fixtures/golden/page-oracle.json`.
- Go past: `--accept-fixtures <token>` (the token is printed and names exactly this re-cut), when every changed page is
  what the batch meant. If one page reads wrong, do not accept: that is the bug the gate exists to catch.

**tests on the candidate** (release-backed tests)
- Protects: the app and pipeline agree with the NEW data before anyone gets it. It runs the tests that read the
  release under test, the whole app check, the feeds worker's tests, and the pipeline's live-data tests, all on the
  candidate, and lists every failure together.
- Stops when: any of them fails (0.8.5's stop 9: seven water-map tests expected the old release's missing column).
- Do: fix the code on `main`, cherry-pick, rerun.
- Go past: `--no-follow-tests "<reason>"`, almost never. 0.8.4 used it (T980) and the skipped failure became 0.8.5's
  stop 1 and a day of red CI.

**push gate**
- Protects: the upload's workflow runs GitHub's copy, so the release's own commits must be on GitHub first.
- Stops when: commits are not pushed. Do: the one `git push` it prints, or rerun with `--push` (it pushes only the
  release's own commits). Safe: always. When someone else's commits are there too, `--push` refuses to push: read
  them, then push by hand. If the release branch moved on GitHub since preflight: `git pull --ff-only`, then rerun.

### Stage 7: staging

- **Changed on disk**: the release folder's content no longer matches what its checks passed (the fingerprint
  moved). Never ship it: rebuild (`python -m pipeline build --force deliver --hold-latest`) and rerun. No escape.
- **Upload prechecks**: the run-timing base matches the release's tiles, `gh` is logged in, the `r2:` remote exists.
  Fix what it names; the earlier stages are skipped on the rerun.
- **Upload failed**, or **GitHub did not start the run-timing workflow** (exit 75): fix the login, or wait for GitHub
  Actions, then rerun. **Dispatched but unreadable** (exit 76): the workflow may be running. Check
  `gh run list --workflow feeds-runtiming.yml` first; do not start it again; rerun once it has finished.
- **Smoke test**: the live staging site does not serve the release correctly. Read its FAIL lines and screenshots;
  fix and rerun (only the smoke runs again).
- **Tags**: `data/<r-id>` already exists on GitHub at another commit, or a local-only tag points elsewhere. Read why
  (a release id names one release; a stale local tag: `git tag -d <tag>`), then rerun: only the tags are left. A
  release staged before the upload commit was recorded cannot be tagged by the command: it prints the `git tag`
  command to run by hand. (T1062) A version tagged final already (`v<version>` on GitHub) gets no new rc, said; a
  change after the final tag ships as the next version.

### Production (`release --prod` and `deploy/prod/upload.sh`)

These are guards, not judgment calls. None has an escape except the site gate's. Every production step is the
owner's (2026-11-22: "only me unless i say otherwise that time"); the rehearsals (`--dry-run`) are anyone's.

- The release passed its checks and reached staging (the ledger), and is unchanged on disk since (its fingerprint).
- Staging still serves it (a later staging release replaced it: ship that one instead).
- Its tags are on GitHub: `data/<r-id>` at the shipped commit (missing: rerun `release --staging`, which tags and skips
  the rest); (T1062) `v<version>` AT the newest rc's commit (missing: `release --final <version>`), and the shipped
  commit is that rc, before it, or it plus only versionCode commits (a data release shipped after a Play build). A
  shipped commit with code the rc does not hold cannot pass: its own ship tagged a newer rc, so the final belongs
  there. With no rc of the version (tagged before T1062): `v<version>` at the shipped commit or before it.
- `CONFIRM_PROD=<r-id>`, an interactive terminal, the id typed again, no agent session.
- The upload itself: staging's live manifest is byte-identical; the app gate; the **pieces gate against the production
  feeds worker** (deploy it first); the **site gate against canifishthis.ca** (deploy the site first;
  `PROD_SITE_CHECK_SKIP=<reason>` skips it, printed).

### The cut (`release --cut`)

- A commit not in the format joined before the check: add it to `exempt` in `app/tools/versioning.json`, with why.
- The typed version disagrees with the computed one: refused, naming both. Only 1.0.0 may be typed over.
- The branch exists already: switch to it. The version is tagged already: cut the next one.

### The final tag and the phone builds (T1062)

- **`release --final <version>`** refuses: no rc on GitHub (the staging ship tags one); HEAD or
  `origin/release/<version>` differ from the newest rc beyond app.json's build numbers (tag a new rc: rerun
  `--staging --push`, look again); `v<version>` exists already (never moved or made twice). No escape.
- **The phone builds' rc check** (on `release/x.y.z`): HEAD must be the newest rc apart from the build numbers;
  otherwise tag a new rc first (`release --staging --push`). `--no-rc-check=<reason>` goes on and prints it.

## Accepts and escapes at a glance

| Flag / setting | Remembered on a rerun? | Safe when | Not when |
| --- | --- | --- | --- |
| `--accept-diff FILE` | yes | every changed rule is one the batch meant | any change surprises you |
| `--accept-fixtures TOKEN` | yes | every changed golden page reads as the batch meant | any page reads wrong |
| `--min-app-version X.Y.Z` | yes | raising it above the derived number on purpose | lowering it (refused) |
| `--baseline DIR` | yes | the live release's run was not kept (its digest is checked) | — |
| `--push` | yes | always (only the release's own commits) | — |
| `--ship-stale-atlas REASON` | no | a wording-only input edit, and you agree | any geometry edit |
| `--no-ci-check REASON` | no | a known flake, or a red area this release does not ship | a real failure |
| `--no-fast-fail REASON` | no | a failed step unrelated to what ships, said | anything that ships |
| `--no-fresh-copy REASON` | no | the commits since its pass are docs only | code changed |
| `--no-follow-tests REASON` | no | almost never | a real failure (it comes back) |
| `NOT_LANDED_OK` / `WRONG_SAVE_OK` row | (a code row) | you say this mark or save waits / ships as is | — |
| `SITE_CHECK_SKIP` / `PROD_SITE_CHECK_SKIP` | no | never for a site that serves the release | — |
| `--no-rc-check=REASON` (phone builds) | no | an emergency build the owner asked for off the newest rc | a store build that will be promoted |

## Simplification ideas (proposals, not built)

Already built since 0.8.5 (T993 lane G2): the minimum app is derived; renumber-only golden pages pass; a binding diff
fully explained by its inputs passes; size headroom is printed; an accepted diff survives a rerun; a rerun does not
rebuild an unchanged candidate. These are left, for you to pick or drop:

1. **Declare the expected changes with the batch.** A ticket could list the bindings it means to move ("Lost Lagoon:
   +1 section"); the diff then passes when it matches the list exactly, even when reach code changed. Today such a
   change always stops (0.8.5's stop 7 would).
2. **Say golden-page changes in words.** Print each changed page as "rule X: old words → new words" instead of asking
   you to read a JSON `git diff`.
3. **Let the release deploy the staging site itself** when the `site` check stops (Claude may deploy staging): one
   less stop, the same order.
4. **Let the release deploy the staging feeds worker itself** when the `pieces` check stops, for the same reason.
5. **Skip the fresh copy when GitHub already ran the same tests on the same commit.** The fresh copy and CI's pipeline
   tests ask the same question; a green pipeline run on HEAD could stand in for it.
6. **Make `--push` the default.** It only ever pushes the release's own commits.
7. **One production website command**, a production twin of `deploy/staging/site-all.sh` (none exists today), instead
   of three hand commands.
8. **Price a new column before the build.** A lane that adds a column to a shipped file records its gzip growth, and
   fast-fail warns when a file's headroom on LATEST is under 5 % (today only the rehearsal prints it).

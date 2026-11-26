# T1062 step 1: release candidates (`vX.Y.Z-rc.N`) instead of a tag that moves (PLAN, agreed, nothing built)

**Agreed by the owner 2026-11-25 ~13:58 (dialog): "Agreed".** His picks in section 5: 1a, 2a, 3a, 4a, 5a.

Written 2026-11-25 by the repo-move chat for the owner to agree. Paths are in `handoff/PLAN/runbooks/` (`rbh` =
release-by-hand.md) and `pipeline/` unless marked. No ticket numbers are new.

## 1. The problem
- 0.9.0's tag was made at staging ship (`data/r-0aec88ed3edb` at a98dd68f9, 04:40 on 11-24). Then T1027, T1034 and
  T1049 were cherry-picked and versionCode 44 committed (65f84020c). Today `v0.9.0` is the later commit, made 14:22.
  So it was deleted and re-made: inferred from tag dates, no log of it exists.
- A moved tag is bad because `git fetch` never overwrites an existing tag: any other clone keeps the old one without
  a sound. Its first commit had already been read by the ledger and `--prod`'s tag check.
- It also leaves prod's own check unhappy: `tag_problems` (release.py:2701) wants `v` at the ship commit or BEFORE it,
  and 65f84020c is after a98dd68f9. Not verified how 0.9.0 passed it (the ledger is not in git).

## 2. Today's steps, and where the tag is made (rbh)
1. `--cut` (rbh:102-115; release.py:2560): makes `release/<x.y.z>` with one version commit. Makes NO tag. Refuses if
   `v<x.y.z>` exists (release.py:2623, 2631).
2. Bullets commit (rbh:122-137), rehearsal (rbh:139-170), staging data `--staging --push` (rbh:172-190).
3. At the end of step 2's staging ship, `tag_release` (release.py:2007-2068) tags the shipped commit
   `data/<r-id>` and, on the version's first ship, `v<x.y.z>`, and pushes both. A rerun skips it: "tagged already"
   (2013), and an older `v` "stays" (2038-2041). Nothing in code ever moves a tag; the move was by hand.
4. Staging look, then phones (rbh:246-311): each Play build commits a versionCode bump on the release branch
   (build-apk.sh:329-335). iPhone's build number is set in the worktree only (build-ios-testflight.sh:44-45, 194).
5. Production data (rbh:334-344) refuses unless both tags are on GitHub at the shipped commit (`tag_problems`, 2677).
6. A late fix = cherry-pick onto the release branch, rerun step 3's command (rbh:407-415). Merge back by PR (rbh:384).

## 3. The proposal
- **rc.1 = the staging ship.** `tag_release` makes `v0.10.0-rc.1` where it makes `v0.10.0` now (same commit, same
  moment, same push). `data/<r-id>` stays as is. The cut still makes no tag.
- **What an rc builds.** The staging data release, the staging site, and the phones (Play Internal, Closed by
  promotion, TestFlight) from the release branch. The phone scripts get one new precheck: the tree equals the newest
  rc's tree except app.json's build numbers; if not, they stop and ask for a new rc first.
- **A late fix** (cherry-pick, as today) makes a new commit; the rerun of the staging command tags `-rc.2` on it. No
  `-rc.1` is touched. Redone: whatever the fix touched. App code: staging site, Play Internal (new versionCode),
  TestFlight (new build). Feeds worker: its deploy. NOT redone: the data release when the r-id is unchanged (it is a
  content hash, rbh:35), its `data/<r-id>` tag, the "What's new" entry, the app_reads row. A fix that changes the data
  gives a new r-id, a new data tag, and the whole data stage reruns under `-rc.2`.
- **The final tag.** A new step `release --final` (run after the owner looks at staging and the chosen Internal build,
  before production data) puts annotated `v0.10.0` on the SAME commit as the rc that passed. It refuses unless that
  commit is the newest rc, the release branch HEAD matches it (build numbers aside), and it is not already tagged.
  Production data's tag check (`tag_problems`) then asks for `v0.10.0` at the newest rc's commit and `data/<r-id>` at
  or before it, replacing "v at ship or before".
- **No tag is ever moved, re-made or deleted.** `-rc.N` and the final are all new names. A wrong rc is skipped by
  tagging the next one.
- **Version files and the rule (T998).** `app.json` and `package.json` say `0.10.0` in every rc; the cut sets them once.
  `release_version.py` ignores rc tags already: `last_tag` and `shipped_versions` match `x.y.z` exactly
  (release_version.py:62, 265-270; release.py:544). The next bump counts commits since the final tag only.
- **Visible app version: recommend plain `0.10.0`.** An rc build is promoted unchanged to Closed (rbh:253-255), so its
  string must already be the final one. Our iPhone script refuses anything but `x.y.z` (build-ios-testflight.sh:156).
  The App Store takes only dot-separated integers in the short version string and Play's versionName is free text:
  this is my knowledge, NOT checked against Apple's or Google's pages today. The rc is read from the build number
  (versionCode), which `play/<versionCode>.json` already ties to a commit (build-apk.sh:44), and so to an rc tag.
- **Build numbers.** Unchanged: +1 per store build, never reset (rbh:87). A rebuild for rc.2 is simply the next number.
- **Changelog / What's new.** One entry per final version, opened by the cut (release.py:565-569, rbh:112); every rc
  build reads it. The developer `CHANGELOG.md` is written once at the cut.
- **Staging release name.** Unchanged `r-<hash>`; an rc is never in it.
- **app_reads row (T1048).** Keyed by the final version, written in the cut's version commit (release.py:538-539);
  rc tags do not count as "shipped" (release.py:544). Unchanged.
- **Other readers of tags.** None found: no workflow has a `tags:` trigger (.github/workflows), `before_push.py:198`
  skips tag pushes, `scripts/ci_status.py` does not read tags, nothing calls `git describe`.

## 4. What changes if he agrees (size M, about 150 lines of code with tests, 5 docs)
- `pipeline/release.py` `tag_release`: tag `-rc.N` (next unused N); a rerun at a new HEAD tags the next rc.
- `pipeline/release.py` new `--final` (function, flag, help), the changed `tag_problems`, new ledger fields.
- `pipeline/tests/test_release_command.py`: rc numbering, `--final` refusals, prod check, rc tags ignored.
- `deploy/staging/build-apk.sh`, `build-ios-testflight.sh`: the tree-equals-rc precheck; the rc name in the email.
- `runbooks/release-by-hand.md` (steps 3, 6, 7, 8, "A fix during the release"), `release.md` (lines 45-47, 179),
  `release-gates-explained.md` (the tag stop), `release/PRODUCTION.md` if it names the tag, T998's item 4 text.
- Optional, no code: a GitHub tag ruleset on `v*` and `data/*`: block update and delete, allow create. Enforceable
  only once nothing needs to move a tag. Today's 3 rulesets cover branches only (repo-move/PREP.md section 1). After the
  transfer the owner makes it in the browser or with `gh api`; add it to the snapshot diff.

## 5. Decisions for the owner
1. **Visible version in an rc build?** (a) plain `0.10.0`, rc shown only by build number and tag (recommended);
   (b) `0.10.0-rc.N` on Android only.
   (a) keeps one build promotable to Closed; (b) shows it but the final needs a rebuild.
2. **Where rc.N is tagged?** (a) at the staging ship, phone builds checked equal to it (recommended: one tagging
   place, already coded); (b) on each Play Internal bump commit: exact for phones, a second tagging place.
3. **Who runs `release --final`?** (a) Claude on the owner's word, like the cut's number (recommended: only a tag
   on a commit already agreed); (b) owner-only with `CONFIRM_PROD`: safer feel, one more command for him.
4. **Tag protection ruleset (v*, data/*; no update/delete)?** (a) yes, after the first rc release works
   (recommended); (b) yes, right after the transfer; (c) never. (a) avoids locking in a flow not yet used; a wrongly
   made tag then needs the owner to lift the rule to delete it.
5. **Data-only releases (`release/data-<date>`) get rcs?** (a) no, they tag only `data/<r-id>` and have no version
   (recommended); (b) yes, `data/<r-id>-rc.N`. (a) keeps one rule for apps, (b) doubles tags for no moved-tag risk.

## 6. Not covered, not verified
- Not built; the first release using it cannot be 0.10.0 (its production run is in progress); next is 0.10.1 or 0.11.0.
- The release ledger (`data/generated/release/ledger.json`) is not in git: its field names and how 0.9.0 passed
  `tag_problems` are not seen. `--prod` and the preflight were read, not run.
- Apple's and Google's version-string rules are from memory (section 3). Whether the iPhone external group review
  keys on version only was not looked at.
- A fix that moves what the app reads (app_reads) after the cut: how the gates catch a stale row was not traced.
- Merge-back, cherry-pick bookkeeping and `git fetch` of tags in other clones are unchanged by this plan.

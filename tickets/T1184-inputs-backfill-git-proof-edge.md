# T1184 — `python -m pipeline.atlas.inputs --backfill`: the git proof passes a file that changed

Status: OPEN — a #286 follow-up; scoped 2026-11-26: real on main

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1184 | `python -m pipeline.atlas.inputs --backfill`: the git proof passes a file that changed | OPEN — a #286 follow-up; scoped 2026-11-26: real on main | — | a lane: prove a backfilled input by the commit the atlas recorded (`code.git.commit`), never by commit time; refuse without one; a test with a merged-later commit |

Source: PR #286's third review (M4) and its body: "The pre-existing `inputs --backfill` (T547) is untouched. It does share the flawed edge: `git_proof` compares a
file's last commit time with `build_ts`, so a branch commit made before the build but merged after it passes ... (a NEW TICKET for the planning chat)."

## Which is true (settled)
Both statements were right about different things. #286 DROPPED the backfill it had added, `--backfill-code` (`import_closure`, `backfill_code`, about 190 lines);
the live atlas is rebuilt at the re-cut instead. The older T547 `--backfill` for curated inputs stays on main: `pipeline/atlas/inputs.py:385-441`
(`git_proof`, `backfill`), wired at L447 and L455.

## The defect
`git_proof` (`inputs.py:397-398`) takes `git log -1 --format=%ct -- <file>` and calls the file proven when that commit's time is before the atlas's `build_ts`.
Commit time is not the time the commit reached this checkout: a commit made before the build but merged or pulled after it passes, and the file the atlas
never read is "proven unchanged". Reproduced in a scratch repo (2026-11-26): base commit Jan 1 (v1), branch commit Jan 2 (v2), atlas "built" Jan 3 reading v1,
merge Jan 4. `git log -1` names the Jan 2 commit, before the build: proven, though the file is v2. `--first-parent` (the owner's option e) names the Jan 4 merge and
refuses, but a fast-forward pull (`git merge --ff-only`) still names the Jan 2 commit on the first-parent line: `--first-parent` fixes only the merge shape.

## Fix (one sentence)
Prove a missing input by `git diff --quiet <code.git.commit> -- <file>` against the commit the atlas recorded at its start (T1171, clean records only); an atlas
with no recorded commit is built before T1171 and is rebuilt anyway ("records no code"), so the time-based proof goes.

## Acceptance
A test with both shapes (a merge commit and a fast-forward of a commit older than the build) refuses the proof; an unchanged file since the recorded commit
is proven. Severity: low (nothing needs a backfill until an input is added to `ATLAS_INPUTS`). Model: Sonnet. Size: S.

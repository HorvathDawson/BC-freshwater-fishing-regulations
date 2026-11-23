# T1019 — The pre-push commit check reads the PR's range (`merge-base(origin/main, HEAD)..HEAD`), not `<remote old>..<local>`

Status: OPEN.

Source: 0.9.0 batch brief (lane L6, 2026-11-22).

## Why
`scripts/before_push.py --pre-push` (the versioned pre-push hook, W47) checks the commit messages of each pushed branch
over `<remote sha>..<local sha>`. docs-ci's `commits` job, the required check, reads the PR's range: the branch's own
commits since it left the trunk. The two differ after a merge of (or a rebase onto) a newer trunk: the remote's old tip
is then not the branch's base, and the hook's range holds the trunk's own commits (merge subjects, history from before
the T998 cutover) that the PR never checks — a push refused, or checked on the wrong commits.

## Work
- The commit RANGE is `<merge base of the local sha with origin/main>..<local sha>` for every pushed branch, new or
  updated. The PATHS (which areas' checks run) stay the pushed change (`<remote sha>..<local sha>`).
- A test: an updated branch's range is the merge base's, not the old tip's; it fails when the range reverts.

## Acceptance
`pipeline/tests/test_before_push.py` pins it; the hook and docs-ci check the same commits.

Depends: T995, T998. Size: S. Owner: no.

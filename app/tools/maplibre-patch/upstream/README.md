# Upstream contribution: feature state as deltas (drafts, not posted)

For maplibre/maplibre-native `main`. Nothing here has been posted; the owner posts.

| file | what |
|---|---|
| `0001-fix-core-apply-feature-state-to-a-tile-once-then-onl.patch` | the fix (6 source files, +22 −7) + a unit test + one render test (its PNG is a copy of `feature-state/vector-source`'s) + changelog lines (`git am`) |
| `0002-fix-core-mark-a-bucket-modified-only-when-a-changed-.patch` | optional follow-up (6 files, +31 −17): a bucket is re-uploaded only when a changed id is in it |
| `issue.md` | the issue (problem, permalinks, our numbers) |
| `pr.md` | the PR description (what, why, tests, changelog, AI disclosure) |

0001 stands alone; 0002 applies on top of it and can be a second PR (or left out).

## Before posting

- MapLibre's AI policy (https://github.com/maplibre/maplibre/blob/main/AI_POLICY.md): you must have read and reviewed the
  code and be able to answer questions on it; issue and PR text should be in your own words (the drafts are a factual
  base, rewrite them); disclose the AI use (pr.md has a line). No CLA or DCO sign-off is required.
- Open PR #4835 (greensopinion, "apply feature state changes in call order") changes the same lines in
  `source_state.cpp`: its removal-merge hunk is identical to ours. Whichever merges second rebases trivially; mention it.
- Open PR #4828 (bucket CPU copies) touches the context lines of 0002; 0002 may need a rebase if it lands first.

## How to post (run these yourself)

```bash
ME=$(gh api user --jq .login)
gh repo fork maplibre/maplibre-native --clone=false
git clone --filter=blob:none https://github.com/maplibre/maplibre-native.git ~/dev/maplibre-native
cd ~/dev/maplibre-native
git checkout -b fix/feature-state-deltas origin/main
git am /Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/app/tools/maplibre-patch/upstream/0001-*.patch
pip install pre-commit && pre-commit run --from-ref origin/main --to-ref HEAD   # clang-format etc.
git remote add fork https://github.com/$ME/maplibre-native.git
git push fork fix/feature-state-deltas

# the issue first, then put its number into pr.md ("Fixes #<n>") and the PR
gh issue create --repo maplibre/maplibre-native \
  --title "Feature state is re-applied to every rendered tile on every frame" \
  --body-file /Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/app/tools/maplibre-patch/upstream/issue.md
gh pr create --repo maplibre/maplibre-native --base main --head "$ME:fix/feature-state-deltas" \
  --title "fix(core): apply feature state to a tile once, then only changes" \
  --body-file /Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/app/tools/maplibre-patch/upstream/pr.md
```

0002 later, as its own PR on top (branch from the 0001 branch, `git am 0002-*.patch`, push, `gh pr create` with a
short body: what, the render tests passed, depends on the first PR).

0001 is rebased onto main ce1d31a61 (2026-11-07 evening; tests + pre-commit pass there). The drafts' permalinks point at d1b09a89 (still valid). If `git am` fails on a newer main, rebase and
re-run the tests (`../tests.sh` style, see "Tests" in pr.md).

Posted 2026-11-08 as maplibre/maplibre-native#4919 (0001 only, no issue; branch HorvathDawson:fix/feature-state-deltas).

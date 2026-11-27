#!/bin/sh
# (T1041, T1193) REBUILD THE LEDGER AND OWNER VIEWS: the one body of the post-checkout, post-commit, post-merge and
# post-rewrite hooks (each is a one-line caller passing its own name). docs/views/LEDGER.md, DONE.md and OWNER.md,
# git-ignored, built from each ticket's row and `## Owner` block by app/tools/ledger.mjs. A branch from before T1041
# has no tool and no views: nothing to build there. Always exits 0 (a hook that fails would block nothing but nag).
hook=${1:-ledger-views}
root=$(git rev-parse --show-toplevel) || exit 0
[ -f "$root/app/tools/ledger.mjs" ] || exit 0
# shellcheck disable=SC2046
unset $(git rev-parse --local-env-vars)
cd "$root" || exit 0
node app/tools/ledger.mjs --quiet || echo "$hook: the ledger views were not rebuilt (see above): node app/tools/ledger.mjs" >&2
exit 0

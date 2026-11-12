#!/usr/bin/env bash
#
# THE FRESH-COPY TEST RUN (T666; owner 2026-11-11: "once, in the RELEASE pre-check, not in every quick check"): HEAD
# cloned into a temp folder — nothing uncommitted, untracked, ignored or generated beside it — and the pipeline's
# no-data test tier run there exactly as CI's "Tests" step runs it (its marker expression, read from the workflow), in
# chunks (scripts/pytest-chunked.sh). Heavy (the whole tier, minutes; ~1 GB of checkout): run it through
# scripts/heavy.sh. `python -m pipeline release --staging` preflight does, once per release.
#
#   scripts/fresh-copy-tests.sh [--only REGEX]      --only: just the test files whose path matches (trying it out)
#
# caught (build 27, 2026-11-11; T667 "Seen on build 27"): tests that passed in this checkout because of a file it holds
# and git does not (a build, a cache, another lane's uncommitted edit) and failed on a fresh clone, found a release run
# at a time. CI is the same question asked later, after a push; this asks it of HEAD before the build.
#
# The clone is `git clone --shared` (no object copy; this checkout is not written to) and is removed at exit. Its
# .venv is a link to this checkout's (the interpreter and its packages; the code under test is the clone's).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
only=()
while (( $# )); do
  case $1 in
    --only) [[ -n ${2:-} ]] || { echo "fresh-copy: --only needs a regex" >&2; exit 2; }; only=(--only "$2"); shift 2 ;;
    *) echo "fresh-copy: unknown argument $1" >&2; exit 2 ;;
  esac
done
[[ -x $ROOT/.venv/bin/python ]] || { echo "fresh-copy: no $ROOT/.venv/bin/python — scripts/bootstrap.sh makes it" >&2; exit 1; }
# NOT under the temp dir: pipeline/_trace.py skips every path under it (tempfile.gettempdir(), /private/var/folders,
# /tmp), so a clone there records no read and test_stepcache fails only here (caught 2026-11-11, release r-483b295de57c)
FC=${XDG_CACHE_HOME:-$HOME/.cache}/bcfr-fresh-copy; mkdir -p "$FC"
T=$(cd "$(mktemp -d "$FC/fresh-copy.XXXXXX")" && pwd -P)
trap 'rm -rf "$T"' EXIT
HEAD=$(git -C "$ROOT" rev-parse HEAD)
t0=$SECONDS
git clone -q --shared --no-checkout "$ROOT" "$T/repo"
git -C "$T/repo" checkout -q --detach "$HEAD"
ln -s "$ROOT/.venv" "$T/repo/.venv"
cd "$T/repo"
echo "fresh-copy: HEAD ${HEAD:0:12} cloned to $T/repo in $((SECONDS - t0)) s ($(git ls-files | wc -l | tr -d ' ') files; no generated data, nothing uncommitted)"
MARKERS=$(.venv/bin/python -c 'import re, yaml
s = [s for j in yaml.safe_load(open(".github/workflows/pipeline-ci.yml"))["jobs"].values() for s in j["steps"]
     if s.get("name") == "Tests"]
assert len(s) == 1, "Tests step not found"
m = re.search(r"-m \"([^\"]+)\"", s[0]["run"])
assert m, "no -m in the Tests step"
print(m.group(1))') || { echo "fresh-copy: cannot read the marker expression from pipeline-ci.yml's Tests step" >&2; exit 1; }
rc=0
bash scripts/pytest-chunked.sh ${only[@]+"${only[@]}"} -- -m "$MARKERS" || rc=$?
if (( rc )); then
  echo "fresh-copy: FAILED (rc=$rc) on a fresh copy of ${HEAD:0:12} — a test above reads something this checkout has and git does not, or HEAD is broken" >&2
  exit "$rc"
fi
echo "fresh-copy: all passed on a fresh copy of ${HEAD:0:12} ($((SECONDS - t0)) s)"

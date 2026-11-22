# shellcheck shell=bash
# (T978) THE LINKS A BUILD NEEDS THAT GIT DOES NOT HOLD — SOURCED by deploy/staging/build-apk.sh and
# deploy/staging/build-ios-testflight.sh inside their precheck (before the heavy lock), from the repo root. Each missing
# one is listed through the caller's `bad` (its `ok` for the rest) WITH THE EXACT FIX, all of them in one run: never a
# silent exit, never the first one alone.
#
# caught (0.8.4 production build, 2026-11-20, run from a worktree): no `cache/` (the MapLibre AAR) — the AAR
# lookup came back empty and build-apk.sh exited with NO message (`set -e` on a failed `find`); no feeds run token (the
# email precheck); and each was found one build attempt at a time.
#
# In a git WORKTREE the fix is a link to the main checkout's copy when it has one (BUILD_MAIN below); in the main
# checkout it is the command that makes the thing. Links are gitignored as files too (`/.venv`, `/cache` in .gitignore),
# so a link never shows as an untracked path to commit.
#
#   build_links_common            build_links_venv (.venv, the pipeline interpreter: fast-fail, CI status, the Play API)
#                                 + build_links_node (app/node_modules: fast-fail's app gates, wrangler)
#   build_links_mln <maven dir>   exactly one android-sdk-vulkan AAR, the pinned one (pin.env); sets MLN_AAR
#   build_links_file <path> <what> <make> [<or …>]  a gitignored file the build reads (run token, debug keystore);
#                                 <make> is the fix where no main checkout holds one to copy
#   build_links_frontend <base>   the review app's node_modules, when fast-fail will typecheck it (a frontend file
#                                 changed since <base>)

# the main checkout (the worktrees' common git dir's parent); BUILD_HERE = this checkout; BUILD_LINKS_DIR = this file's
BUILD_HERE=$(pwd -P)
BUILD_LINKS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
BUILD_MAIN=$(cd "$(git rev-parse --path-format=absolute --git-common-dir)/.." && pwd -P)

# build_fix <repo path> <command that makes it>: the fix to print — a link to the main checkout's copy from a worktree
# that lacks it, else the command
build_fix() {
  if [[ $BUILD_MAIN != "$BUILD_HERE" && -e $BUILD_MAIN/$1 ]]; then
    local d; d=$(dirname "$1")
    if [[ $d == . || -d $d ]]; then echo "ln -s $BUILD_MAIN/$1 $1"
    else echo "mkdir -p $d && ln -s $BUILD_MAIN/$1 $1"; fi
  else
    echo "$2"
  fi
}

build_links_venv() {
  if [[ -x .venv/bin/python ]] && .venv/bin/python -c 'import yaml' >/dev/null 2>&1; then ok ".venv (the pipeline's Python)"
  else bad "no runnable .venv (fast-fail, the CI status, the Play API read it): $(build_fix .venv scripts/bootstrap.sh)"; fi
}
build_links_node() {
  if [[ -f app/node_modules/.modules.yaml ]]; then ok "app/node_modules"
  else bad "no app/node_modules (fast-fail's app gates, wrangler): (cd app && pnpm install --offline --frozen-lockfile)"; fi
}
build_links_common() { build_links_venv; build_links_node; }

# build_links_mln <maven dir>: MLN_AAR = the one AAR there, and only when it is the PINNED upstream build
# (app/tools/maplibre-android/pin.env, T978): an old lean-patch AAR or an experiment is refused, never shipped quietly
build_links_mln() {
  local dir=$1 found MLN_COMMIT MLN_VERSION
  MLN_AAR=
  # shellcheck source=../../app/tools/maplibre-android/pin.env
  source "$BUILD_LINKS_DIR/../../app/tools/maplibre-android/pin.env"
  if [[ ! -d $dir ]]; then
    bad "no pinned MapLibre repo at $dir (T978, upstream ${MLN_COMMIT:0:9}): $(build_fix "${dir#"$BUILD_HERE"/}" \
      "scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh <work dir>")"
    return 0
  fi
  found=$(find -L "$dir" -name 'android-sdk-vulkan-*.aar' 2>/dev/null)
  if [[ -z $found || $(grep -c . <<<"$found") -ne 1 ]]; then
    bad "$dir must hold exactly one android-sdk-vulkan AAR (found: ${found:-none}; app/tools/maplibre-android/README.md)"
  elif [[ $(basename "$found") != "android-sdk-vulkan-$MLN_VERSION.aar" ]]; then
    bad "$found is not the pinned MapLibre ($MLN_VERSION, app/tools/maplibre-android/pin.env): point MLN_LOCAL_MAVEN at cache/maplibre/$MLN_VERSION (unset it for the default), or build it: scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh <work dir>"
  else MLN_AAR=$found; ok "pinned MapLibre AAR $MLN_AAR (upstream ${MLN_COMMIT:0:9}, no patch)"; fi
}

# build_links_file <repo path> <what reads it> <make> [<alternative>]: a gitignored file the build needs. From a
# worktree the fix is a COPY of the main checkout's (the build copies it on anyway, and a secret stays a plain file)
build_links_file() {
  local fix=$3
  if [[ $BUILD_MAIN != "$BUILD_HERE" && -s $BUILD_MAIN/$1 ]]; then
    fix="cp $BUILD_MAIN/$1 $1"; [[ -d $(dirname "$1") ]] || fix="mkdir -p $(dirname "$1") && $fix"
  fi
  if [[ -s $1 ]]; then ok "$1"
  else bad "no $1 ($2): $fix${4:+; or $4}"; fi
}

# build_links_frontend <base>: fast-fail typechecks the review app's frontend when a file of it changed since <base>
build_links_frontend() {
  local base=$1 out rc
  # (T978 review) a git failure is a failed item with its cause, never a skipped check
  if [[ -z $base ]]; then
    local trunk
    # (T995) the trunk, named once: scripts/ci_paths.py
    trunk=$(python3 "$BUILD_LINKS_DIR/../../scripts/ci_paths.py" trunk 2>&1) \
      || { bad "cannot tell whether fast-fail typechecks the review app: scripts/ci_paths.py trunk failed (${trunk: -200})"; return 0; }
    out=$(git merge-base HEAD "origin/$trunk" 2>&1); rc=$?
    if (( rc )); then bad "cannot tell whether fast-fail typechecks the review app: git merge-base HEAD origin/$trunk failed (rc=$rc: ${out: -200}); git fetch origin $trunk"; return 0; fi
    base=$out
  fi
  out=$(git diff --name-only "$base" -- curation-review/frontend 2>&1); rc=$?
  if (( rc )); then bad "cannot tell whether fast-fail typechecks the review app: git diff --name-only $base failed (rc=$rc: ${out: -200})"; return 0; fi
  grep -qE '\.(ts|tsx|json)$' <<<"$out" || return 0
  if [[ -x curation-review/frontend/node_modules/.bin/tsc ]]; then ok "curation-review/frontend/node_modules"
  else bad "no curation-review/frontend/node_modules (fast-fail typechecks the review app: a frontend file changed since ${base:0:12}): (cd curation-review/frontend && npm ci)"; fi
}

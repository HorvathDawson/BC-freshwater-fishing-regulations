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
# and (T993 plan 5, at the end) the gates both builds run: build_reasons, build_check_changelog, build_ff_base, build_gates.

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

# mln_pinned_aar <maven dir>: prints the AAR there when it is the PINNED build — exactly one android-sdk-vulkan AAR,
# named for pin.env's MLN_VERSION, and the repo's SOURCE.txt (written by build.sh) naming pin.env's MLN_COMMIT — else
# nothing. Silent: build_links_mln says why. (One rule for build-apk.sh's outer and inner runs and the precheck.)
mln_pinned_aar() {
  local dir=$1 found MLN_COMMIT MLN_VERSION src_commit=
  # shellcheck source=../../app/tools/maplibre-android/pin.env
  source "$BUILD_LINKS_DIR/../../app/tools/maplibre-android/pin.env"
  [[ -d $dir ]] || return 0
  found=$(find -L "$dir" -name 'android-sdk-vulkan-*.aar' 2>/dev/null)
  [[ -n $found && $(grep -c . <<<"$found") -eq 1 && $(basename "$found") == "android-sdk-vulkan-$MLN_VERSION.aar" ]] || return 0
  if [[ -f $dir/SOURCE.txt ]]; then read -r _ src_commit _ <"$dir/SOURCE.txt"; fi
  [[ $src_commit == "$MLN_COMMIT" ]] || return 0
  echo "$found"
}

# (T1170) how to get the pinned repo today: the stored build when the lock records it as published, else a local compile
MLN_GET="bash app/tools/maplibre-android/build.sh <work dir> (downloads the stored build when maplibre-android.lock.json records it as published; until then it stops: scripts/heavy.sh mln-aar 30 -- bash app/tools/maplibre-android/build.sh --compile <work dir>)"

# build_links_mln <maven dir>: MLN_AAR = the one AAR there, and only when it is the PINNED upstream build
# (app/tools/maplibre-android/pin.env, T978): an old lean-patch AAR or an experiment is refused, never shipped quietly
build_links_mln() {
  local dir=$1 found MLN_COMMIT MLN_VERSION src_commit=
  MLN_AAR=
  # shellcheck source=../../app/tools/maplibre-android/pin.env
  source "$BUILD_LINKS_DIR/../../app/tools/maplibre-android/pin.env"
  if [[ ! -d $dir ]]; then
    bad "no pinned MapLibre repo at $dir (T978, upstream ${MLN_COMMIT:0:9}): $(build_fix "${dir#"$BUILD_HERE"/}" \
      "$MLN_GET")"
    return 0
  fi
  found=$(find -L "$dir" -name 'android-sdk-vulkan-*.aar' 2>/dev/null)
  # (T978 review) the version in a file name can be copied by hand: build.sh's SOURCE.txt beside the repo must name the
  # pinned commit too (its first line: "maplibre-native <commit> (upstream, no patch), …")
  if [[ -f $dir/SOURCE.txt ]]; then read -r _ src_commit _ <"$dir/SOURCE.txt"; fi
  if [[ -z $found || $(grep -c . <<<"$found") -ne 1 ]]; then
    bad "$dir must hold exactly one android-sdk-vulkan AAR (found: ${found:-none}; app/tools/maplibre-android/README.md)"
  elif [[ $(basename "$found") != "android-sdk-vulkan-$MLN_VERSION.aar" ]]; then
    bad "$found is not the pinned MapLibre ($MLN_VERSION, app/tools/maplibre-android/pin.env): point MLN_LOCAL_MAVEN at cache/maplibre/$MLN_VERSION (unset it for the default), or get it: $MLN_GET"
  elif [[ $src_commit != "$MLN_COMMIT" ]]; then
    bad "$dir/SOURCE.txt names ${src_commit:-no commit}, not the pinned $MLN_COMMIT: not build.sh's build of the pin; get it again: $MLN_GET"
  else MLN_AAR=$(mln_pinned_aar "$dir"); ok "pinned MapLibre AAR $MLN_AAR (upstream ${MLN_COMMIT:0:9}, no patch)"; fi
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

# ── (T993 plan 5, T661 item 3) THE GATES BOTH STORE BUILDS RUN BEFORE THE HEAVY LOCK, here once so build-apk.sh and
# build-ios-testflight.sh ask the same things the same way. heavy.sh runs no fast-fail of its own (T661: the gate parked
# in T535 was dropped), so the build runs it. caught (T993 inventory, 2026-11-21): the iPhone build ran no fast-fail, no
# CI check and no changelog check, while five places said heavy.sh ran fast-fail for it.
#   build_reasons <who> <args…>   the reasoned escapes: --no-fast-fail=<why>, --no-ci-check=<why>, --no-notes=<why>,
#                                 --no-rc-check=<why> (bare, or an empty reason: refused, exit 2); sets NOFF, NOCI,
#                                 NONOTES, NORC
#   build_check_changelog         (in the precheck) app.json's version has its changelog entry and it is the newest
#                                 (scripts/check-changelog.mjs): a `bad` unless --no-notes=<why>, which is printed
#   build_check_rc                (in the precheck, T1062) on an app release branch (release/x.y.z), HEAD is the newest
#                                 release candidate's tree apart from the store build numbers (`python -m pipeline
#                                 release --rc-check`): a `bad` naming the command that tags a new rc, unless
#                                 --no-rc-check=<why>, which is printed; on any other branch it says none is asked
#   build_ff_base <who> <sha> <what>  FAST_FAIL_BASE = <sha> (the commit the last build of this kind was made from) when
#                                 it is a commit here, said, FFB set; else FFB empty and fast-fail's own default is said
#   build_gates <who> [--strict]  after the precheck, before the lock: CI on HEAD (scripts/ci_status.py; --strict for a
#                                 production build), then scripts/fast-fail.sh. Each stops the build (exit 1) or is
#                                 skipped with its printed reason. Under the caller's DRY, a red CI is said and the run
#                                 goes on, and fast-fail is only named.
NOFF=; NOCI=; NONOTES=; NORC=; FFB=
build_reasons() {
  local who=$1 a; shift
  for a in "$@"; do
    case $a in
      --no-fast-fail|--no-ci-check|--no-notes|--no-rc-check) echo "$who: $a needs a reason: $a=\"<why>\"" >&2; exit 2 ;;
      --no-fast-fail=*) NOFF=${a#--no-fast-fail=}; [[ -n ${NOFF// /} ]] || { echo "$who: --no-fast-fail= needs a reason" >&2; exit 2; } ;;
      --no-ci-check=*) NOCI=${a#--no-ci-check=}; [[ -n ${NOCI// /} ]] || { echo "$who: --no-ci-check= needs a reason" >&2; exit 2; } ;;
      --no-notes=*) NONOTES=${a#--no-notes=}; [[ -n ${NONOTES// /} ]] || { echo "$who: --no-notes= needs a reason" >&2; exit 2; } ;;
      --no-rc-check=*) NORC=${a#--no-rc-check=}; [[ -n ${NORC// /} ]] || { echo "$who: --no-rc-check= needs a reason" >&2; exit 2; } ;;
    esac
  done
}
build_check_changelog() {
  local out
  if out=$(node scripts/check-changelog.mjs 2>&1); then ok "$out"
  elif [[ -n $NONOTES ]]; then ok "changelog: SKIPPED (--no-notes): $NONOTES — $out"
  else bad "$out (--no-notes=<reason> goes on)"; fi
}
build_check_rc() {
  local out branch
  branch=$(git rev-parse --abbrev-ref HEAD)
  if [[ ! $branch =~ ^release/[0-9]+\.[0-9]+\.[0-9]+$ ]]; then ok "release candidate: not asked on $branch (only on an app release branch, T1062)"
  elif out=$(.venv/bin/python -m pipeline release --rc-check 2>&1); then ok "release candidate: $out"
  elif [[ -n $NORC ]]; then ok "release candidate: SKIPPED (--no-rc-check): $NORC — $out"
  else bad "release candidate: $out (--no-rc-check=<reason> goes on)"; fi
}
#   build_same_head <who> <sha>   (T1062 review) the inner run (past the heavy lock) builds the commit the precheck
#                                 checked (BUILD_PRECHECK_SHA, exported before the lock), or stops: HEAD can move while
#                                 the lock is waited for
build_same_head() {
  local who=$1 sha=$2
  [[ -n ${BUILD_PRECHECK_SHA:-} ]] || { echo "$who: no BUILD_PRECHECK_SHA: the inner run was started by hand; run the script itself (its precheck exports it)" >&2; exit 1; }
  [[ $sha == "$BUILD_PRECHECK_SHA" ]] || { echo "$who: HEAD is ${sha:0:12}, not ${BUILD_PRECHECK_SHA:0:12} the precheck checked (it moved while the lock was waited for); nothing was built: run it again" >&2; exit 1; }
}
build_ff_base() {
  local who=$1 sha=$2 what=$3
  if [[ -n $sha ]] && git rev-parse -q --verify "$sha^{commit}" >/dev/null; then
    FFB=$sha; export FAST_FAIL_BASE=$sha; echo "$who: fast-fail base ${sha:0:12}, $what"
  else
    FFB=; echo "$who: fast-fail base: none found for $what — its default (the commits not on the trunk), NOT since the last build"
  fi
}
build_gates() {
  local who=$1 strict=${2:-}
  if [[ -n $NOCI ]]; then echo "$who: CI check SKIPPED (--no-ci-check): $NOCI"; export CI_CHECK_SKIP="$who --no-ci-check: $NOCI"
  elif ! .venv/bin/python scripts/ci_status.py ${strict:+"$strict"}; then
    if [[ -n ${DRY:-} ]]; then echo "[dry-run] WOULD STOP: CI (above)"
    else echo "$who: CI is red or unreadable (above) — nothing was built; --no-ci-check=<reason> goes on" >&2; exit 1; fi
  fi
  if [[ -n $NOFF ]]; then echo "$who: fast-fail SKIPPED (--no-fast-fail): $NOFF"
  elif [[ -n ${DRY:-} ]]; then echo "[dry-run] scripts/fast-fail.sh"
  else scripts/fast-fail.sh || { echo "$who: scripts/fast-fail.sh failed (above) — nothing was built" >&2; exit 1; }
  fi
}

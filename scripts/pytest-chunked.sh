#!/usr/bin/env bash
#
# The pipeline pytest suite in CHUNKS, one process each (T638). One process for the whole suite grows to ~111 GB (mostly
# compressed: memory_pressure still read 83-86 % free) and macOS kills it; the curation-review files alone grew one chunk
# to 60 GB, so each of them runs in a process of its own. Heavy: run it through scripts/heavy.sh (scripts/test-full.sh does).
#
#   scripts/pytest-chunked.sh [--files N] [--only REGEX] [--dry-run] [-- <pytest args…>]
#
#   --files N     test files per chunk (default 40); a `test_curation_review*` file is always a chunk of its own
#   --only REGEX  only the test files whose path matches (Tier 0 / trying the runner on a few files)
#   --dry-run     print the chunks, run nothing
#   pytest args   passed to every chunk, e.g. `-- -m slow` (pytest.ini's default `-m "not slow"` holds otherwise)
#
# Every chunk runs unfiltered (no rtk) with a FRESH PYTHONPYCACHEPREFIX for the whole run (T638 item 3: matcher.py edited
# twice in one second at the same size kept its old .pyc, so a gating run could test old bytecode). Exit 5 from a chunk
# (every test in it deselected) is not a failure. Exits non-zero when any chunk failed, naming each.
set -uo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
per=40; only=""; dry=0; args=()
while (( $# )); do
  case $1 in
    --files) per=$2; shift 2 ;;
    --only) only=$2; shift 2 ;;
    --dry-run) dry=1; shift ;;
    --) shift; args=("$@"); break ;;
    *) echo "pytest-chunked: unknown argument $1 (pytest args go after --)" >&2; exit 2 ;;
  esac
done
[[ $per =~ ^[1-9][0-9]*$ ]] || { echo "pytest-chunked: --files must be a positive number" >&2; exit 2; }

# the files pytest.ini's testpaths name, in a fixed order
paths=$("$PY" -c 'import configparser; c = configparser.ConfigParser(); c.read("pytest.ini"); print(c["pytest"]["testpaths"])') \
  || { echo "pytest-chunked: cannot read testpaths from pytest.ini" >&2; exit 1; }
# shellcheck disable=SC2086
files=$(find $paths -name 'test_*.py' -type f | LC_ALL=C sort)
[[ -n $only ]] && files=$(grep -E -- "$only" <<<"$files" || true)
[[ -n $files ]] || { echo "pytest-chunked: no test files${only:+ match '$only'}" >&2; exit 1; }

chunks=(); cur=()
flush() { (( ${#cur[@]} )) && chunks+=("${cur[*]}"); cur=(); }
while IFS= read -r f; do
  if [[ ${f##*/} == test_curation_review* ]]; then chunks+=("$f"); continue; fi
  cur+=("$f"); (( ${#cur[@]} >= per )) && flush
done <<<"$files"
flush
echo "pytest-chunked: $(grep -c . <<<"$files") files in ${#chunks[@]} chunks; pytest ${args[*]+"${args[*]}"}"
if (( dry )); then
  for i in "${!chunks[@]}"; do echo "  chunk $((i + 1)): ${chunks[$i]}"; done
  exit 0
fi

PYTHONPYCACHEPREFIX=$(mktemp -d "${TMPDIR:-/tmp}/pycache.XXXXXX"); export PYTHONPYCACHEPREFIX
trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT
t0=$SECONDS; failed=()
for i in "${!chunks[@]}"; do
  n=$((i + 1)); s=$SECONDS
  read -r -a cf <<<"${chunks[$i]}"
  echo "── chunk $n/${#chunks[@]} (${#cf[@]} file(s)${cf[1]:+, ${cf[0]##*/} … ${cf[${#cf[@]}-1]##*/}}${cf[1]:-: ${cf[0]##*/}})"
  "$PY" -m pytest -q -p no:cacheprovider ${args[@]+"${args[@]}"} "${cf[@]}"
  rc=$?
  if (( rc == 0 || rc == 5 )); then echo "   chunk $n ok ($((SECONDS - s)) s)"
  else echo "   chunk $n FAILED rc=$rc ($((SECONDS - s)) s)"; failed+=("$n"); fi
done
echo "pytest-chunked: ${#chunks[@]} chunks in $((SECONDS - t0)) s"
if (( ${#failed[@]} )); then echo "pytest-chunked: FAILED chunk(s): ${failed[*]}"; exit 1; fi
echo "pytest-chunked: all passed"

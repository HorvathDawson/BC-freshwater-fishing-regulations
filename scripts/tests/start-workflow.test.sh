#!/usr/bin/env bash
# Shell test for scripts/start-workflow.sh (T666 item 6): a stand-in `gh` on PATH, no network, no real workflow.
# Run by scripts/fast-fail.sh when start-workflow.sh or this file changes (~2 s).
#
#   bash scripts/tests/start-workflow.test.sh
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SW=$ROOT/scripts/start-workflow.sh
T=$(mktemp -d "${TMPDIR:-/tmp}/start-workflow-test.XXXXXX")
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin"
fails=0

# the stand-in gh: its script is $T/plan, one line per `gh workflow run` call:
#   refuse | run <id> <conclusion> | accept-no-run
# `gh run list` answers with the run the LAST dispatch made; `gh run view` with that run's conclusion.
cat > "$T/bin/gh" <<EOF
#!/bin/bash
n=\$(cat "$T/calls" 2>/dev/null || echo 0)
case "\$1 \$2" in
  "workflow run")
    n=\$((n + 1)); echo \$n > "$T/calls"; echo "\$*" >> "$T/dispatches"
    line=\$(sed -n "\${n}p" "$T/plan")
    echo "\$line" > "$T/last"
    [[ \$line == refuse ]] && { echo "could not create workflow dispatch event: HTTP 500" >&2; exit 1; }
    exit 0 ;;
  "run list")
    read -r kind id _ < "$T/last"
    [[ \$kind == run ]] && echo "\$id"
    exit 0 ;;
  "run view")
    read -r _ _ concl < "$T/last"
    echo "\$concl"; exit 0 ;;
esac
echo "stand-in gh: unexpected \$*" >&2; exit 9
EOF
chmod +x "$T/bin/gh"

run() {   # run <plan lines…> -> sets OUT (stdout), ERR (stderr), RC, CALLS
  rm -f "$T/calls" "$T/dispatches" "$T/last"; printf '%s\n' "$@" > "$T/plan"
  OUT=$(PATH="$T/bin:$PATH" WF_START_TRIES=3 WF_START_WAIT=0 WF_POLLS=2 WF_POLL_WAIT=0 WF_SETTLE=0 \
        bash "$SW" me/repo my-branch feeds-runtiming.yml "feeds — run timing" -f upload=true -f tiles=t-1 2>"$T/err")
  RC=$?; ERR=$(cat "$T/err"); CALLS=$(cat "$T/calls" 2>/dev/null || echo 0)
}
check() {   # check <what> <condition…>
  local what=$1; shift
  if "$@"; then echo "ok   $what"; else echo "FAIL $what"; echo "     rc=$RC calls=$CALLS out=[$OUT]"; sed 's/^/     /' <<<"$ERR"; fails=$((fails + 1)); fi
}
has() { grep -qF -- "$1" <<<"$ERR"; }

run "run 101 "
check "a start that works: one dispatch, the run id on stdout, nothing said" \
  test "$RC $CALLS $OUT" = "0 1 101" -a -z "$ERR"
check "the caller's -f arguments reach gh workflow run" grep -qF -- "-f upload=true -f tiles=t-1" "$T/dispatches"

run "refuse" "run 102 "
check "a refused dispatch is retried, and the retry is said out loud" test "$RC $CALLS $OUT" = "0 2 102"
check "  … with the try number and the bound" has "did not start (try 1 of 3): GitHub refused the dispatch"
check "  … and the try that worked" has "started on try 2 of 3 (run 102)"

run "run 103 startup_failure" "run 104 "
check "a run that ends with a startup failure is a failed start: retried" test "$RC $CALLS $OUT" = "0 2 104"
check "  … naming the run that failed" has "run 103 ended with a startup failure"

run "refuse" "run 105 startup_failure" "refuse"
check "the bound holds: three tries, then exit 75 and no run id" test "$RC $CALLS" = "75 3" -a -z "$OUT"
check "  … saying nothing ran" has "was NOT started after 3 tries"

run "accept-no-run" "run 106 "
check "an accepted dispatch with no run in sight is NOT retried (no double run)" test "$RC $CALLS" = "75 1" -a -z "$OUT"
check "  … and says to look first" has "NOT retried"

run "run 107 failure"
check "a run that merely failed later is not a start failure (the caller watches it)" test "$RC $CALLS $OUT" = "0 1 107"

OUT=$(bash "$SW" only two 2>"$T/err"); RC=$?; ERR=$(cat "$T/err"); CALLS=0
check "bad usage is exit 2" test "$RC" = 2

echo "start-workflow.test: $fails failure(s)"
exit $(( fails > 0 ))

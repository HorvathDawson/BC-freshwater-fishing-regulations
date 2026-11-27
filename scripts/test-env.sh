# THE TEST ENVIRONMENT, shell half (T667 slice 7, 2026-11-14). SOURCED (never run) by every script that runs the
# pipeline suite: test-smoke.sh, test-full.sh, pytest-chunked.sh, fast-fail.sh, fresh-copy-tests.sh. The pytest half
# (which tier a run is, and that a no-data run hides this machine's builds) is pipeline/tests/tiers.py, applied by the
# repo-root conftest.py whoever starts pytest, so no runner passes a plugin flag.
#
#   . scripts/test-env.sh         from the repo root; defines the three below and changes nothing else
#
#   PY                  the interpreter, named ONE way: $PIPELINE_PY when set (CI sets it in pipeline-ci.yml's job env to
#                       setup-python's `python`: a hosted runner has no .venv), else the checkout's .venv (the local
#                       default; scripts/bootstrap.sh makes it). Neither runnable -> the source stops, naming this file
#                       and what to set (caught 2026-11-14: CI red on b07996827, `.venv/bin/python: No such file`).
#   test_env_pycache    one fresh PYTHONPYCACHEPREFIX per run, shared by its child processes and removed at exit (T638
#                       item 3: matcher.py edited twice in one second at the same size kept its old .pyc, so a gating run
#                       tested old bytecode). Reuses the caller's when one is exported already (test-full -> chunked).
#                       Sets an EXIT trap only when it made the directory.
#   no_data_markers     prints the no-data tier's marker expression from its ONE home, CI's "Tests" step in
#                       .github/workflows/pipeline-ci.yml (T535), so the laptop, fast-fail and the fresh copy run CI's tier.
#                       The reader is pipeline/tests/tiers.py `ci_markers` (T1193: a plain `pytest` runs the same one).
PY=${PIPELINE_PY:-.venv/bin/python}
command -v "$PY" >/dev/null 2>&1 || {
  echo "scripts/test-env.sh: no interpreter '$PY' ($([[ -n ${PIPELINE_PY:-} ]] && echo "from PIPELINE_PY" || echo "the local default; PIPELINE_PY is unset")):" \
       "set PIPELINE_PY to the Python that runs the pipeline suite, or run scripts/bootstrap.sh to make .venv" >&2
  exit 1
}

test_env_pycache() {
  [[ -n ${PYTHONPYCACHEPREFIX:-} && -d $PYTHONPYCACHEPREFIX ]] && return 0
  PYTHONPYCACHEPREFIX=$(mktemp -d "${TMPDIR:-/tmp}/pycache.XXXXXX"); export PYTHONPYCACHEPREFIX
  trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT
}

no_data_markers() {
  "$PY" -c 'from pipeline.tests.tiers import ci_markers; print(ci_markers())' \
    || { echo "test-env: cannot read the marker expression from pipeline-ci.yml's Tests step" >&2; return 1; }
}

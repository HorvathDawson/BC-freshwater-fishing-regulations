# THE TEST ENVIRONMENT, shell half (T667 slice 7, 2026-11-14). SOURCED (never run) by every script that runs the
# pipeline suite: test-smoke.sh, test-full.sh, pytest-chunked.sh, fast-fail.sh, fresh-copy-tests.sh. The pytest half
# (which tier a run is, and that a no-data run hides this machine's builds) is pipeline/tests/tiers.py, applied by the
# repo-root conftest.py whoever starts pytest, so no runner passes a plugin flag.
#
#   . scripts/test-env.sh         from the repo root; defines the three below and changes nothing else
#
#   PY                  the interpreter: the checkout's .venv (scripts/bootstrap.sh makes it)
#   test_env_pycache    one fresh PYTHONPYCACHEPREFIX per run, shared by its child processes and removed at exit (T638
#                       item 3: matcher.py edited twice in one second at the same size kept its old .pyc, so a gating run
#                       tested old bytecode). Reuses the caller's when one is exported already (test-full -> chunked).
#                       Sets an EXIT trap only when it made the directory.
#   no_data_markers     prints the no-data tier's marker expression from its ONE home, CI's "Tests" step in
#                       .github/workflows/pipeline-ci.yml (T535), so the laptop, fast-fail and the fresh copy run CI's tier.
PY=.venv/bin/python

test_env_pycache() {
  [[ -n ${PYTHONPYCACHEPREFIX:-} && -d $PYTHONPYCACHEPREFIX ]] && return 0
  PYTHONPYCACHEPREFIX=$(mktemp -d "${TMPDIR:-/tmp}/pycache.XXXXXX"); export PYTHONPYCACHEPREFIX
  trap 'rm -rf "$PYTHONPYCACHEPREFIX"' EXIT
}

no_data_markers() {
  "$PY" -c 'import re, yaml
s = [s for j in yaml.safe_load(open(".github/workflows/pipeline-ci.yml"))["jobs"].values() for s in j["steps"]
     if s.get("name") == "Tests"]
assert len(s) == 1, "Tests step not found"
m = re.search(r"-m \"([^\"]+)\"", s[0]["run"])
assert m, "no -m in the Tests step"
print(m.group(1))' || { echo "test-env: cannot read the marker expression from pipeline-ci.yml's Tests step" >&2; return 1; }
}

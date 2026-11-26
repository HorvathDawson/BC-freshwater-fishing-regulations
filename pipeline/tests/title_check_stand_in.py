"""(T1144) A `node` on PATH that stands in for ONE command, `node app/tools/check-commits.mjs --title <t> [--number N]`,
and hands everything else to the real node.

Why: that command needs `@commitlint/lint` from app/node_modules, and pipeline CI installs no app packages (its first
run of these tests failed on exactly that, PR #276). What the Python side must prove is that `before_pr.sh` and
`before_push.py` hand the title AND its number to the check and act on its exit code; the check itself is proved on the
real CLI in the app tier (`app/tools/check-commits.test.ts`, "a PR title at the limit of its merge commit"), which pins
the same contract this stand-in keeps: exit 0 when "<title> (#N)" fits `headerMax`, 1 when it is longer, the bare title
when no number is given."""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

_made: Path | None = None


def bin_dir() -> str:
    """The folder to put FIRST on PATH (made once per process)."""
    global _made
    if _made is None:
        real = shutil.which("node")
        assert real, "node is not on PATH: these tests hand `node -e` and the hooks' other node calls to the real one"
        header_max = json.loads((Path(REPO_ROOT) / "app" / "tools" / "versioning.json").read_text())["headerMax"]
        d = Path(tempfile.mkdtemp(prefix="title-check-"))
        node = d / "node"
        node.write_text(f"""#!/bin/sh
case " $* " in
  *" app/tools/check-commits.mjs --title "*) ;;
  *) exec "{real}" "$@" ;;
esac
title= number=
while [ $# -gt 0 ]; do
  case $1 in --title) title=$2; shift 2 ;; --number) number=$2; shift 2 ;; *) shift ;; esac
done
subject=$title
[ -n "$number" ] && subject="$title (#$number)"
if [ ${{#subject}} -gt {header_max} ]; then
  echo "the PR title: $title" >&2
  echo "  header must not be longer than {header_max} characters, current length is ${{#subject}} [header-max-length]" >&2
  exit 1
fi
exit 0
""")
        node.chmod(0o755)
        _made = d
    return str(_made)

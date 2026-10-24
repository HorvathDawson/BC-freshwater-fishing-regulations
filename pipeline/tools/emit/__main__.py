"""python -m pipeline.tools.emit [--check]: every generator in one run (see `__init__`). Exit 1 if any is stale."""
from __future__ import annotations

import sys

from pipeline.deliver.answers import model as answers_model
from pipeline.tools.emit import calendar, gauge_policy, pack

GENERATORS = (("gauge policy", gauge_policy), ("calendar", calendar), ("pack", pack),
              ("answers model", answers_model))


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv not in ([], ["--check"]):
        print("usage: python -m pipeline.tools.emit [--check]", file=sys.stderr)
        return 2
    failed = [name for name, mod in GENERATORS if mod.main(list(argv))]
    if failed:
        print(f"pipeline.tools.emit: stale — {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

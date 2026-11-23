"""T779: A DFO PLACE'S BINDING CHECK ON THE ATLAS THAT HAS ITS CUT — run in its own process, because the review app's
reads (`reuse`) bind to ONE atlas and ONE curated root when they are imported (`CURATION_BUILD`, `CURATED_ROOT`).

`apply_proposals` runs it for a DFO place on a cut-changing chain (a new cut, or one landed in this run that the live
atlas has not got): `CURATION_BUILD` = the side atlas (or the one `--build` / `CURATION_BUILD` names) and, when the side
copies were made, `CURATED_ROOT` = the patched root beside it. It prints the same check `proposals.check` makes
(`reuse.dfo_binding_change`: sections before / after, `withheld`, `unaccepted`, `stale_acceptances`) as one JSON line.

    python -m pipeline.regs.review.side_check <location_id> '<changes json>'
"""
from __future__ import annotations

import json
import sys


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    from pipeline.regs.review import proposals as P
    got = P._backend().dfo_binding_change(argv[0], json.loads(argv[1]))
    print(json.dumps(got if got is not None else {"cheap": False, "why": f"no DFO place {argv[0]} on this atlas's root"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

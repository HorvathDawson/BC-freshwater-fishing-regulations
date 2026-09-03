"""Delete cross-region mis-matched (stale) EntryFile entries so they can be re-parsed.

Background: earlier parse runs bound a synopsis row to the ONLY same-name registry item available at
the time, even when it lived in a different region (e.g. Region 1 "White River" bound to the Region 4
gnis:26897 instead of the Region 1 gnis:2923). The registry now holds every same-name item and the
matcher disambiguates by MU overlap, so re-parsing these rows produces the correct binding.

This removes the listed stale entries from the EntryFiles (and the reviewed/ overlay). After running,
re-export with the batch exporter's --skip-existing and have a human run the parser on the freshly
missing rows.

Driven by a JSON list of {file, entry_id} objects (default: scratch_stale_matches.json):

    .venv/bin/python -m pipeline.hack.delete_stale_matches [--list scratch_stale_matches.json] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from pipeline.curated import CURATED, SOURCE

_ROOT = Path(__file__).resolve().parents[2]
_ENTRIES_DIR = CURATED.regulations.entries.synopsis


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--list", default=str(_ROOT / "scratch_stale_matches.json"),
                    help="JSON array of {file, entry_id} to delete")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing")
    args = ap.parse_args()

    targets = json.loads(Path(args.list).read_text(encoding="utf-8"))
    by_file: dict[str, set[str]] = defaultdict(set)
    for t in targets:
        by_file[t["file"]].add(t["entry_id"])

    removed_total = 0
    for fname, ids in sorted(by_file.items()):
        for path in (_ENTRIES_DIR / fname, _ENTRIES_DIR / "reviewed" / fname):
            if not path.exists():
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
            entries = data.get("entries", [])
            keep = [e for e in entries if e.get("entry_id") not in ids]
            removed = len(entries) - len(keep)
            if removed:
                print(f"{path.relative_to(_ROOT)}: removing {removed} entr{'y' if removed == 1 else 'ies'}")
                removed_total += removed
                if not args.dry_run:
                    data["entries"] = keep
                    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"\n{'[dry-run] would remove' if args.dry_run else 'removed'} {removed_total} entries total")


if __name__ == "__main__":
    main()

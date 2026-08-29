"""Compare two build outputs — is the registry the same after a change?

    .venv/bin/python -m pipeline.tools.build_parity output/v2/full output/v2/full_new

Downstream consumes `registry.json`, so that is what parity means: same items, same names, same
section membership, same boundaries. A refactor that is meant to change nothing must produce an
identical fingerprint; a change that is meant to alter the graph should alter exactly the items you
expect and no others.

Prints a per-item diff summary, not just a pass/fail, because "3 items changed" is actionable and
"differs" is not.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def _items(path: Path) -> dict:
    raw = json.loads((path / "registry.json").read_text())
    items = raw["items"] if isinstance(raw, dict) and "items" in raw else raw
    return items if isinstance(items, dict) else {v["id"]: v for v in items}


def fingerprint(item: dict) -> str:
    """Everything downstream depends on, order-normalised so only real change shows."""
    payload = {
        "name": item.get("name"),
        "kind": item.get("kind"),
        "mus": sorted(item.get("mus") or []),
        "sections": sorted(item.get("section_ids") or []),
        "boundaries": sorted((b.get("id"), b.get("ref"), b.get("kind"))
                             for b in (item.get("boundaries") or [])),
        "variants": sorted(str(v) for v in (item.get("variants") or [])),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("before")
    ap.add_argument("after")
    ap.add_argument("--show", type=int, default=15)
    args = ap.parse_args()

    a, b = _items(Path(args.before)), _items(Path(args.after))
    fa = {k: fingerprint(v) for k, v in a.items()}
    fb = {k: fingerprint(v) for k, v in b.items()}
    gone, new = sorted(set(fa) - set(fb)), sorted(set(fb) - set(fa))
    changed = sorted(k for k in set(fa) & set(fb) if fa[k] != fb[k])

    print(f"items  before {len(a):,}   after {len(b):,}")
    print(f"  removed {len(gone):,}   added {len(new):,}   changed {len(changed):,}")
    sa = sum(len(v.get('section_ids') or []) for v in a.values())
    sb = sum(len(v.get('section_ids') or []) for v in b.values())
    print(f"sections  before {sa:,}   after {sb:,}   delta {sb - sa:+,}")

    for label, ids in (("removed", gone), ("added", new), ("changed", changed)):
        if not ids:
            continue
        print(f"\n{label}:")
        for k in ids[:args.show]:
            src = a.get(k) or b.get(k)
            if label == "changed":
                na, nb = len(a[k].get("section_ids") or []), len(b[k].get("section_ids") or [])
                print(f"   {k:<24} {src.get('name','')[:34]:<34} sections {na} -> {nb}")
            else:
                print(f"   {k:<24} {src.get('name','')[:34]:<34} "
                      f"{len(src.get('section_ids') or [])} sections")
        if len(ids) > args.show:
            print(f"   … {len(ids) - args.show:,} more")

    print("\nIDENTICAL" if not (gone or new or changed) else "\nDIFFERS")


if __name__ == "__main__":
    main()

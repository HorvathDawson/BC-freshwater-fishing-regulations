"""Compare two build outputs — is the registry the same after a change?

    .venv/bin/python -m pipeline.tools.build_parity data/generated/atlas/full data/generated/atlas/full_new

Downstream consumes `registry.json`, so that is what parity means: same items, same names, same
section membership, same boundaries. BOUNDARIES ARE REPORTED SEPARATELY and a net loss is shouted
about: they are what a "downstream of the bridge" regulation binds to, and a build run without
`--splits` drops every curated one while still looking healthy on every other measure. A refactor that is meant to change nothing must produce an
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

    # BOUNDARIES GET THEIR OWN LINE, and a net loss is shouted about. The fingerprint above has
    # always covered boundaries, so a build that dropped every curated split DID show up here — as
    # "changed 388", indistinguishable from area churn. The loss was 373 cut-points (Fraser 29->13,
    # Kokish 6->0, Stamp 7->2), it was read as noise, and the build was promoted. A cut-point is what
    # a "downstream of the bridge" regulation binds to, so losing one silently is the most expensive
    # thing this tool can fail to say out loud.
    ba = sum(len(v.get('boundaries') or []) for v in a.values())
    bb = sum(len(v.get('boundaries') or []) for v in b.values())
    print(f"boundaries  before {ba:,}   after {bb:,}   delta {bb - ba:+,}")
    lost_b = sorted(((len(b[k].get('boundaries') or []) - len(a[k].get('boundaries') or []), k)
                     for k in set(a) & set(b)
                     if len(b[k].get('boundaries') or []) < len(a[k].get('boundaries') or [])))
    if lost_b:
        n_lost = -sum(d for d, _ in lost_b)
        print(f"\n  !! {n_lost:,} CUT-POINT(S) LOST across {len(lost_b):,} item(s) — a regulation that "
              f"binds to one of these\n     can no longer resolve. Check that the build was run WITH "
              f"curated splits (--splits).")
        for d, k in lost_b[:args.show]:
            nm = (a[k].get("name") or "")[:34]
            print(f"     {k:<22} {nm:<34} boundaries {len(a[k].get('boundaries') or [])} "
                  f"-> {len(b[k].get('boundaries') or [])}")
        if len(lost_b) > args.show:
            print(f"     … {len(lost_b) - args.show:,} more")

    for label, ids in (("removed", gone), ("added", new), ("changed", changed)):
        if not ids:
            continue
        print(f"\n{label}:")
        for k in ids[:args.show]:
            src = a.get(k) or b.get(k)
            if label == "changed":
                na, nb = len(a[k].get("section_ids") or []), len(b[k].get("section_ids") or [])
                ca, cb = len(a[k].get("boundaries") or []), len(b[k].get("boundaries") or [])
                cuts = f"  cuts {ca} -> {cb}" if ca != cb else ""
                print(f"   {k:<24} {src.get('name','')[:34]:<34} sections {na} -> {nb}{cuts}")
            else:
                print(f"   {k:<24} {src.get('name','')[:34]:<34} "
                      f"{len(src.get('section_ids') or [])} sections")
        if len(ids) > args.show:
            print(f"   … {len(ids) - args.show:,} more")

    print("\nIDENTICAL" if not (gone or new or changed) else "\nDIFFERS")


if __name__ == "__main__":
    main()

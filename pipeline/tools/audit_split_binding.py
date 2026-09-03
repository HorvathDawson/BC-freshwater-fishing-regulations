"""Which curated splits can a rule actually BIND to — and which can never be referenced?

A split is only useful if some registry item carries it as a boundary, because that is the menu
an extent is authored against (`validate_entry_splits`). A split can resolve to a perfectly good
cut and still be unreachable, and nothing in the build says so: the resolver reports success, the
sectionizer quietly drops it, and the boundary simply never appears.

That is how `atnarko_..._campsite_boundary_signs` hid. Its coordinate was a hand-verified user pin,
but `applies_to` scoped it to blk 360836756 — an unnamed order-1 creek 249 m long that merely
happens to be the nearest blue line to the pin (50 m, vs 280 m to the Atnarko). The cut landed on
the ditch, no item carried it, and both rules naming those boundary signs stayed unbound.

    python -m pipeline.tools.audit_split_binding [--build data/generated/atlas/full] [--json]

Verdicts, worst first:

  unnamed_water     the cut landed on a blue line NO named item owns. Almost always a mis-scoped
                    `applies_to` — the anchor found the nearest line rather than the intended one.
  lost              resolved, but never applied and not carried by anything. Unreferenceable.
  mouth_noop        the cut sits at the water's own mouth, so there is nothing to split. Harmless,
                    but it cannot be named in an extent either.
  alias             applied as an ALIAS on an existing boundary (a split landing inside a lake run
                    becomes an alias on that lake). Bindable — this is working as designed.
  ok                carried by at least one registry item.
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

from pipeline.atlas.registry import load_registry
from pipeline.common.io.serialize import read_artifact
from pipeline.atlas.splits.splits import load_split_defs
from pipeline.common.curated import CURATED, GENERATED, SOURCE


def _bare(x) -> str:
    """Boundary refs and aliases carry a `split:` prefix; split ids do not."""
    x = str(x or "")
    return x.split(":", 1)[1] if x.startswith("split:") else x


def audit(build: Path, splits_path: str | None = None) -> list[dict]:
    registry = load_registry(str(build / "registry.json"))
    graph = read_artifact(str(build / "graph.pkl"))
    resolved = json.loads((build / "splits.resolved.json").read_text(encoding="utf-8"))
    defs = {d.id: d for d in load_split_defs(str(splits_path or CURATED.waters.splits))}

    carried: set[str] = set()
    for it in registry.values():
        for b in it.boundaries:
            carried.add(_bare(b.id))
            carried.add(_bare(getattr(b, "ref", "")))
            for a in (getattr(b, "aliases", ()) or ()):
                carried.add(_bare(a))
    carried.discard("")

    cuts: dict[str, list[dict]] = collections.defaultdict(list)
    for r in resolved:
        cuts[r["split_id"]].append(r)

    # blk -> the named items that own a piece of it (areas excluded: they own no NAME)
    blk_items: dict[str, set[str]] = collections.defaultdict(set)
    for iid, it in registry.items():
        if iid.startswith("area:"):
            continue
        for s in it.section_ids:
            s = str(s)
            if not s.startswith("lake:"):
                blk_items[s.split(":")[0]].add(iid)

    mouth: dict[str, float] = {}
    for n in graph.nodes.values():
        if n.blk:
            b = str(n.blk)
            mouth[b] = min(mouth.get(b, n.down_m), n.down_m)

    out: list[dict] = []
    for sid, d in sorted(defs.items()):
        rows = cuts.get(sid) or []
        if not rows:
            out.append({"split_id": sid, "verdict": "never_resolved", "blk": "", "measure": None})
            continue
        r = rows[0]
        blk, m = str(r["blk"]), float(r["route_measure"])
        owners = sorted(blk_items.get(blk, ()))
        if sid in carried:
            verdict = "ok"
        elif not owners:
            verdict = "unnamed_water"
        elif abs(m - mouth.get(blk, 0.0)) < 1.0:
            verdict = "mouth_noop"
        else:
            verdict = "lost"
        scope = (f"blk={d.blk}" if d.blk else f"wsc={d.wsc}" if d.wsc else f"gnis={d.gnis_id}")
        out.append({"split_id": sid, "verdict": verdict, "blk": blk, "measure": round(m, 1),
                    "scope": scope, "owners": [registry[o].name for o in owners[:2]],
                    "label": d.label or ""})
    return out


ORDER = ["unnamed_water", "lost", "never_resolved", "mouth_noop", "ok"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", default=str(GENERATED.build()))
    ap.add_argument("--splits", default=str(CURATED.waters.splits))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    rows = audit(Path(args.build), args.splits)
    if args.json:
        print(json.dumps(rows, indent=1))
        return 0

    counts = collections.Counter(r["verdict"] for r in rows)
    print(f"{len(rows)} curated splits in {args.splits}, against {args.build}\n")
    for v in ORDER:
        if counts.get(v):
            print(f"  {v:15s} {counts[v]:4d}")
    for v in ORDER:
        bad = [r for r in rows if r["verdict"] == v]
        if v == "ok" or not bad:
            continue
        print(f"\n--- {v} ({len(bad)}) " + "-" * 40)
        for r in bad:
            print(f"  {r['split_id'][:64]:64s} {r.get('scope',''):16s} "
                  f"-> blk {r['blk']} @{r['measure']}  {r.get('owners') or ''}")
    # unnamed_water is the only verdict that is always a defect
    return 1 if counts.get("unnamed_water") else 0


if __name__ == "__main__":
    raise SystemExit(main())
